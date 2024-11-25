from copy import deepcopy
from datetime import datetime, timezone
import inspect
import os
import re
import sys
import time


from svcutils.service import Notifier, get_file_mtime
from webutils.bookmarks import BookmarksHandler
from webutils.google.cloud import GoogleCloud

from savegame import NAME, logger
from savegame.lib import (HOSTNAME, REF_FILENAME, Metadata, Reference,
    Report, check_patterns, copy_file, get_file_hash, get_hash, makedirs,
    remove_path, to_json)


RETRY_DELTA = 2 * 3600
GOOGLE_AUTOAUTH_BROWSER_ID = 'chrome'


def path_to_filename(x):
    x = re.sub(r'[<>:"|?*\s]', '_', x)
    x = re.sub(r'[/\\]', '-', x)
    return x.strip('-')


def get_file_mtime_dt(x):
    if os.path.exists(x):
        return datetime.fromtimestamp(get_file_mtime(x), tz=timezone.utc)


def walk_paths(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for item in files + dirs:
            yield os.path.join(root, item)


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


def get_google_cloud(config, headless=True):
    return GoogleCloud(
        oauth_secrets_file=os.path.expanduser(
            config.GOOGLE_CLOUD_SECRETS_FILE),
        browser_id=config.GOOGLE_AUTOAUTH_BROWSER_ID
            or GOOGLE_AUTOAUTH_BROWSER_ID,
        headless=headless,
    )


class BaseSaver:
    id = None
    hostname = None
    src_type = 'local'
    dst_type = 'local'

    def __init__(self, config, src, inclusions, exclusions, dst_path,
                 run_delta, retention_delta):
        self.config = config
        self.src = src
        self.inclusions = inclusions
        self.exclusions = exclusions
        self.dst_path = dst_path
        self.run_delta = run_delta
        self.retention_delta = retention_delta
        self.dst = self.get_dst()
        self.dst_paths = set()
        self.ref = Reference(self.dst)
        self.meta = Metadata()
        self.report = Report()
        self.start_ts = None
        self.end_ts = None
        self.success = None

    def get_dst(self):
        if self.dst_type == 'local':
            return os.path.join(self.dst_path, self.hostname,
                path_to_filename(self.src))
        return self.dst_path

    def notify_error(self, message, exc=None):
        Notifier().send(title=f'{NAME} error', body=message)

    def _must_run(self):
        return time.time() > self.meta.get(self.src).get('next_ts', 0)

    def _update_meta(self):
        self.meta.set(self.src, {
            'dst': self.dst,
            'start_ts': self.start_ts,
            'end_ts': self.end_ts,
            'next_ts': time.time() + (self.run_delta if self.success
                else RETRY_DELTA),
            'success_ts': self.end_ts if self.success
                else self.meta.get(self.src).get('success_ts', 0),
        })

    def do_run(self):
        raise NotImplementedError()

    def _requires_purge(self, path):
        if os.path.isfile(path):
            if path in self.dst_paths:
                return False
            name = os.path.basename(path)
            if name == REF_FILENAME:
                return False
            if not name.startswith(REF_FILENAME) and \
                    get_file_mtime(path) > time.time() - self.retention_delta:
                return False
        elif os.listdir(path):
            return False
        return True

    def _purge_dst(self):
        if self.dst_type != 'local':
            return
        if not self.dst_paths:
            remove_path(self.dst)
            return
        for path in walk_paths(self.dst):
            if self._requires_purge(path):
                remove_path(path)
                self.report.add('removed', self.src, path)

    def run(self, force=False):
        if not (force or self._must_run()):
            return
        self.start_ts = time.time()
        self.ref.src = self.src
        try:
            self.do_run()
            self._purge_dst()
            if os.path.exists(self.ref.dst):
                self.ref.save()
            self.success = True
        except Exception as exc:
            self.success = False
            logger.exception(f'failed to save {self.src}')
            self.notify_error(f'failed to save {self.src}: {exc}', exc=exc)
        self.end_ts = time.time()
        self._update_meta()


class LocalSaver(BaseSaver):
    id = 'local'
    hostname = HOSTNAME

    def _get_src_and_files(self):

        def is_valid(file):
            return (os.path.basename(file) != REF_FILENAME
                and check_patterns(file, self.inclusions, self.exclusions))

        if self.src_type == 'local':
            src = self.src
            if os.path.isfile(src):
                files = [src]
                src = os.path.dirname(src)
            else:
                files = list(walk_files(src))
            return src, {f for f in files if is_valid(f)}
        return self.src, set()

    def do_run(self):
        src, src_files = self._get_src_and_files()
        self.report.add('files', self.src, src_files)
        self.ref.src = src
        self.ref.files = {}
        for src_file in src_files:
            rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, rel_path)
            self.dst_paths.add(dst_file)
            src_hash = get_file_hash(src_file)
            dst_hash = get_file_hash(dst_file)
            try:
                if src_hash != dst_hash:
                    makedirs(os.path.dirname(dst_file))
                    copy_file(src_file, dst_file)
                    self.report.add('saved', self.src, src_file)
                self.ref.files[rel_path] = src_hash
            except Exception:
                self.report.add('failed', self.src, src_file)
                logger.exception(f'failed to save {src_file}')


class GoogleDriveExportSaver(BaseSaver):
    id = 'google_drive_export'
    hostname = 'google_cloud'
    src_type = 'remote'

    def do_run(self):
        gc = get_google_cloud(self.config)
        for file_meta in gc.iterate_file_meta():
            if not file_meta['exportable']:
                self.report.add('skipped', self.src, file_meta['path'])
                continue
            dst_file = os.path.join(self.dst, file_meta['path'])
            self.dst_paths.add(dst_file)
            dt = get_file_mtime_dt(dst_file)
            if dt and dt > file_meta['modified_time']:
                self.report.add('skipped', self.src, dst_file)
                continue
            makedirs(os.path.dirname(dst_file))
            try:
                gc.export_file(file_id=file_meta['id'],
                    path=dst_file, mime_type=file_meta['mime_type'])
                self.report.add('saved', self.src, dst_file)
            except Exception as exc:
                self.report.add('failed', self.src, dst_file)
                logger.error('failed to save google drive file '
                    f'{file_meta["name"]}: {exc}')
        self.ref.files = {os.path.relpath(p, self.dst): get_file_hash(p)
            for p in self.dst_paths}


class GoogleContactsExportSaver(BaseSaver):
    id = 'google_contacts_export'
    hostname = 'google_cloud'
    src_type = 'remote'

    def do_run(self):
        gc = get_google_cloud(self.config)
        contacts = gc.list_contacts()
        data = to_json(contacts)
        rel_path = 'contacts.json'
        dst_file = os.path.join(self.dst, rel_path)
        self.dst_paths.add(dst_file)
        dst_hash = get_hash(data)
        if os.path.exists(dst_file) and \
                dst_hash == self.ref.files.get(rel_path):
            self.report.add('skipped', self.src, dst_file)
        else:
            makedirs(os.path.dirname(dst_file))
            with open(dst_file, 'w', encoding='utf-8',
                    newline='\n') as fd:
                fd.write(data)
            self.report.add('saved', self.src, dst_file)
            logger.info(f'saved {len(contacts)} google contacts')
        self.ref.files = {rel_path: dst_hash}


class BookmarksExportSaver(BaseSaver):
    id = 'bookmarks_export'
    hostname = HOSTNAME

    def do_run(self):
        ref_files = deepcopy(self.ref.files)
        self.ref.files = {}
        for file_meta in BookmarksHandler().export():
            rel_path = file_meta['path']
            dst_file = os.path.join(self.dst, rel_path)
            self.dst_paths.add(dst_file)
            dst_hash = get_hash(file_meta['content'])
            if os.path.exists(dst_file) and \
                    dst_hash == ref_files.get(rel_path):
                self.report.add('skipped', self.src, dst_file)
            else:
                makedirs(os.path.dirname(dst_file))
                with open(dst_file, 'w', encoding='utf-8',
                        newline='\n') as fd:
                    fd.write(file_meta['content'])
                self.report.add('saved', self.src, dst_file)
            self.ref.files[rel_path] = dst_hash


def get_saver_class(saver_id):
    module = sys.modules[__name__]
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if obj.__module__ == module.__name__ \
                and issubclass(obj, BaseSaver) \
                and obj.id == saver_id:
            return obj
    raise Exception(f'invalid saver_id {saver_id}')
