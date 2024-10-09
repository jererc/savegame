import argparse
import atexit
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from fnmatch import fnmatch
import functools
from glob import glob
import gzip
import hashlib
import inspect
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pathlib import PurePath
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.parse

import psutil

import google_chrome
from google_cloud import GoogleCloud, AuthError, RefreshError


SAVES = []
MAX_LOG_FILE_SIZE = 1000 * 1024
RUN_DELTA = 30 * 60
FORCE_RUN_DELTA = 90 * 60
DAEMON_LOOP_DELAY = 10
RETRY_DELTA = 2 * 3600
RETENTION_DELTA = 7 * 24 * 3600
CHECK_DELTA = 8 * 3600
OLD_DELTA = 7 * 24 * 3600
NAME = os.path.splitext(os.path.basename(os.path.realpath(__file__)))[0]
HOME_PATH = os.path.expanduser('~')
WORK_PATH = os.path.join(HOME_PATH, f'.{NAME}')
HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()
REF_FILENAME = f'.{NAME}'
SHARED_USERNAMES = {
    'nt': {'Public'},
    'posix': {'shared'},
}.get(os.name, set())
DST_PATH = os.path.join('~', 'OneDrive')
GOOGLE_CLOUD_SECRETS_FILE = None
GOOGLE_OAUTH_WIN_SCRIPT = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), 'run_google_oauth.pyw')

try:
    from user_settings import *
except ImportError:
    pass


def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


def setup_logging(logger, path):
    logging.basicConfig(level=logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    if sys.stdout and not sys.stdout.isatty():
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(logging.DEBUG)
        logger.addHandler(stdout_handler)
    makedirs(path)
    file_handler = RotatingFileHandler(os.path.join(path, f'{NAME}.log'),
        mode='a', maxBytes=MAX_LOG_FILE_SIZE, backupCount=0,
        encoding='utf-8', delay=0)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)


logger = logging.getLogger(__name__)
makedirs(WORK_PATH)
setup_logging(logger, WORK_PATH)


class UnhandledPath(Exception):
    pass


class InvalidPath(Exception):
    pass


def validate_path(path):
    if os.sep not in path:
        raise UnhandledPath(f'unhandled path {path}: not {os.name}')


def path_to_filename(path):
    return urllib.parse.quote(path, safe='')


def get_file_mtime(file):
    if os.path.exists(file):
        return datetime.fromtimestamp(os.stat(file).st_mtime, tz=timezone.utc)


def get_path_size(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    res = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for filename in files:
            if filename == REF_FILENAME:
                continue
            file = os.path.join(root, filename)
            try:
                res += os.path.getsize(file)
            except Exception:
                logger.error(f'failed to get size for {file}')
    return res


def remove_path(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


def walk_paths(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for item in files + dirs:
            yield os.path.join(root, item)


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


def get_file_hash(file, chunk_size=8192):
    if not os.path.exists(file):
        return None
    md5_hash = hashlib.md5()
    with open(file, 'rb') as fd:
        while chunk := fd.read(chunk_size):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def to_json(data):
    return json.dumps(data, indent=4, sort_keys=True)


def notify(title, body, on_click=None):
    try:
        if os.name == 'nt':
            from win11toast import notify as _notify
            _notify(title=title, body=body, on_click=on_click)
        else:
            env = os.environ.copy()
            env['DISPLAY'] = ':0'
            env['DBUS_SESSION_BUS_ADDRESS'] = 'unix:path=/run/user/1000/bus'
            res = subprocess.run(['notify-send', title, body],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, env=env)
            if res.returncode != 0:
                raise Exception(res.stdout or res.stderr)
    except Exception as exc:
        logger.error(f'failed to notify: {exc}')


def text_file_exists(file, data, encoding='utf-8',
        log_content_changed=False):
    if os.path.exists(file):
        with open(file, encoding=encoding) as fd:
            res = fd.read() == data
            if not res and log_content_changed:
                logger.warning(f'content has changed in {file}')
            return res
    return False


def check_patterns(path, inclusions=None, exclusions=None):
    if exclusions:
        for pattern in exclusions:
            if fnmatch(path, pattern):
                return False
        return True
    if inclusions:
        for pattern in inclusions:
            if fnmatch(path, pattern):
                return True
        return False
    return True


def get_google_cloud():
    secrets_file = os.path.expanduser(GOOGLE_CLOUD_SECRETS_FILE)
    if not os.path.exists(secrets_file):
        raise Exception('missing google secrets file')
    return GoogleCloud(oauth_secrets_file=secrets_file)


class Metadata:
    file = os.path.join(WORK_PATH, 'meta.json')
    data = {}

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
            cls.instance.load()
        return cls.instance

    def load(self):
        if os.path.exists(self.file):
            with open(self.file) as fd:
                self.data = json.load(fd)
            logger.debug(f'loaded {len(self.data)} meta items')

    def get(self, key):
        return self.data.get(key, {})

    def set(self, key, value: dict):
        self.data[key] = value

    def save(self, keys):
        self.data = {k: v for k, v in self.data.items() if k in keys}
        with open(self.file, 'w') as fd:
            fd.write(to_json(self.data))


class Reference:
    def __init__(self, dst):
        self.dst = dst
        self.file = os.path.join(self.dst, REF_FILENAME)
        self.data = None
        self.src = None
        self.files = None
        self._load()

    def _load(self):
        if not os.path.exists(self.file):
            self.data = {}
        else:
            try:
                with open(self.file, 'rb') as fd:
                    self.data = json.loads(
                        gzip.decompress(fd.read()).decode('utf-8'))
            except Exception as exc:
                logger.exception(f'failed to load {self.file}: {exc}')
                self.data = {}
        self.src = self.data.get('src')
        self.files = deepcopy(self.data.get('files', {}))

    def save(self):
        data = {'src': self.src, 'files': self.files}
        if data == self.data:
            return
        with open(self.file, 'wb') as fd:
            fd.write(gzip.compress(
                json.dumps(data, sort_keys=True).encode('utf-8')))
        logger.debug(f'updated ref file {self.file}')
        self.data = data


class Report:
    def __init__(self):
        self.data = defaultdict(lambda: defaultdict(set))

    def add(self, k1, k2, v):
        if isinstance(v, set):
            self.data[k1][k2].update(v)
        else:
            self.data[k1][k2].add(v)

    def merge(self, report):
        for k, v in report.data.items():
            for k2, v2 in v.items():
                self.data[k][k2].update(v2)

    def clean(self, keys=None):
        res = defaultdict(dict)
        for k, v in self.data.items():
            if keys and k not in keys:
                continue
            for k2, v2 in v.items():
                res[k][k2] = sorted(v2)
        return res

    def get_summary(self):
        res = defaultdict(dict)
        for k, v in self.data.items():
            for k2, v2 in v.items():
                res[k][k2] = len(v2)
        return res


class BaseSaver:
    src_type = None
    hostname = None

    def __init__(self, src, inclusions, exclusions, dst_path, run_delta=0,
            retention_delta=RETENTION_DELTA):
        self.src = src
        self.inclusions = inclusions
        self.exclusions = exclusions
        self.dst_path = dst_path
        self.run_delta = run_delta
        self.retention_delta = retention_delta
        self.dst = self.get_dst()
        self.meta = Metadata()
        self.report = Report()
        self.start_ts = None
        self.end_ts = None
        self.success = None
        self.stats = {}

    def get_dst(self):
        return os.path.join(self.dst_path, self.hostname,
            path_to_filename(self.src))

    def can_be_purged(self, path):
        return os.stat(path).st_mtime < time.time() - self.retention_delta

    def notify_error(self, message, exc=None):
        notify(title=f'{NAME} error', body=message)

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

    def check_data(self):
        raise NotImplementedError()

    def do_run(self):
        raise NotImplementedError()

    def run(self, force=False):
        if not (force or self._must_run()):
            return
        self.start_ts = time.time()
        try:
            self.do_run()
            self.success = True
        except Exception as exc:
            self.success = False
            logger.exception(f'failed to save {self.src}')
            self.notify_error(f'failed to save {self.src}: {exc}', exc=exc)
        self.end_ts = time.time()
        self._update_meta()
        self.stats['size_MB'] = get_path_size(self.dst) / 1024 / 1024
        self.stats['duration'] = self.end_ts - self.start_ts


class LocalSaver(BaseSaver):
    src_type = 'local'
    hostname = HOSTNAME

    def _get_src_and_files(self):
        src = self.src
        if os.path.isfile(src):
            files = [src]
            src = os.path.dirname(src)
        else:
            files = list(walk_files(src))
        files = {f for f in files
            if check_patterns(f, self.inclusions, self.exclusions)}
        return src, files

    def check_data(self):
        src, src_files = self._get_src_and_files()
        if not src_files:
            return
        ref = Reference(self.dst)
        src_hashes = {os.path.relpath(f, src): get_file_hash(f)
            for f in src_files}
        dst_hashes = {p: get_file_hash(os.path.join(self.dst, p))
            for p in ref.files.keys()}
        if src_hashes == dst_hashes:
            self.report.add('ok', src, set(src_files))
        else:
            for path in set(list(src_hashes.keys()) + list(dst_hashes.keys())):
                src_h = src_hashes.get(path)
                dst_h = dst_hashes.get(path)
                if not src_h:
                    self.report.add('missing_at_src', src,
                        os.path.join(src, path))
                elif not dst_h:
                    self.report.add('missing_at_dst', src,
                        os.path.join(self.dst, path))
                elif src_h != dst_h:
                    self.report.add('hash_mismatched', src,
                        os.path.join(src, path))

    def _needs_removal(self, dst_path, src, src_files):
        if os.path.isdir(dst_path) and not os.listdir(dst_path):
            return True
        src_path = os.path.join(src, os.path.relpath(dst_path, self.dst))
        if os.path.isfile(dst_path) and src_path not in src_files \
                and self.can_be_purged(dst_path):
            return True
        return False

    def do_run(self):
        src, src_files = self._get_src_and_files()
        self.report.add('files', self.src, set(src_files))

        dst_files = set()
        for dst_path in walk_paths(self.dst):
            if os.path.basename(dst_path) == REF_FILENAME:
                continue
            if self._needs_removal(dst_path, src, src_files):
                remove_path(dst_path)
                self.report.add('removed', self.src, dst_path)
            elif os.path.isfile(dst_path):
                dst_files.add(dst_path)

        if not src_files:
            if not dst_files:
                remove_path(self.dst)
            return

        ref = Reference(self.dst)
        ref.src = src
        ref.files = {}
        for src_file in src_files:
            rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, rel_path)
            src_hash = get_file_hash(src_file)
            dst_hash = get_file_hash(dst_file)
            try:
                if src_hash != dst_hash:
                    makedirs(os.path.dirname(dst_file))
                    shutil.copyfile(src_file, dst_file)
                    self.report.add('saved', self.src, src_file)
                ref.files[rel_path] = src_hash
            except Exception:
                self.report.add('failed', self.src, src_file)
                logger.exception(f'failed to save {src_file}')
        ref.save()
        self.stats['file_count'] = len(src_files)


class GoogleCloudSaver(BaseSaver):
    def notify_error(self, message, exc=None):
        if isinstance(exc, (AuthError, RefreshError)):
            notify(title=f'{NAME} google auth error', body=message,
                on_click=GOOGLE_OAUTH_WIN_SCRIPT)
        else:
            super().notify_error(message, exc)


class GoogleDriveSaver(GoogleCloudSaver):
    src_type = 'google_drive'
    hostname = 'google_cloud'

    def do_run(self):
        gc = get_google_cloud()
        paths = set()
        ref = Reference(self.dst)
        ref.src = self.src
        ref.files = {}
        for file_data in gc.iterate_files():
            if not file_data['exportable']:
                self.report.add('skipped', self.src, file_data['path'])
                continue
            dst_file = os.path.join(self.dst, file_data['path'])
            paths.add(dst_file)
            mtime = get_file_mtime(dst_file)
            if mtime and mtime > file_data['modified_time']:
                self.report.add('skipped', self.src, dst_file)
                continue
            makedirs(os.path.dirname(dst_file))
            try:
                gc.download_file(file_id=file_data['id'],
                    path=dst_file, mime_type=file_data['mime_type'])
                rel_path = os.path.relpath(dst_file, self.dst)
                ref.files[rel_path] = get_file_hash(dst_file)
                self.report.add('saved', self.src, dst_file)
            except Exception as exc:
                self.report.add('failed', self.src, dst_file)
                logger.error('failed to save google drive file '
                    f'{file_data["name"]}: {exc}')
        ref.save()
        for dst_path in walk_paths(self.dst):
            if dst_path not in paths and self.can_be_purged(dst_path):
                remove_path(dst_path)
        self.stats['file_count'] = len(paths)


class GoogleContactsSaver(GoogleCloudSaver):
    src_type = 'google_contacts'
    hostname = 'google_cloud'

    def do_run(self):
        gc = get_google_cloud()
        contacts = gc.list_contacts()
        data = to_json(contacts)
        file = os.path.join(self.dst, f'{self.src_type}.json')
        if text_file_exists(file, data):
            self.report.add('skipped', self.src, file)
        else:
            makedirs(os.path.dirname(file))
            with open(file, 'w', encoding='utf-8') as fd:
                fd.write(data)
            self.report.add('saved', self.src, file)
            logger.info(f'saved {len(contacts)} google contacts')
        ref = Reference(self.dst)
        ref.src = self.src
        ref.files = {os.path.relpath(file, self.dst): get_file_hash(file)}
        ref.save()
        self.stats['file_count'] = 1


class GoogleBookmarksSaver(BaseSaver):
    src_type = 'google_bookmarks'
    hostname = 'google_cloud'

    def _create_bookmark_file(self, title, url, file):
        data = f'<html><body><a href="{url}">{title}</a></body></html>'
        if text_file_exists(file, data, log_content_changed=True):
            self.report.add('skipped', self.src, file)
        else:
            with open(file, 'w', encoding='utf-8') as fd:
                fd.write(data)
            self.report.add('saved', self.src, file)

    def do_run(self):
        bookmarks = google_chrome.get_bookmarks()
        paths = set()
        ref = Reference(self.dst)
        ref.src = self.src
        ref.files = {}
        for bookmark in bookmarks:
            dst_path = os.path.join(self.dst, *(bookmark['path'].split('/')))
            makedirs(dst_path)
            name = bookmark['name'] or bookmark['url']
            dst_file = os.path.join(dst_path, f'{path_to_filename(name)}.html')
            self._create_bookmark_file(title=name, url=bookmark['url'],
                file=dst_file)
            rel_path = os.path.relpath(dst_file, self.dst)
            ref.files[rel_path] = get_file_hash(dst_file)
            paths.add(dst_path)
            paths.add(dst_file)
        ref.save()
        for dst_path in walk_paths(self.dst):
            if dst_path not in paths and self.can_be_purged(dst_path):
                remove_path(dst_path)
        self.stats['file_count'] = len(bookmarks)


class SaveItem:
    def __init__(self, src_paths=None, src_type=LocalSaver.src_type,
            dst_path=DST_PATH, run_delta=0, retention_delta=RETENTION_DELTA,
            restorable=True, os_name=None):
        self.src_paths = self._get_src_paths(src_paths)
        self.src_type = src_type
        self.dst_path = self._get_dst_path(dst_path)
        self.run_delta = run_delta
        self.retention_delta = retention_delta
        self.restorable = restorable
        self.os_name = os_name
        self.saver_cls = self._get_saver_class()

    def _get_src_paths(self, src_paths):
        return [s if isinstance(s, (list, tuple))
            else (s, [], []) for s in (src_paths or [])]

    def _get_dst_path(self, dst_path):
        validate_path(dst_path)
        dst_path = os.path.expanduser(dst_path)
        if not os.path.exists(dst_path):
            raise InvalidPath(f'invalid dst_path {dst_path}: does not exist')
        return os.path.join(dst_path, NAME, self.src_type)

    def _get_saver_class(self):
        module = sys.modules[__name__]
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__ \
                    and issubclass(obj, BaseSaver) \
                    and obj.src_type == self.src_type:
                return obj
        raise Exception(f'invalid src_type {self.src_type}')

    def _generate_src_and_patterns(self):
        if self.src_type == LocalSaver.src_type:
            for src_path, inclusions, exclusions in self.src_paths:
                try:
                    validate_path(src_path)
                except UnhandledPath:
                    continue
                for src in glob(os.path.expanduser(src_path)):
                    yield src, inclusions, exclusions
        else:
            yield self.src_type, None, None

    def generate_savers(self):
        if self.os_name and os.name != self.os_name:
            return
        makedirs(self.dst_path)
        for src_and_patterns in self._generate_src_and_patterns():
            yield self.saver_cls(
                *src_and_patterns,
                dst_path=self.dst_path,
                run_delta=self.run_delta,
                retention_delta=self.retention_delta,
            )


def iterate_save_items(log_unhandled=False, log_invalid=True):
    for save in SAVES:
        try:
            yield SaveItem(**save)
        except UnhandledPath as exc:
            if log_unhandled:
                logger.warning(exc)
            continue
        except InvalidPath as exc:
            if log_invalid:
                logger.warning(exc)
            continue


class SaveHandler:
    def __init__(self, force=False, stats=False):
        self.force = force
        self.stats = stats

    def _generate_savers(self):
        for si in iterate_save_items():
            for saver in si.generate_savers():
                yield saver

    def check_data(self):
        report = Report()
        for saver in self._generate_savers():
            try:
                saver.check_data()
            except NotImplementedError:
                continue
            report.merge(saver.report)
        return report

    def run(self):
        start_ts = time.time()
        savers = list(self._generate_savers())
        report = Report()
        stats = {}
        for saver in savers:
            try:
                saver.run(force=self.force)
            except Exception as exc:
                logger.exception(f'failed to save {saver.src}')
                notify(title=f'{NAME} exception',
                    body=f'failed to save {saver.src}: {exc}')
            stats[saver.src] = saver.stats
            report.merge(saver.report)
        Metadata().save(keys={s.src for s in savers})
        res = report.clean(keys={'saved', 'removed'})
        if res:
            logger.info(f'report:\n{to_json(res)}')
        if self.stats:
            logger.info(f'stats:\n{to_json(stats)}')
        logger.info(f'completed in {time.time() - start_ts:.02f} seconds')


class LocalRestorer:
    def __init__(self, dst_path, hostname=None, username=None,
            include=None, exclude=None, overwrite=False, dry_run=False):
        self.dst_path = dst_path
        self.hostname = hostname or HOSTNAME
        self.username = username or USERNAME
        self.include = include
        self.exclude = exclude
        self.overwrite = overwrite
        self.dry_run = dry_run
        self.hostnames = sorted(os.listdir(self.dst_path))
        self.report = Report()

    def _get_src_file_for_user(self, path):
        pp = PurePath(path)
        home_root = os.path.dirname(HOME_PATH)
        if not pp.is_relative_to(home_root):
            return path
        try:
            username = pp.parts[2]
        except IndexError:
            return path
        if username in SHARED_USERNAMES:
            return path
        if username == self.username:
            return path.replace(os.path.join(home_root, username),
                HOME_PATH, 1)
        return None

    def _iterate_dst_and_ref(self):
        for hostname in self.hostnames:
            if hostname != self.hostname:
                continue
            for dst in glob(os.path.join(self.dst_path, hostname, '*')):
                ref = Reference(dst)
                if ref.src:
                    yield dst, ref

    def check_data(self):
        for dst, ref in self._iterate_dst_and_ref():
            src = ref.src
            try:
                validate_path(src)
            except UnhandledPath:
                self.report.add('skipped_unhandled', src, src)
                continue
            for rel_path, ref_hash in ref.files.items():
                dst_file = os.path.join(dst, rel_path)
                dst_hash = get_file_hash(dst_file)
                if dst_hash != ref_hash:
                    self.report.add('invalid_dst_files', dst, dst_file)
                src_file_raw = os.path.join(src, rel_path)
                src_file = self._get_src_file_for_user(src_file_raw)
                if not src_file:
                    self.report.add('skipped_other_username', src,
                        src_file_raw)
                    continue
                if os.path.exists(src_file):
                    if get_file_hash(src_file) == dst_hash == ref_hash:
                        self.report.add('ok', src, src_file)
                    else:
                        self.report.add('hash_mismatched', src, src_file)
                else:
                    self.report.add('missing_at_src', src, src_file)

    def _requires_restore(self, dst_file, src_file, src):
        if not check_patterns(src_file, self.include, self.exclude):
            return False
        if not os.path.exists(src_file):
            return True
        if get_file_hash(src_file) == get_file_hash(dst_file):
            self.report.add('skipped_identical', src, src_file)
            return False
        if not self.overwrite:
            self.report.add('skipped_hash_mismatched', src, src_file)
            return False
        return True

    def _restore_file(self, dst_file, src_file, src):
        if not self._requires_restore(dst_file, src_file, src):
            return
        if self.dry_run:
            self.report.add('to_restore', src, src_file)
            return
        try:
            if os.path.exists(src_file):
                src_file_bak = f'{src_file}.{NAME}bak'
                if os.path.exists(src_file_bak):
                    os.remove(src_file)
                else:
                    os.rename(src_file, src_file_bak)
                    logger.warning(f'renamed existing src file '
                        f'{src_file} to {src_file_bak}')
                self.report.add('restored_overwritten', src, src_file)
            else:
                self.report.add('restored', src, src_file)
            makedirs(os.path.dirname(src_file))
            shutil.copyfile(dst_file, src_file)
            logger.info(f'restored {src_file} from {dst_file}')
        except Exception as exc:
            self.report.add('failed', src, src_file)
            logger.error(f'failed to restore {src_file} '
                f'from {dst_file}: {exc}')

    def run(self):
        for dst, ref in self._iterate_dst_and_ref():
            src = ref.src
            try:
                validate_path(src)
            except UnhandledPath:
                self.report.add('skipped_unhandled', src, src)
                continue
            rel_paths = set()
            invalid_files = set()
            for rel_path, ref_hash in ref.files.items():
                dst_file = os.path.join(dst, rel_path)
                if get_file_hash(dst_file) != ref_hash:
                    invalid_files.add(dst_file)
                else:
                    rel_paths.add(rel_path)
            if invalid_files:
                self.report.add('invalid_files', src, invalid_files)
                continue
            if not rel_paths:
                self.report.add('empty_dst', src, dst)
                continue
            for rel_path in rel_paths:
                src_file_raw = os.path.join(src, rel_path)
                src_file = self._get_src_file_for_user(src_file_raw)
                if not src_file:
                    self.report.add('skipped_other_username', src,
                        src_file_raw)
                    continue
                self._restore_file(os.path.join(dst, rel_path),
                    src_file, src)


class RestoreHandler:
    def __init__(self, **restorer_args):
        self.restorer_args = restorer_args

    def _iterate_restorers(self):
        dst_paths = {s.dst_path
            for s in iterate_save_items(log_unhandled=True)
            if s.saver_cls == LocalSaver and s.restorable}
        if not dst_paths:
            logger.info('nothing to restore')
        for dst_path in dst_paths:
            yield LocalRestorer(dst_path=dst_path, **self.restorer_args)

    def list_hostnames(self):
        hostnames = set()
        for restorer in self._iterate_restorers():
            hostnames.update(restorer.hostnames)
        return hostnames

    def check_data(self):
        report = Report()
        for restorer in self._iterate_restorers():
            try:
                restorer.check_data()
            except Exception:
                logger.exception(f'failed to check {restorer.dst_path}')
            report.merge(restorer.report)
        return report

    def run(self):
        report = Report()
        for restorer in self._iterate_restorers():
            try:
                restorer.run()
            except Exception:
                logger.exception(f'failed to restore {restorer.dst_path}')
            report.merge(restorer.report)
        logger.info(f'report:\n{to_json(report.clean())}')
        logger.info(f'summary:\n{to_json(report.get_summary())}')


class SaveChecker:
    last_run_file = os.path.join(WORK_PATH, 'last_check')

    def _get_last_run_ts(self):
        try:
            with open(self.last_run_file) as fd:
                return int(fd.read())
        except Exception:
            return 0

    def _set_last_run_ts(self):
        with open(self.last_run_file, 'w') as fd:
            fd.write(str(int(time.time())))

    def run(self):
        now_ts = time.time()
        if now_ts < self._get_last_run_ts() + CHECK_DELTA:
            return
        res = defaultdict(set)
        min_ts = now_ts - OLD_DELTA
        dst_paths = {s.dst_path for s in iterate_save_items(
            log_unhandled=True)}
        for dst_path in dst_paths:
            for hostname in sorted(os.listdir(dst_path)):
                for dst in glob(os.path.join(dst_path, hostname, '*')):
                    ref = Reference(dst)
                    if os.stat(ref.file).st_mtime < min_ts:
                        res[hostname].add(ref.src)

        if res:
            report = {k: sorted(v) for k, v in res.items()}
            logger.warning(f'saves not updated recently:\n{to_json(report)}')
            notify(title=f'{NAME} warning',
                body='Hostname saves not updated recently: '
                    f'{", ".join(sorted(res.keys()))}')
        self._set_last_run_ts()


def with_lockfile():
    lockfile_path = os.path.join(WORK_PATH, 'lock')

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if os.name == 'posix' and os.path.exists(lockfile_path):
                logger.error(f'Lock file {lockfile_path} exists. '
                    'Another process may be running.')
                raise RuntimeError(f'Lock file {lockfile_path} exists. '
                    'Another process may be running.')

            def remove_lockfile():
                if os.path.exists(lockfile_path):
                    os.remove(lockfile_path)

            atexit.register(remove_lockfile)

            def handle_signal(signum, frame):
                remove_lockfile()
                raise SystemExit(f'Program terminated by signal {signum}')

            if os.name == 'posix':
                signal.signal(signal.SIGINT, handle_signal)
                signal.signal(signal.SIGTERM, handle_signal)

            try:
                with open(lockfile_path, 'w') as lockfile:
                    lockfile.write('locked')
                result = func(*args, **kwargs)
            finally:
                remove_lockfile()
            return result

        return wrapper
    return decorator


def is_idle():
    res = psutil.cpu_times_percent(interval=1).idle > 95
    if not res:
        logger.warning('not idle')
    return res


def must_run(last_run_ts):
    now_ts = time.time()
    if now_ts > last_run_ts + FORCE_RUN_DELTA:
        return True
    if now_ts > last_run_ts + RUN_DELTA and is_idle():
        return True
    return False


def savegame(**kwargs):
    SaveHandler(**kwargs).run()
    SaveChecker().run()


def checkgame(hostname=None):
    save_report = SaveHandler().check_data()
    restore_report = RestoreHandler(hostname=hostname).check_data()
    logger.info('save report:\n'
        f'{to_json(save_report.clean())}')
    logger.info('restore report:\n'
        f'{to_json(restore_report.clean())}')
    logger.info('save summary:\n'
        f'{to_json(save_report.get_summary())}')
    logger.info('restore summary:\n'
        f'{to_json(restore_report.get_summary())}')


def restoregame(**kwargs):
    RestoreHandler(**kwargs).run()


def list_hostnames(**kwargs):
    hostnames = RestoreHandler(**kwargs).list_hostnames()
    logger.info(f'available hostnames: {sorted(hostnames)}')


def google_oauth(**kwargs):
    get_google_cloud().get_oauth_creds(interact=True)


class Daemon:
    last_run_ts = 0

    @with_lockfile()
    def run(self):
        while True:
            try:
                if must_run(self.last_run_ts):
                    savegame()
                    self.last_run_ts = time.time()
            except Exception:
                logger.exception('wtf')
            finally:
                logger.debug('sleeping')
                time.sleep(DAEMON_LOOP_DELAY)


class Task:
    last_run_file = os.path.join(WORK_PATH, 'last_run')

    def _get_last_run_ts(self):
        try:
            with open(self.last_run_file) as fd:
                return int(fd.read())
        except Exception:
            return 0

    def _set_last_run_ts(self):
        with open(self.last_run_file, 'w') as fd:
            fd.write(str(int(time.time())))

    @with_lockfile()
    def run(self):
        if must_run(self._get_last_run_ts()):
            savegame()
            self._set_last_run_ts()


def _parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='cmd')
    save_parser = subparsers.add_parser('save')
    save_parser.add_argument('--daemon', action='store_true')
    save_parser.add_argument('--task', action='store_true')
    save_parser.add_argument('--stats', action='store_true')
    check_parser = subparsers.add_parser('check')
    check_parser.add_argument('--hostname')
    restore_parser = subparsers.add_parser('restore')
    restore_parser.add_argument('--hostname')
    restore_parser.add_argument('--username')
    restore_parser.add_argument('--include', nargs='*')
    restore_parser.add_argument('--exclude', nargs='*')
    restore_parser.add_argument('--overwrite', action='store_true')
    restore_parser.add_argument('--dry-run', action='store_true')
    subparsers.add_parser('hostnames')
    subparsers.add_parser('google_oauth')
    return parser.parse_args()


def main():
    args = _parse_args()
    if args.cmd == 'save':
        if args.daemon:
            Daemon().run()
        elif args.task:
            Task().run()
        else:
            savegame(force=True, stats=args.stats)
    else:
        {
            'check': checkgame,
            'restore': restoregame,
            'hostnames': list_hostnames,
            'google_oauth': google_oauth,
        }[args.cmd](**{k: v for k, v in vars(args).items() if k != 'cmd'})


if __name__ == '__main__':
    main()
