import argparse
import atexit
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
from fnmatch import fnmatch
import functools
from glob import glob
import hashlib
import inspect
import json
import logging
from logging.handlers import RotatingFileHandler
import os
from pprint import pprint
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import zlib

import psutil

import google_chrome
from google_cloud import GoogleCloud, AuthError, RefreshError


SAVES = []
MAX_LOG_FILE_SIZE = 1000 * 1024
RETRY_DELTA = 2 * 3600
OLD_DELTA = 2 * 24 * 3600
HASH_CACHE_TTL = 24 * 3600
IDLE_CPU_THRESHOLD = 1
RUN_DELTA = 30 * 60
RETENTION_DELTA = 7 * 24 * 3600
FORCE_RUN_DELTA = 90 * 60
DAEMON_LOOP_DELAY = 10
DST_PATH = os.path.join(os.path.expanduser('~'), 'OneDrive')
NAME = os.path.splitext(os.path.basename(os.path.realpath(__file__)))[0]
WORK_PATH = os.path.join(os.path.expanduser('~'), f'.{NAME}')
HOSTNAME = socket.gethostname()
RE_SPECIAL = re.compile(r'\W+')
GOOGLE_AUTH_WIN_FILE = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), 'google_cloud_auth.pyw')
REF_FILE = f'.{NAME}'
MAX_TARGET_VERSIONS = 4
MIN_SIZE_RATIO = .5
SHARED_USERNAMES = {
    'nt': {'Public'},
    'posix': {'shared'},
}.get(os.name, set())

try:
    from user_settings import *
except ImportError:
    pass


makedirs = lambda x: None if os.path.exists(x) else os.makedirs(x)


def setup_logging(logger, path):
    logging.basicConfig(level=logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    if sys.stdout and not sys.stdout.isatty():
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(logging.DEBUG)
        logger.addHandler(stdout_handler)
    else:
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


get_filename = lambda x: RE_SPECIAL.sub('_', x).strip('_')
get_file_mtime = lambda x: datetime.fromtimestamp(os.stat(x).st_mtime,
    tz=timezone.utc) if os.path.exists(x) else None
to_json = lambda x: json.dumps(x, indent=4, sort_keys=True)


def get_path_size(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    res = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for filename in files:
            if filename == REF_FILE:
                continue
            file = os.path.join(root, filename)
            try:
                res += os.path.getsize(file)
            except Exception:
                logger.error(f'failed to get size for {file}')
    return res


def remove_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)


def walk_paths(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for item in sorted(files + dirs):
            yield os.path.join(root, item)


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in sorted(files):
            yield os.path.join(root, file)


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


def match_any_pattern(path, patterns):
    for pattern in patterns:
        if fnmatch(path, pattern):
            return True
    return False


def is_path_excluded(path, inclusions, exclusions, file_only=True):
    if file_only and os.path.isdir(path):
        return False
    if inclusions and not match_any_pattern(path, inclusions):
        return True
    if exclusions and match_any_pattern(path, exclusions):
        return True
    return False


class InvalidPath(Exception):
    pass


class FileHashManager:
    cache_file = os.path.join(WORK_PATH, 'cache.dat')
    cache = {}

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
            cls.instance.load()
        return cls.instance

    def hash(self, path):
        hash_obj = hashlib.md5()
        with open(path, 'rb') as fd:
            while True:
                buffer = fd.read(8192)
                if not buffer:
                    break
                hash_value = hashlib.md5(buffer).hexdigest()
                hash_obj.update(hash_value.encode('utf-8'))
        return hash_obj.hexdigest()

    def set(self, path, value):
        self.cache[path] = value, int(time.time())

    def get(self, path, use_cache=False):
        if not os.path.exists(path):
            return None
        if use_cache:
            try:
                return self.cache[path][0]
            except KeyError:
                pass
        res = self.hash(path)
        if use_cache:
            self.set(path, res)
        return res

    def load(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'rb') as fd:
                self.cache = json.loads(zlib.decompress(fd.read()))
                logger.debug(f'loaded {len(self.cache)} cached items')

    def save(self):
        started_ts = time.time()
        limit_ts = time.time() - HASH_CACHE_TTL
        for path, (h, ts) in deepcopy(self.cache).items():
            if ts < limit_ts or not os.path.exists(path):
                try:
                    del self.cache[path]
                except KeyError:
                    pass
        with open(self.cache_file, 'wb') as fd:
            fd.write(zlib.compress(json.dumps(self.cache).encode('utf-8')))
        logger.debug(f'saved {len(self.cache)} cached items'
            f' in {time.time() - started_ts:.2f} seconds')


class MetaManager:
    meta_file = os.path.join(WORK_PATH, 'meta.json')
    meta = {}

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
            cls.instance.load()
        return cls.instance

    def load(self):
        if os.path.exists(self.meta_file):
            with open(self.meta_file) as fd:
                self.meta = json.loads(fd.read())
                logger.debug(f'loaded {len(self.meta)} meta items')

    def save(self):
        started_ts = time.time()
        for path in deepcopy(self.meta).keys():
            if not os.path.exists(path):
                try:
                    del self.meta[path]
                except KeyError:
                    pass
        with open(self.meta_file, 'w') as fd:
            fd.write(to_json(self.meta))
        logger.debug(f'saved {len(self.meta)} meta items '
            f'in {time.time() - started_ts:.2f} seconds')

    def set(self, key, value: dict):
        self.meta[key] = value

    def get(self, key):
        return self.meta.get(key, {})

    def check(self):
        now_ts = time.time()
        for path, meta in self.meta.items():
            if meta['updated_ts'] and now_ts > meta['updated_ts'] \
                    + meta['min_delta'] + OLD_DELTA:
                logger.error(f'{meta["source"]} has not been saved recently')
                notify(title=f'{NAME} warning',
                    body=f'{meta["source"]} has not been saved recently')


class AbstractSaver:
    src_type = None

    def __init__(self, src, inclusions, exclusions, dst_path,
            creds_file=None, retention_delta=RETENTION_DELTA):
        self.src = src
        self.inclusions = inclusions
        self.exclusions = exclusions
        self.dst_path = dst_path
        self.retention_delta = retention_delta
        self.creds_file = creds_file
        self.dst = self.get_dst()
        self.file_hash_manager = FileHashManager()
        self.report = defaultdict(lambda: defaultdict(set))
        self.meta = None

    def get_dst(self):
        dst =  os.path.join(self.dst_path, self.src_type)
        makedirs(dst)
        return dst

    def needs_purge(self, path):
        return time.time() - os.stat(path).st_mtime > self.retention_delta


class LocalSaver(AbstractSaver):
    src_type = 'local'

    def get_dst(self):
        target_name = get_filename(self.src)
        src_size = get_path_size(self.src)
        for index in range(1, MAX_TARGET_VERSIONS + 1):
            suffix = '' if index == 1 else f'-{index}'
            dst = os.path.join(self.dst_path, HOSTNAME,
                f'{target_name}{suffix}')
            size = get_path_size(dst)
            if not size or src_size / size > MIN_SIZE_RATIO:
                break
        makedirs(dst)
        return dst

    def _generate_ref_file(self, src):
        file = os.path.join(self.dst, REF_FILE)
        data = str(src)
        if not text_file_exists(file, data, log_content_changed=True):
            logger.info(f'created ref file {file}')
            with open(file, 'w', encoding='utf-8') as fd:
                fd.write(data)

    def run(self):
        src = self.src
        size = 0
        started_ts = time.time()

        if os.path.isfile(src):
            src_files = [src]
            src = os.path.dirname(src)
        else:
            src_files = list(walk_files(src))
            if not src_files:
                logger.debug(f'skipped empty src path {src}')
                return

        for dst_path in walk_paths(self.dst):
            if os.path.basename(dst_path) == REF_FILE:
                continue
            src_path = os.path.join(src, os.path.relpath(dst_path, self.dst))
            if (not os.path.exists(src_path) and self.needs_purge(dst_path)) \
                    or is_path_excluded(src_path,
                        self.inclusions, self.exclusions):
                remove_path(dst_path)
                self.report[src]['removed'].add(dst_path)
                logger.debug(f'removed {dst_path}')

        for src_file in src_files:
            if is_path_excluded(src_file, self.inclusions, self.exclusions):
                self.report[src]['excluded'].add(src_file)
                logger.debug(f'excluded {src_file}')
                continue
            self.report[src]['files'].add(src_file)
            size += os.path.getsize(src_file)
            dst_file = os.path.join(self.dst, os.path.relpath(src_file, src))
            src_hash = self.file_hash_manager.get(src_file, use_cache=False)
            dst_hash = self.file_hash_manager.get(dst_file, use_cache=True)
            if dst_hash == src_hash:
                continue
            try:
                makedirs(os.path.dirname(dst_file))
                shutil.copyfile(src_file, dst_file)
                self.file_hash_manager.set(dst_file, src_hash)
                self.report[src]['saved'].add(dst_file)
                logger.debug(f'saved {src_file}')
            except Exception:
                logger.exception(f'failed to save {src_file}')

        self._generate_ref_file(src)
        logger.debug(f'saved {src} in {time.time() - started_ts:.02f} seconds')
        self.meta = {
            'file_count': len(self.report[src]['files']),
            'size_MB': size / 1024 / 1024,
        }


class GoogleDriveSaver(AbstractSaver):
    src_type = 'google_drive'

    def run(self):
        gc = GoogleCloud(oauth_creds_file=self.creds_file)
        paths = set()
        for file_data in gc.iterate_files():
            dst_file = os.path.join(self.dst, file_data['filename'])
            paths.add(dst_file)
            mtime = get_file_mtime(dst_file)
            if mtime and mtime > file_data['modified_time']:
                self.report[self.src_type]['skipped'].add(dst_file)
                logger.debug(f'skipped saving google drive file {dst_file}: '
                    'already exists')
                continue
            try:
                content = gc.fetch_file_content(file_id=file_data['id'],
                    mime_type=file_data['mime_type'])
                self.report[self.src_type]['saved'].add(dst_file)
                logger.debug(f'saved google drive file {dst_file}')
            except Exception as exc:
                self.report[self.src_type]['failed'].add(dst_file)
                logger.error('failed to save google drive file '
                    f'{file_data["name"]}: {exc}')
                continue
            with open(dst_file, 'wb') as fd:
                fd.write(content)

        for dst_path in walk_paths(self.dst):
            if dst_path not in paths and self.needs_purge(dst_path):
                remove_path(dst_path)

        self.meta = {
            'file_count': len(paths)
        }


class GoogleContactsSaver(AbstractSaver):
    src_type = 'google_contacts'

    def run(self):
        gc = GoogleCloud(oauth_creds_file=self.creds_file)
        contacts = gc.list_contacts()
        data = to_json(contacts)
        file = os.path.join(self.dst, f'{self.src_type}.json')
        if text_file_exists(file, data):
            self.report[self.src_type]['skipped'].add(file)
            logger.debug(f'skipped saving google contacts file {file}: '
                'already exists')
        else:
            with open(file, 'w', encoding='utf-8') as fd:
                fd.write(data)
            self.report[self.src_type]['saved'].add(file)
            logger.info(f'saved {len(contacts)} google contacts')
        self.meta = {
            'file_count': 1,
        }


class GoogleBookmarksSaver(AbstractSaver):
    src_type = 'google_bookmarks'

    def _create_bookmark_file(self, title, url, file):
        data = f'<html><body><a href="{url}">{title}</a></body></html>'
        if text_file_exists(file, data, log_content_changed=True):
            self.report[self.src_type]['skipped'].add(file)
            logger.debug(f'skipped saving google bookmark {file}: '
                'already exists')
        else:
            with open(file, 'w', encoding='utf-8') as fd:
                fd.write(data)
            self.report[self.src_type]['saved'].add(file)
            logger.debug(f'saved google bookmark {file}')

    def run(self):
        bookmarks = google_chrome.get_bookmarks()
        paths = set()
        for bookmark in bookmarks:
            dst_path = os.path.join(self.dst, *(bookmark['path'].split('/')))
            makedirs(dst_path)
            name = bookmark['name'] or bookmark['url']
            dst_file = f'{os.path.join(dst_path, get_filename(name))}.html'
            self._create_bookmark_file(title=name, url=bookmark['url'],
                file=dst_file)
            paths.add(dst_path)
            paths.add(dst_file)

        for dst_path in walk_paths(self.dst):
            if dst_path not in paths and self.needs_purge(dst_path):
                remove_path(dst_path)

        self.meta = {
            'file_count': len(bookmarks),
        }


class SaveItem:
    def __init__(self, src_paths=None, src_type=None, dst_path=DST_PATH,
            min_delta=0, retention_delta=RETENTION_DELTA, creds_file=None,
            restorable=True, force=False,
            ):
        self.src_paths = self._get_src_paths(src_paths)
        self.src_type = src_type or LocalSaver.src_type
        self.dst_path = self._get_dst_path(dst_path)
        self.min_delta = min_delta
        self.retention_delta = retention_delta
        self.creds_file = creds_file
        self.restorable = restorable
        self.force = force
        self.meta_manager = MetaManager()
        self.report = defaultdict(lambda: defaultdict(set))

    def _get_src_paths(self, src_paths):
        return [s if isinstance(s, (list, tuple))
            else (s, [], []) for s in (src_paths or [])]

    def _get_dst_path(self, dst_path):
        if not os.path.exists(dst_path):
            raise InvalidPath(f'invalid dst_path {dst_path}: does not exist')
        if dst_path != os.path.expanduser(dst_path):
            raise InvalidPath(f'invalid dst_path {dst_path}: must be absolute')
        return os.path.join(dst_path, NAME, self.src_type)

    def _check_meta(self, meta):
        if not meta:
            return True
        if time.time() > max(meta['updated_ts'] + self.min_delta,
                meta['next_ts']):
            return True
        return False

    def _update_meta(self, src, dst, started_ts, updated_ts=None,
            retry_delta=0, extra_meta=None):
        now_ts = time.time()
        meta = {
            'source': src,
            'started_ts': started_ts,
            'updated_ts': now_ts if updated_ts is None else updated_ts,
            'next_ts': now_ts + retry_delta,
            'min_delta': self.min_delta,
            'duration': time.time() - started_ts,
        }
        if extra_meta:
            meta.update(extra_meta)
        self.meta_manager.set(dst, meta)

    def _get_saver_class(self):
        module = sys.modules[__name__]
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, AbstractSaver) and obj is not AbstractSaver:
                    if obj.src_type == self.src_type:
                        return obj
        raise Exception(f'invalid src_type {self.src_type}')

    def _iterate_src_and_filters(self):
        if self.src_type == LocalSaver.src_type:
            for path, inclusions, exclusions in self.src_paths:
                for src in glob(os.path.expanduser(path)):
                    if is_path_excluded(src, inclusions, exclusions,
                            file_only=False):
                        logger.debug(f'excluded {src}')
                        continue
                    yield src, inclusions, exclusions
        else:
            yield self.src_type, None, None

    def _notify_error(self, message, exc):
        if isinstance(exc, (AuthError, RefreshError)):
            notify(title=f'{NAME} google auth error', body=message,
                on_click=GOOGLE_AUTH_WIN_FILE)
        else:
            notify(title=f'{NAME} error', body=message)

    def _update_report(self, saver):
        for path, data in saver.report.items():
            for k, v in data.items():
                self.report[path][k].update(v)

    def save(self):
        makedirs(self.dst_path)
        saver_cls = self._get_saver_class()
        for src_and_filters in self._iterate_src_and_filters():
            saver = saver_cls(*src_and_filters,
                dst_path=self.dst_path,
                retention_delta=self.retention_delta,
                creds_file=self.creds_file,
            )
            meta = self.meta_manager.get(saver.dst)
            if not self.force and not self._check_meta(meta):
                continue
            started_ts = time.time()
            updated_ts = None
            retry_delta = 0
            try:
                saver.run()
            except Exception as exc:
                updated_ts = meta.get('updated_ts', 0)
                retry_delta = RETRY_DELTA
                logger.exception(f'failed to save {saver.src}')
                self._notify_error(f'failed to save {saver.src}: {exc}', exc=exc)
            self._update_report(saver)
            self._update_meta(saver.src, saver.dst,
                started_ts=started_ts, updated_ts=updated_ts,
                retry_delta=retry_delta, extra_meta=saver.meta)


class SaveHandler:
    def __init__(self, force=False):
        self.force = force
        self.report = defaultdict(lambda: defaultdict(set))

    def _save(self, save):
        started_ts = time.time()
        try:
            si = SaveItem(**save, force=self.force)
            si.save()
            for path, data in si.report.items():
                for k, v in data.items():
                    self.report[path][k].update(v)
        except InvalidPath as exc:
            logger.warning(exc)
        except Exception as exc:
            logger.exception(f'failed to save {save}')
            notify(title=f'{NAME} exception',
                body=f'failed to save {save}: {exc}')
        finally:
            logger.debug(f'processed {save} in '
                f'{time.time() - started_ts:.02f} seconds')

    def _generate_report(self):
        summary = defaultdict(int)
        keys = {'saved', 'removed'}
        for path, data in self.report.items():
            path_summary = {k: len(v) for k, v in data.items()
                if k in keys and v}
            if path_summary:
                summary[path] = path_summary
        if summary:
            logger.info(f'summary:\n{to_json(summary)}')

    def run(self):
        started_ts = time.time()
        try:
            for save in SAVES:
                self._save(save)
            FileHashManager().save()
            MetaManager().save()
            MetaManager().check()
        finally:
            self._generate_report()
            logger.info(f'completed in {time.time() - started_ts:.02f} seconds')


class RestoreItem:
    def __init__(self, dst_path, from_hostname=None, from_username=None,
            overwrite=False, dry_run=False):
        self.dst_path = dst_path
        self.from_hostname = from_hostname or HOSTNAME
        self.from_username = from_username or os.getlogin()
        self.overwrite = overwrite
        self.dry_run = dry_run
        self.file_hash_manager = FileHashManager()
        self.report = defaultdict(lambda: defaultdict(set))

    def _get_src(self, dst):
        ref_file = os.path.join(dst, REF_FILE)
        if not os.path.exists(ref_file):
            return None
        with open(ref_file) as fd:
            src = fd.read()
        if not src:
            logger.error(f'invalid ref src in {ref_file}')
            return None
        return src

    def _get_valid_src_file(self, path):
        with_end_sep = lambda x: f'{x.rstrip(os.sep)}{os.sep}'
        home_path = os.path.expanduser('~')
        home = os.path.dirname(home_path)
        if not path.startswith(with_end_sep(home)):
            return path
        if path.startswith(with_end_sep(home_path)):
            return path
        username = path.split(os.sep)[2]
        if username in SHARED_USERNAMES:
            return path
        if username == self.from_username:
            return path.replace(with_end_sep(os.path.join(home, username)),
                with_end_sep(home_path), 1)
        logger.debug(f'skipped {path}: the path username '
            f'does not match {self.from_username}')
        return None

    def _requires_restore(self, dst_file, src_file, src):
        if not os.path.exists(src_file):
            return True
        if self.file_hash_manager.hash(src_file) \
                == self.file_hash_manager.hash(dst_file):
            self.report[src]['skipped_identical'].add(src_file)
            return False
        if not self.overwrite:
            self.report[src]['skipped_conflict'].add(src_file)
            return False
        return True

    def _restore_file(self, dst_file, src_file, src):
        if not self._requires_restore(dst_file, src_file, src):
            return
        if self.dry_run:
            self.report[src]['to_restore'].add(src_file)
            logger.debug(f'to restore: {src_file} from {dst_file}')
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
                self.report[src]['restored_overwritten'].add(src_file)
            else:
                self.report[src]['restored'].add(src_file)
            makedirs(os.path.dirname(src_file))
            shutil.copyfile(dst_file, src_file)
            logger.info(f'restored {src_file} from {dst_file}')
        except Exception as exc:
            self.report[src]['failed'].add(src_file)
            logger.error(f'failed to restore {src_file} '
                f'from {dst_file}: {exc}')

    def list_hostnames(self):
        return sorted(os.listdir(self.dst_path))

    def restore(self):
        to_restore = set()
        hostnames = self.list_hostnames()
        for hostname in hostnames:
            if hostname != self.from_hostname:
                continue
            for dst_dir in os.listdir(os.path.join(self.dst_path, hostname)):
                dst = os.path.join(self.dst_path, hostname, dst_dir)
                src = self._get_src(dst)
                if src:
                    to_restore.add((src, dst))

        if not to_restore:
            logger.info(f'nothing to restore from path {self.dst_path} '
                f'and hostname {self.from_hostname} '
                f'(available hostnames: {hostnames})')
            return

        for src, dst in sorted(to_restore):
            for dst_file in walk_files(dst):
                if os.path.basename(dst_file) == REF_FILE:
                    continue
                src_file = os.path.join(src, os.path.relpath(dst_file, dst))
                src_file = self._get_valid_src_file(src_file)
                if src_file:
                    self._restore_file(dst_file, src_file, src)


class RestoreHandler:
    def __init__(self, **restore_item_args):
        self.restore_item_args = restore_item_args
        self.report = defaultdict(lambda: defaultdict(set))

    def _iterate_save_items(self):
        if not SAVES:
            logger.info('missing saves')
            return
        for save in SAVES:
            try:
                si = SaveItem(**save)
                if si.src_type == LocalSaver.src_type and si.restorable:
                    yield si
            except InvalidPath:
                continue

    def _iterate_restore_items(self):
        dst_paths = set()
        for si in self._iterate_save_items():
            dst_paths.add(si.dst_path)
        for dst_path in dst_paths:
            yield RestoreItem(dst_path=dst_path, **self.restore_item_args)

    def list_hostnames(self):
        hostnames = set()
        for ri in self._iterate_restore_items():
            hostnames.update(ri.list_hostnames())
        return hostnames

    def _generate_report(self):
        summary = defaultdict(int)
        for path, data in self.report.items():
            path_summary = {k: len(v) for k, v in data.items() if v}
            if path_summary:
                summary[path] = path_summary
        if summary:
            logger.info(f'summary:\n{to_json(summary)}')

    def run(self):
        for ri in self._iterate_restore_items():
            try:
                ri.restore()
            except Exception:
                logger.exception(f'failed to restore {ri.dst_path}')
            for path, data in ri.report.items():
                for k, v in data.items():
                    self.report[path][k].update(v)

        self._generate_report()


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


def must_run(last_run_ts):
    is_idle = lambda: psutil.cpu_percent(interval=3) < IDLE_CPU_THRESHOLD
    now_ts = time.time()
    if now_ts > last_run_ts + FORCE_RUN_DELTA:
        return True
    if now_ts > last_run_ts + RUN_DELTA and is_idle():
        return True
    return False


def savegame(**kwargs):
    return SaveHandler(**kwargs).run()


def restoregame(**kwargs):
    return RestoreHandler(**kwargs).run()


def list_hostnames(**kwargs):
    hostnames = RestoreHandler(**kwargs).list_hostnames()
    logger.info(f'available hostnames: {sorted(hostnames)}')


def google_oauth(**kwargs):
    GoogleCloud(**kwargs).get_oauth_creds(interact=True)


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
    subparsers = parser.add_subparsers(dest='command')
    save_parser = subparsers.add_parser('save')
    save_parser.add_argument('--daemon', action='store_true')
    save_parser.add_argument('--task', action='store_true')
    restore_parser = subparsers.add_parser('restore')
    restore_parser.add_argument('--from-hostname')
    restore_parser.add_argument('--from-username')
    restore_parser.add_argument('--overwrite', action='store_true')
    restore_parser.add_argument('--dry-run', action='store_true')
    hostnames_parser = subparsers.add_parser('hostnames')
    google_oauth_parser = subparsers.add_parser('google_oauth')
    google_oauth_parser.add_argument('--oauth-creds-file')
    return parser.parse_args()


def main():
    args = _parse_args()
    if args.command == 'save':
        if args.daemon:
            Daemon().run()
        elif args.task:
            Task().run()
        else:
            savegame(force=True)
    elif args.command == 'restore':
        restoregame(**{k: v for k, v in vars(args).items()
            if k != 'command'})
    elif args.command == 'hostnames':
        list_hostnames(**{k: v for k, v in vars(args).items()
            if k != 'command'})
    elif args.command == 'google_oauth':
        google_oauth(**{k: v for k, v in vars(args).items()
            if k != 'command'})


if __name__ == '__main__':
    main()
