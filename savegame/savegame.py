import argparse
import atexit
from copy import deepcopy
from datetime import datetime, timezone
from fnmatch import fnmatch
import functools
from glob import glob
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import pathlib
import re
import shutil
import signal
import socket
import subprocess
import time
import zlib

import psutil

import google_chrome
from google_cloud import GoogleCloud, AuthError, RefreshError


SAVES = []
TARGET_PREFIX = f'{socket.gethostname()}'
FILE = os.path.realpath(__file__)
NAME = os.path.splitext(os.path.basename(FILE))[0]
WORK_PATH = os.path.join(os.path.expanduser('~'), f'.{NAME}')
MAX_LOG_FILE_SIZE = 100 * 1024
IS_WIN = os.name == 'nt'
IS_POSIX = os.name == 'posix'
RE_SPECIAL = re.compile(r'\W+')
RETRY_DELTA = 2 * 3600
OLD_DELTA = 2 * 24 * 3600
HASH_CACHE_TTL = 24 * 3600
IDLE_CPU_THRESHOLD = 1
RUN_DELTA = 30 * 60
FORCE_RUN_DELTA = 90 * 60
DAEMON_LOOP_DELAY = 10
GOOGLE_AUTH_WIN_FILE = os.path.join(os.path.dirname(FILE),
    'google_cloud_auth.pyw')

try:
    from user_settings import *
except ImportError:
    pass


def _makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


def get_file_logging_handler(log_path):
    _makedirs(log_path)
    log_file = os.path.join(log_path, f'{NAME}.log')
    file_handler = RotatingFileHandler(log_file, mode='a',
        maxBytes=MAX_LOG_FILE_SIZE, backupCount=0, encoding=None, delay=0)
    log_formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.INFO)
    return file_handler


_makedirs(WORK_PATH)
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logger.addHandler(get_file_logging_handler(WORK_PATH))

is_win_path = lambda x: not x.startswith('/')
is_supported_path = lambda x: IS_WIN == is_win_path(x)
get_filename = lambda x: RE_SPECIAL.sub('_', x).strip('_')
get_file_mtime = lambda x: datetime.fromtimestamp(os.stat(x).st_mtime,
    tz=timezone.utc) if os.path.exists(x) else None


def _get_path_size(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    res = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for filename in files:
            file = os.path.join(root, filename)
            try:
                res += os.path.getsize(file)
            except Exception:
                logger.error(f'failed to get size for {file}')
    return res


def _remove_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)


def _match_any_pattern(path, patterns):
    for pattern in patterns:
        if fnmatch(path, pattern):
            return True
    return False


def _notify(title, body, on_click=None):
    try:
        if IS_WIN:
            from win11toast import notify
            notify(title=title, body=body, on_click=on_click)
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


class FileHashManager(object):

    cache_file = os.path.join(WORK_PATH, 'cache.dat')
    cache = {}


    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
            cls.instance.load()
        return cls.instance


    def _generate_hash(self, path):
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
        res = self._generate_hash(path)
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


class MetaManager(object):

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
            fd.write(json.dumps(self.meta, sort_keys=True, indent=4))
        logger.debug(f'saved {len(self.meta)} meta items'
            f' in {time.time() - started_ts:.2f} seconds')


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
                _notify(title=f'{NAME} warning',
                    body=f'{meta["source"]} has not been saved recently')


class SaveItem(object):

    def __init__(self,
            dst_path: str,
            src_paths: list = None,
            src_type: str = 'local',
            min_delta: int = 0,
            min_size_ratio: float = .5,
            max_target_versions: int = 4,
            retention_delta: int = 7,
            gc_service_creds_file: str = None,
            gc_oauth_creds_file: str = None,
            ):
        self.dst_path = os.path.expanduser(dst_path)
        self.src_paths = src_paths or []
        self.src_type = src_type
        self.min_delta = min_delta
        self.min_size_ratio = min_size_ratio
        self.max_target_versions = max_target_versions
        self.retention_delta = retention_delta
        self.gc_service_creds_file = gc_service_creds_file
        self.gc_oauth_creds_file = gc_oauth_creds_file
        self.src_and_filters = [s if isinstance(s, (list, tuple))
            else (s, [], []) for s in self.src_paths]
        self.file_hash_manager = FileHashManager()
        self.meta_manager = MetaManager()


    def __str__(self):
        if not self.src_and_filters:
            return self.src_type
        return ', '.join([p for p, i, e in self.src_and_filters])


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
            'source': str(src),
            'started_ts': started_ts,
            'updated_ts': now_ts if updated_ts is None else updated_ts,
            'next_ts': now_ts + retry_delta,
            'min_delta': self.min_delta,
            'duration': time.time() - started_ts,
        }
        if extra_meta:
            meta.update(extra_meta)
        self.meta_manager.set(dst, meta)


    def _iterate_src_files(self, src):
        for root, dirs, files in os.walk(src):
            for file in files:
                yield os.path.join(root, file)


    def _iterate_dst_paths(self, dst):
        for root, dirs, files in os.walk(dst, topdown=False):
            for item in files + dirs:
                yield os.path.join(root, item)


    def _needs_purge(self, path):
        return time.time() - os.stat(path).st_mtime > self.retention_delta


    def _save_local(self, src, dst, inclusions, exclusions):

        def is_excluded(path):
            if os.path.isdir(path):
                return False
            if inclusions and not _match_any_pattern(path, inclusions):
                return True
            if exclusions and _match_any_pattern(path, exclusions):
                return True
            return False

        started_ts = time.time()
        file_count = 0
        removed_count = 0
        synced_count = 0
        size = 0
        _makedirs(dst)

        if src.is_file():
            src_files = [str(src)]
            src = src.parent
        else:
            src_files = list(self._iterate_src_files(src))

        for dst_path in self._iterate_dst_paths(dst):
            src_path = os.path.join(src, os.path.relpath(dst_path, dst))
            if (not os.path.exists(src_path) and self._needs_purge(dst_path)) \
                    or is_excluded(src_path):
                _remove_path(dst_path)
                removed_count += 1
                logger.debug(f'removed {dst_path}')

        for src_file in src_files:
            if is_excluded(src_file):
                logger.debug(f'excluded {src_file}')
                continue
            file_count += 1
            size += os.path.getsize(src_file)
            dst_file = os.path.join(dst, os.path.relpath(src_file, src))
            src_hash = self.file_hash_manager.get(src_file, use_cache=False)
            dst_hash = self.file_hash_manager.get(dst_file, use_cache=True)
            if dst_hash == src_hash:
                continue
            try:
                _makedirs(os.path.dirname(dst_file))
                shutil.copyfile(src_file, dst_file)
                self.file_hash_manager.set(dst_file, src_hash)
                synced_count += 1
                logger.debug(f'synced {src_file}')
            except Exception:
                logger.exception(f'failed to sync {src_file}')

        logger.debug(f'synced {src} in {time.time() - started_ts:.02f} seconds')
        if removed_count:
            logger.info(f'removed {removed_count} files from {dst}')
        if synced_count:
            logger.info(f'synced {synced_count} files from {src}')
        return {
            'file_count': file_count,
            'size_MB': size / 1024 / 1024,
        }


    def _save_google_drive(self, src, dst):
        gc = GoogleCloud(service_creds_file=self.gc_service_creds_file,
            oauth_creds_file=self.gc_oauth_creds_file)
        paths = set()
        for file_data in gc.iterate_files():
            dst_file = os.path.join(dst, file_data['filename'])
            paths.add(dst_file)
            mtime = get_file_mtime(dst_file)
            if mtime and mtime > file_data['modified_time']:
                continue
            try:
                content = gc.fetch_file_content(file_id=file_data['id'],
                    mime_type=file_data['mime_type'])
            except Exception as exc:
                logger.error('failed to save google drive file '
                    f'{file_data["name"]}: {exc}')
                continue
            with open(dst_file, 'wb') as fd:
                fd.write(content)

        for dst_path in self._iterate_dst_paths(dst):
            if dst_path not in paths and self._needs_purge(dst_path):
                _remove_path(dst_path)

        return {
            'file_count': len(paths)
        }


    def _save_google_contacts(self, src, dst):
        dst_file = os.path.join(dst, 'google_contacts.json')
        contacts = GoogleCloud(service_creds_file=self.gc_service_creds_file,
            oauth_creds_file=self.gc_oauth_creds_file).list_contacts()
        with open(dst_file, 'w') as fd:
            fd.write(json.dumps(contacts, sort_keys=True, indent=4))
        logger.info(f'saved {len(contacts)} google contacts')
        return {
            'file_count': 1,
        }


    def _save_bookmark_as_html_file(self, title, url, file):
        data = f'<html><body><a href="{url}">{title}</a></body></html>'
        with open(file, 'w', encoding='utf-8') as fd:
            fd.write(data)


    def _save_google_bookmarks(self, src, dst):
        bookmarks = google_chrome.get_bookmarks()
        paths = set()
        for bookmark in bookmarks:
            dst_path = os.path.join(dst, *(bookmark['path'].split('/')))
            _makedirs(dst_path)
            name = bookmark['name'] or bookmark['url']
            dst_file = f'{os.path.join(dst_path, get_filename(name))}.html'
            self._save_bookmark_as_html_file(title=name, url=bookmark['url'],
                file=dst_file)
            paths.add(dst_path)
            paths.add(dst_file)

        for dst_path in self._iterate_dst_paths(dst):
            if dst_path not in paths and self._needs_purge(dst_path):
                _remove_path(dst_path)

        logger.info(f'saved {len(bookmarks)} google bookmarks')
        return {
            'file_count': len(bookmarks),
        }


    def _get_dst(self, src):
        target_name = get_filename(str(src))
        src_size = _get_path_size(src)
        for index in range(1, self.max_target_versions + 1):
            suffix = '' if index == 1 else f'-{index}'
            dst = os.path.join(self.dst_path,
                f'{TARGET_PREFIX}-{target_name}{suffix}')
            size = _get_path_size(dst)
            if not size or src_size / size > self.min_size_ratio:
                break
        return dst


    def _iterate_save_args(self):
        if self.src_type == 'local':
            for path, inclusions, exclusions in self.src_and_filters:
                for src in map(pathlib.Path, glob(os.path.expanduser(path))):
                    yield {
                        'src': src,
                        'dst': self._get_dst(src),
                        'inclusions': inclusions,
                        'exclusions': exclusions,
                    }
        else:
            yield {
                'src': self.src_type,
                'dst': self.dst_path,
            }


    def _notify_error(self, message, exc):
        if isinstance(exc, (AuthError, RefreshError)):
            _notify(title=f'{NAME} google auth error', body=message,
                on_click=GOOGLE_AUTH_WIN_FILE)
        else:
            _notify(title=f'{NAME} error', body=message)


    def _check_dst(self):
        dst_parent = os.path.dirname(self.dst_path)
        if not os.path.exists(dst_parent):
            raise Exception(f'destination parent {dst_parent} does not exist')
        _makedirs(self.dst_path)


    def save(self):
        if not is_supported_path(self.dst_path):
            logger.debug(f'destination {self.dst_path} is not supported')
            return
        self._check_dst()
        callable_ = getattr(self, f'_save_{self.src_type}')
        for args in self._iterate_save_args():
            meta = self.meta_manager.get(args['dst'])
            if not self._check_meta(meta):
                continue
            started_ts = time.time()
            updated_ts = None
            retry_delta = 0
            res = None
            try:
                res = callable_(**args)
            except Exception as exc:
                updated_ts = meta.get('updated_ts', 0)
                retry_delta = RETRY_DELTA
                logger.exception(f'failed to save {args["src"]}')
                self._notify_error(f'failed to save {args["src"]}: {exc}',
                    exc=exc)
            self._update_meta(args['src'], args['dst'],
                started_ts=started_ts, updated_ts=updated_ts,
                retry_delta=retry_delta, extra_meta=res)


def _process_save(save: dict):
    started_ts = time.time()
    save_item = SaveItem(**save)
    try:
        save_item.save()
    except Exception as exc:
        logger.exception(f'failed to save {save_item}')
        _notify(title=f'{NAME} exception',
            body=f'failed to save {save_item}: {exc}')
    logger.debug(f'processed {save_item} in '
        f'{time.time() - started_ts:.02f} seconds')


def savegame():
    started_ts = time.time()
    for save in SAVES:
        _process_save(save)
    FileHashManager().save()
    MetaManager().save()
    MetaManager().check()
    logger.info(f'completed in {time.time() - started_ts:.02f} seconds')


def _is_idle():
    return psutil.cpu_percent(interval=3) < IDLE_CPU_THRESHOLD


def _must_run(last_run_ts):
    now_ts = time.time()
    if now_ts > last_run_ts + FORCE_RUN_DELTA:
        return True
    if now_ts > last_run_ts + RUN_DELTA and _is_idle():
        return True
    return False


def with_lockfile():
    lockfile_path = os.path.join(WORK_PATH, 'lock')

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if IS_POSIX and os.path.exists(lockfile_path):
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

            if IS_POSIX:
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


class Daemon(object):

    last_run_ts = 0


    @with_lockfile()
    def run(self):
        while True:
            try:
                if _must_run(self.last_run_ts):
                    savegame()
                    self.last_run_ts = time.time()
            except Exception:
                logger.exception('wtf')
            finally:
                logger.debug('sleeping')
                time.sleep(DAEMON_LOOP_DELAY)


class Task(object):

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
        if _must_run(self._get_last_run_ts()):
            savegame()
            self._set_last_run_ts()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--daemon', action='store_true')
    parser.add_argument('-t', '--task', action='store_true')
    args = parser.parse_args()
    if args.daemon:
        Daemon().run()
    elif args.task:
        Task().run()
    else:
        savegame()


if __name__ == '__main__':
    main()
