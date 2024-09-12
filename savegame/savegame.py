import argparse
from copy import deepcopy
from fnmatch import fnmatch
from glob import glob
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import pathlib
import re
import shutil
import socket
import subprocess
import time
import zlib

import psutil

from google_cloud import GoogleCloud, AuthError, RefreshError


SAVES = []
HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()
TARGET_PREFIX = f'{HOSTNAME}-{USERNAME}'
FILE = os.path.realpath(__file__)
NAME = os.path.splitext(os.path.basename(FILE))[0]
WORK_PATH = os.path.join(os.path.expanduser('~'), f'.{NAME}')
MAX_LOG_FILE_SIZE = 100 * 1024
IS_NT = os.name == 'nt'
RE_SPECIAL = re.compile(r'\W+')
RETRY_DELTA = 2 * 3600
OLD_DELTA = 2 * 24 * 3600
HASH_CACHE_TTL = 24 * 3600
SVC_IDLE_CPU_THRESHOLD = 1
SVC_LOOP_DELAY = 10
SVC_RUN_DELTA = 30 * 60
SVC_FORCE_DELTA = 90 * 60
GOOGLE_AUTH_WIN_FILE = os.path.join(os.path.dirname(FILE),
    'google_cloud_auth.pyw')


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


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logger.addHandler(get_file_logging_handler(WORK_PATH))

try:
    from user_settings import *
except ImportError:
    raise Exception('missing user_settings.py')

is_windows_path = lambda x: not x.startswith('/')
is_supported_path = lambda x: IS_NT == is_windows_path(x)
get_target_name = lambda x: RE_SPECIAL.sub('_', str(x)).strip('_')


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
        if IS_NT:
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
            purge_removed_days: int = 7,
            gc_service_creds_file: str = None,
            gc_oauth_creds_file: str = None,
            ):
        self.dst_path = os.path.expanduser(dst_path)
        self.src_paths = src_paths or []
        self.src_type = src_type
        self.min_delta = min_delta
        self.min_size_ratio = min_size_ratio
        self.max_target_versions = max_target_versions
        self.purge_removed_days = purge_removed_days
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


    def _get_dst(self, src):
        target_name = get_target_name(src)
        src_size = _get_path_size(src)
        for index in range(1, self.max_target_versions + 1):
            suffix = '' if index == 1 else f'-{index}'
            dst = os.path.join(self.dst_path,
                f'{TARGET_PREFIX}-{target_name}{suffix}')
            size = _get_path_size(dst)
            if not size or src_size / size > self.min_size_ratio:
                break
        return dst


    def _check_meta(self, meta):
        if not meta:
            return True
        if time.time() > max(meta['updated_ts'] + self.min_delta,
                meta['next_ts']):
            return True
        return False


    def _update_meta(self, src, dst, started_ts, updated_ts=None,
            retry_delta=0):
        now_ts = time.time()
        self.meta_manager.set(dst, {
            'source': str(src),
            'started_ts': started_ts,
            'updated_ts': now_ts if updated_ts is None else updated_ts,
            'next_ts': now_ts + retry_delta,
            'min_delta': self.min_delta,
        })


    def _iterate_src_files(self, src):
        for root, dirs, files in os.walk(src):
            for file in files:
                yield os.path.join(root, file)


    def _iterate_dst_paths(self, dst):
        for root, dirs, files in os.walk(dst, topdown=False):
            for item in files + dirs:
                yield os.path.join(root, item)


    def _save_local(self, src, dst, inclusions, exclusions):

        def is_excluded(path):
            if os.path.isdir(path):
                return False
            if inclusions and not _match_any_pattern(path, inclusions):
                return True
            if exclusions and _match_any_pattern(path, exclusions):
                return True
            return False

        now_ts = time.time()
        purge_delta = self.purge_removed_days * 24 * 3600
        needs_purge = lambda x: now_ts - os.stat(x).st_mtime > purge_delta
        removed_count = 0
        synced_count = 0
        _makedirs(dst)

        if src.is_file():
            src_files = [str(src)]
            src = src.parent
        else:
            src_files = list(self._iterate_src_files(src))

        for dst_path in self._iterate_dst_paths(dst):
            src_path = os.path.join(src, os.path.relpath(dst_path, dst))
            if (not os.path.exists(src_path) and needs_purge(dst_path)) \
                    or is_excluded(src_path):
                _remove_path(dst_path)
                removed_count += 1
                logger.debug(f'removed {dst_path}')

        for src_file in src_files:
            if is_excluded(src_file):
                logger.debug(f'excluded {src_file}')
                continue
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

        logger.debug(f'synced {src} in {time.time() - now_ts:.02f} seconds')
        if removed_count:
            logger.info(f'removed {removed_count} files from {dst}')
        if synced_count:
            logger.info(f'synced {synced_count} files from {src}')


    def _save_google_drive(self, src, dst):
        files = GoogleCloud(service_creds_file=self.gc_service_creds_file,
            oauth_creds_file=self.gc_oauth_creds_file).import_files(dst)
        if files:
            logger.info(f'saved {len(files)} files from google drive')


    def _save_google_contacts(self, src, dst):
        file = GoogleCloud(service_creds_file=self.gc_service_creds_file,
            oauth_creds_file=self.gc_oauth_creds_file).import_contacts(dst)
        if file:
            logger.info('saved google contacts')


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


    def save(self):
        if not is_supported_path(self.dst_path):
            logger.debug(f'destination {self.dst_path} is not supported')
            return
        if not os.path.exists(self.dst_path):
            raise Exception(f'destination {self.dst_path} does not exist')
        callable_ = getattr(self, f'_save_{self.src_type}')
        for args in self._iterate_save_args():
            meta = self.meta_manager.get(args['dst'])
            if not self._check_meta(meta):
                continue
            started_ts = time.time()
            updated_ts = None
            retry_delta = 0
            try:
                callable_(**args)
            except Exception as exc:
                updated_ts = meta.get('updated_ts', 0)
                retry_delta = RETRY_DELTA
                logger.exception(f'failed to save {args["src"]}')
                self._notify_error(f'failed to save {args["src"]}: {exc}',
                    exc=exc)
            self._update_meta(args['src'], args['dst'],
                started_ts=started_ts, updated_ts=updated_ts,
                retry_delta=retry_delta)


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
    return psutil.cpu_percent(interval=3) < SVC_IDLE_CPU_THRESHOLD


def _must_run(last_run_ts):
    now_ts = time.time()
    if now_ts > last_run_ts + SVC_FORCE_DELTA:
        return True
    if now_ts > last_run_ts + SVC_RUN_DELTA and _is_idle():
        return True
    return False


class Daemon(object):

    last_run_ts = 0


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
                time.sleep(SVC_LOOP_DELAY)


class RunIfRequired(object):

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


    def run(self):
        if _must_run(self._get_last_run_ts()):
            savegame()
            self._set_last_run_ts()


def main():
    _makedirs(WORK_PATH)
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--daemon', action='store_true')
    parser.add_argument('-r', '--if-required', action='store_true')
    args = parser.parse_args()
    if args.daemon:
        Daemon().run()
    elif args.if_required:
        RunIfRequired().run()
    else:
        savegame()


if __name__ == '__main__':
    main()
