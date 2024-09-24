import argparse
import atexit
from collections import defaultdict
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
from pprint import pprint
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
GOOGLE_OAUTH_WIN_SCRIPT = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), 'run_google_oauth.pyw')
REF_FILE = f'.{NAME}'
SHARED_USERNAMES = {
    'nt': {'Public'},
    'posix': {'shared'},
}.get(os.name, set())

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


def path_to_filename(path):
    return urllib.parse.quote(path, safe='')


def get_file_mtime(file):
    if os.path.exists(file):
        return datetime.fromtimestamp(os.stat(file).st_mtime, tz=timezone.utc)


def to_json(data):
    return json.dumps(data, indent=4, sort_keys=True)


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


class UnhandledPath(Exception):
    pass


class HashManager:
    cache_file = os.path.join(WORK_PATH, 'cache.dat')
    cache = {}

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
            cls.instance.load()
        return cls.instance

    def hash_file(self, file, chunk_size=8192):
        md5_hash = hashlib.md5()
        with open(file, 'rb') as fd:
            while chunk := fd.read(chunk_size):
                md5_hash.update(chunk)
        return md5_hash.hexdigest()

    def set(self, path, value):
        self.cache[path] = [value, int(time.time())]

    def get(self, path, use_cache=False):
        if not os.path.exists(path):
            return None
        if use_cache:
            try:
                return self.cache[path][0]
            except KeyError:
                pass
        res = self.hash_file(path)
        if use_cache:
            self.set(path, res)
        return res

    def load(self):
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'rb') as fd:
                self.cache = json.loads(gzip.decompress(fd.read()))
            logger.debug(f'loaded {len(self.cache)} cached items')

    def save(self):
        start_ts = time.time()
        limit_ts = time.time() - HASH_CACHE_TTL
        self.cache = {k: v for k, v in self.cache.items()
            if os.path.exists(k) and v[1] > limit_ts}
        with open(self.cache_file, 'wb') as fd:
            fd.write(gzip.compress(json.dumps(self.cache).encode('utf-8')))
        logger.debug(f'saved {len(self.cache)} cached items'
            f' in {time.time() - start_ts:.2f} seconds')


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

    def save(self, keys):
        self.meta = {k: v for k, v in self.meta.items() if k in keys}
        with open(self.meta_file, 'w') as fd:
            fd.write(to_json(self.meta))

    def set(self, key, value: dict):
        self.meta[key] = value

    def get(self, key):
        return self.meta.get(key, {})

    def check(self):
        now_ts = time.time()
        for src, meta in self.meta.items():
            if meta['next_ts'] and now_ts > meta['next_ts'] + OLD_DELTA:
                logger.warning(f'{src} has not been saved recently')
                notify(title=f'{NAME} warning',
                    body=f'{meta["source"]} has not been saved recently')


class BaseSaver:
    src_type = None

    def __init__(self, src, inclusions, exclusions, dst_path, min_delta=0,
            retention_delta=RETENTION_DELTA, creds_file=None):
        self.src = src
        self.inclusions = inclusions
        self.exclusions = exclusions
        self.dst_path = dst_path
        self.min_delta = min_delta
        self.retention_delta = retention_delta
        self.creds_file = creds_file
        self.dst = self._get_dst()
        self.hash_man = HashManager()
        self.meta_man = MetaManager()
        self.report = defaultdict(set)
        self.start_ts = None
        self.end_ts = None
        self.success = None
        self.result = {}

    def generate_dst(self):
        return os.path.join(self.dst_path, self.src_type)

    def _get_dst(self):
        dst = self.generate_dst()
        makedirs(dst)
        return dst

    def needs_purge(self, path):
        return os.stat(path).st_mtime < time.time() - self.retention_delta

    def notify_error(self, message, exc):
        notify(title=f'{NAME} error', body=message)

    def _must_save(self):
        meta = self.meta_man.get(self.src)
        return not meta or time.time() > meta['next_ts']

    def _update_meta(self):
        meta = {k: getattr(self, k) for k in ('dst', 'start_ts', 'end_ts',
            'success', 'result')}
        meta['next_ts'] = time.time() + (self.min_delta if self.success
            else RETRY_DELTA)
        meta['duration'] = self.end_ts - self.start_ts
        self.meta_man.set(self.src, meta)

    def check(self):
        raise NotImplementedError()

    def do_run(self):
        raise NotImplementedError()

    def run(self, force=False):
        if not force and not self._must_save():
            return
        self.start_ts = time.time()
        try:
            self.do_run()
            self.success = True
        except Exception as exc:
            self.success = False
            logger.exception(f'failed to save {self.src}')
            self.notify_error(f'failed to save {self.src}: {exc}', exc=exc)
        self.result['size_MB'] = get_path_size(self.dst) / 1024 / 1024
        self.end_ts = time.time()
        self._update_meta()


class ReferenceData:

    def __init__(self, dst):
        self.dst = dst
        self.file = os.path.join(self.dst, REF_FILE)

    def _normalize_ref_data(self, ref_data):
        ref_data['files'] = sorted(map(list, ref_data['files']))

    def _normalize_json(self, ref_data):
        return json.dumps(ref_data, sort_keys=True)

    def save(self, ref_data):
        self._normalize_ref_data(ref_data)
        try:
            if ref_data == self.load():
                return
        except Exception:
            pass
        data = gzip.compress(self._normalize_json(ref_data).encode('utf-8'))
        with open(self.file, 'wb') as fd:
            fd.write(data)
        logger.debug(f'updated ref file {self.file}')

    def load(self):
        if not os.path.exists(self.file):
            raise Exception(f'missing ref file {self.file}')
        try:
            with open(self.file, 'rb') as fd:
                data = gzip.decompress(fd.read())
            return json.loads(data.decode('utf-8'))
        except Exception as exc:
            raise Exception(f'failed to load ref data from {self.file}: {exc}')


class LocalSaver(BaseSaver):
    src_type = 'local'

    def generate_dst(self):
        return os.path.join(self.dst_path, HOSTNAME,
            path_to_filename(self.src))

    def _get_src_and_files(self):
        src = self.src
        if os.path.isfile(src):
            files = [src]
            src = os.path.dirname(src)
        else:
            files = list(walk_files(src))
        files = [f for f in files
            if not is_path_excluded(f, self.inclusions, self.exclusions)]
        return src, files

    def check(self):
        src, src_files = self._get_src_and_files()
        ref_data = ReferenceData(self.dst).load()
        hm = HashManager()
        src_hashes = {os.path.relpath(f, src): hm.get(f)
            for f in src_files}
        dst_hashes = {p: hm.get(os.path.join(self.dst, p))
            for p, h in ref_data['files']}
        report = defaultdict(lambda: defaultdict(set))
        if src_hashes == dst_hashes:
            report['ok'][src] = set(src_files)
        else:
            for path in set(list(src_hashes.keys()) + list(dst_hashes.keys())):
                src_h = src_hashes.get(path)
                dst_h = dst_hashes.get(path)
                if not src_h:
                    report['missing_at_src'][src].add(os.path.join(src, path))
                elif not dst_h:
                    report['missing_at_dst'][src].add(os.path.join(self.dst, path))
                elif src_h != dst_h:
                    report['hash_mismatched'][src].add(os.path.join(src, path))
        return report

    def _clean_dst(self, src):
        for dst_path in walk_paths(self.dst):
            if os.path.basename(dst_path) == REF_FILE:
                continue
            src_path = os.path.join(src, os.path.relpath(dst_path, self.dst))
            if (not os.path.exists(src_path) and self.needs_purge(dst_path)) \
                    or is_path_excluded(src_path, self.inclusions,
                        self.exclusions):
                remove_path(dst_path)
                self.report['removed'].add(dst_path)
                logger.debug(f'removed {dst_path}')

    def do_run(self):
        src, src_files = self._get_src_and_files()
        self.report['files'] = set(src_files)
        self._clean_dst(src)
        ref_data = {'src': src, 'files': []}
        for src_file in src_files:
            file_rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, file_rel_path)
            src_hash = self.hash_man.get(src_file)
            ref_data['files'].append([file_rel_path, src_hash])
            dst_hash = self.hash_man.get(dst_file, use_cache=True)
            if dst_hash == src_hash:
                continue
            try:
                makedirs(os.path.dirname(dst_file))
                shutil.copyfile(src_file, dst_file)
                self.hash_man.set(dst_file, src_hash)
                self.report['saved'].add(dst_file)
                logger.debug(f'saved {src_file}')
            except Exception:
                logger.exception(f'failed to save {src_file}')
        ReferenceData(self.dst).save(ref_data)
        self.result['file_count'] = len(src_files)


class GoogleDriveSaver(BaseSaver):
    src_type = 'google_drive'

    def notify_error(self, message, exc):
        if isinstance(exc, (AuthError, RefreshError)):
            notify(title=f'{NAME} google auth error', body=message,
                on_click=GOOGLE_OAUTH_WIN_SCRIPT)
        else:
            super().notify(message, exc)

    def do_run(self):
        gc = GoogleCloud(oauth_creds_file=self.creds_file)
        paths = set()
        for file_data in gc.iterate_files():
            dst_file = os.path.join(self.dst, file_data['filename'])
            paths.add(dst_file)
            mtime = get_file_mtime(dst_file)
            if mtime and mtime > file_data['modified_time']:
                self.report['skipped'].add(dst_file)
                continue
            try:
                content = gc.fetch_file_content(file_id=file_data['id'],
                    mime_type=file_data['mime_type'])
                self.report['saved'].add(dst_file)
            except Exception as exc:
                self.report['failed'].add(dst_file)
                logger.error('failed to save google drive file '
                    f'{file_data["name"]}: {exc}')
                continue
            with open(dst_file, 'wb') as fd:
                fd.write(content)

        for dst_path in walk_paths(self.dst):
            if dst_path not in paths and self.needs_purge(dst_path):
                remove_path(dst_path)
        self.result['file_count'] = len(paths)


class GoogleContactsSaver(BaseSaver):
    src_type = 'google_contacts'

    def notify_error(self, message, exc):
        if isinstance(exc, (AuthError, RefreshError)):
            notify(title=f'{NAME} google auth error', body=message,
                on_click=GOOGLE_OAUTH_WIN_SCRIPT)
        else:
            super().notify(message, exc)

    def do_run(self):
        gc = GoogleCloud(oauth_creds_file=self.creds_file)
        contacts = gc.list_contacts()
        data = to_json(contacts)
        file = os.path.join(self.dst, f'{self.src_type}.json')
        if text_file_exists(file, data):
            self.report['skipped'].add(file)
        else:
            with open(file, 'w', encoding='utf-8') as fd:
                fd.write(data)
            self.report['saved'].add(file)
            logger.info(f'saved {len(contacts)} google contacts')
        self.result['file_count'] = 1


class GoogleBookmarksSaver(BaseSaver):
    src_type = 'google_bookmarks'

    def _create_bookmark_file(self, title, url, file):
        data = f'<html><body><a href="{url}">{title}</a></body></html>'
        if text_file_exists(file, data, log_content_changed=True):
            self.report['skipped'].add(file)
        else:
            with open(file, 'w', encoding='utf-8') as fd:
                fd.write(data)
            self.report['saved'].add(file)

    def do_run(self):
        bookmarks = google_chrome.get_bookmarks()
        paths = set()
        for bookmark in bookmarks:
            dst_path = os.path.join(self.dst, *(bookmark['path'].split('/')))
            makedirs(dst_path)
            name = bookmark['name'] or bookmark['url']
            dst_file = f'{os.path.join(dst_path, path_to_filename(name))}.html'
            self._create_bookmark_file(title=name, url=bookmark['url'],
                file=dst_file)
            paths.add(dst_path)
            paths.add(dst_file)

        for dst_path in walk_paths(self.dst):
            if dst_path not in paths and self.needs_purge(dst_path):
                remove_path(dst_path)
        self.result['file_count'] = len(bookmarks)


class SaveItem:
    def __init__(self, src_paths=None, src_type=None, dst_path=DST_PATH,
            min_delta=0, retention_delta=RETENTION_DELTA, creds_file=None,
            restorable=True):
        self.src_paths = self._get_src_paths(src_paths)
        self.src_type = src_type or LocalSaver.src_type
        self.dst_path = self._get_dst_path(dst_path)
        self.min_delta = min_delta
        self.retention_delta = retention_delta
        self.creds_file = creds_file
        self.restorable = restorable
        self.saver_cls = self._get_saver_class()

    def _get_src_paths(self, src_paths):
        return [s if isinstance(s, (list, tuple))
            else (s, [], []) for s in (src_paths or [])]

    def _get_dst_path(self, dst_path):
        if os.sep not in dst_path:
            raise UnhandledPath(f'unhandled dst_path {dst_path}: not {os.name}')
        if not os.path.exists(dst_path):
            raise InvalidPath(f'invalid dst_path {dst_path}: does not exist')
        if dst_path != os.path.expanduser(dst_path):
            raise InvalidPath(f'invalid dst_path {dst_path}: must be absolute')
        return os.path.join(dst_path, NAME, self.src_type)

    def _get_saver_class(self):
        module = sys.modules[__name__]
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__:
                if issubclass(obj, BaseSaver) and obj is not BaseSaver:
                    if obj.src_type == self.src_type:
                        return obj
        raise Exception(f'invalid src_type {self.src_type}')

    def _iterate_src_and_filters(self):
        if self.src_type == LocalSaver.src_type:
            for src_path, inclusions, exclusions in self.src_paths:
                for src in glob(os.path.expanduser(src_path)):
                    if is_path_excluded(src, [], exclusions, file_only=False):
                        logger.debug(f'excluded src_path {src}')
                        continue
                    yield src, inclusions, exclusions
        else:
            yield self.src_type, None, None

    def iterate_savers(self):
        makedirs(self.dst_path)
        for src_and_filters in self._iterate_src_and_filters():
            yield self.saver_cls(*src_and_filters,
                dst_path=self.dst_path,
                min_delta=self.min_delta,
                retention_delta=self.retention_delta,
                creds_file=self.creds_file,
            )


class SaveHandler:
    def __init__(self, force=False):
        self.force = force

    def _iterate_savers(self):
        for save in SAVES:
            try:
                save_item = SaveItem(**save)
            except UnhandledPath as exc:
                logger.debug(exc)
                continue
            except InvalidPath as exc:
                logger.warning(exc)
                continue
            for saver in save_item.iterate_savers():
                yield saver

    def check(self):
        report = defaultdict(lambda: defaultdict(set))
        savers = list(self._iterate_savers())
        for saver in savers:
            try:
                for k, v in saver.check().items():
                    for k2, v2 in v.items():
                        report[k][k2].update(v2)
            except NotImplementedError:
                continue
        return report

    def _generate_summary(self, report):
        summary = defaultdict(int)
        keys = {'saved', 'removed'}
        for src, data in report.items():
            path_summary = {k: len(v) for k, v in data.items()
                if k in keys and v}
            if path_summary:
                summary[src] = path_summary
        if summary:
            logger.info(f'summary:\n{to_json(summary)}')

    def run(self):
        start_ts = time.time()
        savers = list(self._iterate_savers())
        report = defaultdict(lambda: defaultdict(set))
        for saver in savers:
            try:
                saver.run(force=self.force)
            except Exception as exc:
                logger.exception(f'failed to save {saver.src}')
                notify(title=f'{NAME} exception',
                    body=f'failed to save {saver.src}: {exc}')
            report[saver.src] = saver.report
        HashManager().save()
        MetaManager().save(keys={s.src for s in savers})
        MetaManager().check()
        self._generate_summary(report)
        logger.info(f'completed in {time.time() - start_ts:.02f} seconds')


class LocalRestorer:
    def __init__(self, dst_path, from_hostname=None, from_username=None,
            overwrite=False, dry_run=False):
        self.dst_path = dst_path
        self.from_hostname = from_hostname or HOSTNAME
        self.from_username = from_username or os.getlogin()
        self.overwrite = overwrite
        self.dry_run = dry_run
        self.hostnames = sorted(os.listdir(self.dst_path))
        self.hash_man = HashManager()
        self.report = defaultdict(lambda: defaultdict(set))

    def _get_src_file_for_user(self, path):

        def with_end_sep(x):
            return f'{x.rstrip(os.sep)}{os.sep}'

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

    def _iterate_dst_and_ref_data(self):
        for hostname in self.hostnames:
            if hostname != self.from_hostname:
                continue
            for dst_dir in os.listdir(os.path.join(self.dst_path, hostname)):
                dst = os.path.join(self.dst_path, hostname, dst_dir)
                try:
                    ref_data = ReferenceData(dst).load()
                except Exception as exc:
                    logger.error(str(exc))
                    continue
                yield dst, ref_data

    def check(self):
        report = defaultdict(lambda: defaultdict(set))
        for dst, ref_data in self._iterate_dst_and_ref_data():
            src = ref_data['src']
            for rel_path, ref_hash in ref_data['files']:
                dst_file = os.path.join(dst, rel_path)
                dst_hash = self.hash_man.get(dst_file)
                if dst_hash != ref_hash:
                    report['invalid_dst_files'][dst].add(dst_file)
                src_file = self._get_src_file_for_user(
                    os.path.join(src, rel_path))
                if os.path.exists(src_file):
                    if self.hash_man.get(src_file) == dst_hash == ref_hash:
                        report['ok'][src].add(src_file)
                    else:
                        report['hash_mismatched'][src].add(src_file)
                else:
                    report['missing_at_src'][src].add(src_file)
        return report

    def _requires_restore(self, dst_file, src_file, src):
        if not os.path.exists(src_file):
            return True
        if self.hash_man.get(src_file) == self.hash_man.get(dst_file):
            self.report[src]['skipped_identical'].add(src_file)
            return False
        if not self.overwrite:
            self.report[src]['skipped_hash_mismatched'].add(src_file)
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

    def run(self):
        for dst, ref_data in self._iterate_dst_and_ref_data():
            src = ref_data['src']
            rel_paths = set()
            invalid_files = set()
            for rel_path, file_hash in ref_data['files']:
                dst_file = os.path.join(dst, rel_path)
                if self.hash_man.get(dst_file) != file_hash:
                    invalid_files.add(dst_file)
                else:
                    rel_paths.add(rel_path)

            if invalid_files:
                self.report[src]['invalid_files'].update(invalid_files)
                continue
            if not rel_paths:
                self.report[src]['empty_dst'].add(dst)
                continue
            for rel_path in rel_paths:
                src_file = self._get_src_file_for_user(
                    os.path.join(src, rel_path))
                if src_file:
                    self._restore_file(os.path.join(dst, rel_path),
                        src_file, src)


class RestoreHandler:
    def __init__(self, **restorer_args):
        self.restorer_args = restorer_args
        self.report = defaultdict(lambda: defaultdict(set))

    def _iterate_restorers(self):
        dst_paths = set()
        for save in SAVES:
            try:
                si = SaveItem(**save)
                if si.saver_cls == LocalSaver and si.restorable:
                    dst_paths.add(si.dst_path)
            except UnhandledPath as exc:
                logger.warning(str(exc))
                continue
            except InvalidPath as exc:
                logger.warning(str(exc))
                continue
        if not dst_paths:
            logger.info('nothing to restore')
        for dst_path in dst_paths:
            yield LocalRestorer(dst_path=dst_path, **self.restorer_args)

    def list_hostnames(self):
        hostnames = set()
        for restorer in self._iterate_restorers():
            hostnames.update(restorer.hostnames)
        return hostnames

    def _generate_report(self):
        report = {}
        for path, data in self.report.items():
            report[path] = {k: sorted(v) for k, v in data.items() if v}
        logger.info(f'report:\n{to_json(report)}')

    def check(self):
        report = defaultdict(lambda: defaultdict(set))
        for restorer in self._iterate_restorers():
            for k, v in restorer.check().items():
                for k2, v2 in v.items():
                    report[k][k2].update(v2)
        return report

    def run(self):
        for restorer in self._iterate_restorers():
            try:
                restorer.run()
            except Exception:
                logger.exception(f'failed to restore {restorer.dst_path}')
            for path, data in restorer.report.items():
                for k, v in data.items():
                    self.report[path][k].update(v)
        self._generate_report()


class CheckHandler:

    def _clean_report(self, report):
        res = defaultdict(dict)
        for k, v in report.items():
            for k2, v2 in v.items():
                res[k][k2] = sorted(v2)
        return res

    def _get_summary(self, report):
        res = defaultdict(dict)
        for k, v in report.items():
            for k2, v2 in v.items():
                res[k][k2] = len(v2)
        return res

    def run(self):
        save_report = self._clean_report(SaveHandler().check())
        restore_report = self._clean_report(RestoreHandler().check())
        logger.info(f'save report:\n{to_json(save_report)}')
        logger.info(f'restore report:\n{to_json(restore_report)}')
        logger.info(f'save summary:\n{to_json(self._get_summary(save_report))}')
        logger.info(f'restore summary:\n{to_json(self._get_summary(restore_report))}')


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
    return psutil.cpu_percent(interval=3) < IDLE_CPU_THRESHOLD


def must_run(last_run_ts):
    now_ts = time.time()
    if now_ts > last_run_ts + FORCE_RUN_DELTA:
        return True
    if now_ts > last_run_ts + RUN_DELTA and is_idle():
        return True
    return False


def savegame(**kwargs):
    return SaveHandler(**kwargs).run()


def checkgame(**kwargs):
    return CheckHandler(**kwargs).run()


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
    subparsers.add_parser('check')
    restore_parser = subparsers.add_parser('restore')
    restore_parser.add_argument('--from-hostname')
    restore_parser.add_argument('--from-username')
    restore_parser.add_argument('--overwrite', action='store_true')
    restore_parser.add_argument('--dry-run', action='store_true')
    subparsers.add_parser('hostnames')
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
    else:
        callable_ = {
            'check': checkgame,
            'restore': restoregame,
            'hostnames': list_hostnames,
            'google_oauth': google_oauth,
        }[args.command]
        callable_(**{k: v for k, v in vars(args).items()
            if k != 'command'})


if __name__ == '__main__':
    main()
