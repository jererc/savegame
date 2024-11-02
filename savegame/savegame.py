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

from chromium import BookmarksHandler
from google_cloud import GoogleCloud, AuthError, RefreshError


SAVES = []
MAX_LOG_FILE_SIZE = 1000 * 1024
RUN_DELTA = 30 * 60
FORCE_RUN_DELTA = 90 * 60
DAEMON_LOOP_DELAY = 10
RETRY_DELTA = 2 * 3600
RETENTION_DELTA = 7 * 24 * 3600
MONITOR_DELTA = 8 * 3600
STALE_DELTA = 3 * 24 * 3600
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


def makedirs(x):
    if not os.path.exists(x):
        os.makedirs(x)


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
    file_handler = RotatingFileHandler(
        os.path.join(path, f'{NAME}.log'),
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


def validate_path(x):
    if os.sep not in x:
        raise UnhandledPath(f'unhandled path {x}: not {os.name}')


def path_to_filename(x):
    return urllib.parse.quote(x, safe='')


def get_file_mtime(x):
    return os.stat(x).st_mtime


def get_file_mtime_dt(x):
    if os.path.exists(x):
        return datetime.fromtimestamp(get_file_mtime(x), tz=timezone.utc)


def to_json(x):
    return json.dumps(x, indent=4, sort_keys=True)


def to_float(x):
    return float(f'{x:.02f}')


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


def text_file_exists(file, data, encoding='utf-8', log_content_changed=False):
    if not os.path.exists(file):
        return False
    with open(file, encoding=encoding) as fd:
        res = fd.read() == data
    if not res and log_content_changed:
        logger.warning(f'content has changed in {file}')
    return res


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


def switch_os_sep(x):
    return x.replace('\\' if os.sep == '/' else '/', os.sep)


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


class RunFile:
    def __init__(self, file):
        self.file = file

    def get_ts(self, default=0):
        if not os.path.exists(self.file):
            return default
        return get_file_mtime(self.file)

    def touch(self):
        with open(self.file, 'w'):
            pass


class Reference:
    def __init__(self, dst):
        self.dst = dst
        self.file = os.path.join(dst, REF_FILENAME)
        self.data = None
        self.src = None
        self.files = None
        self._load()

    def _load(self, data=None):
        if data:
            self.data = data
        elif not os.path.exists(self.file):
            self.data = {}
        else:
            try:
                with open(self.file, 'rb') as fd:
                    self.data = json.loads(
                        gzip.decompress(fd.read()).decode('utf-8'))
            except Exception as exc:
                logger.exception(f'failed to load {self.file}: {exc}')
                os.remove(self.file)
                self.data = {}
        self.src = self.data.get('src')
        self.files = deepcopy(self.data.get('files', {}))

    def save(self):
        data = {
            'src': self.src,
            'files': self.files,
            'ts': time.time(),
        }
        with open(self.file, 'wb') as fd:
            fd.write(gzip.compress(
                json.dumps(data, sort_keys=True).encode('utf-8')))
        self._load(data)

    @property
    def ts(self):
        return self.data.get('ts', 0)


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


class Notifier:
    def _send_nt(self, title, body, on_click=None):
        from win11toast import notify
        notify(title=title, body=body, on_click=on_click)

    def _send_posix(self, title, body, on_click=None):
        env = os.environ.copy()
        env['DISPLAY'] = ':0'
        env['DBUS_SESSION_BUS_ADDRESS'] = \
            f'unix:path=/run/user/{os.getuid()}/bus'
        subprocess.check_call(['notify-send', title, body], env=env)

    def send(self, *args, **kwargs):
        try:
            {
                'nt': self._send_nt,
                'posix': self._send_posix,
            }[os.name](*args, **kwargs)
        except Exception:
            logger.exception('failed to send notification')


class BaseSaver:
    id = None
    hostname = None
    src_type = 'local'
    dst_type = 'local'

    def __init__(self, src, inclusions, exclusions, dst_path, run_delta=3600,
                 retention_delta=RETENTION_DELTA):
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
        self.stats = {}

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

    def check_data(self):
        raise NotImplementedError()

    def do_run(self):
        raise NotImplementedError()

    def _requires_purge(self, path):
        if os.path.isfile(path):
            name = os.path.basename(path)
            if name == REF_FILENAME:
                return False
            if path in self.dst_paths:
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
        self.stats['duration'] = to_float(self.end_ts - self.start_ts)


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

    def check_data(self):
        src, src_files = self._get_src_and_files()
        if not src_files:
            return
        src_hashes = {os.path.relpath(f, src): get_file_hash(f)
            for f in src_files}
        dst_hashes = {p: get_file_hash(os.path.join(self.dst, p))
            for p in self.ref.files.keys()}
        if src_hashes == dst_hashes:
            self.report.add('ok', src, src_files)
        else:
            for path in set(list(src_hashes.keys()) + list(dst_hashes.keys())):
                src_hash = src_hashes.get(path)
                dst_hash = dst_hashes.get(path)
                src_file = os.path.join(src, path)
                dst_file = os.path.join(self.dst, path)
                if not src_hash:
                    self.report.add('missing_at_src', src, src_file)
                elif not dst_hash:
                    self.report.add('missing_at_dst', src, dst_file)
                elif src_hash != dst_hash:
                    if get_file_mtime(src_file) > get_file_mtime(dst_file):
                        self.report.add('conflict_src_more_recent',
                            src, src_file)
                    else:
                        self.report.add('conflict_dst_more_recent',
                            src, src_file)
                else:
                    self.report.add('ok', src, src_file)

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
                    shutil.copyfile(src_file, dst_file)
                    self.report.add('saved', self.src, src_file)
                self.ref.files[rel_path] = src_hash
            except Exception:
                self.report.add('failed', self.src, src_file)
                logger.exception(f'failed to save {src_file}')


class GoogleCloudSaver(BaseSaver):
    def notify_error(self, message, exc=None):
        if isinstance(exc, (AuthError, RefreshError)):
            Notifier().send(title=f'{NAME} google auth error', body=message,
                on_click=GOOGLE_OAUTH_WIN_SCRIPT)
        else:
            super().notify_error(message, exc)


class GoogleDriveExportSaver(GoogleCloudSaver):
    id = 'google_drive_export'
    hostname = 'google_cloud'
    src_type = 'remote'

    def do_run(self):
        gc = get_google_cloud()
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


class GoogleContactsExportSaver(GoogleCloudSaver):
    id = 'google_contacts_export'
    hostname = 'google_cloud'
    src_type = 'remote'

    def do_run(self):
        gc = get_google_cloud()
        contacts = gc.list_contacts()
        data = to_json(contacts)
        file = os.path.join(self.dst, 'contacts.json')
        self.dst_paths = {file}
        if text_file_exists(file, data):
            self.report.add('skipped', self.src, file)
        else:
            makedirs(os.path.dirname(file))
            with open(file, 'w', encoding='utf-8') as fd:
                fd.write(data)
            self.report.add('saved', self.src, file)
            logger.info(f'saved {len(contacts)} google contacts')
        self.ref.files = {os.path.relpath(file, self.dst): get_file_hash(file)}


class ChromiumBookmarksExportSaver(BaseSaver):
    id = 'chromium_bookmarks_export'
    hostname = HOSTNAME

    def do_run(self):
        for file_meta in BookmarksHandler().export():
            dst_file = os.path.join(self.dst, file_meta['path'])
            self.dst_paths.add(dst_file)
            if text_file_exists(dst_file, file_meta['content'],
                    log_content_changed=True):
                self.report.add('skipped', self.src, dst_file)
            else:
                makedirs(os.path.dirname(dst_file))
                with open(dst_file, 'w', encoding='utf-8') as fd:
                    fd.write(file_meta['content'])
                self.report.add('saved', self.src, dst_file)
        self.ref.files = {os.path.relpath(p, self.dst): get_file_hash(p)
            for p in self.dst_paths}


class SaveItem:
    def __init__(self, src_paths=None, saver_id=LocalSaver.id,
                 dst_path=DST_PATH, run_delta=0,
                 retention_delta=RETENTION_DELTA, loadable=True, os_name=None):
        self.src_paths = self._get_src_paths(src_paths)
        self.saver_id = saver_id
        self.saver_cls = self._get_saver_class()
        self.dst_path = self._get_dst_path(dst_path)
        self.run_delta = run_delta
        self.retention_delta = retention_delta
        self.loadable = loadable
        self.os_name = os_name

    def _get_src_paths(self, src_paths):
        return [s if isinstance(s, (list, tuple))
            else (s, [], []) for s in (src_paths or [])]

    def _get_dst_path(self, dst_path):
        if self.saver_cls.dst_type == 'local':
            validate_path(dst_path)
            dst_path = os.path.expanduser(dst_path)
            if not os.path.exists(dst_path):
                raise InvalidPath(f'invalid dst_path {dst_path}: '
                    'does not exist')
            return os.path.join(dst_path, NAME, self.saver_id)
        return dst_path

    def _get_saver_class(self):
        module = sys.modules[__name__]
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if obj.__module__ == module.__name__ \
                    and issubclass(obj, BaseSaver) \
                    and obj.id == self.saver_id:
                return obj
        raise Exception(f'invalid saver_id {self.saver_id}')

    def _generate_src_and_patterns(self):
        if self.saver_cls.src_type == 'local' and self.src_paths:
            for src_path, inclusions, exclusions in self.src_paths:
                try:
                    validate_path(src_path)
                except UnhandledPath:
                    continue
                for src in glob(os.path.expanduser(src_path)):
                    yield src, inclusions, exclusions
        else:
            yield self.saver_id, None, None

    def generate_savers(self):
        if self.os_name and os.name != self.os_name:
            return
        if self.saver_cls.dst_type == 'local':
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
    def __init__(self, force=False):
        self.force = force

    def _generate_savers(self):
        for si in iterate_save_items():
            yield from si.generate_savers()

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
                Notifier().send(title=f'{NAME} exception',
                    body=f'failed to save {saver.src}: {exc}')
            stats[saver.src] = saver.stats
            report.merge(saver.report)
        Metadata().save(keys={s.src for s in savers})
        print(f'stats:\n{to_json(stats)}')
        report_dict = report.clean(keys={'saved', 'removed'})
        if report_dict:
            logger.info(f'report:\n{to_json(report_dict)}')
        logger.info(f'completed in {time.time() - start_ts:.02f} seconds')


class LocalLoader:
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

    def _iterate_refs(self):
        for hostname in self.hostnames:
            if hostname != self.hostname:
                continue
            for dst in glob(os.path.join(self.dst_path, hostname, '*')):
                ref = Reference(dst)
                if ref.src:
                    yield ref

    def check_data(self):
        for ref in self._iterate_refs():
            try:
                validate_path(ref.src)
            except UnhandledPath:
                self.report.add('unhandled_path', ref.src, ref.src)
                continue
            for rel_path, ref_hash in ref.files.items():
                dst_file = os.path.join(ref.dst, rel_path)
                dst_hash = get_file_hash(dst_file)
                if dst_hash != ref_hash:
                    self.report.add('invalid_dst_files', ref.dst, dst_file)
                src_file_raw = os.path.join(ref.src, rel_path)
                src_file = self._get_src_file_for_user(src_file_raw)
                if not src_file:
                    self.report.add('skipped_other_username', ref.src,
                        src_file_raw)
                    continue
                if os.path.exists(src_file):
                    if get_file_hash(src_file) == dst_hash == ref_hash:
                        self.report.add('ok', ref.src, src_file)
                    else:
                        if get_file_mtime(src_file) > get_file_mtime(dst_file):
                            self.report.add('conflict_src_more_recent',
                                ref.src, src_file)
                        else:
                            self.report.add('conflict_dst_more_recent',
                                ref.src, src_file)
                else:
                    self.report.add('missing_at_src', ref.src, src_file)

    def _requires_load(self, dst_file, src_file, src):
        if not check_patterns(src_file, self.include, self.exclude):
            return False
        if not os.path.exists(src_file):
            return True
        if get_file_hash(src_file) == get_file_hash(dst_file):
            self.report.add('identical', src, src_file)
            return False
        if not self.overwrite:
            if get_file_mtime(src_file) > get_file_mtime(dst_file):
                self.report.add('conflict_src_more_recent',
                    src, src_file)
            else:
                self.report.add('conflict_dst_more_recent',
                    src, src_file)
            return False
        return True

    def _load_file(self, dst_file, src_file, src):
        if not self._requires_load(dst_file, src_file, src):
            return
        if self.dry_run:
            self.report.add('loadable', src, src_file)
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
                self.report.add('loaded_overwritten', src, src_file)
            else:
                self.report.add('loaded', src, src_file)
            makedirs(os.path.dirname(src_file))
            shutil.copyfile(dst_file, src_file)
            logger.info(f'loaded {src_file} from {dst_file}')
        except Exception as exc:
            self.report.add('failed', src, src_file)
            logger.error(f'failed to load {src_file} '
                f'from {dst_file}: {exc}')

    def run(self):
        for ref in self._iterate_refs():
            try:
                validate_path(ref.src)
            except UnhandledPath:
                self.report.add('skipped_unhandled', ref.src, ref.src)
                continue
            rel_paths = set()
            invalid_files = set()
            for rel_path, ref_hash in ref.files.items():
                dst_file = os.path.join(ref.dst, rel_path)
                if get_file_hash(dst_file) != ref_hash:
                    invalid_files.add(dst_file)
                else:
                    rel_paths.add(rel_path)
            if invalid_files:
                self.report.add('invalid_files', ref.src, invalid_files)
                continue
            if not rel_paths:
                self.report.add('empty_dst', ref.src, ref.dst)
                continue
            for rel_path in rel_paths:
                src_file_raw = os.path.join(ref.src, rel_path)
                src_file = self._get_src_file_for_user(src_file_raw)
                if not src_file:
                    self.report.add('skipped_other_username', ref.src,
                        src_file_raw)
                    continue
                self._load_file(os.path.join(ref.dst, rel_path),
                    src_file, ref.src)


class LoadHandler:
    def __init__(self, **loader_args):
        self.loader_args = loader_args

    def _iterate_loaders(self):
        dst_paths = {s.dst_path
            for s in iterate_save_items(log_unhandled=True)
            if s.saver_cls == LocalSaver and s.loadable}
        if not dst_paths:
            logger.info('nothing to load')
        for dst_path in dst_paths:
            yield LocalLoader(dst_path=dst_path, **self.loader_args)

    def list_hostnames(self):
        return {h for lo in self._iterate_loaders()
            for h in lo.hostnames}

    def check_data(self):
        report = Report()
        for loader in self._iterate_loaders():
            try:
                loader.check_data()
            except Exception:
                logger.exception(f'failed to check {loader.dst_path}')
            report.merge(loader.report)
        return report

    def run(self):
        report = Report()
        for loader in self._iterate_loaders():
            try:
                loader.run()
            except Exception:
                logger.exception(f'failed to load {loader.dst_path}')
            report.merge(loader.report)
        logger.info(f'report:\n{to_json(report.clean())}')
        logger.info(f'summary:\n{to_json(report.get_summary())}')


class SaveMonitor:
    def __init__(self, force=False):
        self.force = force
        self.run_file = RunFile(os.path.join(WORK_PATH, 'monitor.run'))

    def _must_run(self):
        return (self.force or time.time() > self.run_file.get_ts()
            + MONITOR_DELTA)

    def _iterate_hostname_refs(self):
        dst_paths = {s.dst_path for s in iterate_save_items()
            if s.saver_cls.dst_type == 'local'}
        for dst_path in dst_paths:
            for hostname in sorted(os.listdir(dst_path)):
                for dst in glob(os.path.join(dst_path, hostname, '*')):
                    ref = Reference(dst)
                    if not os.path.exists(ref.file):
                        logger.error(f'missing ref file {ref.file}')
                        continue
                    yield hostname, ref

    def run(self):
        if not self._must_run():
            return
        min_ts = time.time() - STALE_DELTA
        stale_hostnames = set()
        invalid_files = set()
        for hostname, ref in self._iterate_hostname_refs():
            if ref.ts < min_ts:
                stale_hostnames.add(hostname)
            for rel_path, file_hash in ref.files.items():
                dst_file = os.path.join(ref.dst, switch_os_sep(rel_path))
                if get_file_hash(dst_file) != file_hash:
                    invalid_files.add(dst_file)
                    logger.error(f'invalid file: {dst_file}')
        if stale_hostnames:
            Notifier().send(title=f'{NAME} warning',
                body=f'Stale hostnames: {", ".join(sorted(stale_hostnames))}')
        if invalid_files:
            Notifier().send(title=f'{NAME} warning',
                body=f'Invalid files: {len(invalid_files)}')
        self.run_file.touch()

    def _get_size(self, ref):
        try:
            sizes = [os.path.getsize(os.path.join(ref.dst, switch_os_sep(r)))
                for r in ref.files.keys()]
            return to_float(sum(sizes) / 1024 / 1024)
        except Exception:
            logger.exception(f'failed to get {ref.dst} size')
            return -1

    def generate_report(self, sort_by='last_run', order='desc'):

        def to_human_dt(ts):
            return datetime.fromtimestamp(int(ts)).isoformat(' ')

        headers = {
            'hostname': 'Hostname',
            'src': 'Source path',
            'last_run': 'Last run',
            'size': 'Size (MB)',
            'files': 'Files',
        }
        items = []
        for hostname, ref in self._iterate_hostname_refs():
            items.append({
                'hostname': hostname,
                'src': ref.src,
                'last_run': ref.ts,
                'size': self._get_size(ref),
                'files': len(ref.files),
            })
        rows = [headers] + sorted(items, key=lambda x: x[sort_by],
            reverse=order == 'desc')
        for i, r in enumerate(rows):
            human_dt = to_human_dt(r['last_run']) if i > 0 else r['last_run']
            print(f'{human_dt:19}  {r["hostname"]:20}  {r["size"]:10}  '
                f'{r["files"]:10}  {r["src"]}')


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


def savegame(force=False):
    SaveHandler(force=force).run()
    SaveMonitor(force=False).run()


def status(**kwargs):
    SaveMonitor(force=True).generate_report(**kwargs)


def checkgame(hostname=None):
    save_report = SaveHandler().check_data()
    load_report = LoadHandler(hostname=hostname).check_data()
    print('save report:\n'
        f'{to_json(save_report.clean())}')
    print('load report:\n'
        f'{to_json(load_report.clean())}')
    print('save summary:\n'
        f'{to_json(save_report.get_summary())}')
    print('load summary:\n'
        f'{to_json(load_report.get_summary())}')


def loadgame(**kwargs):
    LoadHandler(**kwargs).run()


def list_hostnames(**kwargs):
    hostnames = "\n".join(sorted(LoadHandler(**kwargs).list_hostnames()))
    print(f'available hostnames:\n{hostnames}')


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
    def __init__(self):
        self.run_file = RunFile(os.path.join(WORK_PATH, 'task.run'))

    @with_lockfile()
    def run(self):
        if must_run(self.run_file.get_ts()):
            savegame()
            self.run_file.touch()


def _parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='cmd')
    save_parser = subparsers.add_parser('save')
    save_parser.add_argument('--daemon', action='store_true')
    save_parser.add_argument('--task', action='store_true')
    status_parser = subparsers.add_parser('status')
    status_parser.add_argument('--sort-by', default='last_run')
    status_parser.add_argument('--order', default='desc')
    check_parser = subparsers.add_parser('check')
    check_parser.add_argument('--hostname')
    load_parser = subparsers.add_parser('load')
    load_parser.add_argument('--hostname')
    load_parser.add_argument('--username')
    load_parser.add_argument('--include', nargs='*')
    load_parser.add_argument('--exclude', nargs='*')
    load_parser.add_argument('--overwrite', action='store_true')
    load_parser.add_argument('--dry-run', action='store_true')
    subparsers.add_parser('hostnames')
    subparsers.add_parser('google_oauth')
    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        sys.exit()
    return args


def main():
    args = _parse_args()
    if args.cmd == 'save':
        if args.daemon:
            Daemon().run()
        elif args.task:
            Task().run()
        else:
            savegame(force=True)
    else:
        {
            'status': status,
            'check': checkgame,
            'load': loadgame,
            'hostnames': list_hostnames,
            'google_oauth': google_oauth,
        }[args.cmd](**{k: v for k, v in vars(args).items() if k != 'cmd'})


if __name__ == '__main__':
    main()
