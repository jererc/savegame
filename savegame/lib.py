from collections import defaultdict
from copy import deepcopy
from fnmatch import fnmatch
import hashlib
import json
import logging
import os
import shutil
import socket
import sys
import time

from svcutils.service import list_mountpoint_labels

from savegame import NAME, WORK_DIR

HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()
REF_FILENAME = f'.{NAME}'
METADATA_MAX_AGE = 3600 * 24 * 90
INVALID_PATH_SEP = {'linux': '\\', 'win32': '/'}[sys.platform]

logger = logging.getLogger(__name__)


class UnhandledPath(Exception):
    pass


class InvalidPath(Exception):
    pass


def get_file_mtime(path, default=None):
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return default


def validate_path(path):
    if INVALID_PATH_SEP in path:
        raise UnhandledPath(f'invalid path {path} on {sys.platform=}')


def to_json(path):
    return json.dumps(path, sort_keys=True, indent=4)


def remove_path(path):
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    except FileNotFoundError:
        pass


def get_file_hash(file, chunk_size=8192):
    if not os.path.exists(file):
        return None
    md5_hash = hashlib.md5()
    start_ts = time.time()
    with open(file, 'rb') as fd:
        while chunk := fd.read(chunk_size):
            md5_hash.update(chunk)
    duration = time.time() - start_ts
    if duration > 10:
        logger.warning(f'get_file_hash {file} took {duration:.02f} seconds ({os.path.getsize(file) / 1024 / 1024:.02f} MB)')
    return md5_hash.hexdigest()


def get_hash(data, encoding='utf-8'):
    return hashlib.md5(data.encode(encoding)).hexdigest()


def get_file_size(file, default=None):
    return os.path.getsize(file) if os.path.exists(file) else default


def check_patterns(path, include=None, exclude=None):
    if exclude:
        for pattern in exclude:
            if fnmatch(path, pattern):
                return False
        return True
    if include:
        for pattern in include:
            if fnmatch(path, pattern):
                return True
        return False
    return True


def list_label_mountpoints():
    return {label: mountpoint for mountpoint, label in list_mountpoint_labels().items() if label}


def coalesce(*values):
    for v in values:
        if v is not None:
            return v
    return None


class Metadata:
    _instance = None
    file = os.path.join(WORK_DIR, '.meta.json')
    data = {}

    def __new__(cls):
        if not cls._instance:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance

    def _load(self):
        try:
            with open(self.file, 'r', encoding='utf-8') as fd:
                self.data = json.load(fd)
        except Exception:
            self.data = {}

    def get(self, key):
        return self.data.get(key, {})

    def set(self, key, value: dict):
        self.data[key] = value

    def save(self):
        max_ts = time.time() - METADATA_MAX_AGE
        self.data = {k: v for k, v in self.data.items() if v.get('next_ts') > max_ts}
        with open(self.file, 'w', encoding='utf-8') as fd:
            json.dump(self.data, fd, sort_keys=True, indent=4)


class SaveReference:
    _instances = {}

    def __new__(cls, dst):
        if dst not in cls._instances:
            cls._instances[dst] = super().__new__(cls)
            cls._instances[dst].dst = dst
            cls._instances[dst].file = os.path.join(dst, REF_FILENAME)
            cls._instances[dst].data = None
            cls._instances[dst].files = None
            cls._instances[dst]._load()
        return cls._instances[dst]

    def _load(self, data=None):
        if data:
            self.data = data
        elif not os.path.exists(self.file):
            self.data = {}
        else:
            try:
                with open(self.file, 'r', encoding='utf-8') as fd:
                    self.data = json.load(fd)
                assert 'src' not in self.data, f'deprecated ref file {self.file}'
            except Exception as e:
                os.remove(self.file)
                logger.error(f'removed invalid ref file {self.file}: {e}')
                self.data = {}
        self.files = defaultdict(dict, deepcopy(self.data.get('files', {})))

    def _purge_files(self):
        for src, files in self.files.items():
            self.files[src] = {k: v for k, v in files.items() if os.path.exists(os.path.join(self.dst, k))}
        self.files = {k: v for k, v in self.files.items() if v}

    def save(self, force=False):
        self._purge_files()
        data = {
            'files': dict(self.files),
        }
        if not (force or data != {k: self.data.get(k) for k in data.keys()}):
            return
        data['ts'] = time.time()
        with open(self.file, 'w', encoding='utf-8') as fd:
            json.dump(data, fd, sort_keys=True, indent=4)
        self._load(data)

    def reset_files(self, src):
        self.files[src] = {}

    def set_file(self, src, rel_path, ref_val):
        self.files[src][rel_path] = ref_val

    def get_files(self, src):
        return self.files.get(src, {})

    def get_dst_files(self, src=None):
        if src:
            return {os.path.join(self.dst, f) for f in self.files[src].keys()}
        return {os.path.join(self.dst, f) for files in self.files.values() for f in files.keys()}

    @property
    def ts(self):
        return self.data.get('ts', 0)


def truncate_middle(s: str, width: int) -> str:
    if len(s) <= width:
        return s.ljust(width)  # pad if shorter
    half = (width - 3) // 2
    return s[:half] + "..." + s[-(width - half - 3):]


class BaseReport:
    def __init__(self):
        self.data = []

    def add(self, obj, **kwargs):
        raise NotImplementedError()

    def update(self, report):
        self.data.extend(report.data)

    def _get_row(self, row):
        return ' '.join([
            f'{row["code"]:20}',
            f'{row["id"]:20}',
            truncate_middle(row["src"], 50),
            truncate_middle(row["rel_path"], 40),
            truncate_middle(row["dst"], 50),
            f'{row["duration"]:>8}',
        ])

    def print_table(self, codes=None):
        rows = []
        for item in sorted(self.data, key=lambda x: (x['code'], x['id'], x['src'], x['rel_path'])):
            if codes and item['code'] not in codes:
                continue
            rows.append(self._get_row(item))
        if rows:
            data = '\n'.join([self._get_row({k: k for k in ('code', 'id', 'src', 'dst', 'rel_path', 'duration')})] + rows)
            logger.info(f'report:\n{data}')

    def print_summary_table(self):
        agg = defaultdict(lambda: defaultdict(int))
        for item in self.data:
            agg[item['code']][item['id']] += 1

        def get_row(row):
            return ' '.join([f'{row["code"]:20}', f'{row["id"]:20}', f'{row["count"]:>6}'])

        rows = []
        for code, v in agg.items():
            for id, count in sorted(v.items()):
                rows.append(get_row({'code': code, 'id': id, 'count': count}))
        if rows:
            data = '\n'.join([get_row({'code': 'code', 'id': 'id', 'count': 'count'})] + rows)
            logger.info(f'summary:\n{data}')


class SaveReport(BaseReport):
    def add(self, saver, rel_path, code, start_ts=None):
        self.data.append({
            'id': saver.id,
            'src': saver.src,
            'dst': saver.dst,
            'rel_path': rel_path,
            'code': code,
            'duration': f'{time.time() - start_ts:.1f}' if start_ts else '',
        })


class LoadReport(BaseReport):
    def add(self, loader, save_ref, src, rel_path, code, start_ts=None):
        self.data.append({
            'id': loader.id,
            'src': src,
            'dst': save_ref.dst,
            'rel_path': rel_path,
            'code': code,
            'duration': f'{time.time() - start_ts:.1f}' if start_ts else '',
        })
