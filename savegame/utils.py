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
MTIME_DRIFT_TOLERANCE = 10
MAX_HASH_FILE_SIZE = 1_000_000_000

logger = logging.getLogger(__name__)


class UnhandledPath(Exception):
    pass


class InvalidPath(Exception):
    pass


class DeprecatedSaveRef(Exception):
    pass


class NotFound(Exception):
    pass


def get_file_mtime(path, default=None):
    try:
        return os.path.getmtime(path)
    except FileNotFoundError:
        return default


def validate_path(path):
    if INVALID_PATH_SEP in path:
        raise UnhandledPath(f'invalid path {path} on {sys.platform=}')


def normalize_path(path):
    return path.replace(INVALID_PATH_SEP, os.path.sep)


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
        logger.warning(f'get_file_hash {file} took {duration:.02f}s ({os.path.getsize(file) / 1024 / 1024:.02f} MB)')
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


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


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

    def set_subkey(self, key, subkey, value):
        self.data[key][subkey] = value

    def save(self):
        max_ts = time.time() - METADATA_MAX_AGE
        self.data = {k: v for k, v in self.data.items() if v.get('next_ts') > max_ts}
        with open(self.file, 'w', encoding='utf-8') as fd:
            json.dump(self.data, fd, sort_keys=True, indent=4)


def nested_dict():
    return defaultdict(nested_dict)


def dict_to_nested(d):
    """Recursively cast a dict into a nested defaultdict."""
    if isinstance(d, dict):
        return defaultdict(nested_dict, {k: dict_to_nested(v) for k, v in d.items()})
    return d


def iterate_save_refs(path):
    for file in walk_files(path):
        if os.path.basename(file) == REF_FILENAME:
            yield SaveRef(os.path.dirname(file))


class FileRef:
    @classmethod
    def from_file(cls, file, has_src_file=True):
        size = get_file_size(file)
        return cls(
            hash=get_file_hash(file) if (size is not None and size < MAX_HASH_FILE_SIZE) else None,
            size=size,
            mtime=get_file_mtime(file),
            has_src_file=has_src_file,
        )

    @classmethod
    def from_ref(cls, ref):
        parts = (ref or '').split(':')
        hash = parts[0] or None
        try:
            size = int(parts[1])
        except Exception:
            size = None
        try:
            mtime = float(parts[2])
        except Exception:
            mtime = None
        try:
            has_src_file = bool(int(parts[3]))
        except Exception:
            has_src_file = True
        return cls(hash=hash, size=size, mtime=mtime, has_src_file=has_src_file)

    def __init__(self, hash=None, size=None, mtime=None, has_src_file=True):
        self.hash = hash
        self.size = size
        self.mtime = mtime
        self.has_src_file = has_src_file

    @property
    def ref(self):
        return ':'.join(map(str, [self.hash or '', self.size or '', self.mtime or '', int(self.has_src_file)]))

    def _check_mtime(self, mtime):
        return abs(mtime - self.mtime) <= MTIME_DRIFT_TOLERANCE

    def check_file(self, file):
        if self.hash:
            return get_file_hash(file) == self.hash
        if self.size is not None and self.mtime is not None:
            return get_file_size(file) == self.size and self._check_mtime(get_file_mtime(file))
        return False


class SaveRef:
    _instances = {}
    _version = '20250928'

    def __new__(cls, dst):
        if dst not in cls._instances:
            cls._instances[dst] = super().__new__(cls)
            cls._instances[dst].dst = dst
            cls._instances[dst].file = os.path.join(dst, REF_FILENAME)
            cls._instances[dst].data = None
            cls._instances[dst].files = None
            cls._instances[dst]._load()
        return cls._instances[dst]

    def _read_file(self):
        try:
            with open(self.file, 'r', encoding='utf-8') as fd:
                data = json.load(fd)
            if data.get('version') != self._version:
                raise DeprecatedSaveRef()
            return data
        except FileNotFoundError:
            pass
        except DeprecatedSaveRef:
            os.remove(self.file)
            logger.info(f'removed deprecated save reference {self.file}')
        except Exception:
            if os.path.exists(self.file):
                os.remove(self.file)
            logger.exception(f'failed to load ref file {self.file}')
        return {'ts': {}, 'version': self._version}

    def _load(self, data=None):
        self.data = data or self._read_file()
        self.files = dict_to_nested(self.data.get('files', {}))

    def _purge_files(self, hostname=HOSTNAME):
        for src, file_refs in self.get_files().items():
            for rel_path in file_refs.keys():
                if not os.path.exists(os.path.join(self.dst, normalize_path(rel_path))):
                    self.files[hostname][src].pop(rel_path, None)
        for src, file_refs in self.get_files().items():
            if not file_refs:
                self.files[hostname].pop(src)
        if not self.files[hostname]:
            self.files.pop(hostname)

    def save(self, hostname=HOSTNAME, force=False):
        self._purge_files(hostname)
        data_update = {'files': self.files}
        if not (force or data_update != {k: self.data.get(k) for k in data_update.keys()}):
            return
        self.data.update(data_update)
        self.data['ts'][hostname] = time.time()
        with open(self.file, 'w', encoding='utf-8') as fd:
            json.dump(self.data, fd, sort_keys=True, indent=4)
        self._load(self.data)

    def get_files(self, src=None, hostname=HOSTNAME):
        return deepcopy(self.files[hostname][src] if src else self.files[hostname])

    def reset_files(self, src, hostname=HOSTNAME):
        files = self.get_files(src, hostname=hostname)
        self.files[hostname][src].clear()
        return files

    def set_file(self, src, rel_path, ref, hostname=HOSTNAME):
        self.files[hostname][src][rel_path] = ref

    def get_dst_files(self, src=None, hostname=HOSTNAME):
        files = self.get_files(hostname=hostname)
        if src:
            return {os.path.join(self.dst, r) for r in files[src].keys()}
        return {os.path.join(self.dst, r) for file_refs in files.values() for r in file_refs.keys()}

    def get_ts(self, hostname=HOSTNAME):
        return self.data.get('ts', {}).get(hostname, 0)
