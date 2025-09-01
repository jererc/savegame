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


logger = logging.getLogger(__name__)


HOSTNAME = socket.gethostname()
REF_FILENAME = f'.{NAME}'
METADATA_MAX_AGE = 3600 * 24 * 90


class UnhandledPath(Exception):
    pass


class InvalidPath(Exception):
    pass


def get_file_mtime(x, default=None):
    return os.stat(x).st_mtime if os.path.exists(x) else default


def validate_path(x):
    if os.path.sep not in x:
        raise UnhandledPath(f'unhandled path {x}: {sys.platform=}')


def to_json(x):
    return json.dumps(x, sort_keys=True, indent=4)


def remove_path(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


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
        logger.warning(f'get_file_hash {file} took {duration:.02f} '
                       f'seconds ({os.path.getsize(file)/1024/1024:.02f} MB)')
    return md5_hash.hexdigest()


def get_hash(data, encoding='utf-8'):
    return hashlib.md5(data.encode(encoding)).hexdigest()


def get_file_size(x, default=None):
    return os.path.getsize(x) if os.path.exists(x) else default


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


def list_label_mountpoints():
    return {label: mountpoint for mountpoint, label in list_mountpoint_labels().items()}


class Metadata:
    file = os.path.join(WORK_DIR, '.meta.json')
    data = {}

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super().__new__(cls)
            cls.instance.load()
        return cls.instance

    def load(self):
        if os.path.exists(self.file):
            with open(self.file, 'r', encoding='utf-8') as fd:
                self.data = json.load(fd)

    def get(self, key):
        return self.data.get(key, {})

    def set(self, key, value: dict):
        self.data[key] = value

    def save(self):
        max_ts = time.time() - METADATA_MAX_AGE
        self.data = {k: v for k, v in self.data.items() if v.get('next_ts') > max_ts}
        with open(self.file, 'w', encoding='utf-8') as fd:
            json.dump(self.data, fd, sort_keys=True, indent=4)


class Reference:
    def __init__(self, dst):
        self.dst = dst
        self.file = os.path.join(dst, REF_FILENAME)
        self.data = None
        self.save_src = None
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
                with open(self.file, 'r', encoding='utf-8') as fd:
                    self.data = json.load(fd)
            except Exception as exc:
                os.remove(self.file)
                logger.exception(f'removed invalid ref file {self.file}: {exc}')
                self.data = {}
        self.save_src = self.data.get('save_src')
        self.src = self.data.get('src')
        self.files = deepcopy(self.data.get('files', {}))

    def save(self, force=False):
        data = {
            'save_src': self.save_src,
            'src': self.src,
            'files': self.files,
        }
        if not (force or data != {k: self.data.get(k) for k in data.keys()}):
            return
        data['ts'] = time.time()
        with open(self.file, 'w', encoding='utf-8') as fd:
            json.dump(data, fd, sort_keys=True, indent=4)
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

    def get(self, k1, k2=None):
        data = self.data.get(k1, {})
        return data.get(k2) if k2 else data

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
