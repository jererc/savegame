from collections import defaultdict
from copy import deepcopy
from fnmatch import fnmatch
import hashlib
import json
import os
import shutil
import socket
import sys
import time

from savegame import NAME, WORK_DIR, logger


HOSTNAME = socket.gethostname()
REF_FILENAME = f'.{NAME}'


class UnhandledPath(Exception):
    pass


class InvalidPath(Exception):
    pass


def makedirs(x):
    if not os.path.exists(x):
        os.makedirs(x)


def get_file_mtime(x):
    return os.stat(x).st_mtime


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
    with open(file, 'rb') as fd:
        while chunk := fd.read(chunk_size):
            md5_hash.update(chunk)
    return md5_hash.hexdigest()


def get_hash(data, encoding='utf-8'):
    return hashlib.md5(data.encode(encoding)).hexdigest()


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


class Metadata:
    file = os.path.join(WORK_DIR, 'meta.json')
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

    def save(self, keys):
        self.data = {k: v for k, v in self.data.items() if k in keys}
        with open(self.file, 'w', encoding='utf-8') as fd:
            json.dump(self.data, fd, sort_keys=True, indent=4)


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
                with open(self.file, 'r', encoding='utf-8') as fd:
                    self.data = json.load(fd)
            except Exception as exc:
                os.remove(self.file)
                logger.exception('removed invalid ref file '
                    f'{self.file}: {exc}')
                self.data = {}
        self.src = self.data.get('src')
        self.files = deepcopy(self.data.get('files', {}))

    def save(self, force=False):
        data = {
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
