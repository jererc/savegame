import filecmp
import importlib
import inspect
import json
import logging
import os
import re
import time

from svcutils.notifier import notify

from savegame import NAME
from savegame.report import SaveReport
from savegame.utils import (HOSTNAME, MTIME_DRIFT_TOLERANCE, REF_FILENAME, FileRef, Metadata, SaveRef, coalesce,
                            get_file_mtime, get_file_hash, get_hash, remove_path, validate_path)

logger = logging.getLogger(__name__)


def path_to_dirname(x):
    x = re.sub(r'[<>:"|?*\s]', '_', x)
    x = re.sub(r'[/\\]', '-', x)
    return x.strip('-')


def walk_paths(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for item in files + dirs:
            yield os.path.join(root, item)


class NotFound(Exception):
    pass


class BaseSaver:
    id = None
    hostname = HOSTNAME
    dst_type = 'local'
    in_place = False
    enable_purge = True
    purge_delta = 15 * 24 * 3600
    retry_delta = 2 * 3600
    file_compare_method = 'hash'

    def __init__(self, config, save_item, src, include, exclude):
        self.config = config
        self.save_item = save_item
        self.src = src
        self.include = include
        self.exclude = exclude
        self.dst = self._get_dst()
        self.save_ref = SaveRef(self.dst)
        self.key = self._get_key()
        self.meta = Metadata()
        self.report = SaveReport()
        self.start_ts = None
        self.end_ts = None
        self.success = None

    @classmethod
    def get_root_dst_path(cls, dst_path, volume_path, root_dirname):
        if not dst_path:
            raise Exception('missing dst_path')
        if cls.dst_type != 'local':
            return dst_path
        if volume_path:
            dst_path = os.path.join(volume_path, dst_path)
        validate_path(dst_path)
        dst_path = os.path.expanduser(dst_path)
        if not os.path.exists(dst_path):
            return None
        if cls.in_place:
            return dst_path
        return os.path.join(dst_path, root_dirname, cls.id)

    def _get_dst(self):
        dst = self.save_item.root_dst_path
        if self.dst_type != 'local' or self.in_place:
            return dst
        return os.path.join(dst, self.hostname, path_to_dirname(self.src))

    def _get_key_src_dst(self, key):
        label = getattr(self.save_item, f'{key}_volume_label', None)
        label_prefix = f'{label}:' if label else ''
        return f'{label_prefix}{getattr(self, key)}'

    def _get_key_data(self):
        return {
            'saver_id': self.id,
            'src': self._get_key_src_dst('src'),
            'dst': self._get_key_src_dst('dst'),
            'include': self.include,
            'exclude': self.exclude,
        }

    def _get_key(self):
        return get_hash(json.dumps(self._get_key_data(), sort_keys=True))

    def _get_retry_delta(self):
        return self.save_item.run_delta if self.success else coalesce(self.save_item.retry_delta, self.retry_delta)

    def _get_next_ts(self):
        return time.time() + self._get_retry_delta()

    def _get_success_ts(self):
        return self.end_ts if self.success else self.meta.get(self.key).get('success_ts', 0)

    def _update_meta(self):
        self.meta.set(self.key, self._get_key_data() | {
            'start_ts': self.start_ts,
            'end_ts': self.end_ts,
            'next_ts': self._get_next_ts(),
            'success_ts': self._get_success_ts(),
        })

    def must_run(self):
        return time.time() > self.meta.get(self.key).get('next_ts', 0)

    def reset_files(self, src):
        return self.save_ref.reset_files(src, hostname=self.hostname)

    def set_file(self, src, rel_path, ref):
        self.save_ref.set_file(src, rel_path, ref, hostname=self.hostname)

    def must_copy_file(self, src_file, dst_file, default_ref):
        src_mtime = get_file_mtime(src_file)
        dst_mtime = get_file_mtime(dst_file)

        if coalesce(self.save_item.file_compare_method, self.file_compare_method) == 'hash':
            src_hash = get_file_hash(src_file)
            equal = src_hash == get_file_hash(dst_file)
            new_ref = FileRef(hash=src_hash).ref
        else:
            equal = filecmp.cmp(src_file, dst_file, shallow=True) if os.path.exists(dst_file) else False
            new_ref = FileRef(size=os.path.getsize(src_file), mtime=src_mtime).ref
        must_copy = not equal
        if equal:
            default_ref = new_ref

        if must_copy and src_mtime and dst_mtime and src_mtime < dst_mtime - MTIME_DRIFT_TOLERANCE:   # never overwrite newer files, useful after a vm restore
            logger.warning(f'{dst_file=} is newer than {src_file=}')
            self.report.add(self, rel_path=os.path.relpath(src_file, self.src), code='failed_dst_newer')
            must_copy = False

        return must_copy, new_ref, default_ref

    def _must_purge_dst_path(self, path, dst_files, cutoff_ts):
        if os.path.isfile(path):
            if path in dst_files:
                return False
            name = os.path.basename(path)
            if name == REF_FILENAME:
                return False
            if not name.startswith(REF_FILENAME) and get_file_mtime(path) > cutoff_ts:
                return False
        elif os.listdir(path):
            return False
        return True

    def _purge_dst(self):
        dst_files = self.save_ref.get_dst_files(hostname=self.hostname)
        if not dst_files and not self.in_place:
            remove_path(self.dst)
            return
        cufoff_ts = time.time() - coalesce(self.save_item.purge_delta, self.purge_delta)
        for path in walk_paths(self.dst):
            if self._must_purge_dst_path(path, dst_files, cufoff_ts):
                remove_path(path)
                self.report.add(self, rel_path=os.path.relpath(path, self.dst), code='removed')

    def do_run(self):
        raise NotImplementedError()

    def run(self):
        self.start_ts = time.time()
        logger.info(f'running {self.id=} {self.src=} {self.dst=}')
        try:
            self.do_run()
            if self.enable_purge and self.save_item.enable_purge:
                self._purge_dst()
            if os.path.exists(self.save_ref.dst):
                self.save_ref.save(hostname=self.hostname, force=self.config.ALWAYS_UPDATE_REF)
            self.success = True
        except Exception as e:
            logger.exception(f'failed to save {self.src=}')
            notify(title='error', body=f'failed to save {self.src}: {e}', app_name=NAME)
            self.success = False
        self.end_ts = time.time()
        self._update_meta()


def iterate_saver_classes(package='savegame.savers'):
    for filename in os.listdir(os.path.dirname(os.path.realpath(__file__))):
        basename, ext = os.path.splitext(filename)
        if ext == '.py' and not filename.startswith('__'):
            module_name = f'{package}.{basename}'
            try:
                module = importlib.import_module(module_name)
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, BaseSaver) and obj.id:
                        yield obj
            except ImportError as e:
                logger.error(f'failed to import {module_name}: {e}')


def get_saver_class(saver_id, package='savegame.savers'):
    for saver_class in iterate_saver_classes(package):
        if saver_class.id == saver_id:
            return saver_class
    raise NotFound(f'saver_id {saver_id} not found')
