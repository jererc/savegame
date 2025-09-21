import importlib
import inspect
import json
import logging
import os
import re
import time

from svcutils.notifier import notify

from savegame import NAME
from savegame.lib import (HOSTNAME, REF_FILENAME, Metadata, SaveReference, SaveReport,
                          coalesce, get_file_mtime, get_hash, remove_path, validate_path)

SAVE_DURATION_THRESHOLD = 30
MTIME_DRIFT_TOLERANCE = 10

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
        self.save_ref = SaveReference(self.dst)
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

    def _check_src_file(self, src_file, dst_file):
        """
        Makes sure we do not overwrite a newer file, useful after a vm restore.
        """
        src_mtime = get_file_mtime(src_file)
        dst_mtime = get_file_mtime(dst_file)
        if src_mtime and dst_mtime and src_mtime < dst_mtime - MTIME_DRIFT_TOLERANCE:
            logger.warning(f'{dst_file=} is newer than {src_file=}')
            self.report.add(self, src_file=src_file, dst_file=dst_file, code='failed')
            return False
        return True

    def _requires_purge(self, path, dst_files, cutoff_ts):
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
        dst_files = self.save_ref.get_dst_files()
        if not dst_files and not self.in_place:
            remove_path(self.dst)
            return
        cufoff_ts = time.time() - coalesce(self.save_item.purge_delta, self.purge_delta)
        for path in walk_paths(self.dst):
            if self._requires_purge(path, dst_files, cufoff_ts):
                remove_path(path)
                self.report.add(self, src_file=None, dst_file=path, code='removed')

    def do_run(self):
        raise NotImplementedError()

    def run(self):
        self.start_ts = time.time()
        self.save_ref.init_files(self.src)
        logger.info(f'running {self.id=} {self.src=} {self.dst=}')
        try:
            self.do_run()
            if self.enable_purge and self.save_item.enable_purge:
                self._purge_dst()
            if os.path.exists(self.save_ref.dst):
                self.save_ref.save(force=self.config.ALWAYS_UPDATE_REF)
            self.success = True
        except Exception as e:
            logger.exception(f'failed to save {self.src}')
            notify(title='error', body=f'failed to save {self.src}: {e}', app_name=NAME)
            self.success = False
        self.end_ts = time.time()
        self._update_meta()
        duration = self.end_ts - self.start_ts
        if duration > SAVE_DURATION_THRESHOLD:
            logger.warning(f'saved {self.src} to {self.dst} in {duration:.02f} seconds')


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
            except ImportError as exc:
                logger.error(f'failed to import {module_name}: {exc}')


def get_saver_class(saver_id, package='savegame.savers'):
    for saver_class in iterate_saver_classes(package):
        if saver_class.id == saver_id:
            return saver_class
    raise NotFound(f'saver_id {saver_id} not found')
