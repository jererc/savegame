import importlib
import inspect
import json
import logging
import os
import re
import time

from svcutils.notifier import notify

from savegame import NAME
from savegame.lib import (REF_FILENAME, Metadata, Reference, Report, get_file_mtime,
                          get_hash, remove_path, validate_path)


SAVE_DURATION_THRESHOLD = 30

logger = logging.getLogger(__name__)


def path_to_dirname(x):
    x = re.sub(r'[<>:"|?*\s]', '_', x)
    x = re.sub(r'[/\\]', '-', x)
    return x.strip('-')


def walk_paths(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for item in files + dirs:
            yield os.path.join(root, item)


class BaseSaver:
    id = None
    hostname = None
    src_type = 'local'
    dst_type = 'local'
    in_place = False
    retry_delta = 2 * 3600

    def __init__(self, config, save_item, src, inclusions, exclusions):
        self.config = config
        self.save_item = save_item
        self.src = src
        self.inclusions = inclusions
        self.exclusions = exclusions
        self.dst = self.get_dst(self.save_item.dst_path)
        self.dst_paths = set()
        self.ref = Reference(self.dst)
        self.key = self._get_key()
        self.meta = Metadata()
        self.report = Report()
        self.start_ts = None
        self.end_ts = None
        self.success = None

    @classmethod
    def get_base_dst_path(cls, dst_path, volume_path, root_dirname):
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

    def get_dst(self, dst_path):
        if self.dst_type != 'local':
            return dst_path
        if self.in_place:
            return dst_path
        if not self.hostname:
            raise Exception(f'undefined hostname for {self.id}')
        return os.path.join(dst_path, self.hostname, path_to_dirname(self.src))

    def _get_key_src_dst(self, key):
        label = getattr(self.save_item, f'{key}_volume_label', None)
        label_prefix = f'{label}:' if label else ''
        return f'{label_prefix}{getattr(self, key)}'

    def _get_key(self):
        return get_hash(json.dumps({
            'src': self._get_key_src_dst('src'),
            'dst': self._get_key_src_dst('dst'),
            'inclusions': self.inclusions,
            'exclusions': self.exclusions,
        }, sort_keys=True))

    def must_run(self):
        return time.time() > self.meta.get(self.key).get('next_ts', 0)

    def _get_retry_delta(self):
        return self.save_item.run_delta if self.success else (self.save_item.retry_delta or self.retry_delta)

    def _get_next_ts(self):
        return time.time() + self._get_retry_delta()

    def _get_success_ts(self):
        return self.end_ts if self.success else self.meta.get(self.key).get('success_ts', 0)

    def _update_meta(self):
        self.meta.set(self.key, {
            'src': self._get_key_src_dst('src'),
            'dst': self._get_key_src_dst('dst'),
            'start_ts': self.start_ts,
            'end_ts': self.end_ts,
            'next_ts': self._get_next_ts(),
            'success_ts': self._get_success_ts(),
        })

    def _requires_purge(self, path):
        if os.path.isfile(path):
            if path in self.dst_paths:
                return False
            name = os.path.basename(path)
            if name == REF_FILENAME:
                return False
            if not name.startswith(REF_FILENAME) and get_file_mtime(path) > time.time() - self.save_item.purge_delta:
                return False
        elif os.listdir(path):
            return False
        return True

    def _purge_dst(self):
        if not self.dst_paths:
            remove_path(self.dst)
            logger.warning(f'purged {self.dst} because no paths were saved')
            return
        for path in walk_paths(self.dst):
            if self._requires_purge(path):
                remove_path(path)
                self.report.add('removed', self.src, path)

    def do_run(self):
        raise NotImplementedError()

    def run(self):
        self.start_ts = time.time()
        self.ref.save_src = self.src
        self.ref.src = self.src
        logger.info(f'saving {self.src} to {self.dst}')
        try:
            self.do_run()
            if self.save_item.enable_purge:
                self._purge_dst()
            if os.path.exists(self.ref.dst):
                self.ref.save(force=self.config.ALWAYS_UPDATE_REF)
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
    raise Exception(f'invalid saver_id {saver_id}')
