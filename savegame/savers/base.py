import importlib
import inspect
import os
import re
import time

from svcutils.service import Notifier

from savegame import NAME, logger
from savegame.lib import (REF_FILENAME, Metadata, Reference, Report,
    get_file_mtime, remove_path)


RETRY_DELTA = 2 * 3600


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
    purge = False
    in_place = False

    def __init__(self, config, src, inclusions, exclusions, dst_path,
                 run_delta, purge_delta):
        self.config = config
        self.src = src
        self.inclusions = inclusions
        self.exclusions = exclusions
        self.dst_path = dst_path
        self.run_delta = run_delta
        self.purge_delta = purge_delta
        self.dst = self.get_dst()
        self.dst_paths = set()
        self.ref = Reference(self.dst)
        self.meta = Metadata()
        self.report = Report()
        self.start_ts = None
        self.end_ts = None
        self.success = None

    def get_dst(self):
        if self.in_place:
            return self.dst_path
        if self.dst_type != 'local':
            return self.dst_path
        return os.path.join(self.dst_path, self.hostname, path_to_dirname(self.src))

    def notify_error(self, message, exc=None):
        Notifier().send(title='error', body=message, app_name=NAME)

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

    def do_run(self):
        raise NotImplementedError()

    def _requires_purge(self, path):
        if os.path.isfile(path):
            if path in self.dst_paths:
                return False
            name = os.path.basename(path)
            if name == REF_FILENAME:
                return False
            if not name.startswith(REF_FILENAME) and \
                    get_file_mtime(path) > time.time() - self.purge_delta:
                return False
        elif os.listdir(path):
            return False
        return True

    def _purge_dst(self):
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
        self.ref.save_src = self.src
        self.ref.src = self.src
        try:
            self.do_run()
            if self.purge:
                self._purge_dst()
            if os.path.exists(self.ref.dst):
                self.ref.save(force=self.config.ALWAYS_UPDATE_REF)
            self.success = True
        except Exception as exc:
            self.success = False
            logger.exception(f'failed to save {self.src}')
            self.notify_error(f'failed to save {self.src}: {exc}', exc=exc)
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
            except ImportError as exc:
                logger.error(f'failed to import {module_name}: {exc}')


def get_saver_class(saver_id, package='savegame.savers'):
    for saver_class in iterate_saver_classes(package):
        if saver_class.id == saver_id:
            return saver_class
    raise Exception(f'invalid saver_id {saver_id}')
