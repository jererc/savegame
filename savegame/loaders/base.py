import importlib
import inspect
import logging
import os

from savegame.report import LoadReport
from savegame.utils import HOSTNAME, USERNAME, NotFound

logger = logging.getLogger(__name__)


class BaseLoader:
    id = None

    def __init__(self, config, root_dst_path, saver_cls, hostname=None, username=None,
                 include=None, exclude=None, force=False, dry_run=False):
        self.config = config
        self.root_dst_path = root_dst_path
        self.saver_cls = saver_cls
        self.hostname = hostname or HOSTNAME
        self.username = username or USERNAME
        self.include = include
        self.exclude = exclude
        self.force = force
        self.dry_run = dry_run
        self.report = LoadReport()


def iterate_loader_classes(package='savegame.loaders'):
    for filename in os.listdir(os.path.dirname(os.path.realpath(__file__))):
        basename, ext = os.path.splitext(filename)
        if ext == '.py' and not filename.startswith('__'):
            module_name = f'{package}.{basename}'
            try:
                module = importlib.import_module(module_name)
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, BaseLoader) and obj.id:
                        yield obj
            except ImportError as exc:
                logger.error(f'failed to import {module_name}: {exc}')


def get_loader_class(loader_id, package='savegame.loaders'):
    for loader_class in iterate_loader_classes(package):
        if loader_class.id == loader_id:
            return loader_class
    raise NotFound(f'loader_id {loader_id} not found')
