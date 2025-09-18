import logging

from savegame.lib import Report, to_json
from savegame.loaders.base import NotFound, get_loader_class
from savegame.save import iterate_save_items

logger = logging.getLogger(__name__)


class LoadHandler:
    def __init__(self, config, **loader_args):
        self.config = config
        self.loader_args = loader_args

    def run(self):
        report = Report()
        for si in iterate_save_items(self.config, log_unhandled=True):
            if not si.is_loadable():
                continue
            try:
                loader = get_loader_class(si.saver_cls.id)(dst_path=si.dst_path, **self.loader_args)
            except NotFound:
                logger.debug(f'no available loader for saver_id {si.saver_cls.id}')
                continue
            try:
                loader.run()
            except Exception:
                logger.exception(f'failed to load {loader.dst_path}')
            report.merge(loader.report)
        logger.info(f'report:\n{to_json(report.clean())}')
        logger.info(f'summary:\n{to_json(report.get_summary())}')


def loadgame(config, **kwargs):
    LoadHandler(config, **kwargs).run()
