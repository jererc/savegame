import logging

from savegame.lib import Report, to_json
from savegame.loaders.base import NotFound, get_loader_class
from savegame.save import iterate_save_items

logger = logging.getLogger(__name__)


class LoadHandler:
    def __init__(self, config, **loader_args):
        self.config = config
        self.loader_id = loader_args.pop('loader_id', None)
        self.loader_args = loader_args

    def _iterate_save_items(self):
        for si in iterate_save_items(self.config, log_unhandled=True):
            if not si.is_loadable():
                continue
            if self.loader_id and si.saver_cls.id != self.loader_id:
                continue
            yield si

    def run(self):
        report = Report()
        saver_id_root_dst_paths = {(s.saver_cls.id, s.root_dst_path) for s in self._iterate_save_items()}
        for saver_id, root_dst_path in sorted(saver_id_root_dst_paths):
            try:
                loader = get_loader_class(saver_id)(self.config, root_dst_path, **self.loader_args)
            except NotFound:
                logger.debug(f'no available loader for {saver_id=}')
                continue
            try:
                loader.run()
            except Exception:
                logger.exception(f'failed to load {loader.id=} {loader.root_dst_path=}')
            report.merge(loader.report)
        logger.info(f'report:\n{to_json(report.clean())}')
        logger.info(f'summary:\n{to_json(report.get_summary())}')


def loadgame(config, **kwargs):
    LoadHandler(config, **kwargs).run()
