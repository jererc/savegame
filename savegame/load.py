import logging

from savegame.lib import LoadReport
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
        report = LoadReport()
        saver_cls_root_dst_paths = {(s.saver_cls, s.root_dst_path) for s in self._iterate_save_items()}
        for saver_cls, root_dst_path in sorted(saver_cls_root_dst_paths, key=lambda x: x[0].id):
            try:
                loader = get_loader_class(saver_cls.id)(self.config, root_dst_path, saver_cls, **self.loader_args)
            except NotFound:
                logger.debug(f'no available loader for {saver_cls.id=}')
                continue
            try:
                loader.run()
            except Exception:
                logger.exception(f'failed to load {loader.id=} {loader.root_dst_path=}')
            report.update(loader.report)
        report.print_table()


def loadgame(config, **kwargs):
    LoadHandler(config, **kwargs).run()
