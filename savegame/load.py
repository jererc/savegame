from glob import glob
import os
from pathlib import PurePath
import shutil

from svcutils.service import get_file_mtime

from savegame import NAME, HOME_PATH, HOSTNAME, USERNAME, logger
from savegame.lib import (Reference, Report, UnhandledPath,
    makedirs, validate_path, check_patterns, get_file_hash, to_json)
from savegame.save import iterate_save_items
from savegame.savers import LocalSaver


SHARED_USERNAMES = {
    'nt': {'Public'},
    'posix': {'shared'},
}.get(os.name, set())


class LocalLoader:
    def __init__(self, dst_path, hostname=None, username=None,
                 include=None, exclude=None, overwrite=False, dry_run=False):
        self.dst_path = dst_path
        self.hostname = hostname or HOSTNAME
        self.username = username or USERNAME
        self.include = include
        self.exclude = exclude
        self.overwrite = overwrite
        self.dry_run = dry_run
        self.hostnames = sorted(os.listdir(self.dst_path))
        self.report = Report()

    def _get_src_file_for_user(self, path):
        pp = PurePath(path)
        home_root = os.path.dirname(HOME_PATH)
        if not pp.is_relative_to(home_root):
            return path
        try:
            username = pp.parts[2]
        except IndexError:
            return path
        if username in SHARED_USERNAMES:
            return path
        if username == self.username:
            return path.replace(os.path.join(home_root, username),
                HOME_PATH, 1)
        return None

    def _iterate_refs(self):
        for hostname in self.hostnames:
            if hostname != self.hostname:
                continue
            for dst in glob(os.path.join(self.dst_path, hostname, '*')):
                ref = Reference(dst)
                if ref.src:
                    yield ref

    def _requires_load(self, dst_file, src_file, src):
        if not check_patterns(src_file, self.include, self.exclude):
            return False
        if not os.path.exists(src_file):
            return True
        if get_file_hash(src_file) == get_file_hash(dst_file):
            self.report.add('identical', src, src_file)
            return False
        if not self.overwrite:
            if get_file_mtime(src_file) > get_file_mtime(dst_file):
                self.report.add('conflict_src_more_recent',
                    src, src_file)
            else:
                self.report.add('conflict_dst_more_recent',
                    src, src_file)
            return False
        return True

    def _load_file(self, dst_file, src_file, src):
        if not self._requires_load(dst_file, src_file, src):
            return
        if self.dry_run:
            self.report.add('loadable', src, src_file)
            return
        try:
            if os.path.exists(src_file):
                src_file_bak = f'{src_file}.{NAME}bak'
                if os.path.exists(src_file_bak):
                    os.remove(src_file)
                else:
                    os.rename(src_file, src_file_bak)
                    logger.warning(f'renamed existing src file '
                        f'{src_file} to {src_file_bak}')
                self.report.add('loaded_overwritten', src, src_file)
            else:
                self.report.add('loaded', src, src_file)
            makedirs(os.path.dirname(src_file))
            shutil.copyfile(dst_file, src_file)
            logger.info(f'loaded {src_file} from {dst_file}')
        except Exception as exc:
            self.report.add('failed', src, src_file)
            logger.error(f'failed to load {src_file} '
                f'from {dst_file}: {exc}')

    def run(self):
        for ref in self._iterate_refs():
            try:
                validate_path(ref.src)
            except UnhandledPath:
                self.report.add('skipped_unhandled', ref.src, ref.src)
                continue
            rel_paths = set()
            invalid_files = set()
            for rel_path, ref_hash in ref.files.items():
                dst_file = os.path.join(ref.dst, rel_path)
                if get_file_hash(dst_file) != ref_hash:
                    invalid_files.add(dst_file)
                else:
                    rel_paths.add(rel_path)
            if invalid_files:
                self.report.add('invalid_files', ref.src, invalid_files)
                continue
            if not rel_paths:
                self.report.add('empty_dst', ref.src, ref.dst)
                continue
            for rel_path in rel_paths:
                src_file_raw = os.path.join(ref.src, rel_path)
                src_file = self._get_src_file_for_user(src_file_raw)
                if not src_file:
                    self.report.add('skipped_other_username', ref.src,
                        src_file_raw)
                    continue
                self._load_file(os.path.join(ref.dst, rel_path),
                    src_file, ref.src)


class LoadHandler:
    def __init__(self, config, **loader_args):
        self.config = config
        self.loader_args = loader_args

    def _iterate_loaders(self):
        dst_paths = {s.dst_path
            for s in iterate_save_items(self.config, log_unhandled=True)
            if s.saver_cls == LocalSaver and s.loadable}
        if not dst_paths:
            logger.info('nothing to load')
        for dst_path in dst_paths:
            yield LocalLoader(dst_path=dst_path, **self.loader_args)

    def run(self):
        report = Report()
        for loader in self._iterate_loaders():
            try:
                loader.run()
            except Exception:
                logger.exception(f'failed to load {loader.dst_path}')
            report.merge(loader.report)
        logger.info(f'report:\n{to_json(report.clean())}')
        logger.info(f'summary:\n{to_json(report.get_summary())}')


def loadgame(config, **kwargs):
    LoadHandler(config, **kwargs).run()
