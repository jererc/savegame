from glob import glob
import logging
import os
from pathlib import PurePath
import shutil
import sys

from savegame import NAME
from savegame.lib import Reference, UnhandledPath, check_patterns, get_file_hash, get_file_mtime, validate_path
from savegame.loaders.base import BaseLoader

HOME_DIR = os.path.expanduser('~')
SHARED_USERNAMES = {'linux': {'shared'}, 'win32': {'Public'}}.get(sys.platform, set())

logger = logging.getLogger(__name__)


class FilesystemLoader(BaseLoader):
    id = 'filesystem'

    def _get_src_file_for_user(self, path):
        pp = PurePath(path)
        home_root = os.path.dirname(HOME_DIR)
        if not pp.is_relative_to(home_root):
            return path
        try:
            username = pp.parts[2]
        except IndexError:
            return path
        if username in SHARED_USERNAMES:
            return path
        if username == self.username:
            return path.replace(os.path.join(home_root, username), HOME_DIR, 1)
        return None

    def _iterate_refs(self):
        if self.saver_cls.in_place:
            yield Reference(self.root_dst_path)
            return
        for hostname in self.hostnames:
            if hostname != self.hostname:
                continue
            for dst in glob(os.path.join(self.root_dst_path, hostname, '*')):
                ref = Reference(dst)
                if ref.src:
                    yield ref

    # def _requires_load(self, dst_file, src_file, src):
    #     if not check_patterns(src_file, self.include, self.exclude):
    #         return False
    #     if not os.path.exists(src_file):
    #         return True
    #     if get_file_hash(src_file) == get_file_hash(dst_file):
    #         self.report.add('match', src, src_file)
    #         return False
    #     if not self.force:
    #         if get_file_mtime(src_file) > get_file_mtime(dst_file):
    #             self.report.add('mismatch_src_newer', src, src_file)
    #         else:
    #             self.report.add('mismatch_dst_newer', src, src_file)
    #         return False
    #     return True

    # def _load_file(self, dst_file, src_file, src):
    #     if not self._requires_load(dst_file, src_file, src):
    #         return
    #     if self.dry_run:
    #         self.report.add('loadable', src, src_file)
    #         return
    #     try:
    #         if os.path.exists(src_file):
    #             src_file_bak = f'{src_file}.{NAME}bak'
    #             if os.path.exists(src_file_bak):
    #                 os.remove(src_file)
    #             else:
    #                 os.rename(src_file, src_file_bak)
    #                 logger.warning(f'renamed existing src file {src_file} to {src_file_bak}')
    #             self.report.add('loaded_overwritten', src, src_file)
    #         else:
    #             self.report.add('loaded', src, src_file)
    #         os.makedirs(os.path.dirname(src_file), exist_ok=True)
    #         shutil.copy2(dst_file, src_file)
    #         logger.info(f'loaded {src_file} from {dst_file}')
    #     except Exception as exc:
    #         self.report.add('failed', src, src_file)
    #         logger.error(f'failed to load {src_file} from {dst_file}: {exc}')

    def _is_file_loadable(self, dst_file, src_file):
        if not check_patterns(src_file, self.include, self.exclude):
            return False, None
        if not os.path.exists(src_file):
            return True, None
        if get_file_hash(src_file) == get_file_hash(dst_file):
            return False, 'match'
        if not self.force:
            if get_file_mtime(src_file) > get_file_mtime(dst_file):
                return False, 'mismatch_src_newer'
            else:
                return False, 'mismatch_dst_newer'
        return True, None

    def _load_from_ref(self, ref):
        try:
            validate_path(ref.src)
        except UnhandledPath:
            self.report.add(self, ref=ref, rel_path=None, code='unhandled')
            return
        rel_paths = set()
        invalid_files = set()
        for rel_path, ref_val in ref.files.items():
            dst_file = os.path.join(ref.dst, rel_path)
            if isinstance(ref_val, str):
                is_valid = get_file_hash(dst_file) == ref_val
            else:
                # TODO: handle git bundle ts
                is_valid = True
            if is_valid:
                rel_paths.add(rel_path)
            else:
                invalid_files.add(rel_path)
                self.report.add(self, ref=ref, rel_path=rel_path, code='invalid')
        if invalid_files:
            return
        if not rel_paths:
            self.report.add(self, ref=ref, rel_path=None, code='no_files')
            return
        for rel_path in rel_paths:
            src_file_raw = os.path.join(ref.src, rel_path)
            src_file = self._get_src_file_for_user(src_file_raw)
            if not src_file:
                self.report.add(self, ref=ref, rel_path=rel_path, code='mismatch_username')
                continue
            # self._load_file(os.path.join(ref.dst, rel_path), src_file, ref.src)
            dst_file = os.path.join(ref.dst, rel_path)
            # if not self._requires_load(dst_file, src_file, ref.src):
            #     continue
            loadable, message = self._is_file_loadable(dst_file, src_file)
            if not loadable:
                if message:
                    self.report.add(self, ref=ref, rel_path=rel_path, code=message)
                continue
            if self.dry_run:
                self.report.add(self, ref=ref, rel_path=rel_path, code='loadable')
                continue
            try:
                if os.path.exists(src_file):
                    src_file_bak = f'{src_file}.{NAME}bak'
                    if os.path.exists(src_file_bak):
                        os.remove(src_file)
                    else:
                        os.rename(src_file, src_file_bak)
                        logger.warning(f'renamed existing src file {src_file} to {src_file_bak}')
                    self.report.add(self, ref=ref, rel_path=rel_path, code='loaded_overwritten')
                else:
                    self.report.add(self, ref=ref, rel_path=rel_path, code='loaded')
                os.makedirs(os.path.dirname(src_file), exist_ok=True)
                shutil.copy2(dst_file, src_file)
                logger.info(f'loaded {src_file} from {dst_file}')
            except Exception as exc:
                self.report.add(self, ref=ref, rel_path=rel_path, code='failed')
                logger.error(f'failed to load {src_file} from {dst_file}: {exc}')

    def run(self):
        for ref in self._iterate_refs():
            self._load_from_ref(ref)


class FilesystemMirrorLoader(FilesystemLoader):
    id = 'filesystem_mirror'


class FilesystemCopyLoader(FilesystemLoader):
    id = 'filesystem_copy'
