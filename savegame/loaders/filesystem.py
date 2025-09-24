from glob import glob
import logging
import os
from pathlib import PurePath
import shutil
import sys
import time

from savegame import NAME
from savegame.lib import SaveReference, UnhandledPath, check_patterns, get_file_hash, get_file_mtime, get_file_size, validate_path
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

    def _iterate_save_refs(self):
        if self.saver_cls.in_place:
            yield SaveReference(self.root_dst_path)
            return
        for hostname in self.hostnames:
            if hostname != self.hostname:
                continue
            for dst in glob(os.path.join(self.root_dst_path, hostname, '*')):
                save_ref = SaveReference(dst)
                if save_ref.files:
                    yield save_ref

    def _must_copy_file(self, dst_file, src_file):
        if not check_patterns(src_file, self.include, self.exclude):
            return False, None
        if not os.path.exists(src_file):
            return True, None
        if get_file_hash(src_file) == get_file_hash(dst_file):
            return False, 'match'
        if not self.force:
            return False, 'mismatch_src_newer' if get_file_mtime(src_file, 0) > get_file_mtime(dst_file, 0) else 'mismatch_dst_newer'
        return True, None

    def _get_src_and_rel_paths(self, save_ref):
        src_rel_paths = set()
        invalid_files = set()
        for src, files in save_ref.files.items():
            try:
                validate_path(src)
                is_src_valid = True
            except UnhandledPath:
                is_src_valid = False
            for rel_path, ref in files.items():
                if is_src_valid:
                    dst_file = os.path.join(save_ref.dst, rel_path)
                    if isinstance(ref, str):
                        is_valid = get_file_hash(dst_file) == ref
                    else:
                        is_valid = True
                else:
                    is_valid = False
                if is_valid:
                    src_rel_paths.add((src, rel_path))
                else:
                    invalid_files.add(rel_path)
                    self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='invalid')
        if invalid_files:
            return set()
        if not src_rel_paths:
            self.report.add(self, save_ref=save_ref, src=None, rel_path=None, code='no_files')
        return src_rel_paths

    def _load_from_save_ref(self, save_ref, exclude_rel_paths=None):
        for src, rel_path in self._get_src_and_rel_paths(save_ref):
            if exclude_rel_paths and rel_path in exclude_rel_paths:
                continue
            raw_src_file = os.path.join(src, rel_path)
            src_file = self._get_src_file_for_user(raw_src_file)
            if not src_file:
                self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='mismatch_username')
                continue
            dst_file = os.path.join(save_ref.dst, rel_path)
            must_copy, message = self._must_copy_file(dst_file, src_file)
            if not must_copy:
                if message:
                    self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code=message)
                continue
            if self.dry_run:
                self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='loadable')
                continue
            try:
                if os.path.exists(src_file):
                    src_file_bak = f'{src_file}.{NAME}bak'
                    if not os.path.exists(src_file_bak):
                        os.rename(src_file, src_file_bak)
                        logger.warning(f'renamed existing {src_file=} to {src_file_bak=}')
                os.makedirs(os.path.dirname(src_file), exist_ok=True)
                start_ts = time.time()
                logger.info(f'copying {dst_file=} to {src_file=} ({get_file_size(dst_file) / 1024 / 1024:.02f} MB)')
                shutil.copy2(dst_file, src_file)
                self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='loaded', start_ts=start_ts)
            except Exception:
                logger.exception(f'failed to copy {dst_file=} to {src_file=}')
                self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='failed')

    def run(self):
        for save_ref in self._iterate_save_refs():
            self._load_from_save_ref(save_ref)


class FilesystemMirrorLoader(FilesystemLoader):
    id = 'filesystem_mirror'


class FilesystemCopyLoader(FilesystemLoader):
    id = 'filesystem_copy'
