import filecmp
import logging
import os
import shutil
import time

from savegame.lib import REF_FILENAME, check_patterns, coalesce, get_file_hash, get_file_size
from savegame.savers.base import BaseSaver

LOG_LIST_DURATION_THRESHOLD = 30
LOG_FILE_SIZE_THRESHOLD = 10 * 1024 * 1024

logger = logging.getLogger(__name__)


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


class FilesystemSaver(BaseSaver):
    id = 'filesystem'
    in_place = False
    enable_purge = True
    file_compare_method = 'hash'

    def _is_file_valid(self, file):
        return os.path.basename(file) != REF_FILENAME and check_patterns(file, self.include, self.exclude)

    def _get_src_and_files(self):
        start_ts = time.time()
        if os.path.isfile(self.src):
            src = os.path.dirname(self.src)
            raw_files = [self.src]
        else:
            src = self.src
            raw_files = list(walk_files(self.src))
        files = {f for f in raw_files if self._is_file_valid(f)}
        duration = time.time() - start_ts
        if duration > LOG_LIST_DURATION_THRESHOLD:
            logger.warning(f'listed {len(files)} files for {self.src=} {self.include=} {self.exclude=} in {duration:.1f}s')
        return src, files

    def _check_dst_volume(self):
        if self.save_item.dst_volume_path and not os.path.exists(self.save_item.dst_volume_path):
            raise Exception(f'volume {self.save_item.dst_volume_path} does not exist')

    def _compare_files_using_hash(self, src_file, dst_file):
        src_hash = get_file_hash(src_file)
        return src_hash == get_file_hash(dst_file), src_hash

    def _compare_files_using_filecmp(self, src_file, dst_file):
        match = filecmp.cmp(src_file, dst_file, shallow=True) if os.path.exists(dst_file) else False
        return match, os.path.getmtime(src_file)

    def _get_file_compare_callable(self):
        return {
            'hash': self._compare_files_using_hash,
            'shallow': self._compare_files_using_filecmp,
        }[coalesce(self.save_item.file_compare_method, self.file_compare_method)]

    def _set_ref_file(self, src, rel_path, ref_val):
        if ref_val:
            self.save_ref.set_file(src, rel_path, ref_val)

    def do_run(self):
        src, src_files = self._get_src_and_files()
        ref_files = self.save_ref.reset_files(src)
        file_compare_callable = self._get_file_compare_callable()
        for src_file in sorted(src_files):
            self._check_dst_volume()
            rel_path = os.path.relpath(src_file, src)
            ref_val = ref_files.get(rel_path)
            dst_file = os.path.join(self.dst, rel_path)
            try:
                match, new_ref_val = file_compare_callable(src_file, dst_file)
                if not match and self._check_src_file(src_file, dst_file):
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    file_size = get_file_size(src_file)
                    if file_size > LOG_FILE_SIZE_THRESHOLD:
                        logger.info(f'copying {src_file} to {dst_file} ({file_size / 1024 / 1024:.02f} MB)')
                    start_ts = time.time()
                    shutil.copy2(src_file, dst_file)
                    ref_val = new_ref_val
                    self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts)
            except Exception:
                logger.exception(f'failed to save {src_file}')
                self.report.add(self, rel_path=rel_path, code='failed')
            self._set_ref_file(src, rel_path, ref_val)


class FilesystemMirrorSaver(FilesystemSaver):
    id = 'filesystem_mirror'
    in_place = True
    enable_purge = True
    purge_delta = 0
    file_compare_method = 'shallow'


class FilesystemCopySaver(FilesystemSaver):
    id = 'filesystem_copy'
    in_place = True
    enable_purge = False
    file_compare_method = 'shallow'
