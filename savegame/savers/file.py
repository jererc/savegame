import logging
import os
import shutil
import time

from savegame.savers.base import BaseSaver
from savegame.utils import REF_FILENAME, check_patterns, get_file_size

LOG_LIST_DURATION_THRESHOLD = 30
LOG_FILE_SIZE_THRESHOLD = 10 * 1024 * 1024

logger = logging.getLogger(__name__)


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


class FileSaver(BaseSaver):
    id = 'file'
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

    def do_run(self):
        src, src_files = self._get_src_and_files()
        file_refs = self.save_ref.reset_files(src)
        for src_file in sorted(src_files):
            self._check_dst_volume()
            rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, rel_path)
            must_copy, new_ref, ref = self.must_copy_file(src_file, dst_file, file_refs.get(rel_path))
            try:
                if must_copy:
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    file_size = get_file_size(src_file)
                    if file_size > LOG_FILE_SIZE_THRESHOLD:
                        logger.info(f'copying {src_file=} to {dst_file=} ({file_size / 1024 / 1024:.02f} MB)')
                    start_ts = time.time()
                    shutil.copy2(src_file, dst_file)
                    ref = new_ref
                    self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts)
            except Exception:
                logger.exception(f'failed to copy {src_file=} to {dst_file=}')
                self.report.add(self, rel_path=rel_path, code='failed')
            self.save_ref.set_file(src, rel_path, ref)


class FileMirrorSaver(FileSaver):
    id = 'file_mirror'
    in_place = True
    enable_purge = True
    purge_delta = 0
    file_compare_method = 'shallow'


class FileCopySaver(FileSaver):
    id = 'file_copy'
    in_place = True
    enable_purge = False
    file_compare_method = 'shallow'
