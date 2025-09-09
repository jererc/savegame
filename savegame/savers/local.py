import filecmp
import logging
import os
import shutil
import time

from savegame.lib import REF_FILENAME, check_patterns, get_file_hash, get_file_size
from savegame.savers.base import BaseSaver


LIST_DURATION_THRESHOLD = 30
COPY_DURATION_THRESHOLD = 30

logger = logging.getLogger(__name__)


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


class LocalSaver(BaseSaver):
    id = 'local'

    def _is_file_valid(self, file):
        return os.path.basename(file) != REF_FILENAME and check_patterns(file, self.inclusions, self.exclusions)

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
        if duration > LIST_DURATION_THRESHOLD:
            logger.warning(f'listed {len(files)} files from {self.src} '
                           f'(inclusions: {self.inclusions}, exclusions: {self.exclusions}) '
                           f'in {duration:.02f} seconds')
        return src, files

    def _check_dst_volume(self):
        if self.save_item.dst_volume_path and not os.path.exists(self.save_item.dst_volume_path):
            raise Exception(f'volume {self.save_item.dst_volume_path} does not exist')

    def compare_files_and_get_ref_value(self, src_file, dst_file):
        src_hash = get_file_hash(src_file)
        return src_hash == get_file_hash(dst_file), src_hash

    def do_run(self):
        src, src_files = self._get_src_and_files()
        self.report.add('files', self.src, src_files)
        self.ref.src = src
        self.ref.files = {}
        for src_file in sorted(src_files):
            self._check_dst_volume()
            rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, rel_path)
            self.register_dst_file(dst_file)
            try:
                equal, ref_value = self.compare_files_and_get_ref_value(src_file, dst_file)
                if not equal:
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    file_size = get_file_size(src_file)
                    logger.info(f'copying {src_file} to {dst_file} ({file_size/1024/1024:.02f} MB)')
                    start_ts = time.time()
                    shutil.copy2(src_file, dst_file)
                    duration = time.time() - start_ts
                    if duration > COPY_DURATION_THRESHOLD:
                        logger.warning(f'copied {src_file} to {dst_file} ({file_size/1024/1024:.02f} MB) in {duration:.02f} seconds')
                    self.report.add('saved', self.src, src_file)
                self.ref.files[rel_path] = ref_value
            except Exception:
                self.report.add('failed', self.src, src_file)
                logger.exception(f'failed to save {src_file}')


class LocalInPlaceSaver(LocalSaver):
    id = 'local_in_place'
    in_place = True

    def compare_files_and_get_ref_value(self, src_file, dst_file):
        if not os.path.exists(dst_file):
            return False, None
        return filecmp.cmp(src_file, dst_file, shallow=True), None
