import filecmp
import os
import shutil
import time

from savegame import logger
from savegame.lib import (HOSTNAME, REF_FILENAME, check_patterns,
                          get_file_hash, get_file_size)
from savegame.savers.base import BaseSaver


BIG_FILE_LIST_DURATION = 30
BIG_FILE_SIZE = 1024 * 1024 * 1024


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


class LocalSaver(BaseSaver):
    id = 'local'
    hostname = HOSTNAME

    def _is_file_valid(self, file):
        return (os.path.basename(file) != REF_FILENAME
                and check_patterns(file, self.inclusions, self.exclusions))

    def _get_src_and_files(self):
        if self.src_type != 'local':
            return self.src, set()
        if os.path.isfile(self.src):
            return os.path.dirname(self.src), {self.src}
        start_ts = time.time()
        files = {f for f in walk_files(self.src) if self._is_file_valid(f)}
        duration = time.time() - start_ts
        if duration > BIG_FILE_LIST_DURATION:
            logger.warning(f'listed {len(files)} files from {self.src} '
                           f'(inclusions: {self.inclusions}, exclusions: {self.exclusions}) '
                           f'in {duration:.02f} seconds')
        return self.src, files

    def compare_files_and_get_ref_value(self, src_file, dst_file):
        src_hash = get_file_hash(src_file)
        return src_hash == get_file_hash(dst_file), src_hash

    def do_run(self):
        src, src_files = self._get_src_and_files()
        self.report.add('files', self.src, src_files)
        self.ref.src = src
        self.ref.files = {}
        for src_file in src_files:
            rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, rel_path)
            self.dst_paths.add(dst_file)
            equal, ref_value = self.compare_files_and_get_ref_value(src_file, dst_file)
            try:
                if not equal:
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    file_size = get_file_size(src_file)
                    if file_size > BIG_FILE_SIZE:
                        logger.info(f'copying {src_file} to {dst_file} ({file_size/1024/1024:.02f} MB)')
                    shutil.copy2(src_file, dst_file)
                    self.report.add('saved', self.src, src_file)
                self.ref.files[rel_path] = ref_value
            except Exception:
                self.report.add('failed', self.src, src_file)
                logger.exception(f'failed to save {src_file}')


class LocalInPlaceSaver(LocalSaver):
    id = 'local_in_place'
    hostname = HOSTNAME
    in_place = True

    def compare_files_and_get_ref_value(self, src_file, dst_file):
        if not os.path.exists(dst_file):
            return False, None
        return filecmp.cmp(src_file, dst_file, shallow=True), None
