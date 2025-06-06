import filecmp
import os
import shutil
import time

from savegame import logger
from savegame.lib import (HOSTNAME, REF_FILENAME, check_patterns,
                          get_file_hash, get_file_mtime, get_file_size)
from savegame.savers.base import BaseSaver


BIG_FILE_COUNT = 1000
BIG_FILE_SIZE = 1000000000


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
        src = self.src
        if os.path.isfile(src):
            files = [src]
            src = os.path.dirname(src)
        else:
            start_time = time.time()
            files = {f for f in walk_files(src) if self._is_file_valid(f)}
            if len(files) > 0:
                logger.info(f'listed {len(files)} files from {src} '
                            f'(inclusions: {self.inclusions}, exclusions: {self.exclusions}) '
                            f'in {time.time() - start_time:.02f} seconds')
        return src, files

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
        equal = (get_file_size(src_file) == get_file_size(dst_file)
                 and get_file_mtime(src_file) == get_file_mtime(dst_file))
        return equal, None


class LocalInPlaceSaver2(LocalInPlaceSaver):
    id = 'local_in_place2'

    def compare_files_and_get_ref_value(self, src_file, dst_file):
        if not os.path.exists(dst_file):
            return False, None
        return filecmp.cmp(src_file, dst_file, shallow=True), None
