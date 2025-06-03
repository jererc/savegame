import os
import shutil

from savegame import logger
from savegame.lib import (HOSTNAME, REF_FILENAME, check_patterns,
                          get_file_hash, get_file_size)
from savegame.savers.base import BaseSaver


BIG_FILE_SIZE = 1000000000


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


class LocalSaver(BaseSaver):
    id = 'local'
    hostname = HOSTNAME
    purge = True

    def _get_src_and_files(self):
        def is_valid(file):
            return (os.path.basename(file) != REF_FILENAME
                and check_patterns(file, self.inclusions, self.exclusions))

        if self.src_type == 'local':
            src = self.src
            if os.path.isfile(src):
                files = [src]
                src = os.path.dirname(src)
            else:
                files = list(walk_files(src))
            return src, {f for f in files if is_valid(f)}
        return self.src, set()

    def do_run(self):
        src, src_files = self._get_src_and_files()
        self.report.add('files', self.src, src_files)
        self.ref.src = src
        self.ref.files = {}
        for src_file in src_files:
            rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, rel_path)
            self.dst_paths.add(dst_file)
            src_hash = get_file_hash(src_file)
            try:
                if src_hash != get_file_hash(dst_file):
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    file_size = os.path.getsize(src_file)
                    if file_size > BIG_FILE_SIZE:
                        logger.info(f'copying {src_file} to {dst_file} ({file_size/1024/1024:.02f} MB)')
                    shutil.copy2(src_file, dst_file)
                    self.report.add('saved', self.src, src_file)
                self.ref.files[rel_path] = src_hash
            except Exception:
                self.report.add('failed', self.src, src_file)
                logger.exception(f'failed to save {src_file}')


class LocalInPlaceSaver(LocalSaver):
    id = 'local_in_place'
    hostname = HOSTNAME
    purge = False
    in_place = True

    def do_run(self):
        src, src_files = self._get_src_and_files()
        self.report.add('files', self.src, src_files)
        self.ref.src = src
        self.ref.files = {}
        for src_file in src_files:
            rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, rel_path)
            self.dst_paths.add(dst_file)
            src_file_size = get_file_size(src_file)
            try:
                if get_file_size(dst_file, default=None) != src_file_size:
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    if src_file_size > BIG_FILE_SIZE:
                        logger.info(f'copying {src_file} to {dst_file} ({src_file_size/1024/1024:.02f} MB)')
                    shutil.copy2(src_file, dst_file)
                    self.report.add('saved', self.src, src_file)
                self.ref.files[rel_path] = None
            except Exception:
                self.report.add('failed', self.src, src_file)
                logger.exception(f'failed to save {src_file}')
