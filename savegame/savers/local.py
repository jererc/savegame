import os
from pprint import pformat
import shutil
import time

from savegame import logger
from savegame.lib import HOSTNAME, REF_FILENAME, check_patterns, get_file_hash
from savegame.savers.base import BaseSaver


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            yield os.path.join(root, file)


class LocalSaver(BaseSaver):
    id = 'local'
    hostname = HOSTNAME

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
        debug_data = {
            'src': self.src,
            'dst': self.dst,
            'hash_duration': 0,
            'copy_duration': 0,
            'copy_count': 0,
            'file_count': 0,
            'list_files_duration': 0,
        }
        t0 = time.time()
        src, src_files = self._get_src_and_files()
        debug_data['file_count'] = len(src_files)
        debug_data['list_files_duration'] = time.time() - t0
        self.report.add('files', self.src, src_files)
        self.ref.src = src
        self.ref.files = {}
        for src_file in src_files:
            rel_path = os.path.relpath(src_file, src)
            dst_file = os.path.join(self.dst, rel_path)
            self.dst_paths.add(dst_file)
            t0 = time.time()
            src_hash = get_file_hash(src_file)
            dst_hash = get_file_hash(dst_file)
            debug_data['hash_duration'] += time.time() - t0
            try:
                if src_hash != dst_hash:
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    t0 = time.time()
                    shutil.copy2(src_file, dst_file)
                    debug_data['copy_duration'] += time.time() - t0
                    debug_data['copy_count'] += 1
                    self.report.add('saved', self.src, src_file)
                self.ref.files[rel_path] = src_hash
            except Exception:
                self.report.add('failed', self.src, src_file)
                logger.exception(f'failed to save {src_file}')
        if len(src_files) > 100:
            logger.info(f'debug data:\n{pformat(debug_data)}')
