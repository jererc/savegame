from glob import glob
import logging
import os
import subprocess
import time

from savegame.lib import remove_path
from savegame.savers.base import BaseSaver

logger = logging.getLogger(__name__)


class Git:
    def __init__(self, path):
        self.path = path

    def is_repo(self):
        try:
            subprocess.run(['git', '-C', self.path, 'rev-parse', '--is-inside-work-tree'],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        except subprocess.CalledProcessError:
            return False

    def bundle(self, dst_file):
        try:
            subprocess.run(['git', '-C', self.path, 'bundle', 'create', dst_file, '--branches'],
                           check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise Exception(e.stderr)


class GitSaver(BaseSaver):
    id = 'git'
    in_place = False
    enable_purge = True

    def _get_dst_file(self, name):
        return os.path.join(self.dst, f'{name}-{int(time.time())}.bundle')

    def _clean_dst_files(self, name):
        files = sorted(glob(os.path.join(self.dst, f'{name}-*.bundle')))
        max_versions = self.save_item.max_versions or 1
        if len(files) >= max_versions:
            list(map(remove_path, files[:-max_versions]))

    def do_run(self):
        for src_path in sorted(glob(os.path.join(self.src, '*'))):
            if not os.path.isdir(src_path):
                continue
            git = Git(src_path)
            if not git.is_repo():
                continue
            name = os.path.basename(src_path)
            dst_file = self._get_dst_file(name)
            self.register_dst_file(dst_file)
            tmp_file = os.path.join(self.dst, f'{name}_tmp.bundle')
            remove_path(tmp_file)
            os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
            try:
                git.bundle(tmp_file)
            except Exception as e:
                logger.error(f'failed to create bundle for {src_path}: {e}')
                continue
            remove_path(dst_file)
            os.rename(tmp_file, dst_file)
            logger.debug(f'created bundle for {src_path} to {dst_file}')
            self.report.add('saved', self.src, src_path)
            self._clean_dst_files(name)
