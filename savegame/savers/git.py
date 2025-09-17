from copy import deepcopy
from glob import glob
import hashlib
import logging
import os
import subprocess
import shutil

from savegame.lib import get_file_hash, remove_path
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

    def get_state_hash(self):
        try:
            result = subprocess.run(
                ['git', '-C', self.path, 'for-each-ref', '--format=%(objectname) %(refname)', 'refs/heads', 'refs/tags'],
                check=True,
                capture_output=True,
                text=True
            )
            normalized = '\n'.join(sorted(result.stdout.strip().splitlines()))
            return hashlib.md5(normalized.encode("utf-8")).hexdigest()
        except subprocess.CalledProcessError as e:
            raise Exception(e.stderr)

    def list_non_committed_files(self):
        def git(*args):
            res = subprocess.run(['git', *args], cwd=self.path, capture_output=True, text=True, check=True)
            return res.stdout.strip().splitlines()

        staged = git('diff', '--cached', '--name-only')
        unstaged = git('diff', '--name-only')
        untracked = git('ls-files', '--others', '--exclude-standard')
        return {os.path.join(self.path, f) for f in staged + unstaged + untracked}


class GitSaver(BaseSaver):
    id = 'git'
    in_place = False
    enable_purge = True

    def do_run(self):
        self.ref.src = self.src
        ref_files = deepcopy(self.ref.files)
        self.ref.files = {}
        for src_path in sorted(glob(os.path.join(self.src, '*'))):
            if not os.path.isdir(src_path):
                continue
            git = Git(src_path)
            if not git.is_repo():
                continue
            name = os.path.basename(src_path)
            rel_path = f'{name}.bundle'
            dst_file = os.path.join(self.dst, rel_path)
            self.register_dst_file(dst_file)

            state_hash = git.get_state_hash()
            if state_hash != ref_files.get(rel_path):
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
            self.ref.files[rel_path] = state_hash

            for src_file in sorted(git.list_non_committed_files()):
                src_hash = get_file_hash(src_file)
                rel_path = os.path.relpath(src_file, self.src)
                dst_file = os.path.join(self.dst, rel_path)
                self.register_dst_file(dst_file)
                dst_hash = get_file_hash(dst_file)
                if dst_hash != src_hash:
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    shutil.copy2(src_file, dst_file)
                    self.report.add('saved', self.src, src_file)
                self.ref.files[rel_path] = dst_hash
