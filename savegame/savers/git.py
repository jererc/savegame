from glob import glob
import hashlib
import logging
import os
import subprocess
import shutil
import time

from savegame.savers.base import BaseSaver
from savegame.utils import remove_path

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

    def get_state_hash(self):
        res = subprocess.run(['git', '-C', self.path, 'for-each-ref', '--format=%(objectname) %(refname)', 'refs/heads', 'refs/tags'],
                             check=True, capture_output=True, text=True)
        normalized = '\n'.join(sorted(res.stdout.strip().splitlines()))
        return hashlib.md5(normalized.encode("utf-8")).hexdigest()

    def get_last_update_ts(self):
        res = subprocess.run(['git', '-C', self.path, 'for-each-ref', '--sort=-committerdate', '--count=1', '--format=%(committerdate:unix)'],
                             capture_output=True, text=True, check=True)
        return int(res.stdout.strip())

    def create_bundle(self, bundle_file):
        try:
            subprocess.run(['git', '-C', self.path, 'bundle', 'create', bundle_file, '--branches'],
                           check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise Exception(e.stderr)

    def clone_bundle(self, bundle_file):
        try:
            subprocess.run(['git', '-C', os.path.dirname(self.path), 'clone', bundle_file], check=True)
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
        file_refs = self.reset_files(self.src)
        for src_path in sorted(glob(os.path.join(self.src, '*'))):
            if not os.path.isdir(src_path):
                continue
            git = Git(src_path)
            if not git.is_repo():
                continue
            name = os.path.basename(src_path)
            rel_path = f'{name}.bundle'
            dst_file = os.path.join(self.dst, rel_path)
            ref = file_refs.get(rel_path, 0)
            new_ref = git.get_last_update_ts()
            try:
                if new_ref > ref:   # never overwrite newer files, useful after a vm restore
                    tmp_file = os.path.join(self.dst, f'{name}_tmp.bundle')
                    remove_path(tmp_file)
                    os.makedirs(os.path.dirname(tmp_file), exist_ok=True)
                    start_ts = time.time()
                    git.create_bundle(tmp_file)
                    remove_path(dst_file)
                    os.rename(tmp_file, dst_file)
                    ref = new_ref
                    self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts)
            except Exception:
                logger.exception(f'failed to create bundle for {src_path}')
                self.report.add(self, rel_path=rel_path, code='failed')
            self.set_file(self.src, rel_path, ref)

            for src_file in sorted(git.list_non_committed_files()):
                rel_path = os.path.relpath(src_file, self.src)
                dst_file = os.path.join(self.dst, rel_path)
                must_copy, new_ref, ref = self.must_copy_file(src_file, dst_file, file_refs.get(rel_path))
                if must_copy:
                    start_ts = time.time()
                    os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                    shutil.copy2(src_file, dst_file)
                    self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts)
                    ref = new_ref
                self.set_file(self.src, rel_path, ref)
