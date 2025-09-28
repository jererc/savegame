import os
import time

from savegame.loaders.file import FileLoader
from savegame.savers.git import Git
from savegame.utils import FileRef


class GitLoader(FileLoader):
    id = 'git'

    def _load_from_save_ref(self, save_ref):
        bundle_rel_paths = set()
        for src, file_refs in save_ref.get_files(hostname=self.hostname).items():
            for rel_path, ref in file_refs.items():
                filename, ext = os.path.splitext(rel_path)
                if ext != '.bundle':
                    continue
                if not FileRef.from_ref(ref).check_file(os.path.join(save_ref.dst, rel_path)):
                    self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='invalid')
                    continue
                bundle_rel_paths.add(rel_path)
                repo_dir = os.path.join(src, filename)
                if os.path.exists(repo_dir):
                    self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='match')
                    continue
                if self.dry_run:
                    self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='loadable')
                    continue
                os.makedirs(os.path.dirname(repo_dir), exist_ok=True)
                start_ts = time.time()
                Git(repo_dir).clone_bundle(os.path.join(save_ref.dst, rel_path))
                self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='loaded', start_ts=start_ts)

        super()._load_from_save_ref(save_ref, exclude_rel_paths=bundle_rel_paths)
