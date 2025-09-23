import os

from savegame.loaders.filesystem import FilesystemLoader
from savegame.savers.git import Git


class GitLoader(FilesystemLoader):
    id = 'git'

    def _load_from_save_ref(self, save_ref):
        for src, files in save_ref.files.items():
            for rel_path, ref_val in files.items():
                if not (rel_path.endswith('.bundle') and isinstance(ref_val, (int, float))):
                    continue
                repo_dir = os.path.join(src, os.path.splitext(rel_path)[0])
                if os.path.exists(repo_dir):
                    continue
                if self.dry_run:
                    self.report.add(self, save_ref=save_ref, src=src, rel_path=rel_path, code='loadable')
                    continue
                os.makedirs(os.path.dirname(repo_dir), exist_ok=True)
                Git(repo_dir).clone_bundle(os.path.join(save_ref.dst, rel_path))

        super()._load_from_save_ref(save_ref)
