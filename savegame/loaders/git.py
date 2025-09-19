import os

from savegame.loaders.filesystem import FilesystemLoader
from savegame.savers.git import Git


class GitLoader(FilesystemLoader):
    id = 'git'

    def _load_from_ref(self, ref):
        for rel_path, ref_val in ref.files.items():
            if not (rel_path.endswith('.bundle') and isinstance(ref_val, (int, float))):
                continue
            repo_dir = os.path.join(ref.src, os.path.splitext(rel_path)[0])
            if os.path.exists(repo_dir):
                continue
            if self.dry_run:
                self.report.add('loadable', ref.src, repo_dir)
                continue
            Git(repo_dir).clone_bundle(os.path.join(ref.dst, rel_path))

        super()._load_from_ref(ref)
