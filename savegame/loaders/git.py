import os

from savegame.loaders.filesystem import FilesystemLoader
from savegame.savers.git import Git


class GitLoader(FilesystemLoader):
    id = 'git'

    def _clone_bundles(self):
        for ref in self._iterate_refs():
            for rel_path, ref_val in ref.files.items():
                if not (rel_path.endswith('.bundle') and isinstance(ref_val, int)):
                    continue
                bundle_file = os.path.join(ref.dst, rel_path)
                repo_dir = os.path.join(ref.src, os.path.splitext(os.path.basename(bundle_file))[0])
                if os.path.exists(repo_dir):
                    continue
                Git(repo_dir).clone_bundle(bundle_file)

    def run(self):
        self._clone_bundles()
        super().run()
