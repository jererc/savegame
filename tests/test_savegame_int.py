import os
from pprint import pprint
import shutil
import subprocess
import sys
import unittest

from tests import WORK_DIR
from tests.test_savegame import BaseTestCase, module


LINUX_SAVE = {
    'src_paths': [
        '/home/jererc/.gitconfig',
        [
            '/home/jererc/.ssh',
            [],
            ['*/id_ed25519'],
        ],
        '/home/jererc/.config/sublime-text',
    ],
    'dst_path': '/home/jererc/tmp',
}
WIN_SAVE = {
    'src_paths': [
        r'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Games',
        r'C:\Users\jerer\AppData\Roaming\Sublime Text 3',
    ],
    'dst_path': r'C:\Users\jerer\tmp',
}


class SavegameTestCase(BaseTestCase):
    @unittest.skipIf(sys.platform != 'win32', 'not windows')
    def test_glob_and_empty_dirs_win(self):
        saves = [
            {
                'src_paths': [
                    [
                        r'C:\Users\Public\Documents\*',
                        [],
                        [r'*\desktop.ini'],
                    ],
                ],
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            self._list_dst_root_paths()

    def test_1(self):
        saves = [LINUX_SAVE, WIN_SAVE]
        for i in range(2):
            self._savegame(saves, force=True)
            self._list_dst_root_paths()


class LoadgameTestCase(BaseTestCase):
    def test_1(self):
        saves = [LINUX_SAVE, WIN_SAVE]
        self._savegame(saves, force=True)
        self._loadgame(dry_run=True)
        self._list_src_root_paths()


class GoogleDriveSaverTestCase(BaseTestCase):
    def test_1(self):
        module.save.google_oauth(self.config)
        saves = [
            {
                'saver_id': 'google_drive',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            self._list_dst_root_paths()


class GoogleContactsSaverTestCase(BaseTestCase):
    def test_1(self):
        module.save.google_oauth(self.config)
        saves = [
            {
                'saver_id': 'google_contacts',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            self._list_dst_root_paths()


class GitSaverTestCase(BaseTestCase):
    def test_1(self):
        saves = [
            {
                'saver_id': 'git',
                'src_paths': [
                    '~/data/code',
                ],
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            dst_paths = self._list_dst_root_paths()
        ref_file = [f for f in dst_paths if os.path.basename(f) == module.lib.REF_FILENAME][0]
        ref = module.lib.Reference(os.path.dirname(ref_file))
        pprint(ref.data)


class GitLoaderTestCase(BaseTestCase):
    def _create_file(self, file, content):
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, 'w') as f:
            f.write(content)

    def test_1(self):
        print(f'{self.src_root=}')
        repo_dir = os.path.join(self.src_root, 'repo')
        subprocess.run(['git', 'init', repo_dir], check=True)
        self._create_file(os.path.join(repo_dir, 'dir1', 'file1.txt'), 'data1')
        subprocess.run(['git', 'add', 'dir1'], cwd=repo_dir, check=True)
        subprocess.run(['git', 'commit', '-m', 'initial commit'], cwd=repo_dir, check=True)
        self._create_file(os.path.join(repo_dir, 'dir2', 'file2.txt'), 'data2')
        subprocess.run(['git', 'add', 'dir2'], cwd=repo_dir, check=True)
        self._create_file(os.path.join(repo_dir, 'dir3', 'file3.txt'), 'data3')
        print(f'{repo_dir=}')

        saves = [
            {
                'saver_id': 'git',
                'src_paths': [
                    self.src_root,
                ],
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            dst_paths = self._list_dst_root_paths()
        ref_file = [f for f in dst_paths if os.path.basename(f) == module.lib.REF_FILENAME][0]
        ref = module.lib.Reference(os.path.dirname(ref_file))
        pprint(ref.data)
        shutil.rmtree(repo_dir)

        self._loadgame()
        self._list_src_root_paths()
