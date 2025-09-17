import os
from pprint import pprint
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
