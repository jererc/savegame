import os
from pprint import pprint
import unittest

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
    @unittest.skipIf(os.name != 'nt', 'not windows')
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
            pprint(self._list_dst_root_paths())

    def test_1(self):
        saves = [LINUX_SAVE, WIN_SAVE]
        for i in range(2):
            self._savegame(saves, force=True)
            pprint(self._list_dst_root_paths())


class LoadgameTestCase(BaseTestCase):
    def test_1(self):
        saves = [LINUX_SAVE, WIN_SAVE]
        self._savegame(saves, force=True)
        self._loadgame(dry_run=True)
        pprint(self._list_src_root_paths())


class GoogleDriveExportTestCase(BaseTestCase):
    def test_1(self):
        module.save.google_oauth(self.config)
        saves = [
            {
                'saver_id': 'google_drive_export',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            pprint(self._list_dst_root_paths())


class GoogleContactsExportTestCase(BaseTestCase):
    def test_1(self):
        module.save.google_oauth(self.config)
        saves = [
            {
                'saver_id': 'google_contacts_export',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            pprint(self._list_dst_root_paths())
