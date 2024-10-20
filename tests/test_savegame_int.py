import os
from pprint import pprint
import unittest

from .test_savegame import BaseTestCase, savegame


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
    'dst_path': '/home/jererc/OneDrive/data',
}
WIN_SAVE = {
    'src_paths': [
        r'C:\ProgramData\Microsoft\Windows\Start Menu\Programs\Games',
        r'C:\Users\jerer\AppData\Roaming\Sublime Text 3',
    ],
    'dst_path': r'C:\Users\jerer\OneDrive\data',
}


class SavegameIntegrationTestCase(BaseTestCase):
    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_glob_and_empty_dirs_win(self):
        savegame.SAVES = [
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
            savegame.savegame(force=True)
            pprint(self._list_dst_root_paths())

    def test_1(self):
        savegame.SAVES = [LINUX_SAVE, WIN_SAVE]
        for i in range(2):
            savegame.savegame(force=True)
            pprint(self._list_dst_root_paths())


class RestoregameIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.SAVES = [LINUX_SAVE, WIN_SAVE]
        savegame.restoregame(dry_run=True)
        pprint(self._list_src_root_paths())


class GoogleDriveIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.google_oauth()
        savegame.SAVES = [
            {
                'saver_id': 'google_drive',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            savegame.savegame(force=True)
            pprint(self._list_dst_root_paths())


class GoogleDriveUploadIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.google_oauth()
        savegame.SAVES = [
            {
                'saver_id': 'google_drive_upload',
                'src_paths': [
                    [
                        r'~/OneDrive/data/conf',
                        ['*/microsoft*'],
                        [],
                    ],
                ],
                'dst_path': 'My Drive',
            },
        ]
        for i in range(2):
            savegame.savegame(force=True)


class GoogleContactsDownloadIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.google_oauth()
        savegame.SAVES = [
            {
                'saver_id': 'google_contacts_download',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            savegame.savegame(force=True)
            pprint(self._list_dst_root_paths())


class ChromiumBookmarksIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.SAVES = [
            {
                'saver_id': 'chromium_bookmarks',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            savegame.savegame(force=True)
            pprint(self._list_dst_root_paths())
