import os
from pprint import pprint
import unittest

from tests.test_savegame import BaseTestCase, savegame


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


class LoadgameIntegrationTestCase(BaseTestCase):
    def test_1(self):
        saves = [LINUX_SAVE, WIN_SAVE]
        self._loadgame(dry_run=True)
        pprint(self._list_src_root_paths())


class GoogleAutoauthIntegrationTestCase(BaseTestCase):
    def test_1(self):
        try:
            savegame.get_google_cloud(self.config,
                headless=True).get_oauth_creds()
        except Exception:
            savegame.get_google_cloud(self.config,
                headless=False).get_oauth_creds()


class GoogleDriveExportIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.google_oauth(self.config)
        saves = [
            {
                'saver_id': 'google_drive_export',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            pprint(self._list_dst_root_paths())


class GoogleContactsExportIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.google_oauth(self.config)
        saves = [
            {
                'saver_id': 'google_contacts_export',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            pprint(self._list_dst_root_paths())


class BookmarksExportIntegrationTestCase(BaseTestCase):
    def test_1(self):
        saves = [
            {
                'saver_id': 'bookmarks_export',
                'dst_path': self.dst_root,
            },
        ]
        for i in range(2):
            self._savegame(saves, force=True)
            pprint(self._list_dst_root_paths())
