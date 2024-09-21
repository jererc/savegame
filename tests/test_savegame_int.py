import os
from pprint import pprint
import unittest

from .test_savegame import BaseTestCase, print_dst_data
import savegame
import google_cloud


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
            savegame.savegame()
            print_dst_data()

    def test_1(self):
        savegame.SAVES = [LINUX_SAVE, WIN_SAVE]
        savegame.savegame()


class RestoregameIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.SAVES = [LINUX_SAVE, WIN_SAVE]
        savegame.restoregame(dry_run=True)


class GoogleDriveIntegrationTestCase(BaseTestCase):
    def test_1(self):
        creds_file = os.path.realpath(os.path.expanduser(
            '~/data/credentials_oauth.json'))
        google_cloud.GoogleCloud(oauth_creds_file=creds_file
            ).get_oauth_creds(interact=True)
        savegame.SAVES = [
            {
                'src_type': 'google_drive',
                'dst_path': self.dst_root,
                'creds_file': creds_file,
                'retention_delta': 0,
                'min_delta': 0,
            },
        ]
        for i in range(2):
            savegame.savegame()
            print_dst_data()


class GoogleContactsIntegrationTestCase(BaseTestCase):
    def test_1(self):
        creds_file = os.path.realpath(os.path.expanduser(
            '~/data/credentials_oauth.json'))
        google_cloud.GoogleCloud(oauth_creds_file=creds_file
            ).get_oauth_creds(interact=True)
        savegame.SAVES = [
            {
                'src_type': 'google_contacts',
                'dst_path': self.dst_root,
                'creds_file': creds_file,
                'min_delta': 0,
            },
        ]
        for i in range(2):
            savegame.savegame()
            print_dst_data()


class GoogleBookmarksIntegrationTestCase(BaseTestCase):
    def test_1(self):
        savegame.SAVES = [
            {
                'src_type': 'google_bookmarks',
                'dst_path': self.dst_root,
                'retention_delta': 0,
                'min_delta': 0,
            },
        ]
        for i in range(2):
            savegame.savegame()
            print_dst_data()
