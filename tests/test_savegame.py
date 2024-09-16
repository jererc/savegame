import logging
import os
from pprint import pprint
import shutil
import sys
import unittest

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
REPO_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MODULE_PATH = os.path.join(REPO_PATH, 'savegame')
sys.path.append(MODULE_PATH)
import savegame
import user_settings
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

savegame.logger.setLevel(logging.DEBUG)
makedirs = lambda x: None if os.path.exists(x) else os.makedirs(x)


def _walk_files_and_dirs(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for item in sorted(files + dirs):
            yield os.path.join(root, item)


class BaseSavegameTestCase(unittest.TestCase):


    def setUp(self):
        assert savegame.WORK_PATH, user_settings.WORK_PATH
        shutil.rmtree(user_settings.WORK_PATH)
        makedirs(user_settings.WORK_PATH)


class SavegameExclusionTestCase(BaseSavegameTestCase):


    def test_1(self):
        src_root = os.path.join(savegame.WORK_PATH, 'src_root')
        dst_root = os.path.join(savegame.WORK_PATH, 'dst_root')
        for s in range(3):
            for d in range(3):
                src_d = os.path.join(src_root, f'src{s}', f'dir{d}')
                makedirs(src_d)
                for f in range(3):
                    with open(os.path.join(src_d, f'file{f}'), 'w') as fd:
                        fd.write(f'data_{s}-{d}-{f}')

        makedirs(dst_root)

        savegame.SAVES = [
            {
                'src_paths': [
                    [
                        os.path.join(src_root, '*'),
                        [],
                        ['*/src0', '*/dir0', '*/file0'],
                    ],
                ],
                'dst_path': dst_root,
            },
            LINUX_SAVE if os.name == 'nt' else WIN_SAVE,
        ]
        savegame.savegame()

        shutil.rmtree(src_root)
        dst_hostname = 'oldhost'
        for base_dir in os.listdir(os.path.join(dst_root)):
            for src_type in os.listdir(os.path.join(dst_root, base_dir)):
                for hostname in os.listdir(os.path.join(dst_root, base_dir, src_type)):
                    os.rename(os.path.join(dst_root, base_dir, src_type, hostname),
                        os.path.join(dst_root, base_dir, src_type, dst_hostname))
                    print(f'renamed host to {dst_hostname}')

        from_username = None
        for i in range(2):
            savegame.restoregame(from_hostname=dst_hostname,
                from_username=from_username, overwrite=False)


class RealSavegameTestCase(BaseSavegameTestCase):


    def test_1(self):
        savegame.SAVES = [LINUX_SAVE, WIN_SAVE]
        savegame.savegame()


class RealRestoregameTestCase(BaseSavegameTestCase):


    def test_1(self):
        savegame.SAVES = [LINUX_SAVE, WIN_SAVE]
        savegame.restoregame(dry_run=True)


class RestoregamePathUsernameTestCase(BaseSavegameTestCase):


    def setUp(self):
        self.username = os.getlogin()
        self.other_username = f'not{self.username}'


    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_win(self):
        obj = savegame.RestoreItem('dst_path')
        path = r'C:\Program Files\some_dir'
        self.assertEqual(obj._replace_username_in_path(path), path)
        path = r'C:\Users\Public\some_dir'
        self.assertEqual(obj._replace_username_in_path(path), path)

        obj = savegame.RestoreItem('dst_path', from_username=self.other_username)
        path = rf'C:\Users\{self.other_username}\some_dir'
        self.assertEqual(obj._replace_username_in_path(path),
            rf'C:\Users\{self.username}\some_dir')


    @unittest.skipIf(os.name != 'posix', 'not linux')
    def test_posix(self):
        obj = savegame.RestoreItem('dst_path')
        path = '/var/some_dir'
        self.assertEqual(obj._replace_username_in_path(path), path)
        path = f'/home/root/some_dir'
        self.assertEqual(obj._replace_username_in_path(path), path)

        obj = savegame.RestoreItem('dst_path', from_username=self.other_username)
        path = f'/home/{self.other_username}/some_dir'
        self.assertEqual(obj._replace_username_in_path(path),
            f'/home/{self.username}/some_dir')


class RestoregameTestCase(BaseSavegameTestCase):


    def test_1(self):
        src_root = os.path.join(savegame.WORK_PATH, 'src_root')
        for s in range(2):
            for d in range(2):
                src_d = os.path.join(src_root, f'src{s}', f'dir{d}')
                makedirs(src_d)
                for f in range(2):
                    with open(os.path.join(src_d, f'file{f}'), 'w') as fd:
                        fd.write(f'data_{s}-{d}-{f}')
        dst_root = os.path.join(savegame.WORK_PATH, 'dst_root')
        makedirs(dst_root)
        savegame.SAVES = [
            {
                'src_paths': [
                    os.path.join(src_root, 'src0'),
                    os.path.join(src_root, 'src1'),
                ],
                'dst_path': dst_root,
            },
            LINUX_SAVE if os.name == 'nt' else WIN_SAVE,
        ]
        savegame.savegame()

        shutil.rmtree(src_root)
        dst_hostname = 'oldhost'
        for base_dir in os.listdir(os.path.join(dst_root)):
            for src_type in os.listdir(os.path.join(dst_root, base_dir)):
                for hostname in os.listdir(os.path.join(dst_root, base_dir, src_type)):
                    os.rename(os.path.join(dst_root, base_dir, src_type, hostname),
                        os.path.join(dst_root, base_dir, src_type, dst_hostname))
                    print(f'renamed host to {dst_hostname}')

        from_username = None
        for i in range(2):
            savegame.restoregame(from_hostname=dst_hostname,
                from_username=from_username, overwrite=False)


class GoogleDriveTestCase(BaseSavegameTestCase):


    def test_1(self):
        creds_file = os.path.realpath(os.path.expanduser(
            '~/data/credentials_oauth.json'))
        google_cloud.GoogleCloud(oauth_creds_file=creds_file
            ).get_oauth_creds(interact=True)
        dst_path = os.path.join(savegame.WORK_PATH, 'dst')
        makedirs(dst_path)
        with open(os.path.join(dst_path, 'old_file.docx'), 'w') as fd:
            fd.write('old content')

        savegame.SAVES = [
            {
                'src_type': 'google_drive',
                'dst_path': dst_path,
                'gc_oauth_creds_file': creds_file,
                'retention_delta': 0,
            },
        ]
        savegame.savegame()
        pprint(savegame.MetaManager().meta)
        meta = savegame.MetaManager().meta
        pprint(meta)
        res = sorted(list(_walk_files_and_dirs(list(meta.keys())[0])))
        pprint(res)


class GoogleContactsTestCase(BaseSavegameTestCase):


    def test_1(self):
        creds_file = os.path.realpath(os.path.expanduser(
            '~/data/credentials_oauth.json'))
        google_cloud.GoogleCloud(oauth_creds_file=creds_file
            ).get_oauth_creds(interact=True)
        dst_path = os.path.join(savegame.WORK_PATH, 'dst')
        makedirs(dst_path)
        savegame.SAVES = [
            {
                'src_type': 'google_contacts',
                'dst_path': dst_path,
                'gc_oauth_creds_file': creds_file,
            },
        ]
        savegame.savegame()
        pprint(savegame.MetaManager().meta)
        meta = savegame.MetaManager().meta
        pprint(meta)
        res = sorted(list(_walk_files_and_dirs(list(meta.keys())[0])))
        pprint(res)


class GoogleBookmarksTestCase(BaseSavegameTestCase):


    def test_1(self):
        dst_path = os.path.join(savegame.WORK_PATH, 'dst')

        old_dir = os.path.join(dst_path, 'old_dir')
        makedirs(old_dir)
        with open(os.path.join(old_dir, 'old_file.html'), 'w') as fd:
            fd.write('old content')

        savegame.SAVES = [
            {
                'src_type': 'google_bookmarks',
                'dst_path': dst_path,
                'retention_delta': 0,
            },
        ]
        savegame.savegame()
        meta = savegame.MetaManager().meta
        pprint(meta)
        res = sorted(list(_walk_files_and_dirs(list(meta.keys())[0])))
        pprint(res)


if __name__ == '__main__':
    unittest.main()
