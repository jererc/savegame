from glob import glob
import logging
import os
from pprint import pprint
import shutil
import socket
import sys
import unittest

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
REPO_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MODULE_PATH = os.path.join(REPO_PATH, 'savegame')
sys.path.append(MODULE_PATH)
import savegame
import user_settings
import google_cloud


HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()
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


def _remove_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


def _walk_files_and_dirs(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for item in sorted(files + dirs):
            yield os.path.join(root, item)


def _print_dst_files():
    meta = savegame.MetaManager().meta
    pprint(meta)
    for dst in sorted(meta.keys()):
        pprint(sorted(list(_walk_files_and_dirs(dst))))


class BaseSavegameTestCase(unittest.TestCase):


    def setUp(self):
        assert savegame.WORK_PATH, user_settings.WORK_PATH

        for path in glob(os.path.join(savegame.WORK_PATH, '*')):
            if os.path.splitext(path)[1] == '.log':
                continue
            _remove_path(path)
        makedirs(user_settings.WORK_PATH)


class SavegameTestCase(BaseSavegameTestCase):


    def test_glob_and_exclusions(self):
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
        for i in range(2):
            savegame.savegame()
        _print_dst_files()


    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_glob_and_empty_dirs_win(self):
        dst_root = os.path.join(savegame.WORK_PATH, 'dst_root')
        makedirs(dst_root)
        savegame.SAVES = [
            {
                'src_paths': [
                    [
                        r'C:\Users\Public\Documents\*',
                        [],
                        [r'*\desktop.ini'],
                    ],
                ],
                'dst_path': dst_root,
            },
        ]
        for i in range(2):
            savegame.savegame()
        _print_dst_files()


    def test_savegame(self):
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
        for i in range(2):
            savegame.savegame()
        _print_dst_files()


class RestoregamePathUsernameTestCase(unittest.TestCase):


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


    def setUp(self):
        super().setUp()
        self.src_root = os.path.join(savegame.WORK_PATH, 'src_root')
        self.dst_root = os.path.join(savegame.WORK_PATH, 'dst_root')
        makedirs(self.dst_root)


    def _generate_src_data(self, index_start, count=2):
        for s in range(index_start, index_start + count):
            for d in range(index_start, index_start + count):
                src_d = os.path.join(self.src_root, f'src{s}', f'dir{d}')
                makedirs(src_d)
                for f in range(index_start, index_start + count):
                    with open(os.path.join(src_d, f'file{f}'), 'w') as fd:
                        fd.write(f'data_{s}-{d}-{f}')


    def _switch_dst_data_hostname(self, from_hostname, to_hostname):
        for base_dir in os.listdir(os.path.join(self.dst_root)):
            for src_type in os.listdir(os.path.join(self.dst_root, base_dir)):
                for hostname in os.listdir(os.path.join(self.dst_root, base_dir, src_type)):
                    if hostname != from_hostname:
                        continue
                    os.rename(os.path.join(self.dst_root, base_dir, src_type, hostname),
                        os.path.join(self.dst_root, base_dir, src_type, to_hostname))


    def _switch_dst_data_username(self, from_username, to_username):

        def switch_ref_path(file):
            with open(file) as fd:
                data = fd.read()
            username_str = f'{os.sep}{from_username}{os.sep}'
            if username_str not in data:
                return
            with open(file, 'w') as fd:
                fd.write(data.replace(username_str,
                    f'{os.sep}{to_username}{os.sep}'))

        for item in _walk_files_and_dirs(self.dst_root):
            if os.path.basename(item) == savegame.REF_FILE:
                switch_ref_path(item)


    def _get_src_paths(self, index_start, data_item_count=2):
        return [os.path.join(self.src_root, f'src{i}')
            for i in range(index_start, index_start + data_item_count)]


    def _savegame(self, index_start, data_item_count=2):
        self._generate_src_data(index_start=index_start, count=data_item_count)
        savegame.SAVES = [
            {
                'src_paths': self._get_src_paths(index_start, data_item_count),
                'dst_path': self.dst_root,
            },
            LINUX_SAVE if os.name == 'nt' else WIN_SAVE,
        ]
        savegame.savegame()
        _remove_path(self.src_root)


    def _restoregame(self, from_hostname=None, from_username=None):
        for i in range(2):
            savegame.restoregame(from_hostname=from_hostname,
                from_username=from_username, overwrite=False)
        print('src data:')
        src_files = list(_walk_files_and_dirs(self.src_root))
        pprint(sorted(src_files))
        _remove_path(self.src_root)
        return {r for r in src_files if os.path.basename(r).startswith('src')}


    def test_from_hostname(self):
        hostname2 = 'hostname2'
        hostname3 = 'hostname3'

        self._savegame(index_start=1)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME, to_hostname=hostname2)
        self._savegame(index_start=3)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME, to_hostname=hostname3)
        self._savegame(index_start=5)

        print('dst data:')
        pprint(sorted(list(_walk_files_and_dirs(self.dst_root))))

        src_paths = self._restoregame(from_hostname=None)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        src_paths = self._restoregame(from_hostname=hostname2)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))
        src_paths = self._restoregame(from_hostname=hostname3)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=3)))
        src_paths = self._restoregame(from_hostname='unknown')
        self.assertEqual(src_paths, set())
        savegame.list_hostnames()


    def test_from_username(self):
        username2 = 'username2'
        username3 = 'username3'

        self._savegame(index_start=1)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username2)
        self._savegame(index_start=3)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username3)
        self._savegame(index_start=5)

        print('src data:')
        pprint(sorted(list(_walk_files_and_dirs(self.src_root))))
        print('dst data:')
        pprint(sorted(list(_walk_files_and_dirs(self.dst_root))))

        src_paths = self._restoregame(from_username=None)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        src_paths = self._restoregame(from_username=username2)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)
            + self._get_src_paths(index_start=1)))
        src_paths = self._restoregame(from_username=username3)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)
            + self._get_src_paths(index_start=3)))
        src_paths = self._restoregame(from_username='unknown')
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))


#
# Integration
#


class SavegameIntegrationTestCase(BaseSavegameTestCase):


    def test_1(self):
        savegame.SAVES = [LINUX_SAVE, WIN_SAVE]
        savegame.savegame()


class RestoregameIntegrationTestCase(BaseSavegameTestCase):


    def test_1(self):
        savegame.SAVES = [LINUX_SAVE, WIN_SAVE]
        savegame.restoregame(dry_run=True)



class GoogleDriveIntegrationTestCase(BaseSavegameTestCase):


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
        for i in range(2):
            savegame.savegame()
        _print_dst_files()


class GoogleContactsIntegrationTestCase(BaseSavegameTestCase):


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
        for i in range(2):
            savegame.savegame()
        _print_dst_files()


class GoogleBookmarksIntegrationTestCase(BaseSavegameTestCase):


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
        for i in range(2):
            savegame.savegame()
        _print_dst_files()


if __name__ == '__main__':
    unittest.main()
