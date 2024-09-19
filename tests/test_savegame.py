from glob import glob
import json
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

savegame.logger.setLevel(logging.DEBUG)
makedirs = lambda x: None if os.path.exists(x) else os.makedirs(x)


def remove_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


def walk_paths(path):
    for root, dirs, files in os.walk(path, topdown=False):
        for item in sorted(files + dirs):
            yield os.path.join(root, item)


def walk_files(path):
    for root, dirs, files in os.walk(path):
        for file in sorted(files):
            yield os.path.join(root, file)


def any_str_contains(strings, substring):
    for s in strings:
        if substring in s:
            return True
    return False


def print_dst_files():
    meta = savegame.MetaManager().meta
    pprint(meta)
    for dst in sorted(meta.keys()):
        pprint(set(walk_paths(dst)))


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


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        assert savegame.WORK_PATH, user_settings.WORK_PATH

        for path in glob(os.path.join(savegame.WORK_PATH, '*')):
            if os.path.splitext(path)[1] == '.log':
                continue
            remove_path(path)
        makedirs(user_settings.WORK_PATH)

        self.src_root = os.path.join(savegame.WORK_PATH, 'src_root')
        self.dst_root = os.path.join(savegame.WORK_PATH, 'dst_root')
        makedirs(self.dst_root)

    def _generate_src_data(self, index_start, src_count=2, dir_count=2,
            file_count=2, file_version=1):
        for s in range(index_start, index_start + src_count):
            s_name = f'src{s}'
            for d in range(index_start, index_start + dir_count):
                d_name = f'dir{d}'
                src_d = os.path.join(self.src_root, s_name, d_name)
                makedirs(src_d)
                for f in range(index_start, index_start + file_count):
                    with open(os.path.join(src_d, f'file{f}'), 'w') as fd:
                        content = {
                            'src': s_name,
                            'dir': d_name,
                            'version': file_version,
                        }
                        fd.write(json.dumps(content, indent=4, sort_keys=True))

    def _get_src_paths(self, index_start=1, src_count=2, **kwargs):
        return [os.path.join(self.src_root, f'src{i}')
            for i in range(index_start, index_start + src_count)]

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

        for item in walk_paths(self.dst_root):
            if os.path.basename(item) == savegame.REF_FILE:
                switch_ref_path(item)

    def _savegame(self, **kwargs):
        self._generate_src_data(**kwargs)
        savegame.SAVES = [
            {
                'src_paths': self._get_src_paths(**kwargs),
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame()
        src_files = set(walk_paths(self.src_root))
        remove_path(self.src_root)
        return src_files


class SavegameTestCase(BaseTestCase):
    def test_glob_and_exclusions(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        savegame.SAVES = [
            {
                'src_paths': [
                    [
                        os.path.join(self.src_root, '*'),
                        [],
                        ['*/src2', '*/dir2/*', '*/file2'],
                    ],
                ],
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame()
        dst_files = set(walk_paths(self.dst_root))
        pprint(dst_files)
        self.assertFalse(any_str_contains(dst_files, 'src2'))
        self.assertFalse(any_str_contains(dst_files, 'dir2'))
        self.assertFalse(any_str_contains(dst_files, 'file2'))
        self.assertTrue(any_str_contains(dst_files, 'src1'))
        self.assertTrue(any_str_contains(dst_files, 'src3'))
        self.assertTrue(any_str_contains(dst_files, 'dir1'))
        self.assertTrue(any_str_contains(dst_files, 'dir3'))
        self.assertTrue(any_str_contains(dst_files, 'file1'))
        self.assertTrue(any_str_contains(dst_files, 'file3'))

    def test_savegame(self):
        src_files = self._savegame(index_start=1, file_count=2)
        pprint(src_files)
        dst_files = set(walk_paths(self.dst_root))
        pprint(dst_files)
        self.assertTrue(any_str_contains(dst_files, 'src1'))
        self.assertTrue(any_str_contains(dst_files, 'src2'))
        self.assertTrue(any_str_contains(dst_files, 'dir1'))
        self.assertTrue(any_str_contains(dst_files, 'dir2'))
        self.assertTrue(any_str_contains(dst_files, 'file1'))
        self.assertTrue(any_str_contains(dst_files, 'file2'))


class RestoregameTestCase(BaseTestCase):
    def _list_src_paths(self):
        print('src data:')
        src_files = set(walk_paths(self.src_root))
        pprint(src_files)
        return {r for r in src_files if os.path.basename(r).startswith('src')}

    def _restoregame(self, **kwargs):
        for i in range(2):
            savegame.restoregame(**kwargs)
        src_paths = self._list_src_paths()
        remove_path(self.src_root)
        return src_paths

    def test_multiple_versions(self):
        hostname2 = 'hostname2'
        hostname3 = 'hostname3'

        self._savegame(index_start=1, file_count=6, file_version=1)
        self._savegame(index_start=1, file_count=3, file_version=2)
        self._savegame(index_start=1, file_count=1, file_version=3)

        print('dst data:')
        pprint(set(walk_paths(self.dst_root)))

        src_paths = self._restoregame(from_hostname=None)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))

    def test_skipped_identical(self):
        src_files = self._savegame(index_start=1, file_count=2)
        pprint(src_files)

        savegame.restoregame(overwrite=False)
        src_files2 = set(walk_paths(self.src_root))
        pprint(src_files2)
        self.assertEqual(src_files2, src_files)

        savegame.restoregame(overwrite=False)
        src_files3 = set(walk_paths(self.src_root))
        pprint(src_files3)
        self.assertEqual(src_files3, src_files)

    def test_skipped_conflict(self):
        src_files = self._savegame(index_start=1, file_count=2)
        pprint(src_files)

        savegame.restoregame(overwrite=False)
        src_files2 = set(walk_paths(self.src_root))
        pprint(src_files2)
        self.assertEqual(src_files2, src_files)
        for file in walk_files(self.src_root):
            with open(file) as fd:
                content = fd.read()
            with open(file, 'w') as fd:
                fd.write(content + file)

        savegame.restoregame(overwrite=False)
        src_files3 = set(walk_paths(self.src_root))
        pprint(src_files3)
        self.assertEqual(src_files3, src_files)

        savegame.restoregame(overwrite=True)
        src_files4 = set(walk_paths(self.src_root))
        pprint(src_files4)
        diff = src_files4 - src_files
        self.assertTrue(diff)
        self.assertTrue(all(os.path.splitext(f)[-1] == '.savegamebak'
            for f in diff))

    def test_from_hostname(self):
        hostname2 = 'hostname2'
        hostname3 = 'hostname3'

        self._savegame(index_start=1)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME, to_hostname=hostname2)
        self._savegame(index_start=3)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME, to_hostname=hostname3)
        self._savegame(index_start=5)

        print('dst data:')
        pprint(set(walk_paths(self.dst_root)))

        src_paths = self._restoregame(from_hostname=None)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        src_paths = self._restoregame(from_hostname=hostname2)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))
        src_paths = self._restoregame(from_hostname=hostname3)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=3)))
        src_paths = self._restoregame(from_hostname='unknown')
        self.assertEqual(src_paths, set())
        savegame.list_hostnames()

    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_shared_username_path(self):
        username2 = 'Public' if os.name == 'nt' else 'shared'
        username3 = 'username3'

        self._savegame(index_start=1)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username2)
        self._savegame(index_start=3)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username3)
        self._savegame(index_start=5)

        print('src data:')
        pprint(set(walk_paths(self.src_root)))
        print('dst data:')
        pprint(set(walk_paths(self.dst_root)))

        src_paths = self._restoregame(from_username=None)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)
            + self._get_src_paths(index_start=5)))
        src_paths = self._restoregame(from_username=username3)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)
            + self._get_src_paths(index_start=3)
            + self._get_src_paths(index_start=5)))

    def test_from_username(self):
        username2 = 'username2'
        username3 = 'username3'

        self._savegame(index_start=1)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username2)
        self._savegame(index_start=3)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username3)
        self._savegame(index_start=5)

        print('src data:')
        pprint(set(walk_paths(self.src_root)))
        print('dst data:')
        pprint(set(walk_paths(self.dst_root)))

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
        print_dst_files()

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
        print_dst_files()


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
        print_dst_files()


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
            print_dst_files()


if __name__ == '__main__':
    unittest.main()
