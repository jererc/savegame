from copy import deepcopy
from glob import glob
import json
import logging
import os
from pprint import pprint
import shutil
import socket
import sys
import time
import unittest
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
REPO_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MODULE_PATH = os.path.join(REPO_PATH, 'savegame')
sys.path.append(MODULE_PATH)
import savegame
import user_settings

HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()

savegame.logger.setLevel(logging.DEBUG)


def makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


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


def print_dst_data():
    meta = savegame.MetaManager().meta
    pprint(meta)
    for data in sorted(meta.values(), key=lambda x: x['dst']):
        pprint(set(walk_paths(data['dst'])))


class RestoregamePathUsernameTestCase(unittest.TestCase):
    def setUp(self):
        self.username = os.getlogin()
        self.other_username = f'not{self.username}'
        self.dst_path = os.path.dirname(__file__)

    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_win(self):
        obj = savegame.LocalRestorer(self.dst_path)
        path = r'C:\Program Files\some_dir'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = r'C:\Users\Public\some_dir'
        self.assertEqual(obj._get_src_file_for_user(path), path)

        obj = savegame.LocalRestorer(self.dst_path,
            from_username=self.other_username)
        path = rf'C:\Users\{self.other_username}\some_dir'
        self.assertEqual(obj._get_src_file_for_user(path),
            rf'C:\Users\{self.username}\some_dir')

    @unittest.skipIf(os.name != 'posix', 'not linux')
    def test_posix(self):
        obj = savegame.LocalRestorer(self.dst_path)
        path = '/var/some_dir'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = '/home/shared/some_dir'
        self.assertEqual(obj._get_src_file_for_user(path), path)

        obj = savegame.LocalRestorer(self.dst_path,
            from_username=self.other_username)
        path = f'/home/{self.other_username}/some_dir'
        self.assertEqual(obj._get_src_file_for_user(path),
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

        savegame.MetaManager().meta = {}
        savegame.HashManager().cache = {}

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

    def _list_src_root_paths(self):
        return set(walk_paths(self.src_root))

    def _list_src_root_src_paths(self):
        return {r for r in self._list_src_root_paths()
            if os.path.basename(r).startswith('src')}

    def _list_dst_root_paths(self):
        return set(walk_paths(self.dst_root))

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
            rd = savegame.ReferenceData(os.path.dirname(file))
            ref_data = rd.load()
            username_str = f'{os.sep}{from_username}{os.sep}'
            if username_str not in ref_data['src']:
                return
            ref_data['src'] = ref_data['src'].replace(username_str,
                f'{os.sep}{to_username}{os.sep}')
            rd.save(ref_data)

        for path in walk_paths(self.dst_root):
            if os.path.basename(path) == savegame.REF_FILE:
                switch_ref_path(path)

    def _savegame(self, min_delta=0, **kwargs):
        self._generate_src_data(**kwargs)
        savegame.SAVES = [
            {
                'src_paths': self._get_src_paths(**kwargs),
                'dst_path': self.dst_root,
                'min_delta': min_delta,
            },
        ]
        savegame.savegame()

    def _restoregame(self, **kwargs):
        savegame.restoregame(**kwargs)


class HashTestCase(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.hm = savegame.HashManager()

    def _generate_file(self, filename, chunk, chunk_count=1000):
        file = os.path.join(self.dst_root, filename)
        chunk = chunk.encode('utf-8')
        with open(file, 'wb') as fd:
            for i in range(1000):
                fd.write(chunk)
        return file

    def test_hash(self):
        file = self._generate_file('file1', chunk='a' * 10000)
        hash1 = self.hm.get(file, use_cache=False)
        self.assertFalse(self.hm.cache)

        og_hash_file = self.hm.hash_file
        with patch.object(self.hm, 'hash_file') as mock_hash_file:
            mock_hash_file.return_value = og_hash_file(file)
            hash2 = self.hm.get(file, use_cache=True)
        self.assertTrue(mock_hash_file.called)
        self.assertEqual(hash2, hash1)

        with patch.object(self.hm, 'hash_file') as mock_hash_file:
            hash2 = self.hm.get(file, use_cache=True)
        self.assertFalse(mock_hash_file.called)
        self.assertEqual(hash2, hash1)

        file2 = self._generate_file('file2', chunk='b' * 10000)
        hash3 = self.hm.get(file2, use_cache=True)

        cache = deepcopy(self.hm.cache)
        self.assertEqual(self.hm.cache[file][0], hash1)
        self.assertEqual(self.hm.cache[file2][0], hash3)
        self.hm.save()
        self.hm.cache = {}
        self.hm.load()
        pprint(self.hm.cache)
        self.assertEqual(self.hm.cache, cache)
        self.hm.cache[file][1] = time.time() - savegame.HASH_CACHE_TTL - 1
        self.hm.save()
        self.hm.cache = {}
        self.hm.load()
        pprint(self.hm.cache)
        self.assertFalse(file in self.hm.cache.keys())


class ReferenceDataTestCase(BaseTestCase):

    def setUp(self):
        super().setUp()
        self.rd = savegame.ReferenceData(self.dst_root)

    def _get_mtime_ts(self):
        return os.stat(self.rd.file).st_mtime

    def test_1(self):
        ref_data = {
            'src': 'src',
            'files': [
                ('file2', 'hash2'),
                ('file1', 'hash1'),
            ],
        }
        self.rd.save(ref_data)
        res = self.rd.load()
        self.assertEqual(res, ref_data)
        ts1 = self._get_mtime_ts()

        time.sleep(.1)
        self.rd.save(ref_data)
        res = self.rd.load()
        self.assertEqual(res, ref_data)
        ts2 = self._get_mtime_ts()
        self.assertEqual(ts2, ts1)

        time.sleep(.1)
        ref_data['files'].append(['new_file', 'new_hash'])
        self.rd.save(ref_data)
        res = self.rd.load()
        self.assertEqual(res, ref_data)
        ts3 = self._get_mtime_ts()
        self.assertTrue(ts3 > ts2)


class SaveItemTestCase(BaseTestCase):

    def test_dst_path(self):
        dst_path = os.path.expanduser('~')
        src_paths = glob(os.path.join(dst_path, '*'))[:3]
        self.assertTrue(src_paths)
        si = savegame.SaveItem(src_paths=src_paths, dst_path=dst_path)
        self.assertTrue(list(si.iterate_savers()))

        if os.name == 'nt':
            dst_path = '/home/jererc/data'
        else:
            dst_path = r'C:\Users\jerer\data'
        self.assertRaises(savegame.UnhandledPath,
            savegame.SaveItem, src_paths=src_paths, dst_path=dst_path)


class SavegameTestCase(BaseTestCase):
    def test_save_glob_and_exclusions(self):
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
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_contains(dst_paths, 'src2'))
        self.assertFalse(any_str_contains(dst_paths, 'dir2'))
        self.assertFalse(any_str_contains(dst_paths, 'file2'))
        self.assertTrue(any_str_contains(dst_paths, 'src1'))
        self.assertTrue(any_str_contains(dst_paths, 'src3'))
        self.assertTrue(any_str_contains(dst_paths, 'dir1'))
        self.assertTrue(any_str_contains(dst_paths, 'dir3'))
        self.assertTrue(any_str_contains(dst_paths, 'file1'))
        self.assertTrue(any_str_contains(dst_paths, 'file3'))

    def test_save(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        savegame.SAVES = [
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src1'),
                ],
                'dst_path': self.dst_root,
                'min_delta': 600,
            },
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src2'),
                ],
                'dst_path': self.dst_root,
                'min_delta': 600,
            },
        ]
        savegame.savegame()
        src_paths = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths)
        dst_paths = self._list_dst_root_paths()
        print('dst data:')
        pprint(dst_paths)
        self.assertTrue(any_str_contains(dst_paths, 'src1'))
        self.assertTrue(any_str_contains(dst_paths, 'src2'))
        self.assertTrue(any_str_contains(dst_paths, 'dir1'))
        self.assertTrue(any_str_contains(dst_paths, 'dir2'))
        self.assertTrue(any_str_contains(dst_paths, 'file1'))
        self.assertTrue(any_str_contains(dst_paths, 'file2'))
        meta = deepcopy(savegame.MetaManager().meta)
        pprint(meta)
        for data in sorted(meta.values(), key=lambda x: x['dst']):
            print('dst data:')
            pprint(set(walk_paths(data['dst'])))

        savegame.savegame()
        meta2 = deepcopy(savegame.MetaManager().meta)
        pprint(meta2)
        self.assertEqual(meta2, meta)
        for data in sorted(meta2.values(), key=lambda x: x['dst']):
            print('dst data:')
            pprint(set(walk_paths(data['dst'])))

    def test_retention(self):
        self._savegame(index_start=1, file_count=4, file_version=1)
        remove_path(self.src_root)
        self._savegame(index_start=1, file_count=2, file_version=2)
        remove_path(self.src_root)

        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertTrue(any_str_contains(dst_paths, 'file3'))
        self.assertTrue(any_str_contains(dst_paths, 'file4'))

        self._restoregame(from_hostname=None)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))

    def test_check(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        savegame.SAVES = [
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src1'),
                ],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src2'),
                ],
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame()
        src_paths = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths)
        dst_paths = self._list_dst_root_paths()
        print('dst data:')
        pprint(dst_paths)

        savegame.checkgame()

        for file in walk_files(self.src_root):
            if file.endswith('/dir1/file1'):
                with open(file) as fd:
                    content = fd.read()
                with open(file, 'w') as fd:
                    fd.write(content + 'a')
            if file.endswith('/dir1/file2'):
                remove_path(file)

        for file in walk_files(self.dst_root):
            if file.endswith('/dir3/file3'):
                with open(file) as fd:
                    content = fd.read()
                with open(file, 'w') as fd:
                    fd.write(content + 'b')
            if file.endswith('/dir3/file2'):
                remove_path(file)

        savegame.checkgame()

    def test_restore_skipped_identical(self):
        self._savegame(index_start=1, file_count=2)
        src_paths = self._list_src_root_paths()
        pprint(src_paths)
        remove_path(self.src_root)

        savegame.restoregame(overwrite=False)
        src_paths2 = self._list_src_root_paths()
        pprint(src_paths2)
        self.assertEqual(src_paths2, src_paths)

        savegame.restoregame(overwrite=False)
        src_paths3 = self._list_src_root_paths()
        pprint(src_paths3)
        self.assertEqual(src_paths3, src_paths)

    def test_restore_skipped_conflict(self):
        self._savegame(index_start=1, file_count=2)
        src_paths = self._list_src_root_paths()
        pprint(src_paths)
        remove_path(self.src_root)

        savegame.restoregame(overwrite=False)
        src_paths2 = self._list_src_root_paths()
        pprint(src_paths2)
        self.assertEqual(src_paths2, src_paths)
        for file in walk_files(self.src_root):
            with open(file) as fd:
                content = fd.read()
            with open(file, 'w') as fd:
                fd.write(content + file)

        savegame.restoregame(overwrite=False)
        src_paths3 = self._list_src_root_paths()
        pprint(src_paths3)
        self.assertEqual(src_paths3, src_paths)

        savegame.restoregame(overwrite=True)
        src_paths4 = self._list_src_root_paths()
        pprint(src_paths4)
        diff = src_paths4 - src_paths
        self.assertTrue(diff)
        self.assertTrue(all(os.path.splitext(f)[-1] == '.savegamebak'
            for f in diff))

    def test_restore_from_hostname(self):
        hostname2 = 'hostname2'
        hostname3 = 'hostname3'

        self._savegame(index_start=1)
        remove_path(self.src_root)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME, to_hostname=hostname2)
        self._savegame(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME, to_hostname=hostname3)
        self._savegame(index_start=5)
        remove_path(self.src_root)

        print('dst data:')
        pprint(self._list_dst_root_paths())

        self._restoregame(from_hostname=None)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        self._restoregame(from_hostname=hostname2)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))
        self._restoregame(from_hostname=hostname3)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=3)))
        self._restoregame(from_hostname='unknown')
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set())
        savegame.list_hostnames()

    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_restore_shared_username_path(self):
        username2 = 'Public' if os.name == 'nt' else 'shared'
        username3 = 'username3'

        self._savegame(index_start=1)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username2)
        self._savegame(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username3)
        self._savegame(index_start=5)
        remove_path(self.src_root)

        print('src data:')
        pprint(self._list_src_root_paths())
        print('dst data:')
        pprint(self._list_dst_root_paths())

        self._restoregame(from_username=None)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)
            + self._get_src_paths(index_start=5)))
        self._restoregame(from_username=username3)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)
            + self._get_src_paths(index_start=3)
            + self._get_src_paths(index_start=5)))

    def test_restore_from_username(self):
        username2 = 'username2'
        username3 = 'username3'

        self._savegame(index_start=1)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username2)
        self._savegame(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username3)
        self._savegame(index_start=5)
        remove_path(self.src_root)

        print('src data:')
        pprint(self._list_src_root_paths())
        print('dst data:')
        pprint(self._list_dst_root_paths())

        self._restoregame(from_username=None)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        self._restoregame(from_username=username2)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)
            + self._get_src_paths(index_start=1)))
        self._restoregame(from_username=username3)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)
            + self._get_src_paths(index_start=3)))
        self._restoregame(from_username='unknown')
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))

    def test_restore_invalid_files(self):
        self._savegame(index_start=1, file_count=2, file_version=1)
        remove_path(self.src_root)
        self._savegame(index_start=1, file_count=2, file_version=1)
        remove_path(self.src_root)
        self._savegame(index_start=1, file_count=2, file_version=2)
        src_paths = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths)
        remove_path(self.src_root)

        print('dst data:')
        pprint(self._list_dst_root_paths())
        for file in walk_files(self.dst_root):
            if os.path.basename(file) == 'file1':
                with open(file, 'w') as fd:
                    fd.write('corrupted data')

        savegame.restoregame()
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)
        self.assertFalse(src_paths2)

    def test_restore(self):
        self._savegame(index_start=1, file_count=2)
        src_paths = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths)
        remove_path(self.src_root)

        print('dst data:')
        pprint(self._list_dst_root_paths())

        savegame.restoregame()
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)
        self.assertEqual(src_paths2, src_paths)
