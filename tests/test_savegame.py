from copy import deepcopy
from fnmatch import fnmatch
from glob import glob
import gzip
import json
import logging
import os
from pprint import pprint
import shutil
import socket
import sys
import time
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
REPO_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(REPO_PATH, 'savegame'))
import savegame
import user_settings
assert savegame.WORK_PATH, user_settings.WORK_PATH


HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()
SRC_DIR = 'src_root'
DST_DIR = 'dst_root'

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


def any_str_matches(strings, pattern):
    for s in strings:
        if fnmatch(s, pattern):
            return True
    return False


def count_matches(strings, pattern):
    res = 0
    for s in strings:
        if fnmatch(s, pattern):
            res += 1
    return res


class RestoregamePathUsernameTestCase(unittest.TestCase):
    def setUp(self):
        self.own_username = os.getlogin()
        self.username2 = f'not{self.own_username}2'
        self.username3 = f'not{self.own_username}3'
        self.dst_path = os.path.dirname(__file__)

    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_win(self):
        obj = savegame.LocalRestorer(self.dst_path)

        path = 'C:\\Program Files\\name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = 'C:\\Users\\Public\\name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = f'C:\\Users\\{self.username2}\\name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'C:\\Users\\{self.own_username}\\name'
        self.assertEqual(obj._get_src_file_for_user(path),
            f'C:\\Users\\{self.own_username}\\name')

    @unittest.skipIf(os.name != 'posix', 'not linux')
    def test_linux(self):
        obj = savegame.LocalRestorer(self.dst_path)

        path = '/var/name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = '/home/shared/name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = f'/home/{self.username2}/name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'/home/{self.own_username}/name'
        self.assertEqual(obj._get_src_file_for_user(path), path)

    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_win_other_username(self):
        obj = savegame.LocalRestorer(self.dst_path,
            username=self.username2)

        path = 'C:\\Program Files\\name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = 'C:\\Users\\Public\\name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = f'C:\\Users\\{self.own_username}\\name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'C:\\Users\\{self.username3}\\name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'C:\\Users\\{self.username2}\\name'
        self.assertEqual(obj._get_src_file_for_user(path),
            f'C:\\Users\\{self.own_username}\\name')

    @unittest.skipIf(os.name != 'posix', 'not linux')
    def test_linux_other_username(self):
        obj = savegame.LocalRestorer(self.dst_path,
            username=self.username2)

        path = '/var/name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = '/home/shared/name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = f'/home/{self.own_username}/name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'/home/{self.username3}/name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'/home/{self.username2}/name'
        self.assertEqual(obj._get_src_file_for_user(path),
            f'/home/{self.own_username}/name')


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        for path in glob(os.path.join(savegame.WORK_PATH, '*')):
            if os.path.splitext(path)[1] == '.log':
                continue
            remove_path(path)
        makedirs(user_settings.WORK_PATH)

        self.src_root = os.path.join(savegame.WORK_PATH, SRC_DIR)
        self.dst_root = os.path.join(savegame.WORK_PATH, DST_DIR)
        makedirs(self.dst_root)

        self.meta = savegame.Metadata()
        self.meta.data = {}

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
            for saver_id in os.listdir(os.path.join(self.dst_root, base_dir)):
                for hostname in os.listdir(os.path.join(self.dst_root,
                        base_dir, saver_id)):
                    if hostname != from_hostname:
                        continue
                    os.rename(os.path.join(self.dst_root,
                            base_dir, saver_id, hostname),
                        os.path.join(self.dst_root,
                            base_dir, saver_id, to_hostname))

    def _switch_dst_data_username(self, from_username, to_username):

        def switch_ref_path(file):
            ref = savegame.Reference(os.path.dirname(file))
            username_str = f'{os.sep}{from_username}{os.sep}'
            if username_str not in ref.src:
                return
            ref.src = ref.src.replace(username_str,
                f'{os.sep}{to_username}{os.sep}')
            ref.save()

        for path in walk_paths(self.dst_root):
            if os.path.basename(path) == savegame.REF_FILENAME:
                switch_ref_path(path)

    def _savegame(self, run_delta=0, **kwargs):
        self._generate_src_data(**kwargs)
        savegame.SAVES = [
            {
                'src_paths': self._get_src_paths(**kwargs),
                'dst_path': self.dst_root,
                'run_delta': run_delta,
            },
        ]
        savegame.savegame()

    def _restoregame(self, **kwargs):
        savegame.restoregame(**kwargs)


class PatternTestCase(unittest.TestCase):
    def setUp(self):
        self.file = os.path.join(os.path.expanduser('~'),
            'first_dir', 'second_dir', 'savegame.py')

    def test_ko(self):
        self.assertFalse(savegame.check_patterns(self.file,
            inclusions=['*third_dir*'],
        ))
        self.assertFalse(savegame.check_patterns(self.file,
            inclusions=['*.bin'],
        ))
        self.assertFalse(savegame.check_patterns(self.file,
            exclusions=['*dir*'],
        ))
        self.assertFalse(savegame.check_patterns(self.file,
            exclusions=['*.py'],
        ))

    def test_ok(self):
        self.assertTrue(savegame.check_patterns(self.file,
            inclusions=['*game*'],
        ))
        self.assertTrue(savegame.check_patterns(self.file,
            inclusions=['*.py'],
        ))
        self.assertTrue(savegame.check_patterns(self.file,
            exclusions=['*third*'],
        ))
        self.assertTrue(savegame.check_patterns(self.file,
            exclusions=['*.bin'],
        ))


class ReferenceTestCase(BaseTestCase):
    def _get_mtime(self, ref):
        return os.stat(ref.file).st_mtime

    def _write_data(self, ref):
        with open(ref.file, 'wb') as fd:
            fd.write(gzip.compress(
                json.dumps(ref.data, sort_keys=True).encode('utf-8')))

    def test_1(self):
        ref1 = savegame.Reference(self.dst_root)
        ref1.src = self.src_root
        ref1.files = {
            'file1': 'hash1',
            'file2': 'hash2',
        }
        ref1.save()
        ts1 = self._get_mtime(ref1)

        time.sleep(.1)
        ref1.save()
        ts2 = self._get_mtime(ref1)
        self.assertEqual(ts2, ts1)

        ref2 = savegame.Reference(self.dst_root)
        self.assertEqual(ref2.data, ref1.data)
        self.assertEqual(ref2.src, ref1.src)
        self.assertEqual(ref2.files, ref1.files)

        time.sleep(.1)
        ref2.files['file3'] = 'hash3'
        ref2.save()
        ts3 = self._get_mtime(ref2)
        self.assertTrue(ts3 > ts2)

        time.sleep(.1)
        ref2.files['file4'] = 'hash4'
        ref2.save()
        ts4 = self._get_mtime(ref2)
        self.assertTrue(ts4 > ts3)

        time.sleep(.1)
        ref2.save()
        ts5 = self._get_mtime(ref2)
        self.assertEqual(ts5, ts4)

        for i in range(10):
            ref2.files[f'new_file{i}'] = f'new_hash{i}'
            ref2.save()
        self.assertTrue(len(ref2.data['ts']) > 10)

        now_ts = int(time.time())
        ref2.data['ts'] = [
            now_ts - savegame.REF_TS_HISTORY_DELTA - 2,
            now_ts - savegame.REF_TS_HISTORY_DELTA - 1,
            now_ts,
        ]
        self._write_data(ref2)
        ref2 = savegame.Reference(self.dst_root)
        ref2.files['another_new_file'] = 'another_new_hash'
        ref2.save()
        self.assertEqual(len(ref2.data['ts']), 3)

        ref2.data['ts'] = [
            now_ts - savegame.REF_TS_HISTORY_DELTA - 2,
            now_ts - savegame.REF_TS_HISTORY_DELTA - 1,
            now_ts - 2,
            now_ts - 1,
            now_ts,
        ]
        self._write_data(ref2)
        ref2 = savegame.Reference(self.dst_root)
        ref2.files['another_new_file2'] = 'another_new_hash2'
        ref2.save()
        self.assertEqual(len(ref2.data['ts']), 4)


class SaveItemTestCase(BaseTestCase):
    def test_dst_path(self):
        dst_path = os.path.expanduser('~')
        src_paths = glob(os.path.join(dst_path, '*'))[:3]
        self.assertTrue(src_paths)
        si = savegame.SaveItem(src_paths=src_paths, dst_path=dst_path)
        self.assertTrue(list(si.generate_savers()))

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
                        ['*/src2*', '*/dir2/*', '*/file2'],
                    ],
                ],
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame()
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*src2*'))
        self.assertFalse(any_str_matches(dst_paths, '*dir2*'))
        self.assertFalse(any_str_matches(dst_paths, '*file2*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*src3*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*'))
        self.assertTrue(any_str_matches(dst_paths, '*file1*'))
        self.assertTrue(any_str_matches(dst_paths, '*file3*'))

    def test_save(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        savegame.SAVES = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
                'run_delta': 600,
            },
            {
                'src_paths': [os.path.join(self.src_root, 'src2')],
                'dst_path': self.dst_root,
                'run_delta': 600,
            },
        ]
        savegame.savegame()
        src_paths = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths)
        dst_paths = self._list_dst_root_paths()
        print('dst data:')
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*'))
        self.assertTrue(any_str_matches(dst_paths, '*file1*'))
        self.assertTrue(any_str_matches(dst_paths, '*file2*'))
        meta = deepcopy(self.meta.data)
        pprint(meta)
        for data in sorted(meta.values(), key=lambda x: x['dst']):
            print('dst data:')
            pprint(set(walk_paths(data['dst'])))

        savegame.savegame()
        meta2 = deepcopy(self.meta.data)
        pprint(meta2)
        self.assertEqual(meta2, meta)
        for data in sorted(meta2.values(), key=lambda x: x['dst']):
            print('dst data:')
            pprint(set(walk_paths(data['dst'])))

    def _get_ref(self):
        return {s: savegame.Reference(d['dst'])
            for s, d in self.meta.data.items()}

    def test_ref(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=2,
            file_count=2)
        src1 = os.path.join(self.src_root, 'src1')
        savegame.SAVES = [
            {
                'src_paths': [src1],
                'dst_path': self.dst_root,
                'run_delta': 0,
            },
        ]
        savegame.savegame()
        ref_files = self._get_ref()[src1].files
        pprint(ref_files)
        self.assertTrue(ref_files.get('dir1/file1'))
        self.assertTrue(ref_files.get('dir1/file2'))
        self.assertTrue(ref_files.get('dir2/file1'))
        self.assertTrue(ref_files.get('dir2/file2'))

        # Source file changed
        for file in walk_files(self.src_root):
            if os.path.basename(file) == 'file1':
                with open(file, 'w') as fd:
                    fd.write(f'new content for {file}')

        def side_copyfile(*args, **kwargs):
            raise Exception('copyfile failed')

        with patch.object(savegame.shutil, 'copyfile') as mock_copyfile:
            mock_copyfile.side_effect = side_copyfile
            savegame.savegame()
        ref_files = self._get_ref()[src1].files
        pprint(ref_files)
        self.assertFalse('dir1/file1' in ref_files)
        self.assertTrue(ref_files.get('dir1/file2'))
        self.assertFalse('dir2/file1' in ref_files)
        self.assertTrue(ref_files.get('dir2/file2'))

        savegame.savegame()
        ref_files = self._get_ref()[src1].files
        pprint(ref_files)
        self.assertTrue(ref_files.get('dir1/file1'))
        self.assertTrue(ref_files.get('dir1/file2'))
        self.assertTrue(ref_files.get('dir2/file1'))
        self.assertTrue(ref_files.get('dir2/file2'))

        # Destination file removed
        for file in walk_files(self.dst_root):
            if os.path.basename(file) == 'file2':
                os.remove(file)
        dst_paths = self._list_dst_root_paths()
        print('dst data:')
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*file2*'))

        savegame.savegame()
        ref_files = self._get_ref()[src1].files
        pprint(ref_files)
        self.assertTrue(ref_files.get('dir1/file1'))
        self.assertTrue(ref_files.get('dir2/file1'))
        self.assertTrue(ref_files.get('dir1/file2'))
        self.assertTrue(ref_files.get('dir2/file2'))

        dst_paths = self._list_dst_root_paths()
        print('dst data:')
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*dir1/file1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1/file2*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2/file1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2/file2*'))

    def test_meta(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=2,
            file_count=2)
        src1 = os.path.join(self.src_root, 'src1')
        src2 = os.path.join(self.src_root, 'src2')
        src3 = os.path.join(self.src_root, 'src3')
        savegame.SAVES = [
            {
                'src_paths': [src1],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [src2],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [src3],
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame()
        pprint(self.meta.data)
        self.assertEqual(set(self.meta.data.keys()), {src1, src2, src3})

        savegame.SAVES = [
            {
                'src_paths': [src1],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [src2],
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame()
        pprint(self.meta.data)
        self.assertEqual(set(self.meta.data.keys()), {src1, src2})

        def side_do_run(*args, **kwargs):
            raise Exception('do_run failed')

        with patch.object(savegame.BaseSaver,
                    'notify_error') as mock_notify_error, \
                patch.object(savegame.LocalSaver,
                    'do_run') as mock_do_run:
            mock_do_run.side_effect = side_do_run
            savegame.savegame()
        pprint(self.meta.data)
        self.assertEqual(set(self.meta.data.keys()), {src1, src2})

    def test_stats(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        savegame.SAVES = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [os.path.join(self.src_root, 'src2')],
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame(stats=True)
        pprint(self.meta.data)

    def test_task(self):
        self._generate_src_data(index_start=1, src_count=4, dir_count=2,
            file_count=2)
        savegame.SAVES = [
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src1'),
                    os.path.join(self.src_root, 'src2'),
                ],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src3'),
                    os.path.join(self.src_root, 'src4'),
                ],
                'dst_path': self.dst_root,
            },
        ]
        with patch.object(savegame, 'savegame') as mock_savegame:
            for i in range(3):
                savegame.Task().run()
        self.assertEqual(len(mock_savegame.call_args_list), 1)

    def test_monitor(self):
        self._generate_src_data(index_start=1, src_count=4, dir_count=2,
            file_count=2)
        savegame.SAVES = [
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src1'),
                    os.path.join(self.src_root, 'src2'),
                ],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src3'),
                    os.path.join(self.src_root, 'src4'),
                ],
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame()
        self.assertFalse(savegame.SaveMonitor()._must_run())

        refs = self._get_ref()
        for src_path, ref in refs.items():
            if src_path.endswith('src1'):
                remove_path(ref.run_file.file)

        with patch.object(savegame, 'notify') as mock_notify:
            sc = savegame.SaveMonitor(force=True)
            sc.run()
        self.assertTrue(mock_notify.call_args_list)

    def test_retention(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=4,
            file_count=4)
        savegame.SAVES = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
                'retention_delta': 300,
            },
        ]
        savegame.savegame()
        remove_path(self.src_root)
        self._generate_src_data(index_start=1, src_count=1, dir_count=2,
            file_count=2)
        src_paths = self._list_src_root_paths()
        savegame.savegame()

        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*file3*'))
        self.assertTrue(any_str_matches(dst_paths, '*file4*'))

        savegame.SAVES = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
                'retention_delta': 0,
            },
        ]
        savegame.savegame()
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*file3*'))
        self.assertFalse(any_str_matches(dst_paths, '*file4*'))

        remove_path(self.src_root)
        self._restoregame(hostname=None)
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)
        self.assertEqual(src_paths2, src_paths)

    def test_src_path_patterns(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=3,
            file_count=3)
        savegame.SAVES = [
            {
                'src_paths': [
                    [
                        os.path.join(self.src_root, '*'),
                        [],
                        ['*src1*'],
                    ],
                ],
                'dst_path': self.dst_root,
                'retention_delta': 0,
            },
        ]
        savegame.savegame()
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file1'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file1'))

    def test_remove_dst_path_patterns(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=3,
            file_count=3)
        savegame.SAVES = [
            {
                'src_paths': [
                    [
                        os.path.join(self.src_root, '*'),
                        [],
                        [],
                    ],
                ],
                'dst_path': self.dst_root,
                'retention_delta': 0,
            },
        ]
        savegame.savegame()
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file*'))

        savegame.SAVES = [
            {
                'src_paths': [
                    [
                        os.path.join(self.src_root, '*'),
                        [],
                        ['*dir1*'],
                    ],
                ],
                'dst_path': self.dst_root,
                'retention_delta': 0,
            },
        ]
        savegame.savegame()
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*dir1*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file*'))

        savegame.SAVES = [
            {
                'src_paths': [
                    [
                        os.path.join(self.src_root, '*'),
                        [],
                        ['*src1*'],
                    ],
                ],
                'dst_path': self.dst_root,
                'retention_delta': 0,
            },
        ]
        savegame.savegame()
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file*'))

    def test_home_path_other_os(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        src_path = {
            'posix': f'~\\{user_settings.TEST_DIR}\\*',
            'nt': f'~/{user_settings.TEST_DIR}/*',
        }[os.name]
        savegame.SAVES = [
            {
                'src_paths': [src_path],
                'dst_path': self.dst_root,
            },
        ]
        savegame.savegame()
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*src*'))

    def test_home_paths(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=2,
            file_count=2)
        savegame.SAVES = [
            {
                'src_paths': [
                    os.path.join('~', user_settings.TEST_DIR, SRC_DIR),
                ],
                'dst_path': os.path.join('~', user_settings.TEST_DIR, DST_DIR),
            },
        ]
        savegame.savegame()
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertEqual(count_matches(dst_paths, '*src*dir*file*'), 8)

    def test_check(self):
        self._generate_src_data(index_start=1, src_count=5, dir_count=2,
            file_count=2)
        savegame.SAVES = [
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src1', '*'),
                    os.path.join(self.src_root, 'src2', '*'),
                ],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [
                    os.path.join(self.src_root, 'src3', '*'),
                    os.path.join(self.src_root, 'src4', '*'),
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
            if fnmatch(file, '*src1*dir1*file1'):
                with open(file) as fd:
                    content = fd.read()
                with open(file, 'w') as fd:
                    fd.write(content + 'a')
            if fnmatch(file, '*src2*dir2*file1'):
                remove_path(file)
        for file in walk_files(self.dst_root):
            if fnmatch(file, '*src4*dir1*file1'):
                with open(file) as fd:
                    content = fd.read()
                with open(file, 'w') as fd:
                    fd.write(content + 'b')
            if fnmatch(file, '*src4*dir2*file2'):
                remove_path(file)

        savegame.checkgame()

        remove_path(os.path.join(self.src_root, 'src4'))
        savegame.checkgame()

        remove_path(os.path.join(self.src_root))
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

    def test_restore_hostname(self):
        hostname2 = 'hostname2'
        hostname3 = 'hostname3'

        self._savegame(index_start=1)
        remove_path(self.src_root)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME,
            to_hostname=hostname2)
        self._savegame(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME,
            to_hostname=hostname3)
        self._savegame(index_start=5)
        remove_path(self.src_root)

        print('dst data:')
        pprint(self._list_dst_root_paths())

        self._restoregame(hostname=None)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        self._restoregame(hostname=hostname2)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))
        self._restoregame(hostname=hostname3)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=3)))
        self._restoregame(hostname='unknown')
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set())
        savegame.list_hostnames()

    def test_restore_username(self):
        username2 = 'username2'
        username3 = 'username3'

        self._savegame(index_start=1)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME,
            to_username=username2)
        self._savegame(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME,
            to_username=username3)
        self._savegame(index_start=5)
        remove_path(self.src_root)

        print('src data:')
        pprint(self._list_src_root_paths())
        print('dst data:')
        pprint(self._list_dst_root_paths())

        self._restoregame(username=None)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        self._restoregame(username=username2)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))
        self._restoregame(username=username3)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=3)))
        self._restoregame(username='unknown')
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertFalse(src_paths)

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

        savegame.restoregame(include=['*dir1*'])
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)

        savegame.restoregame(exclude=['*dir1*'])
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)
