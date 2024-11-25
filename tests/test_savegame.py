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
import time
import unittest
from unittest.mock import patch

from svcutils.service import Config

TEST_DIR = '_test_savegame'
WORK_PATH = os.path.join(os.path.expanduser('~'), TEST_DIR)
import savegame as module
module.WORK_PATH = WORK_PATH
module.logger.setLevel(logging.DEBUG)
module.logger.handlers.clear()
from savegame import lib, load, save, savers


GOOGLE_CLOUD_SECRETS_FILE = os.path.join(os.path.expanduser('~'), 'gcs.json')
HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()
SRC_DIR = 'src_root'
DST_DIR = 'dst_root'


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


def write_ref_data(ref):
    with open(ref.file, 'wb') as fd:
        fd.write(gzip.compress(
            json.dumps(ref.data, sort_keys=True).encode('utf-8')))


class LoadgamePathUsernameTestCase(unittest.TestCase):
    def setUp(self):
        self.own_username = os.getlogin()
        self.username2 = f'not{self.own_username}2'
        self.username3 = f'not{self.own_username}3'
        self.dst_path = os.path.dirname(__file__)

    @unittest.skipIf(os.name != 'nt', 'not windows')
    def test_win(self):
        obj = load.LocalLoader(self.dst_path)

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
        obj = load.LocalLoader(self.dst_path)

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
        obj = load.LocalLoader(self.dst_path,
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
        obj = load.LocalLoader(self.dst_path,
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


class PatternTestCase(unittest.TestCase):
    def setUp(self):
        self.file = os.path.join(os.path.expanduser('~'),
            'first_dir', 'second_dir', 'savegame.py')

    def test_ko(self):
        self.assertFalse(module.lib.check_patterns(self.file,
            inclusions=['*third_dir*'],
        ))
        self.assertFalse(module.lib.check_patterns(self.file,
            inclusions=['*.bin'],
        ))
        self.assertFalse(module.lib.check_patterns(self.file,
            exclusions=['*dir*'],
        ))
        self.assertFalse(module.lib.check_patterns(self.file,
            exclusions=['*.py'],
        ))

    def test_ok(self):
        self.assertTrue(module.lib.check_patterns(self.file,
            inclusions=['*game*'],
        ))
        self.assertTrue(module.lib.check_patterns(self.file,
            inclusions=['*.py'],
        ))
        self.assertTrue(module.lib.check_patterns(self.file,
            exclusions=['*third*'],
        ))
        self.assertTrue(module.lib.check_patterns(self.file,
            exclusions=['*.bin'],
        ))


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        for path in glob(os.path.join(module.WORK_PATH, '*')):
            if os.path.splitext(path)[1] == '.log':
                continue
            remove_path(path)
        makedirs(WORK_PATH)

        self.src_root = os.path.join(module.WORK_PATH, SRC_DIR)
        self.dst_root = os.path.join(module.WORK_PATH, DST_DIR)
        makedirs(self.dst_root)

        self.meta = module.lib.Metadata()
        self.meta.data = {}

        self.config = self._get_config(
            SAVES=[],
            GOOGLE_CLOUD_SECRETS_FILE=GOOGLE_CLOUD_SECRETS_FILE,
        )

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
            ref = module.lib.Reference(os.path.dirname(file))
            username_str = f'{os.sep}{from_username}{os.sep}'
            if username_str not in ref.src:
                return
            ref.src = ref.src.replace(username_str,
                f'{os.sep}{to_username}{os.sep}')
            ref.save()

        for path in walk_paths(self.dst_root):
            if os.path.basename(path) == module.lib.REF_FILENAME:
                switch_ref_path(path)

    def _get_config(self, **kwargs):
        return Config(__file__, **kwargs)

    def _savegame(self, saves, **kwargs):
        self.config.SAVES = saves
        with patch.object(save.Notifier, 'send'):
            config = self._get_config(SAVES=saves, RUN_DELTA=0)
            save.savegame(config, **kwargs)

    def _loadgame(self, **kwargs):
        with patch.object(save.Notifier, 'send'):
            load.loadgame(self.config, **kwargs)


class ReferenceTestCase(BaseTestCase):
    def test_1(self):
        ref1 = module.lib.Reference(self.dst_root)
        ref1.src = self.src_root
        ref1.files = {}
        ref1.files['file1'] = 'hash1'
        ref1.files['file2'] = 'hash2'
        self.assertFalse(ref1.data)
        ref1.save()
        ts1 = ref1.ts

        ref2 = module.lib.Reference(self.dst_root)
        self.assertEqual(ref2.data, ref1.data)
        self.assertEqual(ref2.src, ref1.src)
        self.assertEqual(ref2.files, ref1.files)
        self.assertEqual(ref2.ts, ts1)

        ref2.files['file3'] = 'hash3'
        self.assertTrue('file3' not in ref2.data)
        ts2 = ref2.ts
        time.sleep(.1)
        ref2.save()
        mtime2 = os.stat(ref2.file).st_mtime
        self.assertTrue(ref2.ts > ts2)

        ts3 = ref2.ts
        time.sleep(.1)
        ref2.save()
        self.assertEqual(ref2.ts, ts3)
        self.assertEqual(os.stat(ref2.file).st_mtime, mtime2)


class CopyFileTestCase(BaseTestCase):
    def test_1(self):
        makedirs(self.src_root)
        filename = 'file.txt'
        src_file = os.path.join(self.src_root, filename)
        dst_file = os.path.join(self.dst_root, filename)
        with open(src_file, 'w') as fd:
            fd.write('*' * 100000)
        obj = module.savers.LocalSaver(self.config,
            src='src', inclusions=[], exclusions=[], dst_path='dst_path',
            run_delta=0, retention_delta=0)
        for i in range(2):
            obj._copy_file(src_file, dst_file)
            self.assertEqual(lib.get_file_hash(src_file),
                lib.get_file_hash(dst_file))


class SaveItemTestCase(BaseTestCase):
    def test_dst_path(self):
        dst_path = os.path.expanduser('~')
        src_paths = glob(os.path.join(dst_path, '*'))[:3]
        self.assertTrue(src_paths)
        si = save.SaveItem(self.config, src_paths=src_paths,
            dst_path=dst_path)
        self.assertTrue(list(si.generate_savers()))

        if os.name == 'nt':
            dst_path = '/home/jererc/data'
        else:
            dst_path = r'C:\Users\jerer\data'
        self.assertRaises(module.lib.UnhandledPath,
            save.SaveItem, self.config,
            src_paths=src_paths, dst_path=dst_path)


class DstDirTestCase(unittest.TestCase):
    def test_1(self):
        res = savers.path_to_filename(
            r'C:\Users\jerer\AppData\Roaming\Sublime Text 3')
        self.assertEqual(res, 'C_-Users-jerer-AppData-Roaming-Sublime_Text_3')

    def test_2(self):
        res = savers.path_to_filename('/home/jererc/MEGA/data/savegame')
        self.assertEqual(res, 'home-jererc-MEGA-data-savegame')


class SavegameTestCase(BaseTestCase):
    def test_save_glob_and_exclusions(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        saves = [
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
        self._savegame(saves=saves)
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

    def test_no_save(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        with patch.object(save.Notifier, 'send') as mock_send:
            save.savegame(self.config)
        self.assertTrue(mock_send.call_args_list)

    def test_save(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        saves = [
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
        self._savegame(saves=saves)
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

        self._savegame(saves=saves)
        meta2 = deepcopy(self.meta.data)
        pprint(meta2)
        self.assertEqual(meta2, meta)
        for data in sorted(meta2.values(), key=lambda x: x['dst']):
            print('dst data:')
            pprint(set(walk_paths(data['dst'])))

    def _get_ref(self):
        return {s: module.lib.Reference(d['dst'])
            for s, d in self.meta.data.items()}

    def test_ref(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=2,
            file_count=2)
        src1 = os.path.join(self.src_root, 'src1')
        saves = [
            {
                'src_paths': [src1],
                'dst_path': self.dst_root,
                'run_delta': 0,
            },
        ]
        self._savegame(saves=saves)
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

        def side__copy_file(*args, **kwargs):
            raise Exception('copyfile failed')

        with patch.object(module.savers.LocalSaver, '_copy_file'
                ) as mock__copy_file:
            mock__copy_file.side_effect = side__copy_file
            self._savegame(saves=saves)
        ref_files = self._get_ref()[src1].files
        pprint(ref_files)
        self.assertFalse('dir1/file1' in ref_files)
        self.assertTrue(ref_files.get('dir1/file2'))
        self.assertFalse('dir2/file1' in ref_files)
        self.assertTrue(ref_files.get('dir2/file2'))

        self._savegame(saves=saves)
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

        self._savegame(saves=saves)
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
        saves = [
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
        self._savegame(saves=saves)
        pprint(self.meta.data)
        self.assertEqual(set(self.meta.data.keys()), {src1, src2, src3})

        saves = [
            {
                'src_paths': [src1],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [src2],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves)
        pprint(self.meta.data)
        self.assertEqual(set(self.meta.data.keys()), {src1, src2})

        def side_do_run(*args, **kwargs):
            raise Exception('do_run failed')

        with patch.object(module.savers.BaseSaver, 'notify_error'), \
                patch.object(module.savers.LocalSaver,
                    'do_run') as mock_do_run:
            mock_do_run.side_effect = side_do_run
            self._savegame(saves=saves)
        pprint(self.meta.data)
        self.assertEqual(set(self.meta.data.keys()), {src1, src2})

    def test_stats(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        saves = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [os.path.join(self.src_root, 'src2')],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves)
        pprint(self.meta.data)

    def test_monitor(self):
        self._generate_src_data(index_start=1, src_count=4, dir_count=2,
            file_count=2)
        saves = [
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
        self._savegame(saves=saves)
        self.assertFalse(save.SaveMonitor(self.config)._must_run())

        refs = self._get_ref()
        for src_path, ref in refs.items():
            if src_path.endswith('src1'):
                ref.data['ts'] = time.time() - save.STALE_DELTA - 1
                write_ref_data(ref)

        with patch.object(save.SaveMonitor, '_must_run') as mock__must_run, \
                patch.object(save.Notifier, 'send') as mock_send:
            mock__must_run.return_value = True
            sc = save.SaveMonitor(self.config)
            sc.run()
        print(mock_send.call_args_list)
        self.assertTrue(mock_send.call_args_list)

    def test_retention(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=4,
            file_count=4)
        saves = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
                'retention_delta': 300,
            },
        ]
        self._savegame(saves=saves)
        remove_path(self.src_root)
        self._generate_src_data(index_start=1, src_count=1, dir_count=2,
            file_count=2)
        src_paths = self._list_src_root_paths()
        self._savegame(saves=saves)

        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*file3*'))
        self.assertTrue(any_str_matches(dst_paths, '*file4*'))

        saves = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
                'retention_delta': 0,
            },
        ]
        self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*file3*'))
        self.assertFalse(any_str_matches(dst_paths, '*file4*'))

        remove_path(self.src_root)
        self._loadgame(hostname=None)
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)
        self.assertEqual(src_paths2, src_paths)

    def test_src_path_patterns(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=3,
            file_count=3)
        saves = [
            {
                'src_paths': [
                    [
                        os.path.join(self.src_root, '*'),
                        ['*XXX*'],
                        [],
                    ],
                ],
                'dst_path': self.dst_root,
                'retention_delta': 0,
            },
        ]
        self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(dst_paths)

        saves = [
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
        self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file1'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file1'))

    def test_remove_dst_path_patterns(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=3,
            file_count=3)
        saves = [
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
        self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file*'))

        saves = [
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
        self._savegame(saves=saves)
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

        saves = [
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
        self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file*'))

    def test_old_save(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        saves = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [os.path.join(self.src_root, 'src2')],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [os.path.join(self.src_root, 'src3')],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        print('dst data:')
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*'))
        self.assertTrue(any_str_matches(dst_paths, '*src3*'))

        saves = [
            {
                'src_paths': [os.path.join(self.src_root, 'src2')],
                'dst_path': self.dst_root,
            },
            {
                'src_paths': [os.path.join(self.src_root, 'src3')],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves, force=True)

    def test_home_path_other_os(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3,
            file_count=3)
        src_path = {
            'posix': f'~\\{TEST_DIR}\\*',
            'nt': f'~/{TEST_DIR}/*',
        }[os.name]
        saves = [
            {
                'src_paths': [src_path],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*src*'))

    def test_home_paths(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=2,
            file_count=2)
        saves = [
            {
                'src_paths': [
                    os.path.join('~', TEST_DIR, SRC_DIR),
                ],
                'dst_path': os.path.join('~', TEST_DIR, DST_DIR),
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertEqual(count_matches(dst_paths, '*src*dir*file*'), 8)


class LoadgameTestCase(BaseTestCase):
    def _savegame_with_data(self, run_delta=0, **kwargs):
        self._generate_src_data(**kwargs)
        saves = [
            {
                'src_paths': self._get_src_paths(**kwargs),
                'dst_path': self.dst_root,
                'run_delta': run_delta,
            },
        ]
        with patch.object(save.SaveHandler, '_check_dsts'):
            self._savegame(saves=saves)

    def test_load_skipped_identical(self):
        self._savegame_with_data(index_start=1, file_count=2)
        src_paths = self._list_src_root_paths()
        pprint(src_paths)
        remove_path(self.src_root)

        self._loadgame(overwrite=False)
        src_paths2 = self._list_src_root_paths()
        pprint(src_paths2)
        self.assertEqual(src_paths2, src_paths)

        self._loadgame(overwrite=False)
        src_paths3 = self._list_src_root_paths()
        pprint(src_paths3)
        self.assertEqual(src_paths3, src_paths)

    def test_load_skipped_conflict(self):
        self._savegame_with_data(index_start=1, file_count=2)
        src_paths = self._list_src_root_paths()
        pprint(src_paths)
        remove_path(self.src_root)

        self._loadgame(overwrite=False)
        src_paths2 = self._list_src_root_paths()
        pprint(src_paths2)
        self.assertEqual(src_paths2, src_paths)
        for file in walk_files(self.src_root):
            with open(file) as fd:
                content = fd.read()
            with open(file, 'w') as fd:
                fd.write(content + file)

        self._loadgame(overwrite=False)
        src_paths3 = self._list_src_root_paths()
        pprint(src_paths3)
        self.assertEqual(src_paths3, src_paths)

        self._loadgame(overwrite=True)
        src_paths4 = self._list_src_root_paths()
        pprint(src_paths4)
        diff = src_paths4 - src_paths
        self.assertTrue(diff)
        self.assertTrue(all(os.path.splitext(f)[-1] == '.savegamebak'
            for f in diff))

    def test_load_hostname(self):
        hostname2 = 'hostname2'
        hostname3 = 'hostname3'

        self._savegame_with_data(index_start=1)
        remove_path(self.src_root)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME,
            to_hostname=hostname2)
        self._savegame_with_data(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME,
            to_hostname=hostname3)
        self._savegame_with_data(index_start=5)
        remove_path(self.src_root)

        print('dst data:')
        pprint(self._list_dst_root_paths())

        self._loadgame(hostname=None)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        self._loadgame(hostname=hostname2)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))
        self._loadgame(hostname=hostname3)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=3)))
        self._loadgame(hostname='unknown')
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set())

    def test_load_username(self):
        username2 = 'username2'
        username3 = 'username3'

        self._savegame_with_data(index_start=1)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME,
            to_username=username2)
        self._savegame_with_data(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME,
            to_username=username3)
        self._savegame_with_data(index_start=5)
        remove_path(self.src_root)

        print('src data:')
        pprint(self._list_src_root_paths())
        print('dst data:')
        pprint(self._list_dst_root_paths())

        self._loadgame(username=None)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=5)))
        self._loadgame(username=username2)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=1)))
        self._loadgame(username=username3)
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertEqual(src_paths, set(self._get_src_paths(index_start=3)))
        self._loadgame(username='unknown')
        src_paths = self._list_src_root_src_paths()
        remove_path(self.src_root)
        self.assertFalse(src_paths)

    def test_load_invalid_files(self):
        self._savegame_with_data(index_start=1, file_count=2, file_version=1)
        remove_path(self.src_root)
        self._savegame_with_data(index_start=1, file_count=2, file_version=1)
        remove_path(self.src_root)
        self._savegame_with_data(index_start=1, file_count=2, file_version=2)
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

        self._loadgame()
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)
        self.assertFalse(src_paths2)

    def test_load(self):
        self._savegame_with_data(index_start=1, file_count=2)
        src_paths = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths)
        remove_path(self.src_root)

        print('dst data:')
        pprint(self._list_dst_root_paths())

        self._loadgame()
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)
        self.assertEqual(src_paths2, src_paths)

        self._loadgame(include=['*dir1*'])
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)

        self._loadgame(exclude=['*dir1*'])
        src_paths2 = self._list_src_root_paths()
        print('src data:')
        pprint(src_paths2)
