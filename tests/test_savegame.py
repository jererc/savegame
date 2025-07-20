from copy import deepcopy
from fnmatch import fnmatch
from glob import glob
import json
import os
from pprint import pprint
import shutil
import socket
import sys
import time
import unittest
from unittest.mock import patch

from svcutils.service import Config

from tests import TEST_DIRNAME, WORK_DIR, module
from savegame import load, save, savers


GOOGLE_CREDS = os.path.join(os.path.expanduser('~'), 'gcs-savegame.json')
HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()
SRC_DIR = 'src_root'
DST_DIR = 'dst_root'


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


class LoadgamePathUsernameTestCase(unittest.TestCase):
    def setUp(self):
        self.own_username = os.getlogin()
        self.username2 = f'not{self.own_username}2'
        self.username3 = f'not{self.own_username}3'
        self.dst_path = os.path.dirname(__file__)

    @unittest.skipIf(sys.platform != 'win32', 'not windows')
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

    @unittest.skipIf(sys.platform != 'linux', 'not linux')
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

    @unittest.skipIf(sys.platform != 'win32', 'not windows')
    def test_win_other_username(self):
        obj = load.LocalLoader(self.dst_path, username=self.username2)

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

    @unittest.skipIf(sys.platform != 'linux', 'not linux')
    def test_linux_other_username(self):
        obj = load.LocalLoader(self.dst_path, username=self.username2)

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
        self.assertFalse(module.lib.check_patterns(self.file, inclusions=['*third_dir*']))
        self.assertFalse(module.lib.check_patterns(self.file, inclusions=['*.bin']))
        self.assertFalse(module.lib.check_patterns(self.file, exclusions=['*dir*']))
        self.assertFalse(module.lib.check_patterns(self.file, exclusions=['*.py']))

    def test_ok(self):
        self.assertTrue(module.lib.check_patterns(self.file, inclusions=['*game*']))
        self.assertTrue(module.lib.check_patterns(self.file, inclusions=['*.py']))
        self.assertTrue(module.lib.check_patterns(self.file, exclusions=['*third*']))
        self.assertTrue(module.lib.check_patterns(self.file, exclusions=['*.bin']))


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        for path in glob(os.path.join(module.WORK_DIR, '*')):
            if os.path.splitext(path)[1] == '.log':
                continue
            remove_path(path)
        os.makedirs(WORK_DIR, exist_ok=True)

        self.src_root = os.path.join(module.WORK_DIR, SRC_DIR)
        self.dst_root = os.path.join(module.WORK_DIR, DST_DIR)
        os.makedirs(self.dst_root, exist_ok=True)

        self.meta = module.lib.Metadata()
        self.meta.data = {}

        self.config = self._get_config(
            SAVES=[],
            GOOGLE_CREDS=GOOGLE_CREDS,
        )

    def _generate_src_data(self, index_start, src_count=2, dir_count=2,
                           file_count=2, file_version=1):
        for s in range(index_start, index_start + src_count):
            s_name = f'src{s}'
            for d in range(index_start, index_start + dir_count):
                d_name = f'dir{d}'
                src_d = os.path.join(self.src_root, s_name, d_name)
                os.makedirs(src_d, exist_ok=True)
                for f in range(index_start, index_start + file_count):
                    with open(os.path.join(src_d, f'file{f}'), 'w') as fd:
                        content = {
                            'src': s_name,
                            'dir': d_name,
                            'version': file_version,
                        }
                        fd.write(json.dumps(content, sort_keys=True, indent=4))

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
                for hostname in os.listdir(os.path.join(self.dst_root, base_dir, saver_id)):
                    if hostname != from_hostname:
                        continue
                    os.rename(os.path.join(self.dst_root, base_dir, saver_id, hostname),
                              os.path.join(self.dst_root, base_dir, saver_id, to_hostname))

    def _switch_dst_data_username(self, from_username, to_username):

        def switch_ref_path(file):
            ref = module.lib.Reference(os.path.dirname(file))
            username_str = f'{os.sep}{from_username}{os.sep}'
            if username_str not in ref.src:
                return
            ref.src = ref.src.replace(username_str, f'{os.sep}{to_username}{os.sep}')
            ref.save()

        for path in walk_paths(self.dst_root):
            if os.path.basename(path) == module.lib.REF_FILENAME:
                switch_ref_path(path)

    def _get_config(self, **kwargs):
        args = {
            'DST_ROOT_DIRNAME': 'saves',
            'SAVE_RUN_DELTA': 0,
            'PURGE_DELTA': 7 * 24 * 3600,
            'MONITOR_DELTA_DAYS': 1,
            'ALWAYS_UPDATE_REF': False,
        }
        args.update(kwargs)
        return Config(__file__, **args)

    def _savegame(self, saves, **kwargs):
        self.config.SAVES = saves
        with patch.object(save, 'notify') as mock_notify:
            config = self._get_config(SAVES=saves)
            save.savegame(config, **kwargs)
        if mock_notify.call_args_list:
            print('notify calls:')
            pprint(mock_notify.call_args_list)

    def _loadgame(self, **kwargs):
        with patch.object(save, 'notify'):
            load.loadgame(self.config, **kwargs)


class MetadataTestCase(BaseTestCase):
    def test_1(self):
        meta = module.lib.Metadata()
        now = time.time()
        old_ts = now - 3600 * 24 * 91
        meta.set('key1', {'next_ts': now})
        meta.set('key2', {'next_ts': now})
        meta.save()

        self.assertEqual(meta.get('key1')['next_ts'], now)
        self.assertEqual(meta.get('key2')['next_ts'], now)

        with open(meta.file, 'r', encoding='utf-8') as fd:
            data = json.load(fd)
        data['key2']['next_ts'] = old_ts
        with open(meta.file, 'w', encoding='utf-8') as fd:
            json.dump(data, fd, sort_keys=True, indent=4)

        meta.load()
        self.assertEqual(meta.get('key1')['next_ts'], now)
        self.assertEqual(meta.get('key2')['next_ts'], old_ts)
        meta.save()

        meta = module.lib.Metadata()
        pprint(meta.data)
        self.assertFalse('key2' in meta.data)
        self.assertEqual(meta.get('key1')['next_ts'], now)
        self.assertEqual(meta.get('key2'), {})


class ReferenceTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.force_update = True

    def test_1(self):
        ref1 = module.lib.Reference(self.dst_root)
        ref1.src = self.src_root
        ref1.files = {}
        ref1.files['file1'] = 'hash1'
        ref1.files['file2'] = 'hash2'
        self.assertFalse(ref1.data)
        ref1.save(self.force_update)
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
        ref2.save(self.force_update)
        mtime2 = os.stat(ref2.file).st_mtime
        self.assertTrue(ref2.ts > ts2)

        ts3 = ref2.ts
        time.sleep(.1)
        ref2.save(force=self.force_update)
        if self.force_update:
            self.assertTrue(ref2.ts > ts3)
            self.assertTrue(os.stat(ref2.file).st_mtime > mtime2)
        else:
            self.assertEqual(ref2.ts, ts3)
            self.assertEqual(os.stat(ref2.file).st_mtime, mtime2)


class SaveItemTestCase(BaseTestCase):
    def test_dst_path(self):
        dst_path = os.path.expanduser('~')
        src_paths = glob(os.path.join(dst_path, '*'))[:3]
        self.assertTrue(src_paths)
        si = save.SaveItem(self.config, src_paths=src_paths, dst_path=dst_path)
        self.assertTrue(list(si.generate_savers()))

        if sys.platform == 'win32':
            dst_path = '/home/jererc/data'
        else:
            dst_path = r'C:\Users\jerer\data'
        self.assertRaises(module.lib.UnhandledPath,
                          save.SaveItem,
                          self.config,
                          src_paths=src_paths,
                          dst_path=dst_path)


class DstDirTestCase(unittest.TestCase):
    def test_1(self):
        res = savers.base.path_to_dirname(
            r'C:\Users\jerer\AppData\Roaming\Sublime Text 3')
        self.assertEqual(res, 'C_-Users-jerer-AppData-Roaming-Sublime_Text_3')

    def test_2(self):
        res = savers.base.path_to_dirname('/home/jererc/MEGA/data/savegame')
        self.assertEqual(res, 'home-jererc-MEGA-data-savegame')


class SavegameTestCase(BaseTestCase):
    def test_save_glob_and_exclusions(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3, file_count=3)
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

    def test_save_glob_and_exclusions_file(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3, file_count=3)
        file = os.path.join(self.src_root, 'excluded_file')
        with open(file, 'w') as fd:
            fd.write('content')
        saves = [
            {
                'src_paths': [
                    [
                        os.path.join(self.src_root, '*'),
                        [],
                        ['*/excluded_file', '*/src2*', '*/src3*', '*/dir3*', '*/file3'],
                    ],
                ],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertFalse(any_str_matches(dst_paths, '*excluded_file*'))
        self.assertFalse(any_str_matches(dst_paths, '*src2*'))
        self.assertFalse(any_str_matches(dst_paths, '*src3*'))
        self.assertFalse(any_str_matches(dst_paths, '*dir3*'))
        self.assertFalse(any_str_matches(dst_paths, '*file3*'))

    def test_save(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3, file_count=3)
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
        print('*' * 80)
        pprint(self.meta.data)
        return {d['src']: module.lib.Reference(d['dst'])
                for s, d in self.meta.data.items()}

    def test_ref(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=2, file_count=2)
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

        def side_copy(*args, **kwargs):
            raise Exception('copy failed')

        with patch.object(module.savers.local.shutil, 'copy2',
                          side_effect=side_copy):
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
        self._generate_src_data(index_start=1, src_count=3, dir_count=2, file_count=2)
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
        for i in range(2):
            self._savegame(saves=saves)
        pprint(self.meta.data)
        self.assertEqual(sorted(r['src'] for r in self.meta.data.values()),
                         sorted([src1, src2, src3]))

        def side_do_run(*args, **kwargs):
            raise Exception('do_run failed')

        with patch.object(module.savers.base.BaseSaver, 'notify_error'), \
                patch.object(module.savers.local.LocalSaver, 'do_run',
                             side_effect=side_do_run):
            self._savegame(saves=saves)
        pprint(self.meta.data)
        self.assertEqual(sorted(r['src'] for r in self.meta.data.values()),
                         sorted([src1, src2, src3]))

    def test_stats(self):
        self._generate_src_data(index_start=1, src_count=3, dir_count=3, file_count=3)
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
        self._generate_src_data(index_start=1, src_count=4, dir_count=2, file_count=2)
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

        with patch.object(save.SaveMonitor, '_must_run',
                          return_value=True), \
                patch.object(save, 'notify') as mock_notify:
            sc = save.SaveMonitor(self.config)
            sc.run()
        print(mock_notify.call_args_list)
        self.assertTrue(mock_notify.call_args_list)

    def test_purge(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=4, file_count=4)
        saves = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
                'purge_delta': 300,
            },
        ]
        self._savegame(saves=saves)
        remove_path(self.src_root)
        self._generate_src_data(index_start=1, src_count=1, dir_count=2, file_count=2)
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
                'purge_delta': 0,
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
        self._generate_src_data(index_start=1, src_count=2, dir_count=3, file_count=3)
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
                'purge_delta': 0,
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
                'purge_delta': 0,
            },
        ]
        self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file1'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file1'))

    def test_volume_label_not_found(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=3, file_count=3)
        volumes = {'volume1': self.src_root, 'volume2': self.dst_root}
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        saves = [
            {
                'saver_id': 'local_in_place',
                'enable_purge': False,
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'not_found',
                'dst_volume_label': 'volume2',
            },
        ]
        with patch.object(save, 'list_volumes', return_value=volumes):
            self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*dir*file*'))

        self.meta = module.lib.Metadata()
        self.assertFalse(self.meta.data)

    def test_volume_label_purge(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=3, file_count=3)
        volumes = {'volume1': self.src_root, 'volume2': self.dst_root}
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        with open(os.path.join(dst_path, 'old_file'), 'w') as fd:
            fd.write('data')
        saves = [
            {
                'saver_id': 'local_in_place',
                'enable_purge': False,
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'volume1',
                'dst_volume_label': 'volume2',
            },
        ]
        with patch.object(save, 'list_volumes', return_value=volumes):
            self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertTrue(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*old_file*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file*'))

        saves = [
            {
                'saver_id': 'local_in_place',
                'enable_purge': True,
                'purge_delta': 0,
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'volume1',
                'dst_volume_label': 'volume2',
            },
        ]
        with patch.object(save, 'list_volumes', return_value=volumes):
            self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*old_file*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file*'))

    def test_volume_label_notification(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=3, file_count=3)
        volumes = {'volume1': self.src_root, 'volume2': self.dst_root}
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        with open(os.path.join(dst_path, 'old_file'), 'w') as fd:
            fd.write('data')

        saves = [
            {
                'saver_id': 'local_in_place',
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'volume1',
                'dst_volume_label': 'volume2',
            },
            {
                'saver_id': 'local_in_place',
                'src_paths': ['src2'],
                'dst_path': 'src2',
                'src_volume_label': 'volume2',
                'dst_volume_label': 'volume3',
            },
        ]
        with patch.object(save, 'list_volumes', return_value=volumes):
            self._savegame(saves=saves)
        print('dst data:')
        dst_paths = self._list_dst_root_paths()
        pprint(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*src1*dir*file*'))
        self.assertFalse(any_str_matches(dst_paths, '*src2*'))

    def test_remove_dst_path_patterns(self):
        self._generate_src_data(index_start=1, src_count=2, dir_count=3, file_count=3)
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
                'purge_delta': 0,
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
                'purge_delta': 0,
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
                'purge_delta': 0,
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
        self._generate_src_data(index_start=1, src_count=3, dir_count=3, file_count=3)
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
        self._generate_src_data(index_start=1, src_count=3, dir_count=3, file_count=3)
        src_path = {'win32': f'~/_tests/{TEST_DIRNAME}/*',
                    'linux': f'~\\_tests\\{TEST_DIRNAME}\\*'}[sys.platform]
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
        self._generate_src_data(index_start=1, src_count=2, dir_count=2, file_count=2)
        saves = [
            {
                'src_paths': [
                    os.path.join('~', '_tests', TEST_DIRNAME, SRC_DIR),
                ],
                'dst_path': os.path.join('~', '_tests', TEST_DIRNAME, DST_DIR),
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
        self._switch_dst_data_hostname(from_hostname=HOSTNAME, to_hostname=hostname2)
        self._savegame_with_data(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_hostname(from_hostname=HOSTNAME, to_hostname=hostname3)
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
        self._switch_dst_data_username(from_username=USERNAME, to_username=username2)
        self._savegame_with_data(index_start=3)
        remove_path(self.src_root)
        self._switch_dst_data_username(from_username=USERNAME, to_username=username3)
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
