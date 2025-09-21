from copy import deepcopy
from fnmatch import fnmatch
from glob import glob
import json
import os
from pprint import pformat, pprint
import shutil
import socket
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from svcutils.service import Config

from tests import WORK_DIR, module
from savegame import load, save, savers, lib
from savegame.loaders.filesystem import FilesystemLoader

GOOGLE_CREDS = os.path.join(os.path.expanduser('~'), 'gcs-savegame.json')
HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()
SRC_DIR = 'src_root'
DST_DIR = 'dst_root'


def remove_path(path):
    try:
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    except FileNotFoundError:
        pass


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


class CoalesceTestCase(unittest.TestCase):
    def test_1(self):
        self.assertEqual(lib.coalesce(0, None, 1), 0)
        self.assertEqual(lib.coalesce(1, 2, None), 1)
        self.assertEqual(lib.coalesce(None, 1, None), 1)


class PatternTestCase(unittest.TestCase):
    def setUp(self):
        self.file = os.path.join(os.path.expanduser('~'), 'first_dir', 'second_dir', 'savegame.py')

    def test_ko(self):
        self.assertFalse(module.lib.check_patterns(self.file, include=['*third_dir*']))
        self.assertFalse(module.lib.check_patterns(self.file, include=['*.bin']))
        self.assertFalse(module.lib.check_patterns(self.file, exclude=['*dir*']))
        self.assertFalse(module.lib.check_patterns(self.file, exclude=['*.py']))

    def test_ok(self):
        self.assertTrue(module.lib.check_patterns(self.file, include=['*game*']))
        self.assertTrue(module.lib.check_patterns(self.file, include=['*.py']))
        self.assertTrue(module.lib.check_patterns(self.file, exclude=['*third*']))
        self.assertTrue(module.lib.check_patterns(self.file, exclude=['*.bin']))


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

        module.lib.SaveReference._instances = {}
        self.meta = module.lib.Metadata()
        self.meta.data = {}
        self.config = self._get_config(
            SAVES=[],
            GOOGLE_CREDS=GOOGLE_CREDS,
        )

    def _generate_src_data(self, index_start, nb_srcs=2, nb_dirs=2, nb_files=2, file_version=1):
        for s in range(index_start, index_start + nb_srcs):
            s_name = f'src{s}'
            for d in range(index_start, index_start + nb_dirs):
                d_name = f'dir{d}'
                src_d = os.path.join(self.src_root, s_name, d_name)
                os.makedirs(src_d, exist_ok=True)
                for f in range(index_start, index_start + nb_files):
                    with open(os.path.join(src_d, f'file{f}'), 'w') as fd:
                        content = {
                            'src': s_name,
                            'dir': d_name,
                            'version': file_version,
                        }
                        fd.write(json.dumps(content, sort_keys=True, indent=4))

    def _get_src_paths(self, index_start=1, nb_srcs=2, **kwargs):
        return [os.path.join(self.src_root, f'src{i}') for i in range(index_start, index_start + nb_srcs)]

    def _list_src_root_paths(self):
        res = set(walk_paths(self.src_root))
        print(f'files at {self.src_root}:\n{pformat(res)}')
        return res

    def _list_src_root_src_paths(self):
        return {r for r in self._list_src_root_paths() if os.path.basename(r).startswith('src')}

    def _list_dst_root_paths(self):
        res = set(walk_paths(self.dst_root))
        print(f'files at {self.dst_root}:\n{pformat(res)}')
        return res

    def _list_ref_files(self, dst_paths):
        res = {}
        ref_files = [f for f in dst_paths if os.path.basename(f) == module.lib.REF_FILENAME]
        for ref_file in sorted(ref_files):
            save_ref = module.lib.SaveReference(os.path.dirname(ref_file))
            res[save_ref.dst] = dict(save_ref.files)
        print(f'save ref files:\n{pformat(res)}')
        return res

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
            ref = module.lib.SaveReference(os.path.dirname(file))
            username_str = f'{os.sep}{from_username}{os.sep}'
            src = list(ref.files.keys())[0]
            if username_str not in src:
                return
            new_src = src.replace(username_str, f'{os.sep}{to_username}{os.sep}')
            ref.files = {new_src: ref.files[src]}
            ref.save()

        for path in walk_paths(self.dst_root):
            if os.path.basename(path) == module.lib.REF_FILENAME:
                switch_ref_path(path)

    def _get_config(self, **kwargs):
        args = {
            'DST_ROOT_DIRNAME': 'saves',
            'SAVE_RUN_DELTA': 0,
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
        m1 = module.lib.Metadata()
        m2 = module.lib.Metadata()
        self.assertEqual(m1, m2)

        now = time.time()
        old_ts = now - 3600 * 24 * 91
        m1.set('key1', {'next_ts': now})
        m1.set('key2', {'next_ts': now})
        m1.save()

        self.assertEqual(m1.get('key1')['next_ts'], now)
        self.assertEqual(m1.get('key2')['next_ts'], now)

        with open(m1.file, 'r', encoding='utf-8') as fd:
            data = json.load(fd)
        data['key2']['next_ts'] = old_ts
        with open(m1.file, 'w', encoding='utf-8') as fd:
            json.dump(data, fd, sort_keys=True, indent=4)

        m1._load()
        self.assertEqual(m1.get('key1')['next_ts'], now)
        self.assertEqual(m1.get('key2')['next_ts'], old_ts)
        m1.save()

        pprint(m2.data)
        self.assertFalse('key2' in m2.data)
        self.assertEqual(m2.get('key1')['next_ts'], now)
        self.assertEqual(m2.get('key2'), {})


class SaveReferenceTestCase(BaseTestCase):
    def _create_file(self, file, content):
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, 'w') as fd:
            fd.write(content)

    def test_1(self):
        src1 = os.path.join(self.src_root, 'src1')
        src2 = os.path.join(self.src_root, 'src2')
        dst1 = os.path.join(self.dst_root, 'dst1')
        dst2 = os.path.join(self.dst_root, 'dst2')
        os.makedirs(dst1, exist_ok=True)
        os.makedirs(dst2, exist_ok=True)
        s1 = module.lib.SaveReference(dst1)
        s2 = module.lib.SaveReference(dst1)
        s3 = module.lib.SaveReference(dst2)
        self.assertEqual(s1, s2)
        self.assertNotEqual(s1, s3)

        s1.init_files(src1)
        self._create_file(os.path.join(dst1, 'file1'), 'content1')
        s1.set_file(src1, 'file1', 'hash1')
        s1.save()
        s2.init_files(src2)
        self._create_file(os.path.join(dst1, 'file2'), 'content2')
        s2.set_file(src2, 'file2', 'hash2')
        s2.save()
        s3.init_files(src1)
        self._create_file(os.path.join(dst2, 'file3'), 'content3')
        s3.set_file(src1, 'file3', 'hash3')
        s3.save()
        print(s1.files)
        self.assertTrue(src1 in s1.files)
        self.assertTrue(src2 in s2.files)
        self.assertTrue(src1 in s3.files)
        self.assertEqual(s1.files, s2.files)
        self.assertNotEqual(s1.files, s3.files)


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
        self.assertRaises(module.lib.UnhandledPath, save.SaveItem, self.config, src_paths=src_paths, dst_path=dst_path)


class LoadgamePathUsernameTestCase(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.own_username = os.getlogin()
        self.username2 = f'not{self.own_username}2'
        self.username3 = f'not{self.own_username}3'
        self.save_item = save.SaveItem(self.config, src_paths=[self.src_root], dst_path=self.dst_root)
        os.makedirs(self.save_item.root_dst_path, exist_ok=True)

    @unittest.skipIf(sys.platform != 'win32', 'not windows')
    def test_win(self):
        obj = FilesystemLoader(self.config, self.save_item.root_dst_path, self.save_item.saver_cls)

        path = 'C:\\Program Files\\name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = 'C:\\Users\\Public\\name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = f'C:\\Users\\{self.username2}\\name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'C:\\Users\\{self.own_username}\\name'
        self.assertEqual(obj._get_src_file_for_user(path), f'C:\\Users\\{self.own_username}\\name')

    @unittest.skipIf(sys.platform != 'linux', 'not linux')
    def test_linux(self):
        obj = FilesystemLoader(self.config, self.save_item.root_dst_path, self.save_item.saver_cls)

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
        obj = FilesystemLoader(self.config, self.save_item.root_dst_path, self.save_item.saver_cls, username=self.username2)

        path = 'C:\\Program Files\\name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = 'C:\\Users\\Public\\name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = f'C:\\Users\\{self.own_username}\\name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'C:\\Users\\{self.username3}\\name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'C:\\Users\\{self.username2}\\name'
        self.assertEqual(obj._get_src_file_for_user(path), f'C:\\Users\\{self.own_username}\\name')

    @unittest.skipIf(sys.platform != 'linux', 'not linux')
    def test_linux_other_username(self):
        obj = FilesystemLoader(self.config, self.save_item.root_dst_path, self.save_item.saver_cls, username=self.username2)

        path = '/var/name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = '/home/shared/name'
        self.assertEqual(obj._get_src_file_for_user(path), path)
        path = f'/home/{self.own_username}/name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'/home/{self.username3}/name'
        self.assertEqual(obj._get_src_file_for_user(path), None)
        path = f'/home/{self.username2}/name'
        self.assertEqual(obj._get_src_file_for_user(path), f'/home/{self.own_username}/name')


class DstDirTestCase(unittest.TestCase):
    def test_1(self):
        res = savers.base.path_to_dirname(r'C:\Users\jerer\AppData\Roaming\Sublime Text 3')
        self.assertEqual(res, 'C_-Users-jerer-AppData-Roaming-Sublime_Text_3')

    def test_2(self):
        res = savers.base.path_to_dirname('/home/jererc/MEGA/data/savegame')
        self.assertEqual(res, 'home-jererc-MEGA-data-savegame')


class SavegameTestCase(BaseTestCase):
    def test_save_glob_and_exclude(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=3, nb_files=3)
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
        self.assertFalse(any_str_matches(dst_paths, '*src2*'))
        self.assertFalse(any_str_matches(dst_paths, '*dir2*'))
        self.assertFalse(any_str_matches(dst_paths, '*file2*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*src3*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*'))
        self.assertTrue(any_str_matches(dst_paths, '*file1*'))
        self.assertTrue(any_str_matches(dst_paths, '*file3*'))

    def test_save_glob_and_exclude_file(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=3, nb_files=3)
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
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertFalse(any_str_matches(dst_paths, '*excluded_file*'))
        self.assertFalse(any_str_matches(dst_paths, '*src2*'))
        self.assertFalse(any_str_matches(dst_paths, '*src3*'))
        self.assertFalse(any_str_matches(dst_paths, '*dir3*'))
        self.assertFalse(any_str_matches(dst_paths, '*file3*'))

    def test_save(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=3, nb_files=3)
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
        self._list_src_root_paths()
        dst_paths = self._list_dst_root_paths()
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
        return {d['src']: module.lib.SaveReference(d['dst']) for s, d in self.meta.data.items()}

    def test_ref(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        src1 = os.path.join(self.src_root, 'src1')
        saves = [
            {
                'src_paths': [src1],
                'dst_path': self.dst_root,
                'run_delta': 0,
            },
        ]
        self._savegame(saves=saves)
        ref_files = self._get_ref()[src1].get_src_files(src1)
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

        with patch.object(module.savers.filesystem.shutil, 'copy2', side_effect=side_copy):
            self._savegame(saves=saves)
        ref_files = self._get_ref()[src1].get_src_files(src1)
        pprint(ref_files)
        self.assertFalse('dir1/file1' in ref_files)
        self.assertTrue(ref_files.get('dir1/file2'))
        self.assertFalse('dir2/file1' in ref_files)
        self.assertTrue(ref_files.get('dir2/file2'))

        self._savegame(saves=saves)
        ref_files = self._get_ref()[src1].get_src_files(src1)
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
        self.assertFalse(any_str_matches(dst_paths, '*file2*'))

        self._savegame(saves=saves)
        ref_files = self._get_ref()[src1].get_src_files(src1)
        pprint(ref_files)
        self.assertTrue(ref_files.get('dir1/file1'))
        self.assertTrue(ref_files.get('dir2/file1'))
        self.assertTrue(ref_files.get('dir1/file2'))
        self.assertTrue(ref_files.get('dir2/file2'))

        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dir1/file1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1/file2*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2/file1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2/file2*'))

    def test_meta(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=2, nb_files=2)
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
        self.assertEqual(sorted(r['src'] for r in self.meta.data.values()), sorted([src1, src2, src3]))

        def side_do_run(*args, **kwargs):
            raise Exception('do_run failed')

        with patch.object(module.savers.base, 'notify'), \
                patch.object(module.savers.filesystem.FilesystemSaver, 'do_run', side_effect=side_do_run):
            self._savegame(saves=saves)
        pprint(self.meta.data)
        self.assertEqual(sorted(r['src'] for r in self.meta.data.values()), sorted([src1, src2, src3]))

    def test_stats(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=3, nb_files=3)
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
        self._generate_src_data(index_start=1, nb_srcs=4, nb_dirs=2, nb_files=2)
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

        with patch.object(save.SaveMonitor, '_must_run', return_value=True), \
                patch.object(save, 'notify') as mock_notify:
            sc = save.SaveMonitor(self.config)
            sc.run()
        print(mock_notify.call_args_list)
        self.assertTrue(mock_notify.call_args_list)

    def test_purge(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=4, nb_files=4)
        saves = [
            {
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': self.dst_root,
                'purge_delta': 300,
            },
        ]
        self._savegame(saves=saves)
        remove_path(self.src_root)
        self._generate_src_data(index_start=1, nb_srcs=1, nb_dirs=2, nb_files=2)
        src_paths = self._list_src_root_paths()
        self._savegame(saves=saves)

        dst_paths = self._list_dst_root_paths()
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
        dst_paths = self._list_dst_root_paths()
        self.assertFalse(any_str_matches(dst_paths, '*file3*'))
        self.assertFalse(any_str_matches(dst_paths, '*file4*'))

        remove_path(self.src_root)
        self._loadgame(hostname=None)
        src_paths2 = self._list_src_root_paths()
        self.assertEqual(src_paths2, src_paths)

    def test_filesystem_copy(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        with open(os.path.join(dst_path, 'old_file'), 'w') as fd:
            fd.write('data')
        saves = [
            {
                'saver_id': 'filesystem_copy',
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': dst_path,
            },
        ]
        with patch.object(savers.base, 'get_file_mtime', return_value=time.time() - 365 * 24 * 3600) as mock_get_file_mtime, \
                patch.object(savers.base.BaseSaver, '_check_src_file', return_value=True):
            self._savegame(saves=saves)
        self.assertFalse(mock_get_file_mtime.call_args_list)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*old_file*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))

    def test_filesystem_mirror(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        with open(os.path.join(dst_path, 'old_file'), 'w') as fd:
            fd.write('data')
        saves = [
            {
                'saver_id': 'filesystem_mirror',
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': dst_path,
            },
        ]
        with patch.object(savers.base, 'get_file_mtime', return_value=time.time() - 1) as mock_get_file_mtime, \
                patch.object(savers.base.BaseSaver, '_check_src_file', return_value=True):
            self._savegame(saves=saves)
        self.assertTrue(mock_get_file_mtime.call_args_list)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*old_file*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))

    def test_src_path_patterns(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=3, nb_files=3)
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
        dst_paths = self._list_dst_root_paths()
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
        dst_paths = self._list_dst_root_paths()
        self.assertFalse(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file1'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file1'))

    def test_volume_label_not_found(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=3, nb_files=3)
        volumes = {'volume1': self.src_root, 'volume2': self.dst_root}
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        saves = [
            {
                'saver_id': 'filesystem_copy',
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'not_found',
                'dst_volume_label': 'volume2',
            },
        ]
        with patch.object(save, 'list_label_mountpoints', return_value=volumes):
            self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertFalse(any_str_matches(dst_paths, '*dir*file*'))

        self.meta = module.lib.Metadata()
        self.assertFalse(self.meta.data)

    def test_volume_label_purge(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        volumes = {'volume1': self.src_root, 'volume2': self.dst_root}
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        with open(os.path.join(dst_path, 'old_file'), 'w') as fd:
            fd.write('data')
        saves = [
            {
                'saver_id': 'filesystem_copy',
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'volume1',
                'dst_volume_label': 'volume2',
            },
        ]
        with patch.object(save, 'list_label_mountpoints', return_value=volumes):
            self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(dst_paths)
        self.assertTrue(any_str_matches(dst_paths, '*old_file*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))

        saves = [
            {
                'saver_id': 'filesystem_mirror',
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'volume1',
                'dst_volume_label': 'volume2',
            },
        ]
        with patch.object(save, 'list_label_mountpoints', return_value=volumes):
            self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(dst_paths)
        self.assertFalse(any_str_matches(dst_paths, '*old_file*'))
        self.assertTrue(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))

    def test_volume_label_notification(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=3, nb_files=3)
        volumes = {'volume1': self.src_root, 'volume2': self.dst_root}
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        with open(os.path.join(dst_path, 'old_file'), 'w') as fd:
            fd.write('data')

        saves = [
            {
                'saver_id': 'filesystem_copy',
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'volume1',
                'dst_volume_label': 'volume2',
            },
            {
                'saver_id': 'filesystem_copy',
                'src_paths': ['src2'],
                'dst_path': 'src2',
                'src_volume_label': 'volume2',
                'dst_volume_label': 'volume3',
            },
        ]
        with patch.object(save, 'list_label_mountpoints', return_value=volumes):
            self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*src1*dir*file*'))
        self.assertFalse(any_str_matches(dst_paths, '*src2*'))

    def test_remove_dst_path_patterns(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=3, nb_files=3)
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
        dst_paths = self._list_dst_root_paths()
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
        dst_paths = self._list_dst_root_paths()
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
        dst_paths = self._list_dst_root_paths()
        self.assertFalse(any_str_matches(dst_paths, '*src1*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*'))
        self.assertTrue(any_str_matches(dst_paths, '*src2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir2*file*'))
        self.assertTrue(any_str_matches(dst_paths, '*dir3*file*'))

    def test_old_save(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=3, nb_files=3)
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
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=3, nb_files=3)
        home_path = os.path.expanduser('~')
        self.assertTrue(glob(os.path.join(WORK_DIR, '*')))
        path = ['~'] + os.path.relpath(WORK_DIR, home_path).split(os.sep) + ['*']
        src_path = {'linux': '\\'.join(path), 'win32': '/'.join(path)}[sys.platform]
        print(f'{src_path=}')
        saves = [
            {
                'src_paths': [src_path],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertFalse(any_str_matches(dst_paths, '*src*'))

    def test_home_paths(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        saves = [
            {
                'src_paths': [os.path.join(WORK_DIR, SRC_DIR)],
                'dst_path': os.path.join(WORK_DIR, DST_DIR),
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertEqual(count_matches(dst_paths, '*src*dir*file*'), 8)

    def test_src_older_than_dst(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        old_filenames = [f'old_file{i}' for i in range(3)]
        for old_filename in old_filenames:
            shutil.copy2(os.path.expanduser('~/.bashrc'), os.path.join(self.src_root, old_filename))
        saves = [
            {
                'src_paths': [self.src_root],
                'dst_path': self.dst_root,
            },
        ]

        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        dst_content = 'dst content data'
        dst_old_files = [f for f in dst_paths if os.path.basename(f) in old_filenames]
        self.assertEqual(len(dst_old_files), len(old_filenames))
        for f in dst_old_files:
            with open(f, 'w') as fd:
                fd.write(dst_content)

        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        dst_old_files = [f for f in dst_paths if os.path.basename(f) in old_filenames]
        self.assertEqual(len(dst_old_files), len(old_filenames))
        contents = []
        for f in dst_old_files:
            with open(f) as fd:
                contents.append(fd.read())
        self.assertTrue(all(c == dst_content for c in contents))

    def test_purge_ref_files(self):
        self._generate_src_data(index_start=1, nb_srcs=4, nb_dirs=3, nb_files=2)
        src1 = os.path.join(self.src_root, 'src1')
        src2 = os.path.join(self.src_root, 'src2')
        saves = [
            {
                'saver_id': 'filesystem_copy',
                'src_paths': [
                    [
                        src1,
                        [],
                        [],
                    ],
                ],
                'dst_path': os.path.join(self.dst_root, 'dst2'),
                'purge_delta': 0,
            },
            {
                'saver_id': 'filesystem_copy',
                'src_paths': [
                    [
                        src2,
                        [],
                        [],
                    ],
                ],
                'dst_path': os.path.join(self.dst_root, 'dst2'),
                'purge_delta': 0,
            },
        ]
        [os.makedirs(s['dst_path'], exist_ok=True) for s in saves]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        ref_files = self._list_ref_files(dst_paths)
        self.assertEqual(len(ref_files[os.path.join(self.dst_root, 'dst2')][src1]), 6)
        self.assertEqual(len(ref_files[os.path.join(self.dst_root, 'dst2')][src2]), 6)

        saves[0]['src_paths'] = [
            [
                src1,
                ['*/dir1/*'],
                [],
            ],
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        ref_files = self._list_ref_files(dst_paths)
        self.assertEqual(len(ref_files[os.path.join(self.dst_root, 'dst2')][src1]), 2)
        self.assertEqual(len(ref_files[os.path.join(self.dst_root, 'dst2')][src2]), 6)

        saves[0]['src_paths'] = [
            [
                src1,
                ['*/dirx/*'],
                [],
            ],
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        ref_files = self._list_ref_files(dst_paths)
        self.assertFalse(src1 in ref_files[os.path.join(self.dst_root, 'dst2')])
        self.assertEqual(len(ref_files[os.path.join(self.dst_root, 'dst2')][src2]), 6)


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

    def test_match(self):
        self._savegame_with_data(index_start=1, nb_files=2)
        src_paths = self._list_src_root_paths()
        remove_path(self.src_root)

        self._loadgame(force=False)
        src_paths2 = self._list_src_root_paths()
        self.assertEqual(src_paths2, src_paths)

        self._loadgame(force=False)
        src_paths3 = self._list_src_root_paths()
        self.assertEqual(src_paths3, src_paths)

    def test_mismatch(self):
        self._savegame_with_data(index_start=1, nb_files=2)
        src_paths = self._list_src_root_paths()
        remove_path(self.src_root)

        self._loadgame(force=False)
        src_paths2 = self._list_src_root_paths()
        self.assertEqual(src_paths2, src_paths)
        for file in walk_files(self.src_root):
            with open(file) as fd:
                content = fd.read()
            with open(file, 'w') as fd:
                fd.write(content + file)

        self._loadgame(force=False)
        src_paths3 = self._list_src_root_paths()
        self.assertEqual(src_paths3, src_paths)

        self._loadgame(force=True)
        src_paths4 = self._list_src_root_paths()
        diff = src_paths4 - src_paths
        self.assertTrue(diff)
        self.assertTrue(all(os.path.splitext(f)[-1] == '.savegamebak' for f in diff))

    def test_hostname(self):
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

        self._list_dst_root_paths()

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

    def test_username(self):
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

        self._list_src_root_paths()
        self._list_dst_root_paths()

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

    def test_invalid_files(self):
        self._savegame_with_data(index_start=1, nb_files=2, file_version=1)
        remove_path(self.src_root)
        self._savegame_with_data(index_start=1, nb_files=2, file_version=1)
        remove_path(self.src_root)
        self._savegame_with_data(index_start=1, nb_files=2, file_version=2)
        self._list_src_root_paths()
        remove_path(self.src_root)

        self._list_dst_root_paths()
        for file in walk_files(self.dst_root):
            if os.path.basename(file) == 'file1':
                with open(file, 'w') as fd:
                    fd.write('corrupted data')

        self._loadgame()
        src_paths2 = self._list_src_root_paths()
        self.assertFalse(src_paths2)

    def test_filesystem(self):
        self._savegame_with_data(index_start=1, nb_files=2)
        src_paths = self._list_src_root_paths()
        remove_path(self.src_root)

        self._list_dst_root_paths()

        self._loadgame()
        src_paths2 = self._list_src_root_paths()
        self.assertEqual(src_paths2, src_paths)

        self._loadgame(include=['*dir1*'])
        src_paths2 = self._list_src_root_paths()

        self._loadgame(exclude=['*dir1*'])
        src_paths2 = self._list_src_root_paths()

    def test_filesystem_mirror(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        src_path = os.path.join(self.src_root, 'src1')
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        saves = [
            {
                'saver_id': 'filesystem_mirror',
                'src_paths': [src_path],
                'dst_path': dst_path,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        ref_file = [f for f in dst_paths if os.path.basename(f) == module.lib.REF_FILENAME][0]
        ref = module.lib.SaveReference(os.path.dirname(ref_file))
        pprint(ref.data)

        shutil.rmtree(src_path)
        self._loadgame()
        src_paths = self._list_src_root_paths()
        self.assertTrue(any_str_matches(src_paths, '*src1*dir*file*'))
        self._loadgame()


class WorkflowTestCase(BaseTestCase):
    def test_1(self):
        self._generate_src_data(index_start=1, nb_srcs=4, nb_dirs=3, nb_files=2)
        saves = [
            {
                'saver_id': 'filesystem',
                'src_paths': [
                    [
                        os.path.join(self.src_root, 'src1'),
                        ['*/dir1/*'],
                        [],
                    ],
                ],
                'dst_path': os.path.join(self.dst_root, 'dst1'),
            },
            {
                'saver_id': 'filesystem',
                'src_paths': [
                    [
                        os.path.join(self.src_root, 'src2'),
                        ['*/dir2/*'],
                        [],
                    ],
                ],
                'dst_path': os.path.join(self.dst_root, 'dst1'),
            },
            {
                'saver_id': 'filesystem',
                'src_paths': [
                    os.path.join(self.src_root, 'src4', 'dir1', 'file1'),
                ],
                'dst_path': os.path.join(self.dst_root, 'dst1'),
            },
            {
                'saver_id': 'filesystem_copy',
                'src_paths': [
                    [
                        os.path.join(self.src_root, 'src1'),
                        ['*/dir1/*'],
                        [],
                    ],
                ],
                'dst_path': os.path.join(self.dst_root, 'dst2'),
            },
            {
                'saver_id': 'filesystem_copy',
                'src_paths': [
                    [
                        os.path.join(self.src_root, 'src2'),
                        ['*/dir3/*'],
                        [],
                    ],
                ],
                'dst_path': os.path.join(self.dst_root, 'dst2'),
            },
            {
                'saver_id': 'filesystem_mirror',
                'src_paths': [
                    [
                        os.path.join(self.src_root, 'src3'),
                        ['*/dir1/*'],
                        [],
                    ],
                ],
                'dst_path': os.path.join(self.dst_root, 'dst3'),
            },
            {
                'saver_id': 'filesystem_mirror',
                'src_paths': [
                    [
                        os.path.join(self.src_root, 'src3'),
                        ['*/dir2/*'],
                        [],
                    ],
                ],
                'dst_path': os.path.join(self.dst_root, 'dst3'),
            },
        ]
        [os.makedirs(s['dst_path'], exist_ok=True) for s in saves]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self._list_ref_files(dst_paths)

        saves[0]['src_paths'] = [
            [
                os.path.join(self.src_root, 'src1'),
                ['*/dir3/*'],
                [],
            ],
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self._list_ref_files(dst_paths)

        shutil.rmtree(self.src_root)
        self._loadgame()
        src_paths = self._list_src_root_paths()
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file*'))
        self.assertFalse(any_str_matches(src_paths, '*src1*dir2*file*'))
        self.assertTrue(any_str_matches(src_paths, '*src1*dir3*file*'))

        self.assertTrue(any_str_matches(src_paths, '*src4*dir1*file1*'))

        self.assertFalse(any_str_matches(src_paths, '*src2*dir1*file*'))
        self.assertTrue(any_str_matches(src_paths, '*src2*dir2*file*'))
        self.assertTrue(any_str_matches(src_paths, '*src2*dir3*file*'))

        self.assertFalse(any_str_matches(src_paths, '*src3*dir1*file*'))
        self.assertTrue(any_str_matches(src_paths, '*src3*dir2*file*'))
        self.assertFalse(any_str_matches(src_paths, '*src3*dir3*file*'))

        self._loadgame()


class ReportTestCase(BaseTestCase):
    def test_1(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=2, nb_files=2)
        for dirname in ['dst1', 'dst2', 'dst3']:
            os.makedirs(os.path.join(self.dst_root, dirname), exist_ok=True)
        saves = [
            {
                'saver_id': 'filesystem',
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': os.path.join(self.dst_root, 'dst1'),
            },
            {
                'saver_id': 'filesystem_mirror',
                'src_paths': [os.path.join(self.src_root, 'src2')],
                'dst_path': os.path.join(self.dst_root, 'dst2'),
            },
            {
                'saver_id': 'filesystem_copy',
                'src_paths': [os.path.join(self.src_root, 'src3')],
                'dst_path': os.path.join(self.dst_root, 'dst3'),
            },
        ]
        self._savegame(saves=saves)
        self._list_dst_root_paths()
        self._savegame(saves=saves)

        shutil.rmtree(self.src_root)
        self._loadgame()
        self._list_src_root_paths()
        self._loadgame()


class GitTestCase(BaseTestCase):
    def _create_file(self, file, content):
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, 'w') as fd:
            fd.write(content)

    def _create_repo(self, repo_dirname):
        repo_dir = os.path.join(self.src_root, repo_dirname)
        subprocess.run(['git', 'init', repo_dir], check=True)
        self._create_file(os.path.join(repo_dir, 'dir1', 'file1.txt'), 'data1')
        subprocess.run(['git', 'add', 'dir1'], cwd=repo_dir, check=True)
        subprocess.run(['git', 'commit', '-m', 'initial commit'], cwd=repo_dir, check=True)
        self._create_file(os.path.join(repo_dir, 'dir1', 'file1.txt'), 'new data1')
        self._create_file(os.path.join(repo_dir, 'dir2', 'file2.txt'), 'data2')
        subprocess.run(['git', 'add', 'dir2'], cwd=repo_dir, check=True)
        self._create_file(os.path.join(repo_dir, 'dir3', 'file3.txt'), 'data3')
        return repo_dir

    def test_1(self):
        repo_dir1 = self._create_repo('repo1')
        repo_dir2 = self._create_repo('repo2')
        saves = [
            {
                'saver_id': 'git',
                'src_paths': [self.src_root],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves)
        dst_paths = self._list_dst_root_paths()
        self._list_ref_files(dst_paths)

        [shutil.rmtree(r) for r in [repo_dir1, repo_dir2]]
        self._loadgame()
        src_paths = self._list_src_root_paths()
        self.assertTrue(any_str_matches(src_paths, '*repo1*.git/*'))
        self.assertTrue(any_str_matches(src_paths, '*repo1*dir1*file1*'))
        self.assertTrue(any_str_matches(src_paths, '*repo1*dir2*file2*'))
        self.assertTrue(any_str_matches(src_paths, '*repo1*dir3*file3*'))
        self.assertTrue(any_str_matches(src_paths, '*repo2*.git/*'))
        self.assertTrue(any_str_matches(src_paths, '*repo2*dir1*file1*'))
        self.assertTrue(any_str_matches(src_paths, '*repo2*dir2*file2*'))
        self.assertTrue(any_str_matches(src_paths, '*repo2*dir3*file3*'))
        self._loadgame()
