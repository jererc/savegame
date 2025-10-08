from copy import deepcopy
from datetime import datetime, timedelta, timezone
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
from unittest.mock import Mock, patch

from svcutils.service import Config

from tests import WORK_DIR, module
from savegame import load, save, savers, utils
from savegame.loaders.file import FileLoader
from savegame.savers import virtualbox

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
        self.assertEqual(utils.coalesce(0, None, 1), 0)
        self.assertEqual(utils.coalesce(1, 2, None), 1)
        self.assertEqual(utils.coalesce(None, 1, None), 1)


class PatternTestCase(unittest.TestCase):
    def setUp(self):
        self.file = os.path.join(os.path.expanduser('~'), 'first_dir', 'second_dir', 'savegame.py')

    def test_ko(self):
        self.assertFalse(utils.check_patterns(self.file, include=['*third_dir*']))
        self.assertFalse(utils.check_patterns(self.file, include=['*.bin']))
        self.assertFalse(utils.check_patterns(self.file, exclude=['*dir*']))
        self.assertFalse(utils.check_patterns(self.file, exclude=['*.py']))

    def test_ok(self):
        self.assertTrue(utils.check_patterns(self.file, include=['*game*']))
        self.assertTrue(utils.check_patterns(self.file, include=['*.py']))
        self.assertTrue(utils.check_patterns(self.file, exclude=['*third*']))
        self.assertTrue(utils.check_patterns(self.file, exclude=['*.bin']))


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

        utils.SaveRef._instances = {}
        self.meta = utils.Metadata()
        self.meta.data = {}
        self.config = self._get_config(
            SAVES=[],
            GOOGLE_CREDS=GOOGLE_CREDS,
        )

    def _generate_data(self, dir, index_start, nb_srcs=2, nb_dirs=2, nb_files=2, file_version=1):
        for s in range(index_start, index_start + nb_srcs):
            s_name = f'src{s}'
            for d in range(index_start, index_start + nb_dirs):
                d_name = f'dir{d}'
                src_d = os.path.join(dir, s_name, d_name)
                os.makedirs(src_d, exist_ok=True)
                for f in range(index_start, index_start + nb_files):
                    with open(os.path.join(src_d, f'file{f}'), 'w') as fd:
                        content = {
                            'src': s_name,
                            'dir': d_name,
                            'version': file_version,
                        }
                        fd.write(json.dumps(content, sort_keys=True, indent=4))

    def _generate_src_data(self, index_start, nb_srcs=2, nb_dirs=2, nb_files=2, file_version=1):
        self._generate_data(self.src_root, index_start, nb_srcs, nb_dirs, nb_files, file_version)

    def _generate_dst_data(self, index_start, nb_srcs=2, nb_dirs=2, nb_files=2, file_version=1):
        self._generate_data(self.dst_root, index_start, nb_srcs, nb_dirs, nb_files, file_version)

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

    def _list_save_refs(self, dst_paths):
        res = {}
        ref_files = [f for f in dst_paths if os.path.basename(f) == utils.REF_FILENAME]
        for ref_file in sorted(ref_files):
            save_ref = utils.SaveRef(os.path.dirname(ref_file))
            res[save_ref.dst] = save_ref
        return res

    def _list_save_ref_files(self, dst_paths):
        res = {}
        ref_files = [f for f in dst_paths if os.path.basename(f) == utils.REF_FILENAME]
        for ref_file in sorted(ref_files):
            save_ref = utils.SaveRef(os.path.dirname(ref_file))
            res[save_ref.dst] = save_ref.get_files()
        print(f'save ref files:\n{pformat(res)}')
        return res

    def _get_save_refs(self):
        print('*' * 80)
        pprint(self.meta.data)
        return {d['src']: utils.SaveRef(d['dst']) for s, d in self.meta.data.items()}

    def _switch_dst_data_hostname(self, from_hostname, to_hostname):
        def switch_hostname(file):
            save_ref = utils.SaveRef(os.path.dirname(file))
            save_ref.files[to_hostname] = save_ref.get_files(hostname=from_hostname)
            save_ref.files[from_hostname].clear()
            save_ref.save()

        for base_dir in os.listdir(os.path.join(self.dst_root)):
            for saver_id in os.listdir(os.path.join(self.dst_root, base_dir)):
                for hostname in os.listdir(os.path.join(self.dst_root, base_dir, saver_id)):
                    if hostname != from_hostname:
                        continue
                    os.rename(os.path.join(self.dst_root, base_dir, saver_id, hostname),
                              os.path.join(self.dst_root, base_dir, saver_id, to_hostname))

        for path in walk_paths(self.dst_root):
            if os.path.basename(path) == utils.REF_FILENAME:
                switch_hostname(path)

    def _switch_dst_data_username(self, from_username, to_username):
        def switch_ref_path(file):
            save_ref = utils.SaveRef(os.path.dirname(file))
            username_str = f'{os.sep}{from_username}{os.sep}'
            files = save_ref.get_files()
            src = list(files.keys())[0]
            if username_str not in src:
                return
            new_src = src.replace(username_str, f'{os.sep}{to_username}{os.sep}')
            save_ref.files = {HOSTNAME: {new_src: files[src]}}
            save_ref.save()

        for path in walk_paths(self.dst_root):
            if os.path.basename(path) == utils.REF_FILENAME:
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
        m1 = utils.Metadata()
        m2 = utils.Metadata()
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


class FileRefTestCase(BaseTestCase):
    def _create_file(self, name, content):
        file = os.path.join(self.dst_root, name)
        with open(file, 'w') as fd:
            fd.write(content)
        return file

    def test_from_file(self):
        file = self._create_file('file1', 'content')
        fr = utils.FileRef.from_file(file)
        self.assertEqual(fr.ref, f'{utils.get_file_hash(file)}:{utils.get_file_size(file)}:{utils.get_file_mtime(file)}:1')

    def test_from_attrs(self):
        fr = utils.FileRef(hash='123')
        self.assertEqual(fr.ref, '123:::1')

    def test_from_ref(self):
        fr = utils.FileRef.from_ref(None)
        self.assertEqual(fr.hash, None)
        self.assertEqual(fr.size, None)
        self.assertEqual(fr.mtime, None)
        self.assertEqual(fr.has_src_file, True)
        self.assertEqual(fr.ref, ':::1')

        fr = utils.FileRef.from_ref('')
        self.assertEqual(fr.hash, None)
        self.assertEqual(fr.size, None)
        self.assertEqual(fr.mtime, None)
        self.assertEqual(fr.has_src_file, True)
        self.assertEqual(fr.ref, ':::1')

        fr = utils.FileRef.from_ref('123:')
        self.assertEqual(fr.ref, '123:::1')
        self.assertEqual(fr.hash, '123')
        self.assertEqual(fr.size, None)
        self.assertEqual(fr.mtime, None)
        self.assertEqual(fr.has_src_file, True)

        fr = utils.FileRef.from_ref(':456')
        self.assertEqual(fr.ref, ':456::1')
        self.assertEqual(fr.hash, None)
        self.assertEqual(fr.size, 456)
        self.assertEqual(fr.mtime, None)
        self.assertEqual(fr.has_src_file, True)

        fr = utils.FileRef.from_ref('123:456:789.123')
        self.assertEqual(fr.ref, '123:456:789.123:1')
        self.assertEqual(fr.hash, '123')
        self.assertEqual(fr.size, 456)
        self.assertEqual(fr.mtime, 789.123)
        self.assertEqual(fr.has_src_file, True)

        fr = utils.FileRef.from_ref('123:456:789.123:0')
        self.assertEqual(fr.ref, '123:456:789.123:0')
        self.assertEqual(fr.hash, '123')
        self.assertEqual(fr.size, 456)
        self.assertEqual(fr.mtime, 789.123)
        self.assertEqual(fr.has_src_file, False)

    def test_check_file_ko(self):
        file1 = self._create_file('file1', 'content1')
        file2 = self._create_file('file2', 'content2')
        fr = utils.FileRef.from_file(file1)
        self.assertFalse(fr.check_file(file2))
        fr = utils.FileRef(size=utils.get_file_size(file1))
        self.assertFalse(fr.check_file(file2))
        fr = utils.FileRef(mtime=utils.get_file_mtime(file1))
        self.assertFalse(fr.check_file(file2))

    def test_check_file_equal_with_different_content(self):
        file1 = self._create_file('file1', 'content1')
        file2 = self._create_file('file2', 'content2')
        fr = utils.FileRef.from_file(file1)
        self.assertFalse(fr.check_file(file2))
        fr = utils.FileRef(size=utils.get_file_size(file1), mtime=utils.get_file_mtime(file1))
        self.assertTrue(fr.check_file(file2))

    def test_check_file(self):
        file1 = self._create_file('file1', 'content1')
        file2 = shutil.copy(file1, os.path.join(self.dst_root, 'file2'))
        fr = utils.FileRef.from_file(file1)
        self.assertTrue(fr.check_file(file2))


class SaveRefTestCase(BaseTestCase):
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
        s1 = utils.SaveRef(dst1)
        s2 = utils.SaveRef(dst1)
        self.assertEqual(s1, s2)

        files = s1.reset_files(src1)
        self.assertEqual(files, {})
        self._create_file(os.path.join(dst1, 'file1'), 'content1')
        s1.set_file(src1, 'file1', 'hash1')
        self.assertEqual(s1.get_files(src1), {'file1': 'hash1'})
        s1.save()
        ts1 = s1.get_ts()
        self.assertTrue(s1.get_ts() > 0)

        s1.set_file(src1, 'file1', 'hash1')
        s1.save()
        ts2 = s1.get_ts()
        self.assertEqual(ts2, ts1)

        files = s2.reset_files(src2)
        self.assertEqual(files, {})
        self._create_file(os.path.join(dst1, 'file2'), 'content2')
        s2.set_file(src2, 'file2', 'hash2')
        self.assertEqual(s2.get_files(src2), {'file2': 'hash2'})
        self._create_file(os.path.join(dst1, 'file3'), 'content3')
        s2.set_file(src2, 'file3', 'hash3')
        self.assertEqual(s2.get_files(src2), {'file2': 'hash2', 'file3': 'hash3'})
        self.assertEqual(s2.get_files(), {src1: {'file1': 'hash1'}, src2: {'file2': 'hash2', 'file3': 'hash3'}})
        s2.save()
        self.assertEqual(s2.get_files(), {src1: {'file1': 'hash1'}, src2: {'file2': 'hash2', 'file3': 'hash3'}})
        ts3 = s2.get_ts()
        self.assertTrue(ts3 > ts2)

        self.assertEqual(s1.get_dst_files(), {os.path.join(dst1, 'file1'), os.path.join(dst1, 'file2'), os.path.join(dst1, 'file3')})
        self.assertEqual(s2.get_files(src2), {'file2': 'hash2', 'file3': 'hash3'})

        hostname2 = 'other_hostname'
        files = s2.reset_files(src1, hostname=hostname2)
        self.assertEqual(files, {})
        s2.set_file(src1, 'file4', 'hash4', hostname=hostname2)
        self.assertEqual(s2.get_files(src1, hostname=hostname2), {'file4': 'hash4'})
        s2.save(hostname=hostname2)
        self.assertEqual(s2.get_files(hostname=hostname2), {src1: {'file4': 'hash4'}})

        files = s1.reset_files(src1)
        self.assertEqual(s1.get_files(src1), {})
        self.assertEqual(files, {'file1': 'hash1'})
        self.assertEqual(s2.get_files(hostname=hostname2), {src1: {'file4': 'hash4'}})

        s1.set_file(src1, 'file1', 'hash1')
        s1.set_file(src1, 'file2', 'hash2')
        self._create_file(os.path.join(dst1, 'file4'), 'content4')
        s1.set_file(src1, 'file4', 'hash4')
        s1.save()
        self.assertEqual(s1.get_files(src1), {'file1': 'hash1', 'file2': 'hash2', 'file4': 'hash4'})
        os.remove(os.path.join(dst1, 'file1'))
        s1.save()
        self.assertEqual(s1.get_files(src1), {'file2': 'hash2', 'file4': 'hash4'})
        data1 = deepcopy(s1.data)
        pprint(data1)

        utils.SaveRef._instances = {}
        s3 = utils.SaveRef(dst1)
        self.assertNotEqual(s3, s1)
        self.assertEqual(s3.data, data1)


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
        self.assertRaises(utils.UnhandledPath, save.SaveItem, self.config, src_paths=src_paths, dst_path=dst_path)


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
        obj = FileLoader(self.config, self.save_item.root_dst_path, self.save_item.saver_cls)

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
        obj = FileLoader(self.config, self.save_item.root_dst_path, self.save_item.saver_cls)

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
        obj = FileLoader(self.config, self.save_item.root_dst_path, self.save_item.saver_cls, username=self.username2)

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
        obj = FileLoader(self.config, self.save_item.root_dst_path, self.save_item.saver_cls, username=self.username2)

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

    def test_save_ref(self):
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
        rf = self._get_save_refs()[src1].get_files(src1)
        pprint(rf)
        self.assertTrue(rf.get('dir1/file1'))
        self.assertTrue(rf.get('dir1/file2'))
        self.assertTrue(rf.get('dir2/file1'))
        self.assertTrue(rf.get('dir2/file2'))

        # Source file changed
        for file in walk_files(self.src_root):
            if os.path.basename(file) == 'file1':
                with open(file, 'w') as fd:
                    fd.write(f'new content for {file}')

        def side_copy(*args, **kwargs):
            raise Exception('copy failed')

        with patch.object(module.savers.file.shutil, 'copy2', side_effect=side_copy):
            self._savegame(saves=saves)
        rf2 = self._get_save_refs()[src1].get_files(src1)
        pprint(rf2)
        self.assertEqual(rf2, rf)

        self._savegame(saves=saves)
        rf3 = self._get_save_refs()[src1].get_files(src1)
        pprint(rf3)
        self.assertNotEqual(rf3['dir1/file1'], rf2['dir1/file1'])
        self.assertEqual(rf3['dir1/file2'], rf2['dir1/file2'])
        self.assertNotEqual(rf3['dir2/file1'], rf2['dir2/file1'])
        self.assertEqual(rf3['dir2/file2'], rf2['dir2/file2'])

        # Destination file removed
        for file in walk_files(self.dst_root):
            if os.path.basename(file) == 'file2':
                os.remove(file)
        dst_paths = self._list_dst_root_paths()
        self.assertFalse(any_str_matches(dst_paths, '*file2*'))

        self._savegame(saves=saves)
        rf4 = self._get_save_refs()[src1].get_files(src1)
        pprint(rf4)
        self.assertEqual(rf4, rf3)

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
                patch.object(module.savers.file.FileSaver, 'do_run', side_effect=side_do_run):
            self._savegame(saves=saves)
        pprint(self.meta.data)
        self.assertEqual(sorted(r['src'] for r in self.meta.data.values()), sorted([src1, src2, src3]))

    def test_due_warning(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=2, nb_files=2)
        src1 = os.path.join(self.src_root, 'src1')
        saves = [
            {
                'src_paths': [src1],
                'dst_path': self.dst_root,
                'trigger_volume_labels': ['volume1'],
                'due_warning_delta': 60,
            },
        ]
        with patch.object(module.save.SaveItem, '_list_label_mountpoints', return_value={'volume1': self.src_root}):
            self._savegame(saves=saves)
        key = list(self.meta.data.keys())[0]
        self.meta.data[key]['next_ts'] = time.time() - 61
        for i in range(2):
            self._savegame(saves=saves)
        self.assertTrue(self.meta.data[key]['next_warning_ts'] > time.time())
        pprint(self.meta.data)

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
                'saver_id': 'file_copy',
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

        self.meta = utils.Metadata()
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
                'saver_id': 'file_copy',
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
                'saver_id': 'file_mirror',
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
                'saver_id': 'file_copy',
                'src_paths': ['src1'],
                'dst_path': 'src1',
                'src_volume_label': 'volume1',
                'dst_volume_label': 'volume2',
            },
            {
                'saver_id': 'file_copy',
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

    def test_file_mirror(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        src_path = os.path.join(self.src_root, 'src1')
        dst_path = os.path.join(self.dst_root, 'src1')
        os.makedirs(dst_path, exist_ok=True)
        saves = [
            {
                'saver_id': 'file_mirror',
                'src_paths': [src_path],
                'dst_path': dst_path,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        ref_file = [f for f in dst_paths if os.path.basename(f) == utils.REF_FILENAME][0]
        ref = utils.SaveRef(os.path.dirname(ref_file))
        pprint(ref.data)

        shutil.rmtree(src_path)
        self._loadgame()
        src_paths = self._list_src_root_paths()
        self.assertTrue(any_str_matches(src_paths, '*src1*dir*file*'))
        self._loadgame()


class FileTestCase(BaseTestCase):
    def test_existing_dst_files_without_save_ref(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        self._generate_dst_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        saves = [
            {
                'saver_id': 'file_mirror',
                'src_paths': [self.src_root],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertEqual(count_matches(dst_paths, '*src*dir*file*'), 8)
        rf = list(self._list_save_ref_files(dst_paths).values())[0][self.src_root]
        self.assertEqual(rf.keys(), {
            'src1/dir1/file1',
            'src1/dir1/file2',
            'src1/dir2/file1',
            'src1/dir2/file2',
            'src2/dir1/file1',
            'src2/dir1/file2',
            'src2/dir2/file1',
            'src2/dir2/file2',
        })
        self.assertTrue(all(bool(v) for v in rf.values()))

    def test_dst_files_are_newer(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        saves = [
            {
                'saver_id': 'file_mirror',
                'src_paths': [self.src_root],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertEqual(count_matches(dst_paths, '*src*dir*file*'), 8)

        for f in walk_files(self.src_root):
            if os.path.basename(f) == 'file1':
                with open(f, 'w') as fd:
                    fd.write(f'new src data for {f}')
        savers.base.MTIME_DRIFT_TOLERANCE = 0
        time.sleep(savers.base.MTIME_DRIFT_TOLERANCE + .5)
        for f in walk_files(self.dst_root):
            if os.path.basename(f) == 'file1':
                with open(f, 'w') as fd:
                    fd.write(f'new dst data for {f}')
        hashes = {f: utils.get_file_hash(f) for f in dst_paths if os.path.basename(f) == 'file1'}
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        hashes2 = {f: utils.get_file_hash(f) for f in dst_paths if os.path.basename(f) == 'file1'}
        self.assertEqual(hashes2, hashes)

    def test_file(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        src = os.path.join(self.src_root, 'src1')
        dst = os.path.join(self.dst_root, 'dst1')
        saves = [
            {
                'saver_id': 'file',
                'src_paths': [src],
                'dst_path': dst,
            },
        ]
        [os.makedirs(s['dst_path'], exist_ok=True) for s in saves]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*src1*dir1*file1*'))
        rf = list(self._list_save_ref_files(dst_paths).values())[0][src]
        self.assertEqual(rf.keys(), {'dir1/file1', 'dir1/file2', 'dir2/file1', 'dir2/file2'})

        with open(os.path.join(self.src_root, 'src1', 'dir1', 'file4'), 'w') as fd:
            fd.write('data4')
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*src1*dir1*file4*'))
        rf = list(self._list_save_ref_files(dst_paths).values())[0][src]
        self.assertEqual(rf.keys(), {'dir1/file1', 'dir1/file2', 'dir2/file1', 'dir2/file2', 'dir1/file4'})

        os.remove(os.path.join(self.src_root, 'src1', 'dir1', 'file1'))
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*src1*dir1*file4*'))
        rf = list(self._list_save_ref_files(dst_paths).values())[0][src]
        self.assertEqual(rf.keys(), {'dir1/file2', 'dir2/file1', 'dir2/file2', 'dir1/file4'})

        shutil.rmtree(src)
        self._loadgame()
        src_paths = self._list_src_root_paths()
        self.assertFalse(any_str_matches(src_paths, '*src1*dir1*file1*'))
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file2*'))
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file4*'))
        self._loadgame()

    def test_file_copy(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        src1 = os.path.join(self.src_root, 'src1')
        src2 = os.path.join(self.src_root, 'src2')
        dst = os.path.join(self.dst_root, 'dst1')
        saves = [
            {
                'saver_id': 'file_copy',
                'src_paths': [[src1, ['*/dir1/*'], []]],
                'dst_path': dst,
            },
            {
                'saver_id': 'file_copy',
                'src_paths': [[src2, ['*/dir2/*'], []]],
                'dst_path': dst,
            },
        ]
        [os.makedirs(s['dst_path'], exist_ok=True) for s in saves]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dst1*dir1*file1*'))
        rf = self._list_save_ref_files(dst_paths)[dst]
        self.assertEqual(rf[src1].keys(), {'dir1/file1', 'dir1/file2'})
        self.assertEqual(rf[src2].keys(), {'dir2/file1', 'dir2/file2'})

        with open(os.path.join(self.src_root, 'src1', 'dir1', 'file3'), 'w') as fd:
            fd.write('data4')
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dst1*dir1*file3*'))
        rf = self._list_save_ref_files(dst_paths)[dst]
        self.assertEqual(rf[src1].keys(), {'dir1/file1', 'dir1/file2', 'dir1/file3'})
        self.assertEqual(rf[src2].keys(), {'dir2/file1', 'dir2/file2'})

        os.remove(os.path.join(self.src_root, 'src2', 'dir2', 'file1'))
        self._savegame(saves=saves)
        self._list_src_root_paths()
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dst1*dir2*file1*'))   # no purge
        rf = self._list_save_ref_files(dst_paths)[dst]
        self.assertEqual(rf[src1].keys(), {'dir1/file1', 'dir1/file2', 'dir1/file3'})
        self.assertEqual(rf[src2].keys(), {'dir2/file2'})

        [shutil.rmtree(src) for src in [src1, src2]]
        self._loadgame()
        src_paths = self._list_src_root_paths()
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file1*'))
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file2*'))
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file3*'))
        self.assertFalse(any_str_matches(src_paths, '*src2*dir2*file1*'))
        self.assertTrue(any_str_matches(src_paths, '*src2*dir2*file2*'))
        self._loadgame()

    def test_file_copy_new_src(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=2, nb_files=2)
        src1 = os.path.join(self.src_root, 'src1')
        src2 = os.path.join(self.src_root, 'src2')
        dst = os.path.join(self.dst_root, 'dst1')
        os.makedirs(dst, exist_ok=True)
        saves = [
            {
                'saver_id': 'file_copy',
                'src_paths': [[src1, ['*/dir1/*'], []]],
                'dst_path': dst,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dst1*dir1*file1*'))
        rf = self._list_save_ref_files(dst_paths)[dst]
        self.assertEqual(rf.keys(), {src1})
        self.assertEqual(rf[src1].keys(), {'dir1/file1', 'dir1/file2'})

        saves[0]['src_paths'] = [[src2, ['*/dir2/*'], []]]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dst1*dir1*file1*'))
        rf = self._list_save_ref_files(dst_paths)[dst]
        self.assertEqual(rf[src1].keys(), {'dir1/file1', 'dir1/file2'})
        self.assertEqual(rf[src2].keys(), {'dir2/file1', 'dir2/file2'})

    def test_filesystem_copy_new_src_other_path_sep(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=2, nb_files=2)
        src = os.path.join(self.src_root, 'src1')
        dst = os.path.join(self.dst_root, 'dst1')
        os.makedirs(dst, exist_ok=True)
        saves = [
            {
                'saver_id': 'file_copy',
                'src_paths': [[src, ['*/dir1/*'], []]],
                'dst_path': dst,
            },
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dst1*dir1*file1*'))
        rf = self._list_save_ref_files(dst_paths)[dst]
        self.assertEqual(rf.keys(), {src})
        self.assertEqual(rf[src].keys(), {'dir1/file1', 'dir1/file2'})

        other_src = 'D:\\data\\src1'
        other_files = {'dir1\\file1': 123, 'dir1\\file2': 123}
        other_data = {'files': {'other_hostname': {other_src: other_files}}, 'ts': {'other_hostname': 123}}
        save_ref = utils.SaveRef(dst)
        save_ref._load(other_data)
        self._list_save_ref_files(dst_paths)[dst]

        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dst1*dir1*file1*'))
        save_ref = self._list_save_refs(dst_paths)[dst]
        self.assertEqual(save_ref.get_files()[src].keys(), {'dir1/file1', 'dir1/file2'})
        self.assertEqual(save_ref.get_files(hostname='other_hostname')[other_src].keys(), other_files.keys())

    def test_file_mirror(self):
        self._generate_src_data(index_start=1, nb_srcs=2, nb_dirs=2, nb_files=2)
        src1 = os.path.join(self.src_root, 'src1')
        src2 = os.path.join(self.src_root, 'src2')
        dst1 = os.path.join(self.dst_root, 'dst1')
        dst2 = os.path.join(self.dst_root, 'dst2')
        saves = [
            {
                'saver_id': 'file_mirror',
                'src_paths': [src1],
                'dst_path': dst1,
            },
            {
                'saver_id': 'file_mirror',
                'src_paths': [src2],
                'dst_path': dst2,
            },
        ]
        [os.makedirs(s['dst_path'], exist_ok=True) for s in saves]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dst1*dir1*file1*'))
        self.assertTrue(any_str_matches(dst_paths, '*dst2*dir1*file1*'))
        rf = self._list_save_ref_files(dst_paths)
        self.assertEqual(rf[dst1][src1].keys(), {'dir1/file1', 'dir1/file2', 'dir2/file1', 'dir2/file2'})
        self.assertEqual(rf[dst2][src2].keys(), {'dir1/file1', 'dir1/file2', 'dir2/file1', 'dir2/file2'})

        with open(os.path.join(self.src_root, 'src1', 'dir1', 'file3'), 'w') as fd:
            fd.write('data4')
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*dir1*file3*'))
        rf = self._list_save_ref_files(dst_paths)
        self.assertEqual(rf[dst1][src1].keys(), {'dir1/file1', 'dir1/file2', 'dir2/file1', 'dir2/file2', 'dir1/file3'})
        self.assertEqual(rf[dst2][src2].keys(), {'dir1/file1', 'dir1/file2', 'dir2/file1', 'dir2/file2'})

        os.remove(os.path.join(self.src_root, 'src2', 'dir2', 'file1'))
        self._savegame(saves=saves)
        self._list_src_root_paths()
        dst_paths = self._list_dst_root_paths()
        self.assertFalse(any_str_matches(dst_paths, '*src2*dir2*file1*'))
        rf = self._list_save_ref_files(dst_paths)
        self.assertEqual(rf[dst1][src1].keys(), {'dir1/file1', 'dir1/file2', 'dir2/file1', 'dir2/file2', 'dir1/file3'})
        self.assertEqual(rf[dst2][src2].keys(), {'dir1/file1', 'dir1/file2', 'dir2/file2'})

        [shutil.rmtree(src) for src in [src1, src2]]
        self._loadgame()
        src_paths = self._list_src_root_paths()
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file1*'))
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file2*'))
        self.assertTrue(any_str_matches(src_paths, '*src1*dir1*file3*'))
        self.assertFalse(any_str_matches(src_paths, '*src2*dir2*file1*'))
        self.assertTrue(any_str_matches(src_paths, '*src2*dir2*file2*'))
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
        self._create_repo('repo1')
        self._create_repo('repo2')
        saves = [
            {
                'saver_id': 'git',
                'src_paths': [self.src_root],
                'dst_path': self.dst_root,
            },
        ]
        self._savegame(saves)
        dst_paths = self._list_dst_root_paths()
        rf = list(self._list_save_ref_files(dst_paths).values())[0][self.src_root]
        self.assertEqual(rf.keys(), {
            'repo1.bundle',
            'repo1/dir1/file1.txt',
            'repo1/dir2/file2.txt',
            'repo1/dir3/file3.txt',
            'repo2.bundle',
            'repo2/dir1/file1.txt',
            'repo2/dir2/file2.txt',
            'repo2/dir3/file3.txt',
        })

        shutil.rmtree(os.path.join(self.src_root, 'repo1'))
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


class GoogleDriveTestCase(BaseTestCase):
    def _get_google_cloud(self, dt):
        def iterate_file_meta():
            return [
                {
                    'id': '123',
                    'name': 'file1',
                    'path': 'file1',
                    'modified_time': dt,
                    'exportable': True,
                    'mime_type': 'text/plain',
                },
            ]

        def export_file(file_id, path, mime_type):
            with open(path, 'w') as fd:
                fd.write(f'{file_id=} {mime_type=} {dt.isoformat()=}')

        return Mock(iterate_file_meta=iterate_file_meta, export_file=export_file)

    def test_1(self):
        dst = os.path.join(self.dst_root, 'dst1')
        saves = [
            {
                'saver_id': 'google_drive',
                'dst_path': dst,
            },
        ]
        [os.makedirs(s['dst_path'], exist_ok=True) for s in saves]
        dt = datetime.now(timezone.utc) - timedelta(seconds=10)
        with patch.object(savers.google_cloud, 'get_google_cloud', return_value=self._get_google_cloud(dt)):
            self._savegame(saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*google_drive/file1*'))
        save_ref = list(self._list_save_refs(dst_paths).values())[0]
        fr1 = save_ref.get_files(hostname='google_cloud')['google_drive']
        pprint(fr1)
        self.assertEqual(fr1.keys(), {'file1'})

        with patch.object(savers.google_cloud, 'get_google_cloud', return_value=self._get_google_cloud(dt)):
            self._savegame(saves)
        dst_paths = self._list_dst_root_paths()
        fr2 = save_ref.get_files(hostname='google_cloud')['google_drive']
        pprint(fr2)
        self.assertEqual(fr2, fr1)

        dt2 = datetime.now(timezone.utc)
        with patch.object(savers.google_cloud, 'get_google_cloud', return_value=self._get_google_cloud(dt2)):
            self._savegame(saves)
        dst_paths = self._list_dst_root_paths()
        fr3 = save_ref.get_files(hostname='google_cloud')['google_drive']
        pprint(fr3)
        self.assertNotEqual(fr3, fr1)


class GoogleContactsTestCase(BaseTestCase):
    def _get_google_cloud(self, nb_contacts=10):
        def list_contacts():
            return [{'name': f'contact{i}'} for i in range(nb_contacts)]

        return Mock(list_contacts=list_contacts)

    def test_1(self):
        dst = os.path.join(self.dst_root, 'dst1')
        saves = [
            {
                'saver_id': 'google_contacts',
                'dst_path': dst,
            },
        ]
        [os.makedirs(s['dst_path'], exist_ok=True) for s in saves]
        nb_contacts = 10
        with patch.object(savers.google_cloud, 'get_google_cloud', return_value=self._get_google_cloud(nb_contacts)):
            self._savegame(saves)
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*google_contacts/contacts.json*'))
        save_ref = list(self._list_save_refs(dst_paths).values())[0]
        fr1 = save_ref.get_files(hostname='google_cloud')['google_contacts']
        pprint(fr1)
        self.assertEqual(fr1.keys(), {'contacts.json'})

        with patch.object(savers.google_cloud, 'get_google_cloud', return_value=self._get_google_cloud(nb_contacts)):
            self._savegame(saves)
        dst_paths = self._list_dst_root_paths()
        fr2 = save_ref.get_files(hostname='google_cloud')['google_contacts']
        pprint(fr2)
        self.assertEqual(fr2, fr1)

        nb_contacts2 = 11
        with patch.object(savers.google_cloud, 'get_google_cloud', return_value=self._get_google_cloud(nb_contacts2)):
            self._savegame(saves)
        dst_paths = self._list_dst_root_paths()
        fr3 = save_ref.get_files(hostname='google_cloud')['google_contacts']
        pprint(fr3)
        self.assertNotEqual(fr3, fr1)


class VirtualboxTestCase(BaseTestCase):
    def _run(self, saves, running_vms, vms):
        def side_export_vm(vm, file):
            with open(file, 'w') as fd:
                fd.write(f'{vm} data')

        with patch.object(virtualbox.Virtualbox, 'list_running_vms', return_value=running_vms), \
                patch.object(virtualbox.Virtualbox, 'list_vms', return_value=vms), \
                patch.object(virtualbox.Virtualbox, 'export_vm', side_effect=side_export_vm), \
                patch.object(virtualbox, 'notify') as mock_notify:
            self._savegame(saves=saves)
            pprint(mock_notify.call_args_list)

    def test_1(self):
        dst = os.path.join(self.dst_root, 'dst1')
        saves = [
            {
                'saver_id': 'virtualbox',
                'dst_path': dst,
            },
        ]
        [os.makedirs(s['dst_path'], exist_ok=True) for s in saves]

        self._run(saves, ['ub1'], ['ub1', 'ub2', 'win3', 'test_fed4'])
        dst_paths = self._list_dst_root_paths()
        self.assertFalse(any_str_matches(dst_paths, '*ub1.ova'))
        self.assertTrue(any_str_matches(dst_paths, '*ub2.ova'))
        self.assertTrue(any_str_matches(dst_paths, '*win3.ova'))
        self.assertFalse(any_str_matches(dst_paths, '*test_fed*'))
        rf = self._list_save_ref_files(dst_paths)[dst]['virtualbox']
        self.assertEqual(set(rf.keys()), {'ub2.ova', 'win3.ova'})

        self._run(saves, ['win3'], ['ub1', 'ub2', 'win3', 'test_fed4'])
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*ub1.ova'))
        self.assertTrue(any_str_matches(dst_paths, '*ub2.ova'))
        self.assertTrue(any_str_matches(dst_paths, '*win3.ova'))
        self.assertFalse(any_str_matches(dst_paths, '*test_fed*'))
        rf = self._list_save_ref_files(dst_paths)[dst]['virtualbox']
        self.assertEqual(set(rf.keys()), {'ub1.ova', 'ub2.ova', 'win3.ova'})

        self._run(saves, ['ub2'], ['ub1', 'ub2', 'test_fed4'])
        dst_paths = self._list_dst_root_paths()
        self.assertTrue(any_str_matches(dst_paths, '*ub1.ova'))
        self.assertTrue(any_str_matches(dst_paths, '*ub2.ova'))
        self.assertTrue(any_str_matches(dst_paths, '*win3.ova'))
        self.assertFalse(any_str_matches(dst_paths, '*test_fed*'))
        rf = self._list_save_ref_files(dst_paths)[dst]['virtualbox']
        self.assertEqual(set(rf.keys()), {'ub1.ova', 'ub2.ova'})


class ManySourcesTestCase(BaseTestCase):
    def test_1(self):
        self._generate_src_data(index_start=1, nb_srcs=4, nb_dirs=3, nb_files=2)
        saves = [
            {
                'saver_id': 'file',
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
                'saver_id': 'file',
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
                'saver_id': 'file',
                'src_paths': [
                    os.path.join(self.src_root, 'src4', 'dir1', 'file1'),
                ],
                'dst_path': os.path.join(self.dst_root, 'dst1'),
            },
            {
                'saver_id': 'file_copy',
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
                'saver_id': 'file_copy',
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
                'saver_id': 'file_mirror',
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
                'saver_id': 'file_mirror',
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
        self._list_save_ref_files(dst_paths)

        saves[0]['src_paths'] = [
            [
                os.path.join(self.src_root, 'src1'),
                ['*/dir3/*'],
                [],
            ],
        ]
        self._savegame(saves=saves)
        dst_paths = self._list_dst_root_paths()
        self._list_save_ref_files(dst_paths)

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


class SaveMonitorTestCase(BaseTestCase):
    def test_1(self):
        self._generate_src_data(index_start=1, nb_srcs=4, nb_dirs=3, nb_files=2)
        saves = [
            {
                'saver_id': 'file',
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
                'saver_id': 'file',
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
                'saver_id': 'file',
                'src_paths': [
                    os.path.join(self.src_root, 'src4', 'dir1', 'file1'),
                ],
                'dst_path': os.path.join(self.dst_root, 'dst1'),
            },
            {
                'saver_id': 'file_copy',
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
                'saver_id': 'file_copy',
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
                'saver_id': 'file_mirror',
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
                'saver_id': 'file_mirror',
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

        with patch.object(save.SaveMonitor, '_must_run', return_value=True), \
                patch.object(save, 'notify') as mock_notify:
            sc = save.SaveMonitor(self.config)
            sc.run()
        print(mock_notify.call_args_list)
        self.assertTrue('saves: 6' in mock_notify.call_args_list[0][1]['body'].split(', '))


class ReportTestCase(BaseTestCase):
    def test_1(self):
        self._generate_src_data(index_start=1, nb_srcs=3, nb_dirs=2, nb_files=2)
        for dirname in ['dst1', 'dst2', 'dst3']:
            os.makedirs(os.path.join(self.dst_root, dirname), exist_ok=True)
        saves = [
            {
                'saver_id': 'file',
                'src_paths': [os.path.join(self.src_root, 'src1')],
                'dst_path': os.path.join(self.dst_root, 'dst1'),
            },
            {
                'saver_id': 'file_mirror',
                'src_paths': [os.path.join(self.src_root, 'src2')],
                'dst_path': os.path.join(self.dst_root, 'dst2'),
            },
            {
                'saver_id': 'file_copy',
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
