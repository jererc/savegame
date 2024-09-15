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


class BaseSavegameTestCase(unittest.TestCase):


    def setUp(self):
        assert savegame.WORK_PATH, user_settings.WORK_PATH
        shutil.rmtree(user_settings.WORK_PATH)
        os.makedirs(user_settings.WORK_PATH)



class SavegameTestCase(BaseSavegameTestCase):


    def test_1(self):
        src_path = MODULE_PATH
        dst_path = os.path.join(savegame.WORK_PATH, 'dst')
        savegame.SAVES = [
            {
                'src_paths': [
                    [
                        src_path,
                        [],
                        ['*/__pycache__*'],
                    ],
                ],
                'dst_path': dst_path,
            },
        ]
        savegame.savegame()
        pprint(savegame.MetaManager().meta)


class GoogleContactsTestCase(BaseSavegameTestCase):


    def test_1(self):
        creds_file = os.path.realpath(os.path.expanduser(
            '~/data/credentials_oauth.json'))
        google_cloud.GoogleCloud(oauth_creds_file=creds_file
            ).get_oauth_creds(interact=True)
        dst_path = os.path.join(savegame.WORK_PATH, 'dst')
        savegame.SAVES = [
            {
                'src_type': 'google_contacts',
                'dst_path': dst_path,
                'gc_oauth_creds_file': creds_file,
            },
        ]
        savegame.savegame()
        pprint(savegame.MetaManager().meta)


class GoogleBookmarksTestCase(BaseSavegameTestCase):


    def test_1(self):
        dst_path = os.path.join(savegame.WORK_PATH, 'dst')

        old_dir = os.path.join(dst_path, 'old_dir')
        os.makedirs(old_dir)
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
        pprint(savegame.MetaManager().meta)


if __name__ == '__main__':
    unittest.main()
