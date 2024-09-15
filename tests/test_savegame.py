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


class TestCase(unittest.TestCase):


    def setUp(self):
        assert savegame.WORK_PATH, user_settings.WORK_PATH
        shutil.rmtree(user_settings.WORK_PATH)


    def test_1(self):
        src_path = MODULE_PATH
        dst_path = os.path.join(savegame.WORK_PATH, 'dst')
        if not os.path.exists(dst_path):
            os.makedirs(dst_path)

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


if __name__ == '__main__':
    unittest.main()
