import json
import os
import shutil
import unittest

from savegame.savers.google_api import GoogleCloud


WORK_DIR = os.path.join(os.path.expanduser('~'), '_tests', 'webutils')


def remove_path(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)


class FileTestCase(unittest.TestCase):
    def setUp(self):
        remove_path(WORK_DIR)
        os.makedirs(WORK_DIR, exist_ok=True)

    def test_1(self):
        secrets_file = os.path.join(WORK_DIR, 'secrets.json')
        self.assertRaises(Exception,
                          GoogleCloud,
                          oauth_secrets_file=secrets_file)

        with open(secrets_file, 'w') as fd:
            json.dump({'k': 'v'}, fd)
        res = GoogleCloud(oauth_secrets_file=secrets_file)
        print(res.token_file)
        self.assertEqual(res.token_file,
                         os.path.join(WORK_DIR, 'secrets-token.json'))
