import json
import os
import sys
import unittest
REPO_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(REPO_PATH, 'savegame'))
from google_autoauth import GoogleAutoauth


class GoogleAutoauthTestCase(unittest.TestCase):
    def test_1(self):
        creds_file = os.path.realpath(os.path.expanduser(
            '~/data/credentials_oauth.json'))
        scopes = [
            'https://www.googleapis.com/auth/contacts.readonly',
            'https://www.googleapis.com/auth/drive.readonly',
        ]
        res = GoogleAutoauth(creds_file, scopes).acquire_credentials()
        self.assertTrue(res)
        creds_json = res.to_json()
        print(creds_json)
        creds_dict = json.loads(creds_json)
        self.assertTrue(creds_dict.get('token'))
