import os
from pprint import pprint
import sys
import unittest
REPO_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.insert(0, os.path.join(REPO_PATH, 'savegame'))
import google_chrome


class BookmarksTestCase(unittest.TestCase):
    def test_1(self):
        res = google_chrome.get_bookmarks()
        pprint(res)
        self.assertTrue(res)
