import os
import sys


REPO_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(os.path.join(REPO_PATH, 'savegame'))

from google_autoauth import GoogleAutoauth


def main():
    creds_file = os.path.realpath(os.path.expanduser('~/data/credentials_oauth.json'))
    scopes = [
        'https://www.googleapis.com/auth/contacts.readonly',
        'https://www.googleapis.com/auth/drive.readonly',
    ]
    creds = GoogleAutoauth(creds_file, scopes).acquire_credentials()
    print(creds.to_json())


if __name__ == '__main__':
    main()
