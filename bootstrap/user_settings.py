import os.path

DST_PATH = os.path.join('~', 'MEGA', 'data')
GOOGLE_CLOUD_SECRETS_FILE = r'C:\Users\jerer\google_cloud_secrets.json'
SAVES = [
    {
        'src_paths': [
            r'~\Documents\Battlefield 2042',
            [
                r'C:\Users\Public\Documents\*',
                [],
                [r'*\desktop.ini', r'*\My Music*', r'*\My Pictures*', r'*\My Videos*'],
            ],
        ],
    },
    {
        'src_paths': [
            r'~\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\shortcuts',
            r'~\AppData\Roaming\Sublime Text 3',
            r'C:\Program Files (x86)\Steam\steamapps\common\Steam Controller Configs',
        ],
        'run_delta': 2 * 3600,
    },
    {
        'src_paths': [
            '~/.bash_aliases',
            '~/.config/sublime-text',
            '~/.gitconfig',
            '~/.local/share/applications',
            [
                '~/.ssh',
                [],
                ['*/id_ed25519'],
            ],
            [
                '~/data/code',
                [],
                ['*/.git*', '*/venv*', '*/__pycache__*', '*.pyc', '*/node_modules*'],
            ],
        ],
        'run_delta': 2 * 3600,
    },
    {
        'saver_id': 'google_drive_export',
        'os_name': 'nt',
        'run_delta': 12 * 3600,
    },
    {
        'saver_id': 'google_contacts_export',
        'os_name': 'nt',
        'run_delta': 12 * 3600,
    },
    {
        'saver_id': 'bookmarks_export',
        'run_delta': 12 * 3600,
    },
    {
        'src_paths': [
            os.path.join('~', 'MEGA', 'data', 'env'),
        ],
        'dst_path': r'~\OneDrive\data',
        'os_name': 'nt',
        'run_delta': 12 * 3600,
        'loadable': False,
    },
]
