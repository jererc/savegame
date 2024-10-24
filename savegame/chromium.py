import json
import os
import subprocess

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


CONFIGS = {
    'nt': [
        {
            'binary': r'C:\Program Files\BraveSoftware'
                r'\Brave-Browser\Application\brave.exe',
            'data_dir': os.path.expanduser(
                r'~\AppData\Local\BraveSoftware\Brave-Browser\User Data'),
        },
        {
            'binary': r'C:\Program Files\Google\Chrome\Application\chrome.exe',
            'data_dir': os.path.expanduser(
                r'~\AppData\Local\Google\Chrome\User Data'),
        },
    ],
    'posix': [
        {
            'binary': '/opt/brave.com/brave/brave',
            'data_dir': os.path.expanduser(
                '~/.config/BraveSoftware/Brave-Browser'),
        },
        {
            'binary': '/opt/google/chrome/chrome',
            'data_dir': os.path.expanduser('~/.config/google-chrome'),
        },
    ],
}[os.name]
KILL_CMD = {
    'nt': 'taskkill /IM {binary}',
    'posix': 'pkill {binary}',
}[os.name]
PROFILE_DIR = 'selenium'


class Chromium:
    def __init__(self, profile_dir=PROFILE_DIR):
        self.profile_dir = profile_dir
        config = self._get_config()
        self.data_dir = config['data_dir']
        self.binary = config['binary']
        self.driver = self._get_driver()

    def _get_config(self):
        for config in CONFIGS:
            if all(os.path.exists(p) for p in config.values()):
                return config
        raise Exception('no available browser')

    def _kill_running_browser(self):
        subprocess.call(KILL_CMD.format(
            binary=os.path.basename(self.binary)), shell=True)

    def _get_driver(self):
        self._kill_running_browser()
        options = Options()
        options.add_argument(f'--user-data-dir={self.data_dir}')
        options.add_argument(f'--profile-directory={self.profile_dir}')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('useAutomationExtension', False)
        options.add_experimental_option('excludeSwitches',
            ['enable-automation'])
        options.add_experimental_option('detach', True)
        options.binary_location = self.binary
        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(1)
        return driver

    def quit(self):
        self.driver.quit()


class BookmarksHandler:
    filename = 'Bookmarks'

    def _get_profile_paths(self, data_dir):
        profile_paths = {}
        local_state_path = os.path.join(data_dir, 'Local State')
        if os.path.exists(local_state_path):
            with open(local_state_path, encoding='utf-8') as fd:
                local_state_data = json.load(fd)
            profile_info_cache = local_state_data.get('profile', {}).get(
                'info_cache', {})
            for profile_path, profile_info in profile_info_cache.items():
                full_profile_path = os.path.join(data_dir, profile_path)
                profile_paths[profile_info['name']] = full_profile_path
        return profile_paths

    def _set_date_last_used_to_zero(self, bookmarks):
        def update_date_last_used(bookmark):
            if isinstance(bookmark, dict):
                if 'date_last_used' in bookmark:
                    bookmark['date_last_used'] = '0'
                if 'children' in bookmark:
                    for child in bookmark['children']:
                        update_date_last_used(child)

        roots = bookmarks.get('roots', {})
        for root in roots.values():
            if 'children' in root:
                update_date_last_used(root)

    def _to_html(self, bookmarks):
        lines = ['<!DOCTYPE NETSCAPE-Bookmark-file-1>',
            '<META HTTP-EQUIV="Content-Type" CONTENT="text/html; '
                'charset=UTF-8">',
            '<TITLE>Bookmarks</TITLE>',
            '<H1>Bookmarks</H1>',
            '<DL><p>']

        def parse_folder(folder, indent=1):
            indent_str = '    ' * indent
            lines.append(f'{indent_str}<DT><H3>{folder["name"]}</H3>')
            lines.append(f'{indent_str}<DL><p>')
            for item in folder.get('children', []):
                if item['type'] == 'folder':
                    parse_folder(item, indent + 1)
                elif item['type'] == 'url':
                    lines.append(f'{indent_str}    '
                        f'<DT><A HREF="{item["url"]}">{item["name"]}</A>')
            lines.append(f'{indent_str}</DL><p>')

        roots = bookmarks.get('roots', {})
        for root in roots.values():
            if 'children' in root:
                parse_folder(root)
        lines.append('</DL><p>')
        return '\n'.join(lines)

    def export(self):
        for config in CONFIGS:
            if not os.path.exists(config['binary']):
                continue
            browser_name = os.path.splitext(
                os.path.basename(config['binary']))[0]
            data_dir = os.path.expanduser(config['data_dir'])
            for profile_name, profile_path in self._get_profile_paths(
                    os.path.expanduser(data_dir)).items():
                file = os.path.join(profile_path, self.filename)
                if not os.path.exists(file):
                    continue
                with open(file, encoding='utf-8') as fd:
                    data = json.load(fd)
                self._set_date_last_used_to_zero(data)
                dirname = os.path.basename(profile_path)
                yield {
                    'path': os.path.join(browser_name, dirname,
                        self.filename),
                    'content': json.dumps(data, sort_keys=True, indent=4),
                }
                yield {
                    'path': os.path.join(browser_name, dirname,
                        f'{self.filename}.html'),
                    'content': self._to_html(data),
                }
