import json
import os


DATA_DIRS = {
    'nt': {
        'brave': r'~\AppData\Local\BraveSoftware\Brave-Browser\User Data',
        'chrome': r'~\AppData\Local\Google\Chrome\User Data',
    },
    'posix': {
        'brave': '~/.config/BraveSoftware/Brave-Browser',
        'chrome': '~/.config/google-chrome',
    },
}[os.name]
BOOKMARKS_FILENAME = 'Bookmarks'


def get_profile_paths(data_dir):
    profile_paths = {}
    local_state_path = os.path.join(data_dir, 'Local State')
    if os.path.exists(local_state_path):
        with open(local_state_path, 'r', encoding='utf-8') as f:
            local_state_data = json.load(f)
        profile_info_cache = local_state_data.get('profile', {}).get(
            'info_cache', {})
        for profile_path, profile_info in profile_info_cache.items():
            full_profile_path = os.path.join(data_dir, profile_path)
            profile_paths[profile_info['name']] = full_profile_path
    return profile_paths


def bookmarks_to_html(bookmarks):
    html_content = ['<!DOCTYPE NETSCAPE-Bookmark-file-1>',
        '<META HTTP-EQUIV="Content-Type" CONTENT="text/html; charset=UTF-8">',
        '<TITLE>Bookmarks</TITLE>',
        '<H1>Bookmarks</H1>',
        '<DL><p>']

    def parse_bookmark_folder(folder, indent=1):
        indent_str = '    ' * indent
        html_content.append(f'{indent_str}<DT><H3>{folder["name"]}</H3>')
        html_content.append(f'{indent_str}<DL><p>')
        for item in folder.get('children', []):
            if item['type'] == 'folder':
                parse_bookmark_folder(item, indent + 1)
            elif item['type'] == 'url':
                html_content.append(f'{indent_str}    '
                    f'<DT><A HREF="{item["url"]}">{item["name"]}</A>')
        html_content.append(f'{indent_str}</DL><p>')

    roots = bookmarks.get('roots', {})
    for root in roots.values():
        if 'children' in root:
            parse_bookmark_folder(root)

    html_content.append('</DL><p>')
    return '\n'.join(html_content)


def list_bookmarks():
    for browser_name, data_dir in DATA_DIRS.items():
        for profile_name, profile_path in get_profile_paths(
                os.path.expanduser(data_dir)).items():
            file = os.path.join(profile_path, BOOKMARKS_FILENAME)
            if not os.path.exists(file):
                continue
            with open(file, 'r', encoding='utf-8') as fd:
                data = json.load(fd)
            yield f'{browser_name}-{profile_name}', bookmarks_to_html(data)
