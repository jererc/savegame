import json
import os


IS_WIN = os.name == 'nt'
DATA_DIR = os.path.expanduser(r'~\AppData\Local\Google\Chrome\User Data'
    if IS_WIN else '~/.config/google-chrome')
BOOKMARK_FILE = os.path.join(DATA_DIR, 'Default', 'Bookmarks')


def extract_bookmarks(data, path='', bookmarks_list=None):
    if bookmarks_list is None:
        bookmarks_list = []

    for item in data.get('children', []):
        if item.get('type') == 'url':
            bookmarks_list.append({
                'name': item['name'],
                'url': item['url'],
                'path': path
            })
        elif item.get('type') == 'folder':
            new_path = path + '/' + item['name'] if path else item['name']
            extract_bookmarks(item, new_path, bookmarks_list)

    return bookmarks_list


def get_bookmarks():
    with open(BOOKMARK_FILE, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return extract_bookmarks(data['roots']['bookmark_bar'])
