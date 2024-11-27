from copy import deepcopy
import os

from webutils.bookmarks import BookmarksHandler

from savegame.lib import HOSTNAME, atomic_write_fd, get_hash, makedirs
from savegame.savers.base import BaseSaver


class BookmarksExportSaver(BaseSaver):
    id = 'bookmarks_export'
    hostname = HOSTNAME

    def do_run(self):
        ref_files = deepcopy(self.ref.files)
        self.ref.files = {}
        for file_meta in BookmarksHandler().export():
            rel_path = file_meta['path']
            dst_file = os.path.join(self.dst, rel_path)
            self.dst_paths.add(dst_file)
            dst_hash = get_hash(file_meta['content'])
            if os.path.exists(dst_file) and \
                    dst_hash == ref_files.get(rel_path):
                self.report.add('skipped', self.src, dst_file)
            else:
                makedirs(os.path.dirname(dst_file))
                with atomic_write_fd(dst_file, mode='w', encoding='utf-8',
                        newline='\n') as fd:
                    fd.write(file_meta['content'])
                self.report.add('saved', self.src, dst_file)
            self.ref.files[rel_path] = dst_hash
