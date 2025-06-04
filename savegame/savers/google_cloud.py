from datetime import datetime, timezone
import os

from savegame import logger
from savegame.lib import get_file_hash, get_file_mtime, get_hash, to_json
from savegame.savers.base import BaseSaver
from savegame.savers.google_api import GoogleCloud


def get_file_mtime_dt(x):
    if os.path.exists(x):
        return datetime.fromtimestamp(get_file_mtime(x), tz=timezone.utc)


def get_google_cloud(config, headless=True):
    return GoogleCloud(
        oauth_secrets_file=os.path.expanduser(config.GOOGLE_CREDS),
        headless=headless,
    )


class GoogleDriveExportSaver(BaseSaver):
    id = 'google_drive_export'
    hostname = 'google_cloud'
    src_type = 'remote'

    def do_run(self):
        gc = get_google_cloud(self.config)
        for file_meta in gc.iterate_file_meta():
            if not file_meta['exportable']:
                self.report.add('skipped', self.src, file_meta['path'])
                continue
            dst_file = os.path.join(self.dst, file_meta['path'])
            self.dst_paths.add(dst_file)
            dt = get_file_mtime_dt(dst_file)
            if dt and dt > file_meta['modified_time']:
                self.report.add('skipped', self.src, dst_file)
                continue
            os.makedirs(os.path.dirname(dst_file), exist_ok=True)
            try:
                gc.export_file(file_id=file_meta['id'],
                               path=dst_file,
                               mime_type=file_meta['mime_type'])
                self.report.add('saved', self.src, dst_file)
            except Exception as exc:
                self.report.add('failed', self.src, dst_file)
                logger.error('failed to save google drive file '
                             f'{file_meta["name"]}: {exc}')
        self.ref.files = {os.path.relpath(p, self.dst): get_file_hash(p)
                          for p in self.dst_paths}


class GoogleContactsExportSaver(BaseSaver):
    id = 'google_contacts_export'
    hostname = 'google_cloud'
    src_type = 'remote'

    def do_run(self):
        gc = get_google_cloud(self.config)
        contacts = gc.list_contacts()
        data = to_json(contacts)
        rel_path = 'contacts.json'
        dst_file = os.path.join(self.dst, rel_path)
        self.dst_paths.add(dst_file)
        dst_hash = get_hash(data)
        if os.path.exists(dst_file) and \
                dst_hash == self.ref.files.get(rel_path):
            self.report.add('skipped', self.src, dst_file)
        else:
            os.makedirs(os.path.dirname(dst_file), exist_ok=True)
            with open(dst_file, 'w', encoding='utf-8', newline='\n') as fd:
                fd.write(data)
            self.report.add('saved', self.src, dst_file)
            logger.info(f'saved {len(contacts)} google contacts')
        self.ref.files = {rel_path: dst_hash}
