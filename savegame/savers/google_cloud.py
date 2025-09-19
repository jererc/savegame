from datetime import datetime, timezone
import logging
import os

from savegame.lib import get_file_hash, get_file_mtime, get_hash, to_json
from savegame.savers.base import BaseSaver
from savegame.savers.google_api import GoogleCloud

logger = logging.getLogger(__name__)


def get_file_mtime_dt(x):
    if os.path.exists(x):
        return datetime.fromtimestamp(get_file_mtime(x), tz=timezone.utc)


def get_google_cloud(config, headless=True):
    return GoogleCloud(oauth_secrets_file=os.path.expanduser(config.GOOGLE_CREDS), headless=headless)


class GoogleDriveSaver(BaseSaver):
    id = 'google_drive'
    hostname = 'google_cloud'

    def do_run(self):
        gc = get_google_cloud(self.config)
        for file_meta in gc.iterate_file_meta():
            if not file_meta['exportable']:
                self.report.add('skipped', self.src, file_meta['path'])
                continue
            dst_file = os.path.join(self.dst, file_meta['path'])
            self.add_existing_dst_path(dst_file)
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
                logger.error(f'failed to save google drive file {file_meta["name"]}: {exc}')
        self.ref.files = {os.path.relpath(p, self.dst): get_file_hash(p) for p in self.existing_dst_paths}


class GoogleContactsSaver(BaseSaver):
    id = 'google_contacts'
    hostname = 'google_cloud'

    def do_run(self):
        gc = get_google_cloud(self.config)
        contacts = gc.list_contacts()
        data = to_json(contacts)
        rel_path = 'contacts.json'
        dst_file = os.path.join(self.dst, rel_path)
        self.add_existing_dst_path(dst_file)
        dst_hash = get_hash(data)
        if os.path.exists(dst_file) and dst_hash == self.ref.files.get(rel_path):
            self.report.add('skipped', self.src, dst_file)
        else:
            os.makedirs(os.path.dirname(dst_file), exist_ok=True)
            with open(dst_file, 'w', encoding='utf-8', newline='\n') as fd:
                fd.write(data)
            self.report.add('saved', self.src, dst_file)
            logger.info(f'saved {len(contacts)} google contacts')
        self.ref.files = {rel_path: dst_hash}
