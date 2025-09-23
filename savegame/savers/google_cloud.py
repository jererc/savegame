from datetime import datetime, timezone
import logging
import os
import time

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
        ref_files = self.save_ref.get_files(self.src)
        self.save_ref.reset_files(self.src)
        for file_meta in gc.iterate_file_meta():
            if not file_meta['exportable']:
                logger.debug(f'skipping not exportable file {file_meta["path"]}')
                continue
            rel_path = file_meta['path']
            ref_val = ref_files.get(rel_path)
            dst_file = os.path.join(self.dst, rel_path)
            dst_dt = get_file_mtime_dt(dst_file)
            if not dst_dt or dst_dt < file_meta['modified_time']:
                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                start_ts = time.time()
                try:
                    gc.export_file(file_id=file_meta['id'], path=dst_file, mime_type=file_meta['mime_type'])
                    ref_val = get_file_hash(dst_file)
                    self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts)
                except Exception as e:
                    logger.error(f'failed to save google drive file {file_meta["name"]}: {e}')
                    self.report.add(self, rel_path=rel_path, code='failed')
            self.save_ref.set_file(self.src, rel_path, ref_val)


class GoogleContactsSaver(BaseSaver):
    id = 'google_contacts'
    hostname = 'google_cloud'

    def do_run(self):
        gc = get_google_cloud(self.config)
        ref_files = self.save_ref.get_files(self.src)
        self.save_ref.reset_files(self.src)
        start_ts = time.time()
        contacts = gc.list_contacts()
        data = to_json(contacts)
        rel_path = 'contacts.json'
        ref_val = ref_files.get(rel_path)
        dst_file = os.path.join(self.dst, rel_path)
        dst_hash = get_hash(data)
        if not os.path.exists(dst_file) or dst_hash != ref_val:
            os.makedirs(os.path.dirname(dst_file), exist_ok=True)
            with open(dst_file, 'w', encoding='utf-8', newline='\n') as fd:
                fd.write(data)
            ref_val = dst_hash
            self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts)
        self.save_ref.set_file(self.src, rel_path, ref_val)
