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
        self.save_ref.init_files(self.src)
        for file_meta in gc.iterate_file_meta():
            if not file_meta['exportable']:
                logger.debug(f'skipping not exportable file {file_meta["path"]}')
                continue
            dst_file = os.path.join(self.dst, file_meta['path'])
            dt = get_file_mtime_dt(dst_file)
            if not dt or dt < file_meta['modified_time']:
                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                try:
                    gc.export_file(file_id=file_meta['id'], path=dst_file, mime_type=file_meta['mime_type'])
                    self.report.add(self, src_file=file_meta['path'], dst_file=dst_file, code='saved')
                except Exception as exc:
                    self.report.add(self, src_file=file_meta['path'], dst_file=dst_file, code='failed')
                    logger.error(f'failed to save google drive file {file_meta["name"]}: {exc}')
            self.save_ref.set_file(self.src, file_meta['path'], get_file_hash(dst_file))


class GoogleContactsSaver(BaseSaver):
    id = 'google_contacts'
    hostname = 'google_cloud'

    def do_run(self):
        gc = get_google_cloud(self.config)
        ref_files = self.save_ref.get_src_files(self.src)
        self.save_ref.init_files(self.src)
        contacts = gc.list_contacts()
        data = to_json(contacts)
        rel_path = 'contacts.json'
        dst_file = os.path.join(self.dst, rel_path)
        dst_hash = get_hash(data)
        if not os.path.exists(dst_file) or dst_hash != ref_files.get(rel_path):
            os.makedirs(os.path.dirname(dst_file), exist_ok=True)
            with open(dst_file, 'w', encoding='utf-8', newline='\n') as fd:
                fd.write(data)
            self.report.add(self, src_file=rel_path, dst_file=dst_file, code='saved')
            logger.info(f'saved {len(contacts)} google contacts')
        self.save_ref.set_file(self.src, rel_path, dst_hash)
