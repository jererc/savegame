from datetime import datetime, timezone
import logging
import os
import time

from savegame.savers.base import BaseSaver
from savegame.savers.google_api import GoogleCloud
from savegame.utils import FileRef, get_file_mtime, get_file_size, get_hash, to_json

logger = logging.getLogger(__name__)


def ts_to_dt(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def get_file_mtime_dt(x):
    return ts_to_dt(get_file_mtime(x)) if os.path.exists(x) else None


def get_google_cloud(config, headless=True):
    oauth_secrets_file = os.path.expanduser(config.GOOGLE_CREDS)
    if not os.path.exists(oauth_secrets_file):
        logger.warning(f'{oauth_secrets_file} does not exist')
        return None
    return GoogleCloud(oauth_secrets_file=oauth_secrets_file, headless=headless)


class GoogleDriveSaver(BaseSaver):
    id = 'google_drive'
    hostname = 'google_cloud'

    def do_run(self):
        gc = get_google_cloud(self.config)
        if not gc:
            return
        file_refs = self.reset_files(self.src)
        for file_meta in gc.iterate_file_meta():
            if not file_meta['exportable']:
                logger.debug(f'skipping not exportable file {file_meta["path"]}')
                continue
            rel_path = file_meta['path']
            file_ref = FileRef.from_ref(file_refs.get(rel_path))
            dst_file = os.path.join(self.dst, rel_path)
            dst_dt = get_file_mtime_dt(dst_file)
            if not dst_dt or dst_dt < file_meta['modified_time']:
                os.makedirs(os.path.dirname(dst_file), exist_ok=True)
                start_ts = time.time()
                try:
                    gc.export_file(file_id=file_meta['id'], path=dst_file, mime_type=file_meta['mime_type'])
                    file_ref = FileRef.from_file(dst_file, has_src_file=False)
                    self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts, size=get_file_size(dst_file))
                except Exception as e:
                    logger.error(f'failed to save google drive file {file_meta["name"]}: {e}')
                    self.report.add(self, rel_path=rel_path, code='failed')
            elif os.path.exists(dst_file):
                file_ref = FileRef.from_file(dst_file, has_src_file=False)
            self.set_file(self.src, rel_path, file_ref.ref)


class GoogleContactsSaver(BaseSaver):
    id = 'google_contacts'
    hostname = 'google_cloud'

    def do_run(self):
        gc = get_google_cloud(self.config)
        if not gc:
            return
        file_refs = self.reset_files(self.src)
        start_ts = time.time()
        contacts = gc.list_contacts()
        data = to_json(contacts)
        rel_path = 'contacts.json'
        file_ref = FileRef.from_ref(file_refs.get(rel_path))
        dst_file = os.path.join(self.dst, rel_path)
        dst_hash = get_hash(data)
        if not os.path.exists(dst_file) or dst_hash != file_ref.hash:
            os.makedirs(os.path.dirname(dst_file), exist_ok=True)
            with open(dst_file, 'w', encoding='utf-8', newline='\n') as fd:
                fd.write(data)
            file_ref = FileRef(hash=dst_hash, has_src_file=False)
            self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts, size=get_file_size(dst_file))
        self.set_file(self.src, rel_path, file_ref.ref)
