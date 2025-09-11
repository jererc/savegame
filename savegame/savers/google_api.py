import io
import logging
import os

from dateutil.parser import parse as parse_dt
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from goth.autoauth import Autoauth

SCOPES = [
    'https://www.googleapis.com/auth/contacts.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
]
EXPORT_SIZE_LIMIT = 50000000
MIME_TYPE_MAP = {
    # https://developers.google.com/drive/api/guides/ref-export-formats
    'application/vnd.google-apps.document': ('application/vnd.openxmlformats-officedocument.wordprocessingml.document',
                                             '.docx'),
    'application/vnd.google-apps.spreadsheet': ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                                                '.xlsx'),
}

logger = logging.getLogger(__name__)


def get_file(path):
    if not path:
        return None
    if os.path.exists(path):
        return path
    raise Exception(f'{path} does not exist')


class GoogleCloud:
    def __init__(self, oauth_secrets_file=None, service_secrets_file=None, **browser_args):
        self.oauth_secrets_file = get_file(oauth_secrets_file)
        self.service_secrets_file = get_file(service_secrets_file)
        self.browser_args = browser_args
        if not (self.oauth_secrets_file or self.service_secrets_file):
            raise Exception('requires a secrets file')
        self.token_file = self._get_token_file()
        self.service_creds = None
        self.oauth_creds = None
        self._file_cache = {}

    def _get_token_file(self):
        dirname, basename = os.path.split(self.oauth_secrets_file or self.service_secrets_file)
        name, ext = os.path.splitext(basename)
        return os.path.join(dirname, f'{name}-token{ext}')

    def _get_service_creds(self):
        if not self.service_secrets_file:
            raise Exception('missing service account secrets')
        return service_account.Credentials.from_service_account_file(self.service_secrets_file, scopes=SCOPES)

    # def _manual_auth(self):
    #     flow = InstalledAppFlow.from_client_secrets_file(self.oauth_secrets_file, SCOPES)
    #     try:
    #         return flow.run_local_server(port=0, open_browser=True, timeout_seconds=60)
    #     except Exception:
    #         raise Exception('failed to auth')

    def _auth(self):
        return Autoauth(
            client_secrets_file=self.oauth_secrets_file,
            scopes=SCOPES,
            **self.browser_args
        ).acquire_credentials()

    def get_oauth_creds(self):
        if not self.oauth_secrets_file:
            raise Exception('missing oauth secrets')
        creds = None
        if os.path.exists(self.token_file):
            creds = Credentials.from_authorized_user_file(self.token_file, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except RefreshError as exc:
                    if exc.args[1]['error'] != 'invalid_grant':
                        raise
                    creds = self._auth()
            else:
                creds = self._auth()
            with open(self.token_file, 'w') as fd:
                fd.write(creds.to_json())
        return creds

    # def _get_drive_service(self):
    #     if not self.service_creds:
    #         self.service_creds = self._get_service_creds()
    #     return build('drive', 'v3', credentials=self.service_creds)

    def _get_drive_service(self):
        if not self.oauth_creds:
            self.oauth_creds = self.get_oauth_creds()
        return build('drive', 'v3', credentials=self.oauth_creds)

    def _get_file_path(self, service, file_meta):
        def get_parent_id(file_meta):
            try:
                return file_meta['parents'][0]
            except KeyError:
                return None

        path = file_meta['name']
        parent_id = get_parent_id(file_meta)
        while parent_id:
            try:
                file_meta = self._file_cache[parent_id]
            except KeyError:
                file_meta = service.files().get(fileId=parent_id, fields='id, name, parents').execute()
                self._file_cache[parent_id] = file_meta
            path = os.path.join(file_meta['name'], path)
            parent_id = get_parent_id(file_meta)
        return path

    def _list_file_meta(self):
        service = self._get_drive_service()
        res = []
        page_token = None
        query = ' or '.join([f"mimeType='{r}'" for r in MIME_TYPE_MAP.keys()])
        while True:
            response = (
                service.files()
                .list(
                    q=f'trashed=false and ({query})',
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, modifiedTime, size, parents)',
                    pageToken=page_token,
                )
                .execute()
            )
            for file_meta in response.get('files', []):
                file_meta['path'] = self._get_file_path(service, file_meta)
                res.append(file_meta)
            page_token = response.get('nextPageToken')
            if page_token is None:
                break
        return res

    def iterate_file_meta(self):
        for file_meta in self._list_file_meta():
            mime_type, ext = MIME_TYPE_MAP[file_meta['mimeType']]
            yield {
                'id': file_meta['id'],
                'name': file_meta['name'],
                'path': f'{file_meta["path"]}{ext}',
                'modified_time': parse_dt(file_meta['modifiedTime']),
                'mime_type': mime_type,
                'exportable': int(file_meta['size']) < EXPORT_SIZE_LIMIT,
            }

    def export_file(self, file_id, path, mime_type):
        service = self._get_drive_service()
        request = service.files().export_media(fileId=file_id, mimeType=mime_type)
        fh = io.FileIO(path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.debug(f'Download progress: {int(status.progress() * 100)}%')

    def _get_people_service(self):
        if not self.oauth_creds:
            self.oauth_creds = self.get_oauth_creds()
        return build('people', 'v1', credentials=self.oauth_creds)

    def list_contacts(self):
        contacts = []
        page_token = None
        while True:
            response = (
                self._get_people_service().people()
                .connections()
                .list(
                    resourceName='people/me',
                    pageSize=1000,
                    personFields='names,emailAddresses,phoneNumbers,addresses',
                    pageToken=page_token,
                )
                .execute()
            )
            contacts_ = response.get('connections', [])
            if contacts_:
                contacts.extend(contacts_)
            page_token = response.get('nextPageToken')
            if page_token is None:
                break
        return contacts
