import io
import logging
import mimetypes
import os

from dateutil.parser import parse as parse_dt
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

from google_autoauth import GoogleAutoauth


CREDS_FILENAME = 'gc.json'
OAUTH_TIMEOUT = 60   # seconds
SCOPES = [
    'https://www.googleapis.com/auth/contacts.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/drive.file',
]
SIZE_LIMIT = 50000000
MIME_TYPE_MAP = {
    # https://developers.google.com/drive/api/guides/ref-export-formats
    'application/vnd.google-apps.document': 'application/'
        'vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.google-apps.spreadsheet': 'application/'
        'vnd.openxmlformats-officedocument.spreadsheetml.sheet',
}

logger = logging.getLogger(__name__)

# Windows fix
mimetypes.add_type('application/'
    'vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx')
mimetypes.add_type('application/'
    'vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx')


def get_file(path):
    if not path:
        return None
    if os.path.exists(path):
        return path
    raise Exception(f'{path} does not exist')


def get_file_ext(mime_type):
    ext = mimetypes.guess_extension(mime_type)
    if not ext:
        logger.warning(f'failed to guess {mime_type} extension')
        return ''
    return ext


class AuthError(Exception):
    pass


class GoogleCloud:

    def __init__(self, oauth_secrets_file=None, service_secrets_file=None):
        self.oauth_secrets_file = get_file(oauth_secrets_file)
        self.service_secrets_file = get_file(service_secrets_file)
        if not (self.oauth_secrets_file or self.service_secrets_file):
            raise Exception('requires a secrets file')
        self.creds_file = os.path.join(os.path.dirname(
            self.oauth_secrets_file or self.service_secrets_file),
            CREDS_FILENAME)
        self.service_creds = None
        self.oauth_creds = None
        self._file_cache = {}

    def _get_service_creds(self):
        if not self.service_secrets_file:
            raise Exception('missing service account secrets')
        return service_account.Credentials.from_service_account_file(
            self.service_secrets_file, scopes=SCOPES)

    def _auth(self):
        try:
            creds = GoogleAutoauth(self.oauth_secrets_file,
                SCOPES).acquire_credentials()
        except Exception as exc:
            logger.error(f'failed to auto: {exc}')
            flow = InstalledAppFlow.from_client_secrets_file(
                self.oauth_secrets_file, SCOPES)
            try:
                creds = flow.run_local_server(port=0, open_browser=True,
                    timeout_seconds=OAUTH_TIMEOUT)
            except Exception:
                raise Exception('failed to auth')
        return creds

    def get_oauth_creds(self, interact=False):
        """
        https://google-auth-oauthlib.readthedocs.io/en/latest/reference/google_auth_oauthlib.flow.html
        """
        if not self.oauth_secrets_file:
            raise Exception('missing oauth secrets')
        creds = None
        if os.path.exists(self.creds_file):
            creds = Credentials.from_authorized_user_file(self.creds_file,
                SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except RefreshError as exc:
                    if exc.args[1]['error'] != 'invalid_grant' \
                            or not interact:
                        raise
                    creds = self._auth()
            else:
                if not interact:
                    raise AuthError('requires auth')
                creds = self._auth()
            with open(self.creds_file, 'w') as fd:
                fd.write(creds.to_json())
        return creds

    #
    # Drive
    #

    # def _get_drive_service(self):
    #     if not self.service_creds:
    #         self.service_creds = self._get_service_creds()
    #     return build('drive', 'v3', credentials=self.service_creds)

    def _get_drive_service(self):
        if not self.oauth_creds:
            self.oauth_creds = self.get_oauth_creds()
        return build('drive', 'v3', credentials=self.oauth_creds)

    def _get_file_path(self, service, file_data):

        def get_parent_id(file_data):
            try:
                return file_data['parents'][0]
            except KeyError:
                return None

        path = file_data['name']
        parent_id = get_parent_id(file_data)
        while parent_id:
            try:
                file_data = self._file_cache[parent_id]
            except KeyError:
                file_data = service.files().get(fileId=parent_id,
                    fields='id, name, parents').execute()
                self._file_cache[parent_id] = file_data
            path = os.path.join(file_data['name'], path)
            parent_id = get_parent_id(file_data)
        return path

    def _list_files(self):
        """
        https://developers.google.com/drive/api/reference/rest/v3
        https://developers.google.com/drive/api/guides/search-files#python
        """
        service = self._get_drive_service()
        res = []
        page_token = None
        while True:
            response = (service.files()
                .list(
                    q='trashed=false',
                    spaces='drive',
                    fields='nextPageToken, files(id, name, mimeType, '
                        'modifiedTime, size, parents)',
                    pageToken=page_token,
                )
                .execute()
            )
            for file_data in response.get('files', []):
                if file_data['mimeType'] == 'application/' \
                        'vnd.google-apps.folder':
                    continue
                file_data['path'] = self._get_file_path(service, file_data)
                res.append(file_data)
            page_token = response.get('nextPageToken')
            if page_token is None:
                break
        return res

    def iterate_files(self):
        for file in self._list_files():
            try:
                mime_type = MIME_TYPE_MAP[file['mimeType']]
                path = f'{file["path"]}{get_file_ext(mime_type)}'
            except KeyError:
                mime_type = None
                path = file['path']
            yield {
                'id': file['id'],
                'name': file['name'],
                'path': path,
                'modified_time': parse_dt(file['modifiedTime']),
                'mime_type': mime_type,
                'exportable': int(file['size']) < SIZE_LIMIT,
            }

    def download_file(self, file_id, path, mime_type=None):
        service = self._get_drive_service()
        if mime_type:
            request = service.files().export_media(fileId=file_id,
                mimeType=mime_type)
        else:
            request = service.files().get_media(fileId=file_id)
        fh = io.FileIO(path, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.debug('Download progress: '
                f'{int(status.progress() * 100)}%')

    def upload_file(self, path, filename, file_id=None):
        service = self._get_drive_service()
        metadata = {'name': filename}
        media = MediaFileUpload(path, resumable=True)
        if file_id:
            service.files().update(fileId=file_id,
                media_body=media).execute()
        else:
            service.files().create(body=metadata,
                media_body=media, fields='id').execute()

    #
    # People
    #

    def _get_people_service(self):
        if not self.oauth_creds:
            self.oauth_creds = self.get_oauth_creds()
        return build('people', 'v1', credentials=self.oauth_creds)

    def list_contacts(self):
        """
        https://developers.google.com/people/api/rest/?apix=true
        """
        contacts = []
        page_token = None
        while True:
            response = (self._get_people_service().people()
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
