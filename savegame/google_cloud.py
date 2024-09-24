import logging
import os

from dateutil.parser import parse as parse_dt
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from google_autoauth import GoogleAutoauth


ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
SERVICE_CREDS_FILE = os.path.join(ROOT_PATH, 'credentials_service.json')
OAUTH_CREDS_FILE = os.path.join(ROOT_PATH, 'credentials_oauth.json')
CREDS_FILE = os.path.join(ROOT_PATH, 'credentials.json')
OAUTH_TIMEOUT = 60   # seconds
SCOPES = [
    'https://www.googleapis.com/auth/contacts.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
]
MIME_TYPE_MAP = {
    # https://developers.google.com/drive/api/guides/ref-export-formats
    'application/vnd.google-apps.document': {
        'mime_type': 'application/vnd.openxmlformats-officedocument'
            '.wordprocessingml.document',
        'ext': '.docx',
    },
    'application/vnd.google-apps.spreadsheet': {
        'mime_type': 'application/vnd.openxmlformats-officedocument'
            '.spreadsheetml.sheet',
        'ext': '.xlsx',
    },

}

logger = logging.getLogger(__name__)


def get_file(x):
    return x if (x and os.path.exists(x)) else None


class AuthError(Exception):
    pass


class GoogleCloud:
    def __init__(self, service_creds_file=None, oauth_creds_file=None,
            creds_file=None):
        self.service_creds_file = get_file(
            service_creds_file or SERVICE_CREDS_FILE)
        self.oauth_creds_file = get_file(
            oauth_creds_file or OAUTH_CREDS_FILE)
        self.creds_file = creds_file or CREDS_FILE
        self.service_creds = None
        self.oauth_creds = None

    def _get_service_creds(self):
        if not self.service_creds_file:
            raise Exception('missing service account credentials')
        creds = service_account.Credentials.from_service_account_file(
            self.service_creds_file, scopes=SCOPES,
        )
        # creds = creds.with_subject(self.user_id)
        return creds

    def _auth(self):
        try:
            creds = GoogleAutoauth(self.oauth_creds_file, SCOPES
                ).acquire_credentials()
        except Exception as exc:
            logger.error(f'failed to auto: {exc}')
            flow = InstalledAppFlow.from_client_secrets_file(
                self.oauth_creds_file, SCOPES)
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
        if not self.oauth_creds_file:
            raise Exception('missing oauth credentials')

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

    def _list_files(self):
        """
        https://developers.google.com/drive/api/reference/rest/v3
        https://developers.google.com/drive/api/guides/search-files#python
        """
        drive_service = self._get_drive_service()
        files = []
        page_token = None
        while True:
            response = (drive_service.files()
                .list(
                    q='trashed=false',
                    spaces='drive',
                    fields='nextPageToken, '
                        'files(id, name, mimeType, modifiedTime, parents)',
                    pageToken=page_token,
                )
                .execute()
            )
            files_ = response.get('files', [])
            if files_:
                files.extend(files_)
            page_token = response.get('nextPageToken')
            if page_token is None:
                break
        return files

    def iterate_files(self):
        for file in self._list_files():
            try:
                mime_data = MIME_TYPE_MAP[file['mimeType']]
            except Exception:
                continue
            yield {
                'id': file['id'],
                'name': file['name'],
                'filename': f'{file["name"]}-{file["id"]}{mime_data["ext"]}',
                'modified_time': parse_dt(file['modifiedTime']),
                'mime_type': mime_data['mime_type'],
            }

    def fetch_file_content(self, file_id, mime_type):
        try:
            return self._get_drive_service().files().export(fileId=file_id,
                mimeType=mime_type).execute()
        except HttpError as exc:
            logger.error(f'failed to export file id {file_id}: {exc}')
            if exc.error_details[0]['reason'] == 'exportSizeLimitExceeded':
                return b''
            raise

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
        people_service = self._get_people_service()
        contacts = []
        page_token = None
        while True:
            response = (people_service.people()
                .connections()
                .list(
                    resourceName='people/me',
                    pageSize=100,
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
