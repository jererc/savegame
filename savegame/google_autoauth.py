import logging
import os
import subprocess
import time

from google_auth_oauthlib.flow import InstalledAppFlow
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException,
    ElementNotInteractableException)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


IS_WIN = os.name == 'nt'
DATA_DIR = os.path.expanduser(r'~\AppData\Local\Google\Chrome\User Data'
    if IS_WIN else '~/.config/google-chrome')
PROFILE_DIR = 'selenium'

logger = logging.getLogger(__name__)


class GoogleAutoauth:
    def __init__(self, client_secrets_file, scopes,
            data_dir=DATA_DIR, profile_dir=PROFILE_DIR):
        self.client_secrets_file = client_secrets_file
        self.scopes = scopes
        self.data_dir = data_dir
        self.profile_dir = profile_dir
        self.driver = self._get_driver()

    def _get_driver(self):
        if not os.path.exists(self.data_dir):
            raise Exception(f'chrome data dir {self.data_dir} does not exist')
        subprocess.call('taskkill /IM chrome.exe'
            if IS_WIN else 'pkill chrome', shell=True)
        options = Options()
        options.add_argument(f'--user-data-dir={self.data_dir}')
        options.add_argument(f'--profile-directory={self.profile_dir}')
        options.add_argument('--start-maximized')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option('useAutomationExtension', False)
        options.add_experimental_option('excludeSwitches',
            ['enable-automation'])
        options.add_experimental_option('detach', True)
        driver = webdriver.Chrome(options=options)
        driver.implicitly_wait(1)
        return driver

    def _wait_for_element(self, element):
        wait = WebDriverWait(self.driver, timeout=5, poll_frequency=.2,
            ignored_exceptions=[NoSuchElementException,
                ElementNotInteractableException])
        wait.until(lambda x: element.is_displayed())

    def _select_user(self):
        self.driver.find_element(By.XPATH, '//div[@data-authuser="0"]').click()

    def _click_continue(self):
        try:
            self.driver.find_element(By.XPATH,
                '//button[contains(., "Continue")]').click()
            return True
        except NoSuchElementException:
            return False

    def _wait_for_login(self, url, poll_frequency=1, timeout=120):
        self.driver.get(url)
        end_ts = time.time() + timeout
        while time.time() < end_ts:
            try:
                self._select_user()
            except NoSuchElementException:
                if self._click_continue():
                    return
            else:
                if self._click_continue():
                    return
            time.sleep(poll_frequency)
        raise Exception('login timeout')

    def _fetch_code(self, auth_url):
        self._wait_for_login(auth_url, poll_frequency=1, timeout=120)
        self.driver.find_element(By.XPATH,
            '//input[@type="checkbox" and @aria-label="Select all"]',
            ).click()
        el_continue = self.driver.find_element(By.XPATH,
            '//button[contains(., "Continue")]')
        self._wait_for_element(el_continue)
        el_continue.click()
        el_textarea = self.driver.find_element(By.XPATH, '//textarea')
        self._wait_for_element(el_textarea)
        res = el_textarea.get_attribute('innerHTML')
        self.driver.quit()
        return res

    def acquire_credentials(self):
        """
        https://google-auth-oauthlib.readthedocs.io/en/latest/reference/google_auth_oauthlib.flow.html
        """
        flow = InstalledAppFlow.from_client_secrets_file(
            client_secrets_file=self.client_secrets_file,
            scopes=self.scopes,
            redirect_uri='urn:ietf:wg:oauth:2.0:oob',
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        logger.debug(f'auth url: {auth_url}')
        code = self._fetch_code(auth_url)
        flow.fetch_token(code=code)
        return flow.credentials
