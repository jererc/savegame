import logging
import time

from google_auth_oauthlib.flow import InstalledAppFlow
from selenium.common.exceptions import (NoSuchElementException,
    ElementNotInteractableException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from chromium import Chromium


logger = logging.getLogger(__name__)


class GoogleAutoauth(Chromium):
    def __init__(self, client_secrets_file, scopes):
        super().__init__()
        self.client_secrets_file = client_secrets_file
        self.scopes = scopes

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
