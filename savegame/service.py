import atexit
import functools
import logging
from logging.handlers import RotatingFileHandler
import os
import signal
import subprocess
import sys
import time

import psutil


logger = logging.getLogger(__name__)


def makedirs(x):
    if not os.path.exists(x):
        os.makedirs(x)


def setup_logging(logger, path, name, max_size=1024000):
    logging.basicConfig(level=logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s')
    if sys.stdout and not sys.stdout.isatty():
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(logging.DEBUG)
        logger.addHandler(stdout_handler)
    makedirs(path)
    file_handler = RotatingFileHandler(
        os.path.join(path, f'{name}.log'),
        mode='a', maxBytes=max_size, backupCount=0,
        encoding='utf-8', delay=0)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)


def get_file_mtime(x):
    return os.stat(x).st_mtime


class RunFile:
    def __init__(self, file):
        self.file = file

    def get_ts(self, default=0):
        if not os.path.exists(self.file):
            return default
        return get_file_mtime(self.file)

    def touch(self):
        with open(self.file, 'w'):
            pass


class Notifier:
    def _send_nt(self, title, body, on_click=None):
        from win11toast import notify
        notify(title=title, body=body, on_click=on_click)

    def _send_posix(self, title, body, on_click=None):
        env = os.environ.copy()
        env['DISPLAY'] = ':0'
        env['DBUS_SESSION_BUS_ADDRESS'] = \
            f'unix:path=/run/user/{os.getuid()}/bus'
        subprocess.check_call(['notify-send', title, body], env=env)

    def send(self, *args, **kwargs):
        try:
            {
                'nt': self._send_nt,
                'posix': self._send_posix,
            }[os.name](*args, **kwargs)
        except Exception:
            logger.exception('failed to send notification')


def with_lockfile(path):
    lockfile_path = os.path.join(path, 'lock')

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if os.name == 'posix' and os.path.exists(lockfile_path):
                logger.error(f'Lock file {lockfile_path} exists. '
                    'Another process may be running.')
                raise RuntimeError(f'Lock file {lockfile_path} exists. '
                    'Another process may be running.')

            def remove_lockfile():
                if os.path.exists(lockfile_path):
                    os.remove(lockfile_path)

            atexit.register(remove_lockfile)

            def handle_signal(signum, frame):
                remove_lockfile()
                raise SystemExit(f'Program terminated by signal {signum}')

            if os.name == 'posix':
                signal.signal(signal.SIGINT, handle_signal)
                signal.signal(signal.SIGTERM, handle_signal)

            try:
                with open(lockfile_path, 'w') as lockfile:
                    lockfile.write('locked')
                result = func(*args, **kwargs)
            finally:
                remove_lockfile()
            return result

        return wrapper
    return decorator


def is_idle():
    res = psutil.cpu_percent(interval=1) < 5
    if not res:
        logger.warning('not idle')
    return res


def must_run(last_run_ts, run_delta, force_run_delta):
    now_ts = time.time()
    if now_ts > last_run_ts + force_run_delta:
        return True
    if now_ts > last_run_ts + run_delta and is_idle():
        return True
    return False


class Daemon:
    def __init__(self, callable, work_path, run_delta, force_run_delta,
            run_file_path, loop_delay=30):
        self.callable = callable
        self.work_path = work_path
        self.run_delta = run_delta
        self.force_run_delta = force_run_delta
        self.run_file = RunFile(run_file_path)
        self.loop_delay = loop_delay

    def run(self):
        @with_lockfile(self.work_path)
        def run():
            while True:
                try:
                    if must_run(self.run_file.get_ts(),
                            self.run_delta, self.force_run_delta):
                        self.callable()
                        self.run_file.touch()
                except Exception:
                    logger.exception('failed')
                finally:
                    logger.debug(f'sleeping for {self.loop_delay} seconds')
                    time.sleep(self.loop_delay)

        run()


class Task:
    def __init__(self, callable, work_path, run_delta, force_run_delta,
            run_file_path):
        self.callable = callable
        self.work_path = work_path
        self.run_delta = run_delta
        self.force_run_delta = force_run_delta
        self.run_file = RunFile(run_file_path)

    def run(self):
        @with_lockfile(self.work_path)
        def run():
            try:
                if must_run(self.run_file.get_ts(),
                        self.run_delta, self.force_run_delta):
                    self.callable()
                    self.run_file.touch()
            except Exception:
                logger.exception('failed')

        run()
