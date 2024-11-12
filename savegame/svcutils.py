import atexit
import ctypes
import functools
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import signal
import socket
import subprocess
import sys
import time


VENV_DIR = 'venv'
SERVICE_LOOP_DELAY = 30
CRONTAB_SCHEDULE = '*/2 * * * *'

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


def is_online(host='8.8.8.8', port=53, timeout=3):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((host, port))
        return True
    except OSError:
        return False


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


class ServiceTracker:
    def __init__(self, work_path, min_running_time, requires_online=False):
        self.file = os.path.join(work_path, 'tracker.json')
        self.min_running_time = min_running_time
        self.requires_online = requires_online

    def _load(self):
        if not os.path.exists(self.file):
            return []
        with open(self.file) as fd:
            return json.load(fd)

    def _update(self):
        now_ts = time.time()
        begin = now_ts - self.min_running_time * 2
        data = [r for r in self._load() if r[0] > begin] \
            + [(int(now_ts), is_online())]
        with open(self.file, 'w') as fd:
            fd.write(json.dumps(data))
        return data

    def check(self):
        data = self._update()
        ts = time.time() - self.min_running_time
        res = [t - ts for t, o in data if (o or not self.requires_online)]
        return len(res) > 0 and min(res) < 0 and max(res) > 0


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


class Service:
    def __init__(self, callable, work_path, run_delta, force_run_delta=None,
                 min_running_time=None, requires_online=False,
                 max_cpu_percent=None, loop_delay=SERVICE_LOOP_DELAY):
        self.callable = callable
        self.work_path = work_path
        self.run_delta = run_delta
        self.force_run_delta = force_run_delta or run_delta * 2
        if min_running_time:
            self.tracker = ServiceTracker(work_path, min_running_time,
                requires_online)
        else:
            self.tracker = None
        self.max_cpu_percent = max_cpu_percent
        self.loop_delay = loop_delay
        self.run_file = RunFile(os.path.join(work_path, 'service.run'))

    def _must_run(self):
        if self.tracker and not self.tracker.check():
            logger.info('running time is less than '
                f'{self.tracker.min_running_time} seconds')
            return False
        run_ts = self.run_file.get_ts()
        now_ts = time.time()
        if self.force_run_delta and now_ts > run_ts + self.force_run_delta:
            return True
        if now_ts > run_ts + self.run_delta:
            if self.max_cpu_percent:
                import psutil
                cpu_percent = psutil.cpu_percent(interval=1)
                if cpu_percent < self.max_cpu_percent:
                    return True
                logger.info('cpu percent is greater than '
                    f'{self.max_cpu_percent} ({cpu_percent})')
            else:
                return True
        return False

    def _run_once(self):
        try:
            if self._must_run():
                try:
                    self.callable()
                finally:
                    self.run_file.touch()
        except Exception:
            logger.exception('failed')

    def run_once(self):
        @with_lockfile(self.work_path)
        def run():
            self._run_once()

        run()

    def run(self):
        @with_lockfile(self.work_path)
        def run():
            while True:
                self._run_once()
                logger.debug(f'sleeping for {self.loop_delay} seconds')
                time.sleep(self.loop_delay)

        run()


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


class Bootstrapper:
    def __init__(self, script_path, requirements_file=None, venv_dir=VENV_DIR,
                 crontab_schedule=CRONTAB_SCHEDULE, linux_args=None,
                 windows_args=None,
                 ):
        self.script_path = os.path.realpath(script_path)
        self.requirements_file = requirements_file or os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'requirements.txt')
        self.venv_dir = venv_dir
        self.crontab_schedule = crontab_schedule
        self.linux_args = linux_args
        self.windows_args = windows_args
        self.script_name = os.path.splitext(os.path.basename(
            self.script_path))[0]
        self.root_venv_path = os.path.join(os.path.expanduser('~'),
            self.venv_dir)
        self.venv_path = os.path.join(self.root_venv_path, self.script_name)
        self.pip_path = {
            'nt': os.path.join(self.venv_path, r'Scripts\pip.exe'),
            'posix': os.path.join(self.venv_path, 'bin/pip'),
        }[os.name]
        self.svc_py_path = {
            'nt': os.path.join(self.venv_path, r'Scripts\pythonw.exe'),
            'posix': os.path.join(self.venv_path, 'bin/python'),
        }[os.name]

    def _setup_venv(self):
        makedirs(self.root_venv_path)
        if not os.path.exists(self.svc_py_path):
            if os.name == 'nt':   # requires python3-virtualenv on linux
                subprocess.check_call(['pip', 'install', 'virtualenv'])
            subprocess.check_call(['virtualenv', self.venv_path])
        subprocess.check_call([self.pip_path, 'install', '-r',
            self.requirements_file])
        print(f'Created the virtualenv in {self.venv_path}')

    def _setup_linux_crontab(self, cmd):
        res = subprocess.run(['crontab', '-l'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        current_crontab = res.stdout if res.returncode == 0 else ''
        new_job = f'{self.crontab_schedule} {cmd}\n'
        updated_crontab = ''
        job_found = False
        for line in current_crontab.splitlines():
            if cmd in line:
                updated_crontab += new_job
                job_found = True
            else:
                updated_crontab += f'{line}\n'
        if not job_found:
            updated_crontab += new_job
        res = subprocess.run(['crontab', '-'], input=updated_crontab,
            text=True)
        if res.returncode != 0:
            raise SystemExit('Failed to update crontab')
        print('Successfully updated crontab')

    def _setup_windows_task(self, cmd, task_name):
        if ctypes.windll.shell32.IsUserAnAdmin() == 0:
            raise SystemExit('Failed: must run as admin')
        subprocess.check_call(['schtasks', '/create',
            '/tn', task_name,
            '/tr', cmd,
            '/sc', 'onlogon',
            '/rl', 'highest',
            '/f',
        ])
        subprocess.check_call(['schtasks', '/run',
            '/tn', task_name,
        ])

    def _get_cmd(self, args):
        args = f' {" ".join(args)}' if args else ''
        return f'{self.svc_py_path} {self.script_path}{args}'

    def run(self):
        self._setup_venv()
        if os.name == 'nt':
            self._setup_windows_task(cmd=self._get_cmd(self.windows_args),
                task_name=self.script_name)
        else:
            self._setup_linux_crontab(cmd=self._get_cmd(self.linux_args))
