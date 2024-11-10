import atexit
import ctypes
import functools
import logging
from logging.handlers import RotatingFileHandler
import os
import signal
import subprocess
import sys
import time


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
    import psutil
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
    def __init__(self, callable, work_path, run_delta, force_run_delta=None,
                 loop_delay=30):
        self.callable = callable
        self.work_path = work_path
        self.run_delta = run_delta
        self.force_run_delta = force_run_delta or run_delta * 2
        self.run_file = RunFile(os.path.join(work_path, 'daemon.run'))
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
    def __init__(self, callable, work_path, run_delta, force_run_delta=None):
        self.callable = callable
        self.work_path = work_path
        self.run_delta = run_delta
        self.force_run_delta = force_run_delta or run_delta * 2
        self.run_file = RunFile(os.path.join(work_path, 'task.run'))

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


class Bootstrapper:
    def __init__(self, script_path, requirements_file=None, venv_dir='venv',
                 crontab_schedule='*/2 * * * *', linux_args=None,
                 windows_args=None,
                 ):
        self.script_path = os.path.realpath(script_path)
        self.requirements_file = requirements_file or os.path.join(
            os.path.dirname(os.path.realpath(__file__)), 'requirements.txt')
        self.venv_dir = venv_dir
        self.crontab_schedule = crontab_schedule
        self.linux_args = linux_args
        self.windows_args = windows_args
        self.script_filename = os.path.basename(self.script_path)
        self.script_name = os.path.splitext(self.script_filename)[0]
        self.root_venv_path = os.path.join(os.path.expanduser('~'),
            self.venv_dir)
        self.venv_path = os.path.join(self.root_venv_path, self.script_name)
        self.pip_path = {
            'nt': os.path.join(self.venv_path, r'Scripts\pip.exe'),
            'posix': os.path.join(self.venv_path, 'bin/pip'),
        }[os.name]
        self.py_path = {
            'nt': os.path.join(self.venv_path, r'Scripts\python.exe'),
            'posix': os.path.join(self.venv_path, 'bin/python'),
        }[os.name]
        self.svc_py_path = {
            'nt': os.path.join(self.venv_path, r'Scripts\pythonw.exe'),
            'posix': os.path.join(self.venv_path, 'bin/python'),
        }[os.name]

    def _setup_venv(self):
        makedirs(self.root_venv_path)
        if not os.path.exists(self.py_path):
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
