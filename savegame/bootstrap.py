import ctypes
import os
import subprocess
import sys


ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
SCRIPT_PATH = os.path.join(ROOT_PATH, 'savegame.py')
NAME = os.path.splitext(os.path.basename(SCRIPT_PATH))[0]
ROOT_VENV_PATH = os.path.join(os.path.expanduser('~'), 'venv')
VENV_PATH = os.path.join(ROOT_VENV_PATH, NAME)
VENV_ACTIVATE_PATH = {
    'nt': os.path.join(VENV_PATH, r'Scripts\activate'),
    'posix': os.path.join(VENV_PATH, 'bin/activate'),
}[os.name]
PIP_PATH = {
    'nt': os.path.join(VENV_PATH, r'Scripts\pip.exe'),
    'posix': os.path.join(VENV_PATH, 'bin/pip'),
}[os.name]
PY_PATH = {
    'nt': os.path.join(VENV_PATH, r'Scripts\pythonw.exe'),
    'posix': os.path.join(VENV_PATH, 'bin/python'),
}[os.name]
LINUX_PY_MODULES = [
    'dateutils',
    'google-api-python-client',
    'google-auth-httplib2',
    'google-auth-oauthlib',
    'psutil',
    'selenium',
]
WIN_PY_MODULES = LINUX_PY_MODULES + [
    'win11toast'
]
PY_MODULES = {
    'nt': WIN_PY_MODULES,
    'posix': LINUX_PY_MODULES,
}[os.name]
CRONTAB_SCHEDULE = '*/2 * * * *'
COMMAND = 'setup'


class Bootstrapper:
    def __init__(self):
        self._setup_venv()

    def _check_venv(self):
        if not os.path.exists(PIP_PATH):
            return False
        res = subprocess.run([PIP_PATH, 'freeze'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if res.returncode != 0:
            return False
        venv_modules = {r.split('==')[0] for r in res.stdout.splitlines()}
        return venv_modules >= set(PY_MODULES)

    def _setup_venv(self):
        if not os.path.exists(ROOT_VENV_PATH):
            os.makedirs(ROOT_VENV_PATH)
        if self._check_venv():
            return
        if not os.path.exists(VENV_ACTIVATE_PATH):
            if os.name == 'nt':   # requires python3-virtualenv on linux
                subprocess.check_call(['pip', 'install', 'virtualenv'])
            subprocess.check_call(['virtualenv', VENV_PATH])
        subprocess.check_call([PIP_PATH, 'install'] + PY_MODULES,
            cwd=ROOT_PATH)
        print(f'Created the virtualenv in {VENV_PATH}')

    def _setup_linux_crontab(self, cmd):
        res = subprocess.run(['crontab', '-l'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        current_crontab = res.stdout if res.returncode == 0 else ''
        new_job = f'{CRONTAB_SCHEDULE} {cmd}\n'
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
        if res.returncode == 0:
            print('Crontab updated successfully')
        else:
            print('Failed to update crontab')

    def _setup_win_task(self, task_name, cmd):
        if ctypes.windll.shell32.IsUserAnAdmin() == 0:
            raise Exception('must run as admin')
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

    def setup(self):
        if os.name == 'nt':
            self._setup_win_task(task_name=NAME,
                cmd=f'{PY_PATH} {SCRIPT_PATH} save --daemon')
        else:
            self._setup_linux_crontab(
                cmd=f'{PY_PATH} {SCRIPT_PATH} save --task')

    def run_savegame_cmd(self):
        res = subprocess.run([PY_PATH, SCRIPT_PATH] + sys.argv[1:],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            cwd=ROOT_PATH)
        sys.stdout.write(res.stdout or res.stderr)

    def run(self):
        try:
            if sys.argv[1] == COMMAND:
                self.setup()
            else:
                self.run_savegame_cmd()
        except IndexError:
            print(f'Missing command: {COMMAND} or any savegame command')


def main():
    Bootstrapper().run()


if __name__ == '__main__':
    main()
