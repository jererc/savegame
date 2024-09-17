import argparse
import ctypes
import os
import subprocess
import sys


NAME = 'savegame'
ROOT_PATH = os.path.dirname(os.path.realpath(__file__))
SCRIPT_PATH = os.path.join(ROOT_PATH, 'savegame.py')
ROOT_VENV_PATH = os.path.expanduser(os.path.join('~', 'venv'))
VENV_PATH = os.path.join(ROOT_VENV_PATH, NAME)
LINUX_VENV_ACTIVATE_PATH = os.path.join(VENV_PATH, 'bin/activate')
WIN_VENV_ACTIVATE_PATH = os.path.join(VENV_PATH, r'Scripts\activate')
LINUX_PYTHON_MODULES = [
    'dateutils',
    'google-api-python-client',
    'google-auth-httplib2',
    'google-auth-oauthlib',
    'psutil',
    'selenium',
]
WIN_PYTHON_MODULES = LINUX_PYTHON_MODULES + [
    'win11toast'
]
LINUX_PYTHON_PATH = os.path.join(VENV_PATH, 'bin/python')
WIN_PYTHON_PATH = os.path.join(VENV_PATH, r'Scripts\pythonw.exe')
CRONTAB_SCHEDULE = '*/2 * * * *'


def _setup_linux_venv():
    if os.path.exists(LINUX_VENV_ACTIVATE_PATH):
        return
    subprocess.check_call(['virtualenv', VENV_PATH])
    subprocess.check_call(f'. {LINUX_VENV_ACTIVATE_PATH}; '
        f'pip install {" ".join(LINUX_PYTHON_MODULES)}',
        shell=True, cwd=ROOT_PATH)


def _setup_win_venv():
    if os.path.exists(WIN_VENV_ACTIVATE_PATH):
        return
    subprocess.check_call(['pip', 'install', 'virtualenv'])
    subprocess.check_call(['virtualenv', VENV_PATH])
    subprocess.check_call(f'{WIN_VENV_ACTIVATE_PATH} && '
        f'pip install {" ".join(WIN_PYTHON_MODULES)}',
        shell=True, cwd=ROOT_PATH)


def setup_venv():
    if not os.path.exists(ROOT_VENV_PATH):
        os.makedirs(ROOT_VENV_PATH)
    if os.name == 'nt':
        _setup_win_venv()
    else:
        _setup_linux_venv()


def _setup_linux_crontab(cmd):
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


def _setup_win_task(task_name, cmd):
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


def setup_savegame():
    if os.name == 'nt':
        if ctypes.windll.shell32.IsUserAnAdmin() == 0:
            raise Exception('must run as admin')
        _setup_win_task(task_name=NAME,
            cmd=f'{WIN_PYTHON_PATH} {SCRIPT_PATH} save --daemon')
    else:
        _setup_linux_crontab(
            cmd=f'{LINUX_PYTHON_PATH} {SCRIPT_PATH} save --task')


def run_savegame_cmd():
    python_path = WIN_PYTHON_PATH if os.name == 'nt' else LINUX_PYTHON_PATH
    cmd = [python_path, SCRIPT_PATH] + sys.argv[1:]
    res = subprocess.run(cmd, cwd=ROOT_PATH,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode == 0:
        sys.stdout.write(res.stdout)
    else:
        sys.stdout.write(res.stderr or res.stdout)


def _parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command')
    setup_parser = subparsers.add_parser('setup')
    hostnames_parser = subparsers.add_parser('hostnames')
    restore_parser = subparsers.add_parser('restore')
    restore_parser.add_argument('-f', '--from-hostname')
    restore_parser.add_argument('-u', '--from-username')
    restore_parser.add_argument('-o', '--overwrite', action='store_true')
    restore_parser.add_argument('-d', '--dry-run', action='store_true')
    return parser.parse_args()


def main():
    args = _parse_args()
    setup_venv()
    if args.command == 'setup':
        setup_savegame()
    else:
        run_savegame_cmd()


if __name__ == '__main__':
    main()
