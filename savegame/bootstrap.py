import ctypes
import os
import subprocess


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
    if not os.path.isfile(LINUX_VENV_ACTIVATE_PATH):
        subprocess.check_call(['virtualenv', VENV_PATH])
    subprocess.check_call(f'. {LINUX_VENV_ACTIVATE_PATH}; '
        f'pip install {" ".join(LINUX_PYTHON_MODULES)}',
        shell=True, cwd=ROOT_PATH)


def _setup_linux_crontab():
    res = subprocess.run(['crontab', '-l'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    current_crontab = res.stdout if res.returncode == 0 else ''
    job_command = f'{LINUX_PYTHON_PATH} {SCRIPT_PATH} --if-required'
    new_job = f'{CRONTAB_SCHEDULE} {job_command}\n'
    updated_crontab = ''
    job_found = False
    for line in current_crontab.splitlines():
        if job_command in line:
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


def bootstrap_linux():
    _setup_linux_venv()
    _setup_linux_crontab()


def _setup_win_venv():
    if not os.path.isfile(WIN_VENV_ACTIVATE_PATH):
        subprocess.check_call(['pip', 'install', 'virtualenv'])
        subprocess.check_call(['virtualenv', VENV_PATH])
    subprocess.check_call(f'{WIN_VENV_ACTIVATE_PATH} && '
        f'pip install {" ".join(WIN_PYTHON_MODULES)}',
        shell=True, cwd=ROOT_PATH)


# def _setup_win_service():
#     """
#     requires pywin32
#     pywin32 issue:
#     ModuleNotFoundError: No module named 'servicemanager'

#     install the service and set the log on user:
#     python service.py --username .\\jerer --password 123 --startup auto install
#     python service.py start

#     stop and uninstall the service:
#     python service.py stop
#     python service.py remove
#     """
#     WIN_SVC_FILENAME = 'service_win.py'

#     if ctypes.windll.shell32.IsUserAnAdmin() == 0:
#         raise Exception('must run as admin')
#     pywin32_script = os.path.join(VENV_PATH, r'Scripts\pywin32_postinstall.py')
#     subprocess.check_call(f'{WIN_VENV_ACTIVATE_PATH} && '
#         f'python {pywin32_script} -install',
#         shell=True, cwd=ROOT_PATH)
#     venv_cmd_prefix = f'{WIN_VENV_ACTIVATE_PATH} && '
#     username = os.getlogin()
#     password = input(f'{username} password: ')
#     subprocess.check_call(
#         f'{WIN_VENV_ACTIVATE_PATH} && '
#         f'python {WIN_SVC_FILENAME} --username .\\{username} --password {password} '
#         '--startup auto install',
#         shell=True, cwd=ROOT_PATH)
#     subprocess.check_call(f'{WIN_VENV_ACTIVATE_PATH} && '
#         f'python {WIN_SVC_FILENAME} start',
#         shell=True, cwd=ROOT_PATH)


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


def bootstrap_win():
    if ctypes.windll.shell32.IsUserAnAdmin() == 0:
        raise Exception('must run as admin')
    _setup_win_venv()
    _setup_win_task(task_name=NAME,
        cmd=f'{WIN_PYTHON_PATH} {SCRIPT_PATH} --daemon')


def main():
    if not os.path.exists(ROOT_VENV_PATH):
        os.makedirs(ROOT_VENV_PATH)
    if os.name == 'nt':
        bootstrap_win()
    else:
        bootstrap_linux()


if __name__ == '__main__':
    main()
