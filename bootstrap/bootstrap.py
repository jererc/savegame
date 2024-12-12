import os
import urllib.request

url = 'https://raw.githubusercontent.com/jererc/svcutils/refs/heads/main/svcutils/bootstrap.py'
exec(urllib.request.urlopen(url).read().decode('utf-8'))
Bootstrapper(
    name='savegame',
    cmd_args=['savegame.main', '-p', os.getcwd(), 'save', '--task'],
    install_requires=[
        # 'git+https://github.com/jererc/savegame.git',
        'savegame @ https://github.com/jererc/savegame/archive/refs/heads/main.zip',
    ],
    force_reinstall=True,
    extra_cmds=[
        ['playwright', 'install-deps'],
        ['playwright', 'install', 'chromium'],
    ],
    download_assets=[
        ('user_settings.py', 'https://raw.githubusercontent.com/jererc/savegame/refs/heads/main/bootstrap/user_settings.py'),
    ],
).setup_task()
