import os
import urllib.request

url = 'https://raw.githubusercontent.com/jererc/svcutils/refs/heads/main/svcutils/bootstrap.py'
exec(urllib.request.urlopen(url).read().decode('utf-8'))
path = os.path.dirname(os.path.realpath(__file__))
Bootstrapper(
    name='savegame',
    script_module='savegame.main',
    script_args=['-p', path, 'save', '--task'],
    install_requires=[
        # 'git+https://github.com/jererc/savegame.git',
        'savegame @ https://github.com/jererc/savegame/archive/refs/heads/main.zip',
    ],
    force_reinstall=True,
).run()
