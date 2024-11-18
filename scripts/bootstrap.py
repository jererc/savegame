import os
import urllib.request

url = 'https://raw.githubusercontent.com/jererc/svcutils/refs/heads/main/svcutils/bootstrap.py'
exec(urllib.request.urlopen(url).read().decode('utf-8'))
Bootstrapper(
    name='savegame',
    target_url='https://raw.githubusercontent.com/jererc/savegame/refs/heads/main/scripts/run.py',
    target_dir=os.path.dirname(os.path.realpath(__file__)),
    target_args=['save', '--task'],
    force_reinstall=True,
    requires=[
        # 'git+https://github.com/jererc/savegame.git',
        'savegame @ https://github.com/jererc/savegame/archive/refs/heads/main.zip',
    ],
).run()
