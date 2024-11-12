import os

from svcutils import Bootstrapper


Bootstrapper(
    script_path=os.path.join(os.path.dirname(os.path.realpath(__file__)),
        'savegame.py'),
    crontab_schedule='*/2 * * * *',
    linux_args=['save', '--task'],
    windows_args=['save', '--daemon'],
).run()
