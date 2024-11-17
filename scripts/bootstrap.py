import os
import urllib.request

url = 'https://raw.githubusercontent.com/jererc/svcutils/refs/heads/main/svcutils/bootstrap.py'
response = urllib.request.urlopen(url)
code = response.read().decode('utf-8')
exec(code)
Bootstrapper(
    name='savegame',
    target_path=os.path.join(os.path.dirname(
        os.path.realpath(__file__)), 'run.py'),
    force_reinstall=True,
    requires=[
        'git+https://github.com/jererc/savegame.git',
    ],
    args=['save', '--task'],
).run()
