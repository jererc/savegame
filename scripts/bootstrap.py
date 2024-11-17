import os
import urllib.request

def get(url):
    return urllib.request.urlopen(url).read().decode('utf-8')

bootstrap_url = 'https://raw.githubusercontent.com/jererc/svcutils/refs/heads/main/svcutils/bootstrap.py'
target_url = 'https://raw.githubusercontent.com/jererc/savegame/refs/heads/main/scripts/run.py'

exec(get(bootstrap_url))
target_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
    os.path.basename(target_url))
content = get(target_url)
with open(target_path, 'w') as fd:
    fd.write(content)

Bootstrapper(
    name='savegame',
    target_path=target_path,
    force_reinstall=True,
    requires=[
        'git+https://github.com/jererc/savegame.git',
    ],
    args=['save', '--task'],
).run()
