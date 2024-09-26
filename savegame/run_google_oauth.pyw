import os
import subprocess
import sys


root_path = os.path.dirname(os.path.realpath(__file__))
res = subprocess.run([
    os.path.join(os.path.dirname(sys.executable), 'pythonw.exe'),
    os.path.join(root_path, 'bootstrap.py'),
    'google_oauth',
    '--oauth-secrets-file', os.path.join(root_path, 'gcs.json')
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    cwd=root_path)
print(res.stdout)
