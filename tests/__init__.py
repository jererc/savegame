import logging
import os
import shutil

WORK_DIR = os.path.expanduser('~/tmp/tests/savegame')
shutil.rmtree(WORK_DIR, ignore_errors=True)
os.makedirs(WORK_DIR, exist_ok=True)
import savegame as module
module.WORK_DIR = WORK_DIR
logging.getLogger('').handlers.clear()
