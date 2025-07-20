import logging
import os

TEST_DIRNAME = 'savegame'
WORK_DIR = os.path.join(os.path.expanduser('~'), '_tests', TEST_DIRNAME)
import savegame as module
module.WORK_DIR = WORK_DIR
logging.getLogger('').handlers.clear()
