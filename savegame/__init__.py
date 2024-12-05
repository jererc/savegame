import os

from svcutils.service import get_logger


NAME = 'savegame'
WORK_DIR = os.path.join(os.path.expanduser('~'), f'.{NAME}')

if not os.path.exists(WORK_DIR):
    os.makedirs(WORK_DIR)
logger = get_logger(path=WORK_DIR, name=NAME)
