import os

from svcutils.service import get_logger


NAME = 'savegame'
WORK_PATH = os.path.join(os.path.expanduser('~'), f'.{NAME}')

if not os.path.exists(WORK_PATH):
    os.makedirs(WORK_PATH)
logger = get_logger(path=WORK_PATH, name=NAME)
