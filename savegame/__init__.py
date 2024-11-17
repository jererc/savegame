import os
import socket

from svcutils.service import get_logger


NAME = 'savegame'
HOME_PATH = os.path.expanduser('~')
WORK_PATH = os.path.join(HOME_PATH, f'.{NAME}')
HOSTNAME = socket.gethostname()
USERNAME = os.getlogin()

if not os.path.exists(WORK_PATH):
    os.makedirs(WORK_PATH)
logger = get_logger(path=WORK_PATH, name=NAME)
