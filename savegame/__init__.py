from svcutils.service import get_work_dir, setup_logging

NAME = 'savegame'
WORK_DIR = get_work_dir(NAME)
setup_logging(path=WORK_DIR, name=NAME)
