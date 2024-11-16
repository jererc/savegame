import argparse
import os
import sys

from savegame import savegame
from svcutils.service import Config, Service


CWD = os.path.dirname(os.path.realpath(__file__))


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='cmd')
    save_parser = subparsers.add_parser('save')
    save_parser.add_argument('--daemon', action='store_true')
    save_parser.add_argument('--task', action='store_true')
    status_parser = subparsers.add_parser('status')
    status_parser.add_argument('--order-by', default='last_run')
    load_parser = subparsers.add_parser('load')
    load_parser.add_argument('--hostname')
    load_parser.add_argument('--username')
    load_parser.add_argument('--include', nargs='*')
    load_parser.add_argument('--exclude', nargs='*')
    load_parser.add_argument('--overwrite', action='store_true')
    load_parser.add_argument('--dry-run', action='store_true')
    subparsers.add_parser('google_oauth')
    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        sys.exit()
    return args


def main():
    args = parse_args()
    config = Config(os.path.join(CWD, 'user_settings.py'))
    if args.cmd == 'save':
        service = Service(
            target=savegame.savegame,
            args=(config,),
            work_path=savegame.WORK_PATH,
            run_delta=30 * 60,
            force_run_delta=90 * 60,
            min_runtime=300,
            requires_online=False,
            max_cpu_percent=10,
        )
        if args.daemon:
            service.run()
        elif args.task:
            service.run_once()
        else:
            savegame.savegame(force=True)
    else:
        {
            'status': savegame.status,
            'load': savegame.loadgame,
            'google_oauth': savegame.google_oauth,
        }[args.cmd](config, **{k: v for k, v in vars(args).items()
            if k != 'cmd'})


if __name__ == '__main__':
    main()
