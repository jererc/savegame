import argparse
import os
import sys

from svcutils.service import Config, Service

from savegame import WORK_PATH, load, save


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', '-p', default=os.getcwd())
    subparsers = parser.add_subparsers(dest='cmd')
    save_parser = subparsers.add_parser('save')
    save_parser.add_argument('--daemon', action='store_true')
    save_parser.add_argument('--task', action='store_true')
    status_parser = subparsers.add_parser('status')
    status_parser.add_argument('--order-by', default='modified,hostname')
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
    path = os.path.realpath(os.path.expanduser(args.path))
    config = Config(os.path.join(path, 'user_settings.py'))
    if args.cmd == 'save':
        service = Service(
            target=save.savegame,
            args=(config,),
            work_path=WORK_PATH,
            run_delta=30 * 60,
            force_run_delta=90 * 60,
            min_uptime=300,
            requires_online=False,
            max_cpu_percent=10,
        )
        if args.daemon:
            service.run()
        elif args.task:
            service.run_once()
        else:
            save.savegame(config, force=True)
    else:
        {
            'status': save.status,
            'load': load.loadgame,
            'google_oauth': save.google_oauth,
        }[args.cmd](config, **{k: v for k, v in vars(args).items()
            if k not in ('cmd', 'path')})


if __name__ == '__main__':
    main()
