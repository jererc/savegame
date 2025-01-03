import argparse
import os
import sys

from svcutils.service import Config, Service

from savegame import WORK_DIR, load, save


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', '-p', default=os.getcwd())
    subparsers = parser.add_subparsers(dest='cmd')
    save_parser = subparsers.add_parser('save')
    save_parser.add_argument('--daemon', action='store_true')
    save_parser.add_argument('--task', action='store_true')
    status_parser = subparsers.add_parser('status')
    status_parser.add_argument('--order-by', default='hostname,modified')
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
    config = Config(
        os.path.join(path, 'user_settings.py'),
        DST_ROOT_DIR='saves',
        SAVE_RUN_DELTA=3600,
        RETENTION_DELTA=7 * 24 * 3600,
        ALWAYS_UPDATE_REF=False,
        RUN_DELTA=30 * 60,
        MONITOR_DELTA=16 * 3600,
        GOOGLE_CREDS=os.path.join(WORK_DIR, 'google_creds.json'),
    )
    if args.cmd == 'save':
        service = Service(
            target=save.savegame,
            args=(config,),
            work_dir=WORK_DIR,
            run_delta=config.RUN_DELTA,
            force_run_delta=3 * config.RUN_DELTA,
            max_cpu_percent=10,
            min_uptime=180,
            requires_online=False,
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
