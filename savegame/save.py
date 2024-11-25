from datetime import datetime
from glob import glob
import os
import time

from svcutils.service import Notifier, RunFile, get_file_mtime

from savegame import NAME, WORK_PATH, logger
from savegame.lib import (Metadata, Reference, Report, UnhandledPath,
    InvalidPath, validate_path, to_json, get_file_hash)
from savegame.savers import LocalSaver, get_google_cloud, get_saver_class


DST_ROOT_DIR = 'saves'
RUN_DELTA = 3600
RETENTION_DELTA = 7 * 24 * 3600
MONITOR_DELTA = 12 * 3600
STALE_DELTA = 3 * 24 * 3600


def ts_to_str(x):
    return datetime.fromtimestamp(int(x)).isoformat(' ')


def to_float(x):
    return float(f'{x:.02f}')


def get_local_path(x):
    return x.replace('\\' if os.path.sep == '/' else '/', os.path.sep)


def get_path_separator(path):
    for sep in ('/', '\\'):
        if sep in path:
            return sep
    return os.path.sep


def get_else(x, default):
    return default if x is None else x


class SaveItem:
    def __init__(self, config, src_paths=None, saver_id=LocalSaver.id,
                 dst_path=None, run_delta=None, retention_delta=None,
                 loadable=True, os_name=None):
        self.config = config
        self.src_paths = self._get_src_paths(src_paths)
        self.saver_id = saver_id
        self.saver_cls = get_saver_class(self.saver_id)
        self.dst_path = self._get_dst_path(dst_path or self.config.DST_PATH)
        self.run_delta = get_else(run_delta,
            get_else(self.config.RUN_DELTA, RUN_DELTA))
        self.retention_delta = get_else(retention_delta,
            get_else(self.config.RETENTION_DELTA, RETENTION_DELTA))
        self.loadable = loadable
        self.os_name = os_name

    def _get_src_paths(self, src_paths):
        return [s if isinstance(s, (list, tuple))
            else (s, [], []) for s in (src_paths or [])]

    def _get_dst_path(self, dst_path):
        if not dst_path:
            raise Exception('missing dst_path')
        if self.saver_cls.dst_type == 'local':
            validate_path(dst_path)
            dst_path = os.path.expanduser(dst_path)
            if not os.path.exists(dst_path):
                raise InvalidPath(
                    f'invalid dst_path {dst_path}: does not exist')
            return os.path.join(dst_path, self.config.DST_ROOT_DIR
                or DST_ROOT_DIR, self.saver_id)
        return dst_path

    def _generate_src_and_patterns(self):
        if self.saver_cls.src_type == 'local' and self.src_paths:
            for src_path, inclusions, exclusions in self.src_paths:
                try:
                    validate_path(src_path)
                except UnhandledPath:
                    continue
                for src in glob(os.path.expanduser(src_path)):
                    yield src, inclusions, exclusions
        else:
            yield self.saver_id, None, None

    def generate_savers(self):
        if self.os_name and os.name != self.os_name:
            return
        for src_and_patterns in self._generate_src_and_patterns():
            yield self.saver_cls(
                self.config,
                *src_and_patterns,
                dst_path=self.dst_path,
                run_delta=self.run_delta,
                retention_delta=self.retention_delta,
            )


def iterate_save_items(config, log_unhandled=False, log_invalid=True):
    for save in config.SAVES:
        try:
            yield SaveItem(config, **save)
        except UnhandledPath as exc:
            if log_unhandled:
                logger.warning(exc)
            continue
        except InvalidPath as exc:
            if log_invalid:
                logger.warning(exc)
            continue


class SaveHandler:
    def __init__(self, config, force=False):
        self.config = config
        self.force = force

    def _generate_savers(self):
        for si in iterate_save_items(self.config):
            yield from si.generate_savers()

    def _check_dsts(self, savers):
        dsts = {s.dst for s in savers}
        orphans = set()
        for dirname in {os.path.dirname(r) for r in dsts}:
            orphans.update(set(glob(os.path.join(dirname, '*'))) - dsts)
        for orphan in orphans:
            logger.warning(f'orphan path: {orphan}')

    def run(self):
        start_ts = time.time()
        savers = list(self._generate_savers())
        if not savers:
            raise Exception('nothing to save')
        report = Report()
        for saver in savers:
            try:
                saver.run(force=self.force)
            except Exception as exc:
                logger.exception(f'failed to save {saver.src}')
                Notifier().send(title=f'{NAME} exception',
                    body=f'failed to save {saver.src}: {exc}')
            report.merge(saver.report)
        Metadata().save(keys={s.src for s in savers})
        report_dict = report.clean(keys={'saved', 'removed'})
        if report_dict:
            logger.info(f'report:\n{to_json(report_dict)}')
        self._check_dsts(savers)
        logger.info(f'processed {len(savers)} saves in '
            f'{time.time() - start_ts:.02f} seconds')


class SaveMonitor:
    def __init__(self, config):
        self.config = config
        self.run_file = RunFile(os.path.join(WORK_PATH, 'monitor.run'))

    def _must_run(self):
        return time.time() > self.run_file.get_ts() + MONITOR_DELTA

    def _iterate_hostname_refs(self):
        dst_paths = {s.dst_path for s in iterate_save_items(self.config)
            if s.saver_cls.dst_type == 'local' and os.path.exists(s.dst_path)}
        for dst_path in dst_paths:
            for hostname in sorted(os.listdir(dst_path)):
                for dst in glob(os.path.join(dst_path, hostname, '*')):
                    ref = Reference(dst)
                    if not os.path.exists(ref.file):
                        logger.error(f'missing ref file {ref.file}')
                        continue
                    yield hostname, ref

    def _get_size(self, ref):
        def get_size(x):
            return os.path.getsize(x) if os.path.exists(x) else 0

        try:
            sizes = [get_size(os.path.join(ref.dst, get_local_path(r)))
                for r in ref.files.keys()]
            return to_float(sum(sizes) / 1024 / 1024)
        except Exception:
            logger.exception(f'failed to get {ref.dst} size')
            return -1

    def _get_src(self, ref):
        if len(ref.files) == 1:
            sep = get_path_separator(ref.src)
            return f'{ref.src}{sep}{list(ref.files.keys())[0]}'
        return ref.src

    def _monitor(self):
        saves = []
        for hostname, ref in self._iterate_hostname_refs():
            src = self._get_src(ref)
            mtimes = []
            invalid_files = []
            for rel_path, ref_hash in ref.files.items():
                dst_file = os.path.join(ref.dst, get_local_path(rel_path))
                dst_exists = os.path.exists(dst_file)
                if dst_exists:
                    mtimes.append(get_file_mtime(dst_file))
                if get_file_hash(dst_file) != ref_hash:
                    invalid_files.append(dst_file)
                    logger.error(f'{"invalid" if dst_exists else "missing"} '
                        f'file: {dst_file}')
            saves.append({
                'hostname': hostname,
                'src': src,
                'last_run': ref.ts,
                'last_modified': sorted(mtimes)[-1] if mtimes else 0,
                'size_MB': self._get_size(ref),
                'files': len(ref.files),
                'invalid': len(invalid_files),
            })
        report = {
            'saves': saves,
            'invalid': [r for r in saves if r['invalid']],
        }
        report['message'] = ', '.join([f'{k}: {len(report[k])}'
            for k in ('saves', 'invalid')])
        return report

    def run(self):
        if not self._must_run():
            return
        report = self._monitor()
        Notifier().send(title=f'{NAME} status', body=report['message'])
        self.run_file.touch()

    def _print_saves(self, saves, order_by):
        if not saves:
            return
        headers = {k: k for k in saves[0].keys()}
        rows = [headers] + sorted(saves,
            key=lambda x: (x[order_by], x['src']), reverse=True)
        for i, r in enumerate(rows):
            h_last_run = ts_to_str(r['last_run']) \
                if i > 0 else r['last_run']
            h_last_modified = ts_to_str(r['last_modified']) \
                if i > 0 else r['last_modified']
            print(f'{h_last_run:19}  {h_last_modified:19}  '
                f'{r["hostname"]:20}  {r["size_MB"]:10}  {r["files"]:8}  '
                f'{r["invalid"] or "":8}  {r["src"]}')

    def get_status(self, order_by='last_run'):
        report = self._monitor()
        self._print_saves(report['saves'], order_by=order_by)
        print(report['message'])


def savegame(config, force=False):
    try:
        SaveHandler(config, force=force).run()
    except Exception as exc:
        logger.exception('failed to save')
        Notifier().send(title=f'{NAME} error', body=str(exc))
    try:
        SaveMonitor(config).run()
    except Exception as exc:
        logger.exception('failed to monitor')
        Notifier().send(title=f'{NAME} error', body=str(exc))


def status(config, **kwargs):
    SaveMonitor(config).get_status(**kwargs)


def google_oauth(config, **kwargs):
    get_google_cloud(config, headless=False).get_oauth_creds()
