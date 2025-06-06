from datetime import datetime
from glob import glob
import os
import sys
import time

from svcutils.service import Notifier, RunFile

from savegame import NAME, WORK_DIR, logger
from savegame.lib import (HOSTNAME, Metadata, Reference, Report,
                          InvalidPath, UnhandledPath, get_file_hash,
                          get_file_mtime, get_file_size, list_volumes,
                          to_json, validate_path)
from savegame.savers.base import get_saver_class, iterate_saver_classes
from savegame.savers.google_cloud import get_google_cloud
from savegame.savers.local import LocalSaver


def ts_to_str(x):
    return datetime.fromtimestamp(int(x)).isoformat(' ')


def to_float(x):
    return float(f'{x:.02f}')


def get_local_path(x):
    return x.replace('\\' if os.path.sep == '/' else '/', os.path.sep)


class SaveItem:
    def __init__(self, config, src_paths=None, saver_id=LocalSaver.id,
                 dst_path=None, run_delta=None, purge_delta=None,
                 enable_purge=True, loadable=True, platform=None,
                 hostname=None, src_volume_label=None, dst_volume_label=None,
                 ):
        self.config = config
        self.src_volume_label = src_volume_label
        self.dst_volume_label = dst_volume_label
        self.src_paths = self._get_src_paths(src_paths)
        self.saver_cls = get_saver_class(saver_id)
        self.dst_path = self._get_dst_path(dst_path or self.config.DST_PATH)
        self.run_delta = (self.config.SAVE_RUN_DELTA
                          if run_delta is None else run_delta)
        self.purge_delta = (self.config.PURGE_DELTA
                            if purge_delta is None else purge_delta)
        self.enable_purge = enable_purge
        self.loadable = loadable
        self.platform = platform
        self.hostname = hostname

    def _get_src_paths(self, src_paths):
        return [s if isinstance(s, (list, tuple))
                else (s, [], []) for s in (src_paths or [])]

    def _get_volume_path_by_label(self, label):
        if not hasattr(self, '_volumes_by_label'):
            self._volumes_by_label = list_volumes()
        return self._volumes_by_label.get(label)

    def _get_dst_path(self, dst_path):
        if self.dst_volume_label:
            volume_path = self._get_volume_path_by_label(self.dst_volume_label)
            if not volume_path:
                raise UnhandledPath(f'volume {self.dst_volume_label} not found')
        else:
            volume_path = None
        return self.saver_cls.get_base_dst_path(
            dst_path,
            volume_path=volume_path,
            root_dirname=self.config.DST_ROOT_DIRNAME,
        )

    def _generate_src_and_patterns(self):
        if self.src_paths:
            for src_path, inclusions, exclusions in self.src_paths:
                if self.src_volume_label:
                    volume_path = self._get_volume_path_by_label(self.src_volume_label)
                    if not volume_path:
                        continue
                    src_path = os.path.join(volume_path, src_path)
                try:
                    validate_path(src_path)
                except UnhandledPath:
                    continue
                for src in glob(os.path.expanduser(src_path)):
                    yield src, inclusions, exclusions
        else:
            yield self.saver_cls.id, None, None

    def generate_savers(self):
        if self.platform and sys.platform != self.platform:
            return
        if self.hostname and HOSTNAME != self.hostname:
            return
        if not self.dst_path:
            return
        for src_and_patterns in self._generate_src_and_patterns():
            yield self.saver_cls(self.config, self, *src_and_patterns)

    def is_loadable(self):
        return self.saver_cls == LocalSaver and self.loadable


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

    def run(self):
        start_ts = time.time()
        savers = list(self._generate_savers())
        report = Report()
        volume_labels = set()
        for saver in savers:
            try:
                saver.run(force=self.force)
            except Exception as exc:
                logger.exception(f'failed to save {saver.src}')
                Notifier().send(title='exception',
                                body=f'failed to save {saver.src}: {exc}',
                                app_name=NAME)
            report.merge(saver.report)
            for attr in ('src_volume_label', 'dst_volume_label'):
                volume_label = getattr(saver.save_item, attr)
                if volume_label:
                    volume_labels.add(volume_label)

        Metadata().save()
        report_dict = report.clean(keys={'saved', 'removed'})
        if report_dict:
            logger.info(f'report:\n{to_json(report_dict)}')
        run_count = len(report.get('run').keys())
        not_run_count = len(report.get('not_run').keys())
        logger.info(f'processed {run_count}/{not_run_count + run_count} '
                    f'saves in {time.time() - start_ts:.02f} seconds')
        if volume_labels:
            Notifier().send(title='saved volumes',
                            body=', '.join(sorted(volume_labels)),
                            app_name=NAME)


class SaveMonitor:
    def __init__(self, config):
        self.config = config
        self.run_file = RunFile(os.path.join(WORK_DIR, '.monitor.run'))
        self.saver_hostnames = {r.hostname for r in iterate_saver_classes()}

    def _must_run(self):
        return time.time() > self.run_file.get_ts() + self.config.MONITOR_DELTA

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
        try:
            sizes = [get_file_size(os.path.join(ref.dst, get_local_path(r)), default=0)
                     for r in ref.files.keys()]
            return to_float(sum(sizes) / 1024 / 1024)
        except Exception:
            logger.exception(f'failed to get {ref.dst} size')
            return -1

    def _check_file(self, hostname, ref, rel_path, ref_hash):
        rel_path = get_local_path(rel_path)
        dst_file = os.path.join(ref.dst, rel_path)
        if not os.path.exists(dst_file):
            return f'missing dst file {dst_file}'
        dst_hash = get_file_hash(dst_file)
        if dst_hash != ref_hash:
            return f'conflicting dst file {dst_file}'
        if hostname == HOSTNAME and os.path.exists(ref.src):
            src_file = os.path.join(ref.src, rel_path)
            if not os.path.exists(src_file):
                return f'missing src file {src_file}'
            if get_file_hash(src_file) != ref_hash:
                return f'conflicting src file {src_file}'
        return None

    def _generate_savers(self):
        for si in iterate_save_items(self.config):
            yield from si.generate_savers()

    def _get_orphan_dsts(self):
        dsts = {s.dst for s in self._generate_savers()}
        res = set()
        for dirname in {os.path.dirname(r) for r in dsts}:
            res.update(set(glob(os.path.join(dirname, '*'))) - dsts)
        return res

    def _get_duration(self, hostname, ref):
        if hostname in self.saver_hostnames:
            by_src = {r['src']: r for r in Metadata().data.values()}
            meta = by_src.get(ref.save_src)
            if meta:
                return meta['end_ts'] - meta['start_ts']
        return None

    def _monitor(self):
        saves = []
        for hostname, ref in self._iterate_hostname_refs():
            mtimes = []
            desynced = []
            for rel_path, ref_hash in ref.files.items():
                dst_file = os.path.join(ref.dst, get_local_path(rel_path))
                if os.path.exists(dst_file):
                    mtimes.append(get_file_mtime(dst_file))
                error = self._check_file(hostname, ref, rel_path, ref_hash)
                if error:
                    desynced.append(rel_path)
                    logger.error(f'inconsistency detected: {error}')
            saves.append({
                'hostname': hostname,
                'src': ref.save_src,
                'modified': max(mtimes) if mtimes else 0,
                'size_MB': self._get_size(ref),
                'files': len(ref.files),
                'desynced': len(desynced),
                'duration': self._get_duration(hostname, ref),
            })
        orphan_dsts = sorted(self._get_orphan_dsts())
        for orphan_dst in orphan_dsts:
            logger.warning(f'no matching save: {orphan_dst}')
        report = {
            'saves': saves,
            'desynced': [r for r in saves if r['desynced']],
            'orphans': orphan_dsts,
        }
        report['message'] = ', '.join([f'{k}: {len(report[k])}'
                                       for k in ('saves', 'desynced', 'orphans')])
        return report

    def run(self):
        if not self._must_run():
            return
        report = self._monitor()
        Notifier().send(title='status', body=report['message'], app_name=NAME)
        self.run_file.touch()

    def _print_saves(self, saves, order_by):
        if not saves:
            return
        headers = {k: k for k in saves[0].keys()}
        order_by_cols = order_by.split(',') + ['src']
        rows = [headers] + sorted(saves,
                                  key=lambda x: [x[k] for k in order_by_cols],
                                  reverse=True)
        for i, r in enumerate(rows):
            if i > 0:
                r['modified'] = ts_to_str(r['modified'])
                r['duration'] = (f'{r["duration"]:.02f}'
                                 if r['duration'] is not None else '')
                r['desynced'] = r['desynced'] or ''
            print(f'{r["modified"]:19}  {r["size_MB"]:10}  {r["files"]:8}  '
                  f'{r["desynced"]:10}  {r["hostname"]:20}  {r["duration"]:10}  '
                  f'{r["src"]}')

    def get_status(self, order_by='hostname,modified'):
        report = self._monitor()
        self._print_saves(report['saves'], order_by=order_by)
        print(report['message'])


def savegame(config, force=False):
    try:
        SaveHandler(config, force=force).run()
    except Exception as exc:
        logger.exception('failed to save')
        Notifier().send(title='error', body=str(exc), app_name=NAME)
    try:
        SaveMonitor(config).run()
    except Exception as exc:
        logger.exception('failed to monitor')
        Notifier().send(title='error', body=str(exc), app_name=NAME)


def status(config, **kwargs):
    SaveMonitor(config).get_status(**kwargs)


def google_oauth(config, **kwargs):
    get_google_cloud(config, headless=False).get_oauth_creds()
