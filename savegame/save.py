from datetime import datetime
from glob import glob
import logging
import os
import sys
import time

from svcutils.notifier import get_notifier
from svcutils.service import RunFile

from savegame import NAME, WORK_DIR
from savegame.report import SaveReport
from savegame.savers.base import get_saver_class, iterate_saver_classes
from savegame.savers.google_cloud import get_google_cloud
from savegame.savers.file import FileSaver
from savegame.utils import (HOSTNAME, FileRef, Metadata, InvalidPath, UnhandledPath, coalesce, get_file_mtime,
                            get_file_size, iterate_save_refs, normalize_path, list_label_mountpoints, validate_path)

logger = logging.getLogger(__name__)


class SaveItem:
    def __init__(self, config, src_paths=None, saver_id=FileSaver.id, dst_path=None,
                 run_delta=None, purge_delta=None, enable_purge=True, loadable=True, platform=None,
                 hostname=None, src_volume_label=None, dst_volume_label=None, trigger_volume_labels=None,
                 retry_delta=None, file_compare_method=None, due_warning_delta=7 * 24 * 3600,
                 next_warning_delta=24 * 3600):
        self.config = config
        self.src_volume_label = src_volume_label
        self.dst_volume_label = dst_volume_label
        self.src_paths = self._get_src_paths(src_paths)
        self.saver_cls = get_saver_class(saver_id)
        self.dst_volume_path = self._get_dst_volume_path()
        self.dst_path = dst_path or self.config.DST_PATH
        self.root_dst_path = self._get_root_dst_path(self.dst_path)
        self.run_delta = coalesce(run_delta, self.config.SAVE_RUN_DELTA)
        self.purge_delta = purge_delta
        self.enable_purge = enable_purge
        self.loadable = loadable
        self.platform = platform
        self.hostname = hostname
        self.trigger_volume_labels = trigger_volume_labels or []
        self.retry_delta = retry_delta
        self.file_compare_method = file_compare_method
        self.due_warning_delta = due_warning_delta
        self.next_warning_delta = next_warning_delta
        self.notifier = get_notifier(app_name=NAME, telegram_bot_token=self.config.TELEGRAM_BOT_TOKEN, telegram_chat_id=self.config.TELEGRAM_CHAT_ID)

    def _get_src_paths(self, src_paths):
        return [s if isinstance(s, (list, tuple)) else (s, [], []) for s in (src_paths or [])]

    def _list_label_mountpoints(self):
        if not hasattr(self, '_label_mountpoints'):
            self._label_mountpoints = list_label_mountpoints()
        return self._label_mountpoints

    def _get_dst_volume_path(self):
        if not self.dst_volume_label:
            return None
        volume_path = self._list_label_mountpoints().get(self.dst_volume_label)
        if not volume_path:
            raise UnhandledPath(f'volume {self.dst_volume_label} not found')
        return volume_path

    def _get_root_dst_path(self, dst_path):
        return self.saver_cls.get_root_dst_path(
            dst_path,
            volume_path=self.dst_volume_path,
            root_dirname=self.config.DST_ROOT_DIRNAME,
        )

    def _check_trigger_volume_labels(self):
        return bool(set(self.trigger_volume_labels).intersection(set(self._list_label_mountpoints().keys())))

    def _check_due(self, saver):
        now = time.time()
        next_ts = saver.meta.get(saver.key).get('next_ts', 0)
        next_warning_ts = saver.meta.get(saver.key).get('next_warning_ts', 0)
        if next_ts and now > next_ts + self.due_warning_delta and now > next_warning_ts:
            self.notifier.send(title=f'{saver.id} is due',
                               body=f'next run was scheduled for {datetime.fromtimestamp(next_ts).isoformat()}',
                               replace_key=f'{saver.id}-due_warning')
            saver.meta.set_subkey(saver.key, 'next_warning_ts', now + self.next_warning_delta)

    def _generate_src_and_patterns(self):
        if self.src_paths:
            for src_path, include, exclude in self.src_paths:
                if self.src_volume_label:
                    volume_path = self._list_label_mountpoints().get(self.src_volume_label)
                    if not volume_path:
                        continue
                    src_path = os.path.join(volume_path, src_path)
                try:
                    validate_path(src_path)
                except UnhandledPath:
                    continue
                for src in glob(os.path.expanduser(src_path)):
                    yield src, include, exclude
        else:
            yield self.saver_cls.id, None, None

    def generate_savers(self):
        if self.platform and sys.platform != self.platform:
            return
        if self.hostname and HOSTNAME != self.hostname:
            return
        if not self.root_dst_path:
            logger.debug(f'invalid dst_path {self.dst_path} for {self.saver_cls.id}')
            return
        is_ready = not self.trigger_volume_labels or self._check_trigger_volume_labels()
        for src_and_patterns in self._generate_src_and_patterns():
            saver = self.saver_cls(self.config, self, *src_and_patterns)
            if not is_ready:
                self._check_due(saver)
                continue
            yield saver

    def is_loadable(self):
        return self.loadable and self.root_dst_path


def iterate_save_items(config, log_unhandled=False, log_invalid=True):
    for save in config.SAVES:
        try:
            yield SaveItem(config, **save)
        except UnhandledPath as e:
            if log_unhandled:
                logger.warning(e)
            continue
        except InvalidPath as e:
            if log_invalid:
                logger.warning(e)
            continue


class SaveHandler:
    def __init__(self, config, force=False):
        self.config = config
        self.force = force
        self.notifier = get_notifier(app_name=NAME, telegram_bot_token=self.config.TELEGRAM_BOT_TOKEN, telegram_chat_id=self.config.TELEGRAM_CHAT_ID)

    def _generate_savers(self):
        for si in iterate_save_items(self.config):
            yield from si.generate_savers()

    def run(self):
        logger.info('running save handler')
        start_ts = time.time()
        savers = list(self._generate_savers())
        runnable_savers = [s for s in savers if self.force or s.must_run()]
        report = SaveReport()
        failed_savers = []
        volume_labels = set()
        for saver in runnable_savers:
            try:
                saver.run()
            except Exception:
                logger.exception(f'failed to save {saver.src}')
                failed_savers.append(saver)
            report.update(saver.report)
            for attr in ('src_volume_label', 'dst_volume_label'):
                volume_label = getattr(saver.save_item, attr)
                if volume_label:
                    volume_labels.add(volume_label)
        if failed_savers:
            self.notifier.send(title='failed savers', body=', '.join(sorted(r.src for r in failed_savers)), replace_key='failed-savers')
        Metadata().save()

        report.print_table(exclude_codes=None if self.force else {'purgeable'})
        failed_files = [r for r in report.data if r['code'] == 'failed']
        if failed_files:
            self.notifier.send(title='failed files', body=f'{len(failed_files)} failed files', replace_key='failed-files')
        if volume_labels:
            self.notifier.send(title='saved volumes', body=', '.join(sorted(volume_labels)), replace_key='saved-volumes')
        logger.info(f'completed {len(runnable_savers)}/{len(savers)} saves in {time.time() - start_ts:.02f}s')


class SaveMonitor:
    def __init__(self, config):
        self.config = config
        self.run_file = RunFile(os.path.join(WORK_DIR, '.monitor.run'))
        self.saver_hostnames = {r.hostname for r in iterate_saver_classes()}
        self.notifier = get_notifier(app_name=NAME, telegram_bot_token=self.config.TELEGRAM_BOT_TOKEN, telegram_chat_id=self.config.TELEGRAM_CHAT_ID)

    def _must_run(self):
        return time.time() >= self.run_file.get_ts() + self.config.MONITOR_RUN_DELTA

    def _iterate_save_refs(self):
        root_dst_paths = {s.root_dst_path for s in iterate_save_items(self.config)
                          if s.saver_cls.dst_type == 'local' and s.root_dst_path and os.path.exists(s.root_dst_path)}
        for root_dst_path in root_dst_paths:
            yield from iterate_save_refs(root_dst_path)

    def _get_size(self, save_ref, files):
        try:
            sizes = [get_file_size(os.path.join(save_ref.dst, normalize_path(r)), default=0) for r in files.keys()]
            return float(f'{sum(sizes) / 1024 / 1024:.02f}')
        except Exception:
            logger.exception(f'failed to get {save_ref.dst} size')
            return -1

    def _check_file(self, hostname, save_ref, src, rel_path, ref):
        if not isinstance(ref, str):
            return
        rel_path = normalize_path(rel_path)
        dst_file = os.path.join(save_ref.dst, rel_path)
        if not os.path.exists(dst_file):
            return f'missing dst file {dst_file}'
        file_ref = FileRef.from_ref(ref)
        if not file_ref.check_file(dst_file):
            return f'conflicting dst file {dst_file}'
        if file_ref.has_src_file and hostname == HOSTNAME:
            src_file = os.path.join(src, rel_path)
            if not os.path.exists(src_file):
                return f'missing src file {src_file}'
            if not file_ref.check_file(src_file):
                return f'conflicting src file {src_file}'

    def _generate_savers(self):
        for si in iterate_save_items(self.config):
            yield from si.generate_savers()

    def _get_orphan_dsts(self):
        dsts = {s.dst for s in self._generate_savers() if not s.in_place}
        res = set()
        for dirname in {os.path.dirname(r) for r in dsts}:
            res.update(set(glob(os.path.join(dirname, '*'))) - dsts)
        return res

    def _generate_report(self):
        saves = []
        for save_ref in self._iterate_save_refs():
            for hostname, files in save_ref.files.items():
                mtimes = []
                desynced = []
                for src, file_refs in files.items():
                    for rel_path, ref in file_refs.items():
                        dst_file = os.path.join(save_ref.dst, normalize_path(rel_path))
                        if os.path.exists(dst_file):
                            mtimes.append(get_file_mtime(dst_file))
                        error = self._check_file(hostname, save_ref, src, rel_path, ref)
                        if error:
                            desynced.append(rel_path)
                            logger.error(f'inconsistency in {save_ref.dst}: {error}')
                    saves.append({
                        'hostname': hostname,
                        'src': f'{src} ({list(file_refs.keys())[0]})' if len(file_refs) == 1 else src,
                        'modified': max(mtimes) if mtimes else 0,
                        'size_MB': self._get_size(save_ref, file_refs),
                        'files': len(file_refs),
                        'desynced': len(desynced),
                    })
        orphan_dsts = sorted(self._get_orphan_dsts())
        for orphan_dst in orphan_dsts:
            logger.warning(f'no matching save: {orphan_dst}')
        report = {
            'saves': saves,
            'desynced': [r for r in saves if r['desynced']],
            'orphans': orphan_dsts,
        }
        report['message'] = ', '.join([f'{len(report[k])} {k}' for k in ('saves', 'desynced', 'orphans')])
        return report

    def run(self):
        if not self._must_run():
            return
        logger.info('running save monitor')
        start_ts = time.time()
        report = self._generate_report()
        self.notifier.send(title='status', body=report['message'], replace_key='status')
        self.run_file.touch()
        logger.info(f'completed save monitor in {time.time() - start_ts:.02f}s')

    def get_status(self, order_by='hostname,modified'):
        report = self._generate_report()
        if report['saves']:
            headers = {k: k for k in report['saves'][0].keys()}
            order_by_cols = order_by.split(',') + ['src']
            rows = [headers] + sorted(report['saves'], key=lambda x: [x[k] for k in order_by_cols], reverse=True)
            for i, r in enumerate(rows):
                if i > 0:
                    r['modified'] = datetime.fromtimestamp(int(r['modified'])).isoformat(' ')
                    r['desynced'] = r['desynced'] or ''
                print(f'{r["modified"]:19}  {r["size_MB"]:10}  {r["files"]:8}  {r["desynced"]:10}  {r["hostname"]:20}  {r["src"]}')
        print(report['message'])


def savegame(config, force=False):
    def notify(title, body, replace_key):
        notifier = get_notifier(app_name=NAME, telegram_bot_token=config.TELEGRAM_BOT_TOKEN, telegram_chat_id=config.TELEGRAM_CHAT_ID)
        notifier.send(title=title, body=body, replace_key=replace_key)

    try:
        SaveHandler(config, force=force).run()
    except Exception as e:
        logger.exception('failed to save')
        notify('error', str(e), 'save-error')
    try:
        SaveMonitor(config).run()
    except Exception as e:
        logger.exception('failed to monitor')
        notify('error', str(e), 'status-error')


def status(config, **kwargs):
    SaveMonitor(config).get_status(**kwargs)


def google_oauth(config, **kwargs):
    get_google_cloud(config, headless=False).get_oauth_creds()
