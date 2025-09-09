import logging
import os
import re
import subprocess
import sys
import time

from svcutils.notifier import notify

from savegame import NAME
from savegame.lib import remove_path
from savegame.savers.base import BaseSaver


logger = logging.getLogger(__name__)


class Virtualbox:
    bin_file = {'win32': r'C:\Program Files\Oracle\VirtualBox\VBoxManage.exe',
                'linux': '/usr/bin/VBoxManage'}[sys.platform]
    creationflags = subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0

    def _list(self, command):
        stdout = subprocess.check_output([self.bin_file, 'list', command], creationflags=self.creationflags)
        return re.findall(r'"([^"]+)"', stdout.decode('utf-8'))

    def list_vms(self):
        return self._list('vms')

    def list_running_vms(self):
        return self._list('runningvms')

    def _wait_for_stopped(self, vm, timeout=20, retry_interval=2):
        end_ts = time.time() + timeout
        while time.time() < end_ts:
            if vm not in self.list_running_vms():
                return
            time.sleep(retry_interval)
        raise Exception(f'timed out waiting for {vm=} to stop')

    def _run_cmd(self, *args):
        cmd = [self.bin_file, *args]
        logger.debug(f'running {cmd=}')
        try:
            subprocess.run(cmd, check=True, stdout=sys.stdout, creationflags=self.creationflags)
        except subprocess.CalledProcessError:
            logger.exception(f'failed to run {cmd=}')
            raise

    def clone_vm(self, vm, name):
        self._run_cmd('clonevm', vm, '--name', name, '--register')

    def stop_vm(self, vm):
        self._run_cmd('controlvm', vm, 'acpipowerbutton')
        self._wait_for_stopped(vm)

    def start_vm(self, vm):
        self._run_cmd('startvm', vm)

    def export_vm(self, vm, file):
        self._run_cmd('export', vm, '--output', file)


class VirtualboxExportSaver(BaseSaver):
    id = 'virtualbox_export'
    in_place = True
    retry_delta = 30

    def do_run(self):
        vb = Virtualbox()
        running_vms = vb.list_running_vms()
        errors = []
        for vm in vb.list_vms():
            notif_key = f'{self.id}-{vm}'
            if vm.lower().startswith('test'):
                logger.debug(f'skipping {vm=}')
                continue
            dst_file = os.path.join(self.dst, f'{vm}.ova')
            self.add_seen_file(dst_file)
            if vm in running_vms:
                errors.append(f'{vm} is running')
                continue
            tmp_file = os.path.join(self.dst, f'{vm}_tmp.ova')
            remove_path(tmp_file)
            start_ts = time.time()
            logger.debug(f'exporting {vm=} to {dst_file=}')
            notify(title=f'exporting vm {vm}',
                   body=f'file: {dst_file}',
                   app_name=NAME,
                   replace_key=notif_key)
            try:
                vb.export_vm(vm, tmp_file)
            except Exception as e:
                logger.exception(f'failed to export {vm=}')
                errors.append(f'{vm}: {e}')
                continue
            remove_path(dst_file)
            os.rename(tmp_file, dst_file)
            duration = time.time() - start_ts
            logger.debug(f'exported {vm=} to {dst_file=} in {duration:.02f} seconds')
            notify(title=f'exported vm {vm}',
                   body=f'file: {dst_file}, size: {os.path.getsize(dst_file) / 1024 / 1024:.02f} MB, duration: {duration:.02f} seconds',
                   app_name=NAME,
                   replace_key=notif_key)
        if errors:
            raise Exception(f'{", ".join(errors)}')
