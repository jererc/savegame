import logging
import os
import subprocess
import sys
import time

from svcutils.notifier import notify, clear_notification

from savegame import NAME
from savegame.savers.base import BaseSaver


logger = logging.getLogger(__name__)


class Virtualbox:
    bin_file = {'win32': r'C:\Program Files\Oracle\VirtualBox\VBoxManage.exe',
                'linux': '/usr/bin/VBoxManage'}[sys.platform]

    def _list(self, command):
        stdout = subprocess.check_output([self.bin_file, 'list', command])
        return {r.rsplit(None, 1)[0].strip('"') for r in stdout.decode('utf-8').splitlines()}

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
            subprocess.run(cmd, check=True, stdout=sys.stdout)
        except subprocess.CalledProcessError:
            logger.exception(f'failed to run {cmd=}')

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
    src_type = 'remote'
    in_place = True

    def do_run(self):
        vb = Virtualbox()
        running_vms = vb.list_running_vms()
        for vm in vb.list_vms():
            if vm.lower().startswith('test'):
                logger.debug(f'skipping {vm=}')
                continue
            if vm in running_vms:
                logger.debug(f'{vm=} is running, skipping')
                continue
            file = os.path.join(self.dst, f'{vm}.ova')
            if os.path.exists(file):
                os.remove(file)
            logger.info(f'exporting {vm=} to {file=}')
            notif_key = f'{self.id}-{vm}'
            notify(title=f'exporting vm {vm}', body=f'{vm=} to {file=}', app_name=NAME, replace_key=notif_key)
            vb.export_vm(vm, file)
            clear_notification(app_name=NAME, replace_key=notif_key)
