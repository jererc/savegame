import logging
import os
import re
import subprocess
import sys
import time

from svcutils.notifier import notify

from savegame import NAME
from savegame.savers.base import BaseSaver
from savegame.utils import FileRef, get_file_size, remove_path

logger = logging.getLogger(__name__)


class Virtualbox:
    bin_file = {'linux': '/usr/bin/VBoxManage',
                'win32': r'C:\Program Files\Oracle\VirtualBox\VBoxManage.exe'}[sys.platform]
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

    def _get_vm_config_file(self, vm):
        cmd = [self.bin_file, 'showvminfo', vm, '--machinereadable']
        logger.debug(f'running {cmd=}')
        result = subprocess.run(cmd, capture_output=True, text=True, creationflags=self.creationflags)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to get VM info: {result.stderr.strip()}")
        for line in result.stdout.splitlines():
            if line.startswith('CfgFile='):
                config_file = line.split('=', 1)[1].strip('"')
                if not os.path.isfile(config_file):
                    raise RuntimeError(f'Config for {vm=} does not exist: {config_file=}')
                return config_file
        raise RuntimeError(f"Config file not found for VM '{vm}'")

    def get_vm_mtime(self, vm):
        try:
            return os.path.getmtime(self._get_vm_config_file(vm))
        except Exception:
            logger.exception(f'failed to get config file for {vm=}')
            return time.time()

    def clone_vm(self, vm, name):
        self._run_cmd('clonevm', vm, '--name', name, '--register')

    def stop_vm(self, vm):
        self._run_cmd('controlvm', vm, 'acpipowerbutton')
        self._wait_for_stopped(vm)

    def start_vm(self, vm):
        self._run_cmd('startvm', vm)

    def export_vm(self, vm, file):
        self._run_cmd('export', vm, '--output', file)


class VirtualboxSaver(BaseSaver):
    id = 'virtualbox'
    in_place = True
    enable_purge = False
    retry_delta = 30

    def do_run(self):
        file_refs = self.reset_files(self.src)
        vb = Virtualbox()
        running_vms = vb.list_running_vms()
        for vm in vb.list_vms():
            notif_key = f'{self.id}-{vm}'
            if vm.lower().startswith('test'):
                logger.debug(f'skipping {vm=}')
                continue
            vm_mtime = vb.get_vm_mtime(vm)
            rel_path = f'{vm}.ova'
            file_ref = FileRef.from_ref(file_refs.get(rel_path))
            dst_file = os.path.join(self.dst, rel_path)
            if vm in running_vms:
                notify(title=f'cannot export vm {vm}', body=f'{vm} is running', app_name=NAME, replace_key=notif_key)
            elif not file_ref.mtime or file_ref.mtime < vm_mtime:
                tmp_file = os.path.join(self.dst, f'{vm}_tmp.ova')
                remove_path(tmp_file)
                logger.info(f'exporting {vm=} to {dst_file=}')
                notify(title=f'exporting vm {vm}', body=f'to {dst_file}', app_name=NAME, replace_key=notif_key)
                start_ts = time.time()
                try:
                    vb.export_vm(vm, tmp_file)
                except Exception as e:
                    logger.exception(f'failed to export {vm=}')
                    self.report.add(self, rel_path=rel_path, code='failed')
                    notify(title=f'failed to export vm {vm}', body=str(e), app_name=NAME, replace_key=notif_key)
                else:
                    remove_path(dst_file)
                    os.rename(tmp_file, dst_file)
                    file_ref = FileRef.from_file(dst_file, has_src_file=False)
                    self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts, size=get_file_size(dst_file))
                    notify(title=f'exported vm {vm}', body=f'to {dst_file}', app_name=NAME, replace_key=notif_key)
            self.set_file(self.src, rel_path, file_ref.ref)
