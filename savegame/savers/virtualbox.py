import logging
import os
import time

from vbox.virtualbox import Virtualbox

from savegame.savers.base import BaseSaver
from savegame.utils import FileRef, get_file_size, remove_path

logger = logging.getLogger(__name__)


class VirtualboxSaver(BaseSaver):
    id = 'virtualbox'
    in_place = True
    enable_purge = False
    retry_delta = 30

    def do_run(self):
        try:
            vb = Virtualbox()
        except FileNotFoundError as e:
            logger.debug(f'skipping {self.id} saver: {str(e)}')
            return
        file_refs = self.reset_files(self.src)
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
                self.notifier.send(title=f'cannot export vm {vm}', body=f'{vm} is running', replace_key=notif_key)
            elif not file_ref.mtime or file_ref.mtime < vm_mtime:
                tmp_file = os.path.join(self.dst, f'{vm}_tmp.ova')
                remove_path(tmp_file)
                logger.info(f'exporting {vm=} to {dst_file=}')
                self.notifier.send(title=f'exporting vm {vm}', body=f'to {dst_file}', replace_key=notif_key)
                start_ts = time.time()
                try:
                    vb.export_vm(vm, tmp_file)
                except Exception as e:
                    logger.exception(f'failed to export {vm=}')
                    self.report.add(self, rel_path=rel_path, code='failed')
                    self.notifier.send(title=f'failed to export vm {vm}', body=str(e), replace_key=notif_key)
                else:
                    remove_path(dst_file)
                    os.rename(tmp_file, dst_file)
                    file_ref = FileRef.from_file(dst_file, has_src_file=False)
                    self.report.add(self, rel_path=rel_path, code='saved', start_ts=start_ts, size=get_file_size(dst_file))
                    self.notifier.send(title=f'exported vm {vm}', body=f'to {dst_file}', replace_key=notif_key)
            self.set_file(self.src, rel_path, file_ref.ref)
