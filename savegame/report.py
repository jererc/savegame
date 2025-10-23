import logging
import time

logger = logging.getLogger(__name__)


def truncate_middle(s: str, width: int) -> str:
    if len(s) <= width:
        return s.ljust(width)  # pad if shorter
    half = (width - 3) // 2
    return s[:half] + "..." + s[-(width - half - 3):]


class BaseReport:
    def __init__(self):
        self.data = []

    def add(self, obj, **kwargs):
        raise NotImplementedError()

    def update(self, report):
        self.data.extend(report.data)

    def _get_row(self, row):
        return ' '.join([
            f'{row["code"]:20}',
            f'{row["id"]:15}',
            truncate_middle(row["src"] or '', 60),
            truncate_middle(row["rel_path"] or '', 40),
            truncate_middle(row["dst"] or '', 60),
            f'{row["duration"]:>8}',
            f'{row["size"]:>8}',
        ])

    def print_table(self, include_codes=None, exclude_codes=None):
        rows = []
        for item in sorted(self.data, key=lambda x: (x['code'], x['id'], x['src'], x['rel_path'], x['dst'])):
            if include_codes and item['code'] not in include_codes:
                continue
            if exclude_codes and item['code'] in exclude_codes:
                continue
            rows.append(self._get_row(item))
        if rows:
            data = '\n'.join([self._get_row({k: k for k in ('code', 'id', 'src', 'dst', 'rel_path', 'duration', 'size')})] + rows)
            logger.info(f'report:\n{data}')


class SaveReport(BaseReport):
    def add(self, saver, rel_path, code, start_ts=None, size=None):
        self.data.append({
            'id': saver.id,
            'src': f'{saver.src} ({saver.save_item.src_volume_label})' if saver.save_item.src_volume_label else saver.src,
            'dst': f'{saver.dst} ({saver.save_item.dst_volume_label})' if saver.save_item.dst_volume_label else saver.dst,
            'rel_path': rel_path,
            'code': code,
            'duration': f'{time.time() - start_ts:.1f}' if start_ts else '',
            'size': f'{size / 1024 / 1024:.1f}' if size else '',
        })


class LoadReport(BaseReport):
    def add(self, loader, save_ref, src, rel_path, code, start_ts=None, size=None):
        self.data.append({
            'id': loader.id,
            'src': src,
            'dst': save_ref.dst,
            'rel_path': rel_path,
            'code': code,
            'duration': f'{time.time() - start_ts:.1f}' if start_ts else '',
            'size': f'{size / 1024 / 1024:.1f}' if size else '',
        })
