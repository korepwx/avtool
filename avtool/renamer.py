import os
import shutil

from .crawler import *
from .scanner import *

__all__ = [
    'AVRenamer'
]


class AVRenamer(object):
    """Renames AV files according to its movie information."""

    def __init__(self,
                 target_dir: str,
                 structure: str = '{short_series}/{movie_id} {short_actors}',
                 max_chars: int = 64,
                 max_actors: int = 3):
        self.target_dir = target_dir
        self.structure = structure
        self.max_chars = max_chars
        self.max_actors = max_actors

    def get_info_dict(self, info: AVInfo):
        def cut(s):
            if len(s) > self.max_chars:
                s = s[:self.max_chars - 1] + '…'
            return s

        ret = {
            'movie_id': info.movie_id,
            'short_series': cut(info.series or '未知系列'),
            'short_title': cut(info.title or '未知标题'),
        }

        # actors
        if info.actors:
            ret['short_actors'] = '、'.join(info.actors[:self.max_actors])
            if len(info.actors) > self.max_actors:
                ret['short_actors'] += '等'
        else:
            ret['short_actors'] = '未知演员'

        return ret

    def rename(self, entry: AVEntry, info: AVInfo, overwrite: bool = False) -> str:
        """
        Renames the AV entry according to its information.

        Args:
            entry: The AV entry.
            info: The AV information.
            overwrite: Whether or not to overwrite the existing files?

        Returns:
            The target directory, where movie files have been moved to.

        Raises:
            IOError: If target file exists, and `overwrite` is not True.
        """
        target_dir = os.path.join(
            self.target_dir,
            self.structure.format(**self.get_info_dict(info))
        )
        if os.path.exists(target_dir) and not overwrite:
            raise IOError('Target directory already exists: ' + target_dir)

        # rename to targets
        def rename_to(target_name):
            source_file = os.path.join(entry.parent_dir, file_name)
            file_ext = os.path.splitext(file_name)[-1]
            target_file = os.path.join(target_dir, f'{target_name}{file_ext}')
            if os.path.exists(target_file):
                os.remove(target_file)
            shutil.move(source_file, target_file)

        os.makedirs(target_dir, exist_ok=True)
        for i, file_name in enumerate(entry.movie_files):
            if i >= 1:
                rename_to(f'{info.movie_id}-{i}')
            else:
                rename_to(info.movie_id)

        for file_name in (entry.asset_files or ()):
            rename_to(info.movie_id)

        # cleanup the source directory, if `own_dir` is True, or the source directory becomes empty
        if entry.own_dir:
            shutil.rmtree(entry.parent_dir)
        elif not os.listdir(entry.parent_dir):
            os.rmdir(entry.parent_dir)

        return target_dir
