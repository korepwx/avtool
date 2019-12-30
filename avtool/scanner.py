"""Scan directory and find AV movies."""
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from typing import *

__all__ = ['AVEntry', 'AVScanner']


@dataclass
class AVEntry(object):
    """An AV movie entry."""

    movie_id: str
    """The AV movie ID."""

    parent_dir: str
    """The parent directory."""

    own_dir: bool
    """Whether or not this AV movie owns the whole parent directory?"""

    movie_files: List[str]
    """The movie files names."""

    asset_files: Optional[List[str]]
    """The asset file names of the AV movie."""


class AVScanner(object):

    MOVIE_EXTENSIONS = ['mp4', 'wmv', 'mkv', 'avi', 'rm', 'rmvb']
    ASSET_EXTENSIONS = ['json', 'nfo', 'jpg', 'png', 'zip']
    DEFAULT_MOVIE_FILE_PATTERN = re.compile(
        r'^(?P<id>(?:[A-Z]+)-(?:[0-9]+))'
        r'(?:\s*\((?P<order1>\d+)\)|-(?P<order2>\d+)|\((?P<order3>[a-z])\))?'
        r'\.(?P<ext>' +
        '|'.join(MOVIE_EXTENSIONS) +
        r'?)$',
        re.I
    )
    FILE_PATTERNS = [DEFAULT_MOVIE_FILE_PATTERN]

    def __init__(self):
        self._dir_handlers = [
            self._handle_av_dir,
            self._handle_everaver_dir,
        ]

    def find_iter(self, root_dir: str) -> Generator[AVEntry, None, None]:
        yield from self._scan_dir(os.path.abspath(root_dir))

    def _gather_assets_files(self,
                             parent_dir: str,
                             base_name: str) -> List[str]:
        ret = []
        for ext in self.ASSET_EXTENSIONS:
            name = f'{base_name}.{ext}'
            path = os.path.join(parent_dir, name)
            if os.path.isfile(path):
                path = os.path.realpath(path)
                ret.append(os.path.split(path)[-1])
        return ret

    def _scan_dir(self, parent_dir: str) -> Generator[AVEntry, None, None]:
        files_to_process = defaultdict(list)

        for name in os.listdir(parent_dir):
            path = os.path.join(parent_dir, name)
            if os.path.isdir(path):
                e = self._handle_dir(path, name)
                if e is not None:
                    yield e
                else:
                    yield from self._scan_dir(path)
            else:
                for pattern in self.FILE_PATTERNS:
                    m = pattern.match(name)
                    if m:
                        m_dict = m.groupdict()
                        m_id = m_dict['id']
                        m_order = int(
                            m_dict.get('order1') or
                            m_dict.get('order2') or
                            m_dict.get('order3') or
                            0
                        )
                        files_to_process[m_id].append((m_order, name))

        for m_id, m_list in files_to_process.items():
            m_list.sort()
            yield AVEntry(
                movie_id=m_id.upper(),
                parent_dir=parent_dir,
                own_dir=False,
                movie_files=[m_name for _, m_name in m_list],
                asset_files=None
            )

    # ---- directory handlers ----
    _AV_DIR_PATTERN = re.compile(r'^(?P<id>(?:[A-Z]+)-(?:[0-9]+))$', re.I)

    def _handle_av_dir(self, parent_dir: str, dir_name: str
                       ) -> Optional[AVEntry]:
        file_list = []
        has_other_files = False
        m = self._AV_DIR_PATTERN.match(dir_name)
        if m:
            for name in os.listdir(parent_dir):
                m = self.DEFAULT_MOVIE_FILE_PATTERN.match(name)
                if m:
                    m_dict = m.groupdict()
                    if m_dict['id'].lower() == dir_name.lower():
                        m_order = int(
                            m_dict.get('order1') or
                            m_dict.get('order2') or
                            m_dict.get('order3') or
                            0
                        )
                        file_list.append((m_order, name))
                        continue
                if not name.startswith('.'):
                    left, right = os.path.splitext(name)
                    if right.lower() not in self.ASSET_EXTENSIONS or \
                            left.lower() != dir_name.lower():
                        has_other_files = True

            if file_list:
                file_list.sort()
                return AVEntry(
                    movie_id=dir_name.upper(),
                    parent_dir=parent_dir,
                    own_dir=not has_other_files,
                    movie_files=[s for _, s in file_list],
                    asset_files=self._gather_assets_files(parent_dir, dir_name)
                )

    _EVERAVER_PATTERN = re.compile(r'^.*\[(?P<id>(?:[A-Z]+)-(?:[0-9]+))\]$', re.I)

    def _handle_everaver_dir(self, parent_dir: str, dir_name: str
                             ) -> Optional[AVEntry]:
        m = self._EVERAVER_PATTERN.match(dir_name)
        if m:
            for name in os.listdir(parent_dir):
                left, right = os.path.splitext(name)
                if left == dir_name and \
                        right.lower()[1:] in self.MOVIE_EXTENSIONS:
                    return AVEntry(
                        movie_id=m.groupdict()['id'],
                        parent_dir=parent_dir,
                        own_dir=True,
                        movie_files=[name],
                        asset_files=self._gather_assets_files(parent_dir, left)
                    )

    def _handle_dir(self, parent_dir: str, dir_name: str) -> Optional[AVEntry]:
        for fn in self._dir_handlers:
            ret = fn(parent_dir, dir_name)
            if ret is not None:
                return ret
