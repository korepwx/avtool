"""Scan directory and find AV movies."""
import os
import re
from collections import defaultdict
from typing import *

import mltk

__all__ = [
    'AVEntry',
    'AVFilesMatcher', 'AVDirectoryMatcher',
    'AVScanner',
]

MOVIE_EXTENSIONS = ['mp4', 'wmv', 'mkv', 'avi', 'rm', 'rmvb']
ASSET_EXTENSIONS = ['json', 'yml', 'nfo', 'jpg', 'jpeg', 'png', 'zip']
DOWNLOADING_EXTENSIONS = ['ut!', 'part']


class AVEntry(mltk.Config):
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


class Forbidden(object):
    pass


FORBIDDEN = Forbidden()


class AVFilesMatcher(object):

    FILE_PATTERNS = [
        (3, re.compile(
            r'^(?:HD-|\[[a-z0-9]+\.[a-z]+\])?'
            r'(?P<id>(?:[A-Z0-9]+)-(?:[0-9]+))'
            r'(?:\s*\((?P<order1>\d+)\)|-(?P<order2>\d+)|\((?P<order3>[a-z])\))?'
            r'(?:\s+.*)?'
            r'\.(?P<ext>' + '|'.join(MOVIE_EXTENSIONS) + r'?)'
            r'(?:\.(?P<download_ext>' + '|'.join(DOWNLOADING_EXTENSIONS) + r'))?'
            r'$',
            re.I
        ))
    ]

    def __init__(self, parent_dir: str, movie_id: Optional[str] = None):
        if movie_id is not None:
            movie_id = movie_id.upper()
        self.parent_dir = parent_dir
        self.movie_id = movie_id
        self.files_to_process: MutableMapping[str, Union[List[(int, str)], Forbidden]] = defaultdict(list)

    def get(self) -> List[AVEntry]:
        ret = []
        for m_id, m_list in self.files_to_process.items():
            if m_list is FORBIDDEN:
                continue
            m_list.sort()
            ret.append(AVEntry(
                movie_id=m_id.upper(),
                parent_dir=self.parent_dir,
                own_dir=False,
                movie_files=[m_name for _, m_name in m_list],
                asset_files=None
            ))
        return ret

    def match(self, name: str):
        path = os.path.join(self.parent_dir, name)
        if os.path.isfile(path):
            for n_orders, pattern in self.FILE_PATTERNS:
                m = pattern.match(name)
                if m:
                    m_dict = m.groupdict()
                    m_id = m_dict['id'].upper()
                    m_download_ext = m_dict.get('download_ext')
                    if m_download_ext:
                        self.files_to_process[m_id] = FORBIDDEN
                        continue
                    m_order = 0
                    for order_idx in range(1, n_orders + 1):
                        this_order = m_dict.get(f'order{order_idx}')
                        if this_order is not None:
                            m_order = ord(this_order) - ord('0')
                    if self.movie_id is None or m_id == self.movie_id:
                        if self.files_to_process[m_id] is not FORBIDDEN:
                            self.files_to_process[m_id].append((m_order, name))
                        break

    def match_all(self):
        for name in os.listdir(self.parent_dir):
            self.match(name)


class AVDirectoryMatcher(object):
    """Base class for directory matcher."""

    def collect_assets(self,
                       parent_dir: str,
                       base_name: str) -> List[str]:
        """
        Gather assets files for a particular movie under a given directory.

        Args:
            parent_dir: The parent directory.
            base_name: The base name of the assets files.

        Returns:
            The collected assets file names.
        """
        ret = []
        for ext in ASSET_EXTENSIONS:
            name = f'{base_name}.{ext}'
            path = os.path.join(parent_dir, name)
            if os.path.isfile(path):
                path = os.path.realpath(path)
                ret.append(os.path.split(path)[-1])  # use the system case
        return ret

    def match(self, path: str) -> Optional[AVEntry]:
        """
        Attempt to match a directory `path`.

        Args:
            path: The path of the directory.

        Returns:
            AVEntry object if matches, otherwise None.
        """
        raise NotImplementedError()


class DefaultAVDirectoryMatcher(AVDirectoryMatcher):
    """The default AV directory matcher."""

    DIR_PATTERN = re.compile(r'^(?:HD-)?(?P<id>(?:[A-Z0-9]+)-(?:[0-9]+))(?:\s+.*)?$', re.I)

    def match(self, path: str) -> Optional[AVEntry]:
        base_name = os.path.split(path)[-1]
        m = self.DIR_PATTERN.match(base_name)
        if m:
            movie_id = m.groupdict()['id'].upper()
            files_matcher = AVFilesMatcher(path, movie_id)
            files_matcher.match_all()
            entries = files_matcher.get()
            if entries:
                e = entries[0]
                e.own_dir = True
                e.asset_files = self.collect_assets(path, base_name)
                if base_name.upper() != movie_id:
                    e.asset_files += self.collect_assets(path, movie_id)
                e.asset_files = e.asset_files or None
                return e


class EverAverDirectoryMatcher(AVDirectoryMatcher):
    """The directory matcher for EverAver organized directories."""

    DIR_PATTERN = re.compile(r'^.*\[(?P<id>(?:[A-Z0-9]+)-(?:[0-9]+))\]$', re.I)

    def match(self, path: str) -> Optional[AVEntry]:
        base_name = os.path.split(path)[-1]
        base_name_upper = base_name.upper()
        m = self.DIR_PATTERN.match(base_name)
        if m:
            for name in os.listdir(path):
                left, right = os.path.splitext(name)
                if left.upper().endswith(base_name_upper) and \
                        right.lower()[1:] in MOVIE_EXTENSIONS:
                    return AVEntry(
                        movie_id=m.groupdict()['id'],
                        parent_dir=path,
                        own_dir=True,
                        movie_files=[name],
                        asset_files=self.collect_assets(path, base_name)
                    )


class AVScanner(object):
    """Scanner that collects AV entries from a root directory."""

    def __init__(self):
        self.dir_matchers = [
            DefaultAVDirectoryMatcher(),
            EverAverDirectoryMatcher(),
        ]

    def find_iter(self, root_dir: str) -> Generator[AVEntry, None, None]:
        """
        Iterate though all AV entries under a given root directory.

        Args:
            root_dir: The root directory.

        Yields:
            The discovered AV entries.
        """
        def g(parent_dir: str) -> Generator[AVEntry, None, None]:
            for dir_matcher in self.dir_matchers:
                e = dir_matcher.match(parent_dir)
                if e is not None:
                    yield e
                    break
            else:
                files_matcher = AVFilesMatcher(parent_dir)
                for name in os.listdir(parent_dir):
                    path = os.path.join(parent_dir, name)
                    if os.path.isdir(path):
                        yield from g(path)
                    else:
                        files_matcher.match(name)
                yield from files_matcher.get()

        if os.path.isdir(root_dir):
            yield from g(root_dir)
