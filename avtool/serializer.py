import os
import zipfile
from typing import *

from .crawler import AVInfo, ImageAttachment

__all__ = [
    'write_av_info'
]


def make_file_name(base_name: str, suffix: str = '', uri: Optional[str] = None,
                   ext: Optional[str] = None):
    if ext is None:
        if not uri:
            raise ValueError('`uri` must be specified when `ext` is not given.')
        uri_file_name = uri.rsplit('/', 1)[-1]
        ext = os.path.splitext(uri_file_name)[-1]
    return f'{base_name}{suffix}{ext}'


def write_to_dir(dir_path: str, file_name: str, content: bytes):
    os.makedirs(dir_path, exist_ok=True)
    with open(os.path.join(dir_path, file_name), 'wb') as f:
        f.write(content)


def write_to_zip(zip_file: zipfile.ZipFile, file_name: str, content: bytes):
    with zip_file.open(file_name, 'w') as f:
        f.write(content)


def write_av_info(info: AVInfo, root_dir: str, base_name: str):
    """
    Save `info` to disk.

    The AV information will be saved to f"{root_dir}/{base_name}.json" and
    f"{root_dir}/{base_name}.nfo", the cover image will be saved to
    f"{root_dir}/{base_name}.{extension}" (where extension is typically
    "jpg" or "png"), and the preview images will be packed as an archive
    at f"{root_dir}/{base_name}.zip".

    Args:
        info: The AV information object.
        root_dir: The root directory, where to save all contents.
        base_name: The base name for all the generated files.
    """
    # generate cover image
    if info.cover is not None:
        if info.cover.uri and info.cover.content:
            write_to_dir(
                dir_path=root_dir,
                file_name=make_file_name(base_name, uri=info.cover.uri),
                content=info.cover.content,
            )

        if info.cover.thumbnail_uri and info.cover.thumbnail_content:
            write_to_dir(
                dir_path=root_dir,
                file_name=make_file_name(
                    base_name, uri=info.cover.thumbnail_uri),
                content=info.cover.thumbnail_content,
            )

