import codecs
import json
import os

import mltk

from .crawler import *
from .scanner import *

__all__ = [
    'JSONIndexer'
]


class JSONIndexer(object):

    def __init__(self, path: str):
        self.path = os.path.abspath(path)
        self.root_dir = os.path.dirname(path)
        self.file_object = codecs.open(path, 'wb', 'utf-8')
        self._is_first_entry = True

    def add(self, e: AVEntry, info: AVInfo):
        # compose the info dict
        info_dict = mltk.config_to_dict(info)
        info_dict['assets_zip'] = os.path.join(
            os.path.relpath(os.path.abspath(e.parent_dir), self.root_dir),
            os.path.splitext(e.movie_files[0])[0] + '.zip'
        )
        if info.cover_image:
            info_dict['cover_image'] = mltk.config_to_dict(info.cover_image)
        for key in ('fanart_images', 'screenshot_images'):
            images = getattr(info, key)
            if images:
                info_dict[key] = [mltk.config_to_dict(i) for i in images]

        # serialize info dict to json
        info_json = json.dumps(info_dict, ensure_ascii=False)

        # write to file
        self.file_object.write('[\n' if self._is_first_entry else ',\n')
        self.file_object.write(info_json)
        self._is_first_entry = False

    def close(self):
        if self._is_first_entry:
            self.file_object.write('[]\n')
        else:
            self.file_object.write('\n]\n')
        self.file_object.close()
        self.file_object = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
