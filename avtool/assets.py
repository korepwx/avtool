import codecs
import json
import mimetypes
import os
import zipfile
from datetime import datetime
from io import BytesIO
from typing import *

import mltk
import requests
from lxml import etree
from PIL import Image

from .crawler import *

__all__ = [
    'AssetsFetcher', 'AssetsDBMaker', 'AssetsDB',
    'make_av_assets', 'make_nfo_file',
]


class AssetsFetcher(object):

    def fetch(self, uri: str, base_name: Optional[str] = None) -> Tuple[str, bytes]:
        r = requests.get(uri)

        file_name = uri.rsplit('/', 1)[-1] or ''
        ext = ''
        if file_name and '.' in file_name:
            ext = os.path.splitext(file_name)[-1]
        elif 'content-type' in r.headers:
            mime_type = r.headers['content-type'].split(';')[0].strip() or ''
            if mime_type:
                ext = mimetypes.guess_extension(mime_type)

        if base_name:
            file_name = f'{base_name}{ext}'
        elif not file_name:
            file_name = f'noname{ext}'

        return file_name, r.content


class AssetsDBMaker(object):

    def __init__(self, path: str):
        self.path = path
        self.zip_file = zipfile.ZipFile(path, mode='w')
        self.meta_dict: Dict[str, Any] = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        self.zip_file.close()

    def add(self, name: str, content: bytes, meta: Optional[Dict[str, Any]] = None) -> str:
        # uniquify the name
        if name in self.meta_dict:
            base_name, ext = os.path.splitext(name)
            idx = 1
            while True:
                new_name = f'{base_name}_{idx}'
                if new_name not in self.meta_dict:
                    base_name = new_name
                    break
            name = f'{base_name}{ext}'

        # add the entry
        meta = dict(meta or {})
        meta_json = json.dumps(meta, ensure_ascii=False, indent=2, separators=(', ', ': ')).encode('utf-8')
        self.zip_file.writestr(name, content)
        self.zip_file.writestr(f'{name}.json', meta_json)
        self.meta_dict[name] = meta

        return name


class AssetsDB(object):

    def __init__(self, path: str):
        self.path = path
        self.zip_file = zipfile.ZipFile(path, mode='r')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        for info in self.zip_file.infolist():
            if not info.name.endswith('.json'):
                yield info.name

    def close(self):
        self.zip_file.close()

    def get_content(self, file_name: str) -> Optional[bytes]:
        try:
            info = self.zip_file.getinfo(file_name)
        except KeyError:
            return None
        else:
            f = self.zip_file.open(info, mode='r')
            try:
                return f.read()
            finally:
                if hasattr(f, 'close'):
                    f.close()

    def get_meta(self, file_name: str) -> Optional[Dict[str, Any]]:
        cnt = self.get_content(f'{file_name}.json')
        if cnt:
            return dict(json.loads(cnt))


def crop_cover_image(input_content: bytes) -> bytes:
    with BytesIO(input_content) as input_stream:
        img: Image.Image = Image.open(input_stream, mode='r')
        try:
            cropped_img = img.crop((round(img.width - img.height * 0.704), 0, img.width, img.height))
            try:
                with BytesIO() as output_stream:
                    cropped_img.save(output_stream, format='JPEG')
                    return output_stream.getvalue()
            finally:
                cropped_img.close()
        finally:
            img.close()


def make_av_assets(info: AVInfo, parent_dir: str, base_name: str):
    os.makedirs(parent_dir, exist_ok=True)

    # generate the assets archive
    fetcher = AssetsFetcher()

    def fetch_asset(asset: AVInfoImage, base_name: str):
        c1, c2 = None, None
        if asset.file:
            n, c1 = fetcher.fetch(asset.file, base_name)
            asset.file = db.add(n, c1, {'uri': asset.file})
        if asset.thumbnail:
            n, c2 = fetcher.fetch(asset.thumbnail, f'{base_name}.thumbnail')
            asset.thumbnail = db.add(n, c2, {'uri': asset.thumbnail})
        return c1, c2

    with AssetsDBMaker(os.path.join(parent_dir, f'{base_name}.zip')) as db:
        # fanarts
        buf: List[Tuple[bytes, bytes]] = []
        if info.fanart_images:
            for i, fanart_image in enumerate(info.fanart_images):
                buf.append(fetch_asset(fanart_image, f'fanart_{i}'))

        # cover
        if info.cover_image is not None:
            fetch_asset(info.cover_image, 'cover')
        elif buf:
            info.cover_image = AVInfoImage()

            # generate the cover image from fanart images, if not given
            if buf[0][0]:
                info.cover_image.file = db.add('cover.jpg', crop_cover_image(buf[0][0]))
            if buf[0][1]:
                info.cover_image.thumbnail = db.add('cover.thumbnail.jpg', crop_cover_image(buf[0][1]))
        buf.clear()

        # screenshots
        if info.screenshot_images:
            for i, screenshot_image in enumerate(info.screenshot_images):
                fetch_asset(screenshot_image, f'screenshot_{i}')

    # save the meta json
    meta_dict = mltk.config_to_dict(info)
    if info.cover_image is not None:
        meta_dict['cover_image'] = mltk.config_to_dict(info.cover_image)
    for key in ('fanart_images', 'screenshot_images'):
        if getattr(info, key):
            meta_dict[key] = [mltk.config_to_dict(i) for i in getattr(info, key)]

    meta_json = json.dumps(meta_dict, ensure_ascii=False, indent=2, separators=(', ', ': '))
    with codecs.open(os.path.join(parent_dir, f'{base_name}.json'), 'wb', 'utf-8') as f:
        f.write(meta_json)


def make_nfo_file(parent_dir: str, base_name: str):
    # load the av info object
    loader = mltk.ConfigLoader(AVInfo)
    loader.load_file(os.path.join(parent_dir, f'{base_name}.json'))
    info = loader.get()

    # load the cover and the fanarts
    with AssetsDB(os.path.join(parent_dir, f'{base_name}.zip')) as db:
        def load_image(img: Optional[AVInfoImage]) -> Optional[bytes]:
            if img is not None:
                file_name = img.file or img.thumbnail
                if file_name:
                    return db.get_content(file_name)

        cover = load_image(info.cover_image)
        fanart = load_image((info.fanart_images and info.fanart_images[0]) or None)

    # generate the nfo file
    # see: https://kodi.wiki/view/NFO_files/Movies
    _DIRECT_MAPPED_KEYS = (
        'title', 'outline', 'plot', 'director', 'premiered', 'studio',
        'publisher',
    )
    _KEY_MAPPING = {  # see: https://kodi.wiki/view/NFO_files/Movies
        'movie_id': 'unique_id',
    }

    root = etree.Element('movie')

    def add_node(key, value):
        if value is not None:
            c = etree.Element(key)
            if isinstance(value, dict):
                for key, val in value.items():
                    cc = etree.Element(key)
                    cc.text = val
                    c.append(cc)
            else:
                c.text = value
            root.append(c)

    for key in _DIRECT_MAPPED_KEYS:
        add_node(key, getattr(info, key))
    for key, mapped_key in _KEY_MAPPING.items():
        add_node(mapped_key, getattr(info, key))
    if info.series:
        add_node('set', {'name': info.series})
    if info.tags:
        for tag in info.tags:
            add_node('genre', tag)
    if info.info_born_time is not None:
        dt_str = datetime.fromtimestamp(info.info_born_time).strftime('%Y-%m-%d %H:%M:%S')
        add_node('dateadded', dt_str)
    if info.actors:
        for i, actor in enumerate(info.actors):
            add_node('actor', {'name': actor, 'order': str(i)})
    if cover is not None:
        file_name = f'{base_name}.jpg'
        with open(os.path.join(parent_dir, file_name), 'wb') as f:
            f.write(cover)
        add_node('thumb', file_name)
    if fanart is not None:
        file_name = f'{base_name}.jpeg'
        with open(os.path.join(parent_dir, file_name), 'wb') as f:
            f.write(fanart)
        add_node('fanart', {'thumb': file_name})

    s = etree.tostring(root, pretty_print=True, encoding='utf-8')
    with open(os.path.join(parent_dir, f'{base_name}.nfo'), 'wb') as f:
        f.write(s)
