import requests
from dataclasses import dataclass
from typing import *

from bs4 import BeautifulSoup

__all__ = [
    'ImageAttachment', 'AVInfo', 'fetch_av_info', 'fetch_av_images',
]


@dataclass
class ImageAttachment(object):
    uri: Optional[str] = None
    content: Optional[bytes] = None
    thumbnail_uri: Optional[str] = None
    thumbnail_content: Optional[bytes] = None


@dataclass
class AVInfo(object):
    # designed according to: https://kodi.wiki/view/NFO_files/Movies

    id: Optional[str] = None
    """The ID of the AVI, i.e., the AV number."""

    series: Optional[str] = None
    """The movie series."""

    title: Optional[str] = None

    tags: Optional[List[str]] = None

    outline: Optional[str] = None
    """Should be short, will be displayed on a single line."""

    plot: Optional[str] = None
    """Can contain more information on multiple lines, will be wrapped."""

    director: Optional[str] = None
    """Movie Director."""

    studio: Optional[str] = None
    """Production studio."""

    publisher: Optional[str] = None
    """Movie publisher."""

    actors: Optional[List[str]] = None

    premiered: Optional[str] = None
    """Release date of movie. Format as 2019-01-31."""

    cover: Optional[ImageAttachment] = None
    """The cover image of the movie."""

    preview_images: Optional[List[ImageAttachment]] = None
    """The preview images, usually snapshot taken from the movie."""

    movie_length: Optional[str] = None


def fetch_av_info(id: str) -> AVInfo:
    """
    Fetch AV info from various online sources.

    Args:
        id: The AV id, i.e., the AV number.
    """
    content = requests.get(f'https://javbus.com/{id}')
    content.raise_for_status()
    tree = BeautifulSoup(content.content)
    info = AVInfo()

    # fill information
    info.title = tree.select_one('div.container > h3').text
    info_table = tree.select_one('div.container > div.row.movie > div.info')
    info_keys_mapping = {
        '識別碼': 'id',
        '發行日期': 'premiered',
        '導演': 'director',
        '製作商': 'studio',
        '發行商': 'publisher',
        '系列': 'series',
        '長度': 'movie_length',
    }
    last_row = ''
    for info_row in info_table.select('p'):
        if last_row:
            # last row is a header, parse the content accordingly
            if last_row == 'categories':
                tags = [e.text.strip() for e in info_row.select('span.genre')]
                info.tags = [s for s in tags if s]
            elif last_row == 'actors':
                actors = [e.text.strip() for e in info_row.select('span.genre')]
                info.actors = [s for s in actors if s]
            last_row = ''
        else:
            # last row is not a header, parse it
            row_items = info_row.text.strip().split(':')
            if len(row_items) == 2:
                # "key: value"
                raw_key, raw_value = info_row.text.split(':')
                raw_key = raw_key.strip()
                raw_value = raw_value.strip()
                if raw_key in info_keys_mapping and raw_value:
                    setattr(info, info_keys_mapping[raw_key], raw_value)
                elif raw_key == '類別':
                    last_row = 'categories'
                elif raw_key == '演員':
                    last_row = 'actors'

    # fill cover image
    cover_image = tree.select_one('div.container > div.row.movie '
                                  'a.bigImage')
    info.cover = ImageAttachment(
        uri=cover_image['href'],
        thumbnail_uri=cover_image.select_one('img')['src'],
    )

    # fill preview images
    preview_images = tree.select('div.container > div#sample-waterfall >'
                                 'a.sample-box')
    if preview_images:
        info.preview_images = []
        for preview_image in preview_images:
            uri = preview_image['href']
            thumbnail_uri = preview_image.select_one('img')['src']
            info.preview_images.append(ImageAttachment(
                uri=uri,
                thumbnail_uri=thumbnail_uri,
            ))

    return info


def fetch_av_images(info: AVInfo):
    """Fetch the image attachments."""

    def do_fetch(img: ImageAttachment):
        if img.uri is not None and img.content is None:
            img.content = requests.get(img.uri).content
        if img.thumbnail_uri is not None and img.content is None:
            img.thumbnail_content = requests.get(img.uri).content

    if info.cover is not None:
        do_fetch(info.cover)
    if info.preview_images:
        for preview_image in info.preview_images:
            do_fetch(preview_image)
