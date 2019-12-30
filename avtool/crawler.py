import time

import requests
from typing import *

import mltk
from bs4 import BeautifulSoup

__all__ = [
    'AVInfoImage', 'AVInfo',
    'AVInfoCrawler', 'JavBusCrawler',
]


class AVInfoImage(mltk.Config):
    """Image entry in :class:`AVInfo`."""

    file: Optional[str]
    """The name or the URI of the image file."""

    thumbnail: Optional[str]
    """The name or the URI of the thumbnail image file."""


class AVInfo(mltk.Config):
    # designed according to: https://kodi.wiki/view/NFO_files/Movies

    movie_id: str
    """The ID of the AVI, i.e., the AV number."""

    series: Optional[str]
    """The movie series."""

    title: Optional[str]

    tags: Optional[List[str]]

    outline: Optional[str]
    """Should be short, will be displayed on a single line."""

    plot: Optional[str]
    """Can contain more information on multiple lines, will be wrapped."""

    director: Optional[str]
    """Movie Director."""

    studio: Optional[str]
    """Production studio."""

    publisher: Optional[str]
    """Movie publisher."""

    actors: Optional[List[str]]
    """The movie actors."""

    movie_length: Optional[str]
    """The movie length."""

    premiered: Optional[str]
    """Release date of movie. Format as 2019-01-31."""

    cover_image: Optional[AVInfoImage]
    """The cover image of the AV."""
    
    fanart_images: Optional[List[AVInfoImage]]
    """The fanart images of the AV."""

    screenshot_images: Optional[List[AVInfoImage]]
    """The screenshot images taken from the movie."""

    info_born_time: Optional[float]
    """Timestamp when this information object is generated."""


class AVInfoCrawler(object):
    """Base class for all AVInfo crawlers."""

    def fetch(self, movie_id: str) -> Optional[AVInfo]:
        """Fetch the information of a specified movie."""
        raise NotImplementedError()


class JavBusCrawler(AVInfoCrawler):
    """AVInfo crawler that fetches AV information from javbus.com."""

    def fetch(self, movie_id: str) -> Optional[AVInfo]:
        """
        Fetch AV info from various online sources.

        Args:
            movie_id: The AV id, i.e., the AV number.
        """
        movie_id = movie_id.upper()
        content = requests.get(f'https://javbus.com/{movie_id}')
        if content.status_code == 404:
            return None
        content.raise_for_status()
        tree = BeautifulSoup(content.content, features='html.parser')
        info = AVInfo(movie_id=movie_id)

        # fill information
        info.title = tree.select_one('div.container > h3').text
        info_table = tree.select_one('div.container > div.row.movie > div.info')
        info_keys_mapping = {
            '識別碼': 'movie_id',
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

        # fill fanart images
        fanart_image = tree.select_one('div.container > div.row.movie a.bigImage')
        fanart_image = AVInfoImage(
            file=fanart_image['href'],
            thumbnail=fanart_image.select_one('img')['src'],
        )
        if fanart_image.thumbnail == fanart_image.file:
            fanart_image.thumbnail = None
        info.fanart_images = [fanart_image]

        # fill screenshot images
        screenshot_images = tree.select('div.container > div#sample-waterfall > a.sample-box')
        if screenshot_images:
            info.screenshot_images = []
            for screenshot_image in screenshot_images:
                uri = screenshot_image['href']
                thumbnail_uri = screenshot_image.select_one('img')['src']
                if thumbnail_uri == uri:
                    thumbnail_uri = None
                info.screenshot_images.append(AVInfoImage(file=uri, thumbnail=thumbnail_uri))

        info.info_born_time = time.time()
        return info
