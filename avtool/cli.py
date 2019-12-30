import os
import shutil
import sys
import traceback
from contextlib import contextmanager
from multiprocessing.pool import ThreadPool
from threading import RLock
from typing import *

import click
import mltk

from .assets import *
from .crawler import *
from .renamer import *
from .scanner import *
from .transcode import *

__all__ = ['entry']


class IndexFormatter(object):
    def __init__(self, total):
        self.total = total
        self.index_length = len(str(total))

    def __call__(self, index):
        return f'{str(index):>{self.index_length}}/{self.total}'

    def left_padding(self) -> str:
        return ' ' * (2 * self.index_length + 1)


class AtomicCounter(object):
    def __init__(self, value: int = 0):
        self.value = value
        self.lock = RLock()

    def add_get(self, n: int) -> int:
        with self.lock:
            self.value += n
            return self.value


@contextmanager
def try_execute(fn):
    try:
        fn()
    except Exception:
        print(''.join(traceback.format_exception(*sys.exc_info())).rstrip())


@click.group()
def entry():
    """AV movies command line tool."""


@entry.command('collect')
@click.option('-i', '--input-dir', required=True, default='.',
              help='Specify the input files directory.')
@click.option('-S', '--simulate', required=False, default=False, is_flag=True,
              help='Simulate, do not execute.')
@click.option('-C', '--cleanup', required=False, default=True, is_flag=True,
              help='Cleanup empty directories.')
@click.argument('output-dir', required=True)
def collect(input_dir, output_dir, simulate, cleanup):
    # gather the entries
    entries: List[AVEntry] = []
    scanner = AVScanner()
    for e in scanner.find_iter(input_dir):
        entries.append(e)

    def get_file_list_without_trivial_files(parent_dir, trivial_files=('.DS_Store', 'Thumbs.db')):
        return [f for f in os.listdir(parent_dir) if f not in trivial_files]

    def move_entry(e: AVEntry, target_dir: str):
        # prepare for the move jobs
        actions = {}

        for i, movie_file in enumerate(e.movie_files):
            if i >= 1:
                base_name, ext = os.path.splitext(movie_file)
                target_file = f'{base_name}-{i}{ext}'
            else:
                target_file = movie_file
            target_path = os.path.join(target_dir, target_file)
            if os.path.exists(target_path):
                raise IOError(f'Target file already exists: {target_path}')
            actions[movie_file] = target_path

        if e.asset_files:
            for asset_file in e.asset_files:
                target_path = os.path.join(target_dir, asset_file)
                if os.path.exists(target_path):
                    raise IOError(f'Target file already exists: {target_path}')
                actions[asset_file] = target_path

        # do execute the moving actions
        if not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)
        for source_file, target_path in actions.items():
            shutil.move(os.path.join(e.parent_dir, source_file), target_path)

        # cleanup source directories
        if cleanup:
            the_input_dir = os.path.realpath(os.path.abspath(input_dir))
            this_dir = os.path.realpath(os.path.abspath(e.parent_dir))
            while this_dir.startswith(the_input_dir) and this_dir != the_input_dir and \
                    not get_file_list_without_trivial_files(this_dir):
                print(index_fmt.left_padding() + f'  Remove dir: {this_dir}')
                if not simulate:
                    shutil.rmtree(this_dir)
                this_dir = os.path.split(this_dir)[0]

    index_fmt = IndexFormatter(len(entries))
    for i, e in enumerate(entries, 1):
        target_dir = os.path.join(output_dir, e.movie_id)
        print(f'{index_fmt(i)}: {e} -> {target_dir}')
        if not simulate:
            try_execute(lambda: move_entry(e, target_dir))


@entry.command('assets')
@click.option('-t', '--thread-num', default=20, required=True, type=click.INT,
              help='The number of fetcher threads.')
@click.option('-F', '--force', default=False, required=True, is_flag=True,
              help='Force fetching the assets even if present.')
@click.argument('work-dir', default='.', required=False)
def fetch_assets(work_dir, thread_num, force):
    # gather movie files
    scanner = AVScanner()
    entries = list(scanner.find_iter(work_dir))
    atomic_counter = AtomicCounter(len(entries))
    index_fmt = IndexFormatter(len(entries))
    print(f'Submitted {len(entries)} jobs to queue.')

    # fetch the assets
    def fetch_asset_for(e: AVEntry):
        try:
            base_name = os.path.splitext(e.movie_files[0])[0]
            asset_files = [os.path.join(e.parent_dir, f'{base_name}.{ext}')
                           for ext in ('zip', 'json')]
            if force or not all(os.path.isfile(asset_file) for asset_file in asset_files):
                make_av_assets(
                    JavBusCrawler().fetch(e.movie_id),
                    e.parent_dir,
                    base_name,
                )
                msg = f'finished: {e}'
            else:
                msg = f'skipped: {e}'
        except Exception:
            msg = (
                f'failed: {e}\n' +
                ''.join(traceback.format_exception(*sys.exc_info())).rstrip()
            )
        print(f'{index_fmt(atomic_counter.add_get(-1) + 1)}: {msg}')

    thread_pool = ThreadPool(thread_num)
    thread_pool.map(fetch_asset_for, entries)
    thread_pool.close()


@entry.command('nfo')
@click.option('-F', '--force', default=False, required=True, is_flag=True,
              help='Force fetching the assets even if present.')
@click.argument('work-dir', default='.', required=False)
def make_nfo(work_dir, force):
    # gather movie files
    scanner = AVScanner()
    entries = list(scanner.find_iter(work_dir))
    index_fmt = IndexFormatter(len(entries))

    # make nfo files
    for i, e in enumerate(entries, 1):
        base_name = os.path.splitext(e.movie_files[0])[0]
        if force or not os.path.exists(os.path.join(e.parent_dir, f'{base_name}.nfo')):
            print(f'{index_fmt(i)}: generated: {e}')
            try_execute(lambda: make_nfo_file(e.parent_dir, base_name))
        else:
            print(f'{index_fmt(i)}: skipped: {e}')


@entry.command('transcode')
@click.argument('work-dir', default='.', required=False)
def transcode(work_dir):
    # gather movie files
    scanner = AVScanner()
    entries = list(scanner.find_iter(work_dir))
    index_fmt = IndexFormatter(len(entries))

    # do transcode
    for i, e in enumerate(entries, 1):
        base_name = os.path.splitext(e.movie_files[0])[0]
        print(f'{index_fmt(i)}: {e}')
        input_files = [os.path.join(e.parent_dir, n) for n in e.movie_files]
        output_file = os.path.join(e.parent_dir, f'{base_name}.mp4')
        try_execute(lambda: transcode_movies(input_files, output_file))


@entry.command('rename')
@click.option('--overwrite', required=False, default=False, is_flag=True,
              help='If specified, overwrite the entries in target directory.')
@click.option('-i', '--source-dir', default='.', required=False)
@click.option('-S', '--simulate', required=False, default=False, is_flag=True,
              help='Simulate, do not execute.')
@click.argument('output-dir', required=True)
def rename(source_dir, output_dir, overwrite, simulate):
    # gather movie files
    scanner = AVScanner()
    entries = list(scanner.find_iter(source_dir))
    index_fmt = IndexFormatter(len(entries))

    # do rename
    def do_rename(e: AVEntry):
        base_name = os.path.splitext(e.movie_files[0])[0]
        loader = mltk.ConfigLoader(AVInfo)
        loader.load_file(os.path.join(e.parent_dir, f'{base_name}.json'))
        info = loader.get()
        renamer.rename(e, info, overwrite=overwrite)

    renamer = AVRenamer(output_dir)
    for i, e in enumerate(entries, 1):
        print(f'{index_fmt(i)}: {e}')
        if not simulate:
            try_execute(lambda: do_rename(e))


if __name__ == '__main__':
    entry()
