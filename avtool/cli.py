import os
import shutil
import subprocess
import sys
import traceback
from contextlib import contextmanager
from multiprocessing.pool import ThreadPool
from threading import RLock
from typing import *

import click
import mltk

from avtool.indexing import JSONIndexer
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


def load_info_by_entry(e: AVEntry) -> AVInfo:
    base_name = os.path.splitext(e.movie_files[0])[0]
    loader = mltk.ConfigLoader(AVInfo)
    loader.load_file(os.path.join(e.parent_dir, f'{base_name}.json'))
    return loader.get()


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
            base_name, ext = os.path.splitext(movie_file)
            if i >= 1:
                target_file = f'{e.movie_id}-{i}{ext}'
            else:
                target_file = f'{e.movie_id}{ext}'
            target_path = os.path.join(target_dir, target_file)
            if os.path.exists(target_path):
                raise IOError(f'Target file already exists: {target_path}')
            actions[movie_file] = target_path

        if e.asset_files:
            for asset_file in e.asset_files:
                base_name, ext = os.path.splitext(asset_file)
                target_path = os.path.join(target_dir, f'{base_name}{ext}')
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
@click.option('-t', '--thread-num', default=10, required=True, type=click.INT,
              help='The number of fetcher threads.')
@click.option('-F', '--force', default=False, required=True, is_flag=True,
              help='Force fetching the assets even if present.')
@click.option('-S', '--simulate', required=False, default=False, is_flag=True,
              help='Simulate, do not execute.')
@click.argument('work-dir', default='.', required=False)
def fetch_assets(work_dir, thread_num, force, simulate):
    # gather movie files
    scanner = AVScanner()
    entries = list(scanner.find_iter(work_dir))
    atomic_counter = AtomicCounter(len(entries))
    index_fmt = IndexFormatter(len(entries))
    print(f'Submitted {len(entries)} jobs to queue.')

    # fetch the assets
    def fetch_asset_for(e: AVEntry, retry: int = 3):
        try:
            while True:
                try:
                    base_name = os.path.splitext(e.movie_files[0])[0]
                    asset_files = [os.path.join(e.parent_dir, f'{base_name}.{ext}')
                                   for ext in ('zip', 'json')]
                    if force or not all(os.path.isfile(asset_file)
                                        for asset_file in asset_files):
                        if not simulate:
                            make_av_assets(
                                JavBusCrawler().fetch(e.movie_id),
                                e.parent_dir,
                                base_name,
                            )
                        msg = f'finished: {e}'
                    else:
                        msg = f'skipped: {e}'
                    break
                except Exception:
                    retry -= 1
                    if retry < 0:
                        raise
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
@click.option('-S', '--simulate', required=False, default=False, is_flag=True,
              help='Simulate, do not execute.')
@click.argument('work-dir', default='.', required=False)
def make_nfo(work_dir, force, simulate):
    # gather movie files
    scanner = AVScanner()
    entries = list(scanner.find_iter(work_dir))
    index_fmt = IndexFormatter(len(entries))

    # make nfo files
    for i, e in enumerate(entries, 1):
        base_name = os.path.splitext(e.movie_files[0])[0]
        if force or not os.path.exists(os.path.join(e.parent_dir, f'{base_name}.nfo')):
            print(f'{index_fmt(i)}: generate: {e}')
            if not simulate:
                try_execute(lambda: make_nfo_file(e.parent_dir, base_name))
        else:
            print(f'{index_fmt(i)}: skipped: {e}')


@entry.command('transcode')
@click.option('--no-delete-input', default=False, required=False, is_flag=True,
              help='Do not delete input files.')
@click.option('-S', '--simulate', required=False, default=False, is_flag=True,
              help='Simulate, do not execute.')
@click.argument('work-dir', default='.', required=False)
def transcode(work_dir, no_delete_input, simulate):
    # gather movie files
    scanner = AVScanner()
    entries = list(scanner.find_iter(work_dir))
    index_fmt = IndexFormatter(len(entries))

    # do transcode
    def do_transcode(input_files: Sequence[str], output_file: str):
        if not simulate:
            transcode_movies(input_files, output_file)
            if not no_delete_input:
                for input_file in input_files:
                    if not os.path.samefile(input_file, output_file):
                        print(index_fmt.left_padding() + f'  Remove: {input_file}')
                        os.remove(input_file)

    for i, e in enumerate(entries, 1):
        base_name = os.path.splitext(e.movie_files[0])[0]
        print(f'{index_fmt(i)}: {e}')
        input_files = [os.path.join(e.parent_dir, n) for n in e.movie_files]
        output_file = os.path.join(e.parent_dir, f'{base_name}.mp4')
        try_execute(lambda: do_transcode(input_files, output_file))


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
        info = load_info_by_entry(e)
        renamer.rename(e, info, overwrite=overwrite)

    renamer = AVRenamer(output_dir)
    for i, e in enumerate(entries, 1):
        print(f'{index_fmt(i)}: {e}')
        if not simulate:
            try_execute(lambda: do_rename(e))


@entry.command('index')
@click.option('-i', '--input-dir', required=True, default='.',
              help='Specify the input files directory.')
@click.argument('output-file', required=True, default='index.json')
def index(input_dir, output_file):
    # gather movie files
    scanner = AVScanner()
    entries = list(scanner.find_iter(input_dir))
    index_fmt = IndexFormatter(len(entries))

    # do index
    def index_entry(e: AVEntry):
        info = load_info_by_entry(e)
        json_indexer.add(e, info)

    with JSONIndexer(output_file) as json_indexer:
        for i, e in enumerate(entries):
            print(f'{index_fmt(i)}: {e}')
            try_execute(lambda: index_entry(e))


@entry.command('auto')
@click.option('-i', '--input-dir', required=True, default='.',
              help='Specify the input files directory.')
@click.argument('output-dir', required=True)
def auto_jobs(input_dir, output_dir):
    input_dir = os.path.abspath(input_dir)
    output_dir = os.path.abspath(output_dir)

    def check_call(args, cwd=None):
        print(f'> {args}')
        print('')
        kwargs = {'cwd': cwd} if cwd is not None else {}
        exit_code = subprocess.check_call(args, **kwargs)
        print('')
        print(f'Exit code: {exit_code}')

    # collect
    check_call(['avtool', 'collect', '-i', input_dir, output_dir])
    # fetch-assets
    check_call(['avtool', 'assets'], cwd=output_dir)
    # make-nfo
    check_call(['avtool', 'nfo'], cwd=output_dir)
    # transcode
    check_call(['avtool', 'transcode'], cwd=output_dir)


if __name__ == '__main__':
    entry()
