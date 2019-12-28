import codecs
import os
import re
import sys
import traceback
from typing import *

import click
import mltk
import yaml

from .transcode import *

__all__ = ['entry']

MOVIE_PATTERN = re.compile(r'.*\.(mp4|wmv|mkv|avi|rm(vb)?)$', re.I)
AV_PATTERN = re.compile(r'^([A-Z]+)-([0-9]+)$')
NUMBER_SUFFIX_PATTERN = re.compile(r'^(?:-([0-9]+)|\s*\(([0-9]+)\))$')


@click.group()
def entry():
    """AV movies command line tool."""


@entry.command('transcode')
@click.option('-i', 'input_files', help='The input files.', multiple=True,
              required=True)
@click.option('-o', 'output_file', help='The output file.', required=True)
@click.option('-D', '--delete-input-files', is_flag=True, default=False)
def transcode(input_files, output_file, delete_input_files):
    """Transcode movie file(s)."""
    transcode_movies(input_files, output_file)
    if delete_input_files:
        for input_file in input_files:
            if os.path.exists(input_file) and \
                    not os.path.samefile(input_file, output_file):
                os.remove(input_file)


class JobConfig(mltk.Config):
    input: Union[List[str], str]
    output: str


@entry.command('batch-transcode')
@click.option('-f', 'job_file', help='The job file.', required=True)
@click.option('-D', '--delete-input-files', is_flag=True, default=False)
def batch_transcode(job_file, delete_input_files):
    # load the job file
    with codecs.open(job_file, 'rb', 'utf-8') as f:
        jobs = yaml.load(f.read(), Loader=yaml.SafeLoader)
    jobs: List[JobConfig] = mltk.type_check.type_info(List[JobConfig]). \
        check_value(jobs)

    # execute the jobs
    max_counter = len(jobs)
    counter_width = len(str(max_counter))
    for i, job in enumerate(jobs, 1):
        try:
            print(f'{str(i):>{counter_width}}/{max_counter}: '
                  f'{job.input} -> {job.output}')
            input_files = job.input
            if not isinstance(input_files, list):
                input_files = [input_files]
            output_file = job.output
            transcode_movies(input_files, output_file)
            if delete_input_files:
                for input_file in input_files:
                    if os.path.exists(input_file) and \
                            not os.path.samefile(input_file, output_file):
                        os.remove(input_file)
        except Exception:
            print(''.join(traceback.format_exception(*sys.exc_info())))


@entry.command('generate-batch-transcode')
@click.option('-R', '--root-dir', required=True, default='.',
              help='Root directory, where to discover movie files.')
def generate_batch_transcode(root_dir):
    # gather the jobs
    jobs = []

    for name in os.listdir(root_dir):
        # check whether or not it's a directory with name "[series]-[number]"
        m = AV_PATTERN.match(name)
        if not m:
            continue
        av_series, av_number = m.group(1), m.group(2)
        path = os.path.join(root_dir, name)
        if not os.path.isdir(path):
            continue

        # gather the movie files
        movie_files = []
        for sub_name in os.listdir(path):
            if not MOVIE_PATTERN.match(sub_name):
                continue
            file_name, file_ext = os.path.splitext(sub_name)
            if not file_name.lower().startswith(name.lower()):
                continue
            m = NUMBER_SUFFIX_PATTERN.match(file_name[len(name):])
            if m:
                file_number = int(next(i is not None for i in m.groups()))
            else:
                file_number = 0
            movie_files.append((file_number, sub_name))

        # now generate the job
        movie_files.sort()
        if movie_files:
            job = JobConfig(
                input=[os.path.join(path, m[1]) for m in movie_files],
                output=os.path.join(path, f'{name}.mp4')
            )
            if len(job.input) == 1:
                job.input = job.input[0]
            jobs.append(job)

    # generate the yaml file
    print(yaml.dump([
        {'input': job.input, 'output': job.output}
        for job in jobs
    ]))


if __name__ == '__main__':
    entry()
