import codecs
import os
import uuid
from dataclasses import dataclass
from itertools import chain
from tempfile import TemporaryDirectory
from typing import *

import ffmpeg

__all__ = [
    'get_movie_codec', 'transcode_movies',
]


@dataclass
class MovieCodec(object):
    video: Dict[str, Any]
    audio: Dict[str, Any]

    def is_desired_video_codec(self) -> bool:
        return self.video.get('codec_name') in ('h264', 'hevc')


def get_movie_codec(file_path: str) -> MovieCodec:
    def extract_keys(d: Mapping[str, Any],
                     keys: Sequence[str]) -> Dict[str, Any]:
        return {key: d[key] for key in keys if key in d}

    codec_keys = ['codec_name', 'profile', 'pix_fmt']
    codec = MovieCodec(video={}, audio={})
    info = ffmpeg.probe(file_path)
    for stream in info.get('streams', ()):
        codec_type = stream.get('codec_type', None)
        if codec_type == 'video':
            codec.video.update(extract_keys(stream, codec_keys))
        elif codec_type == 'audio':
            codec.audio.update(extract_keys(stream, codec_keys))
    return codec


def transcode_movies(input_files: Sequence[str],
                     output_file: str):
    # check the parameters
    input_files = list(input_files)
    if not input_files:
        raise ValueError('`input_files` must not be empty.')

    # inspect input codecs
    input_codecs: List[MovieCodec] = [
        get_movie_codec(input_file)
        for input_file in input_files]

    # generate the temporary file names
    name, ext = os.path.splitext(output_file)
    temp_output_file = f'{name}_{uuid.uuid4().hex}{ext}'
    temp_output_file2 = f'{name}_{uuid.uuid4().hex}{ext}'

    try:
        # determine the output codecs
        audio_need_transcode = True
        video_need_transcode = True

        if (len(input_codecs) == 1 or
                all(a.audio == b.audio
                    for a, b in zip(input_codecs[:-1], input_codecs[1:]))):
            audio_need_transcode = False

        if (len(input_codecs) == 1 or
                all(a.video == b.video
                    for a, b in zip(input_codecs[:-1], input_codecs[1:]))):
            if input_codecs[0].is_desired_video_codec():
                video_need_transcode = False

        # now do the movie transcoding
        with TemporaryDirectory() as temp_dir:
            if not audio_need_transcode and not video_need_transcode:
                # video and audio codecs are all desired, use copy codec
                if len(input_files) > 1:
                    # we need the concat demuxer
                    list_file = os.path.join(temp_dir, 'list.txt')
                    with codecs.open(list_file, 'wb', 'utf-8') as f:
                        for input_file in input_files:
                            f.write(f'file \'{os.path.abspath(input_file)}\'\n')
                        input_stream = ffmpeg.input(
                            list_file, format='concat', safe='0')
                else:
                    # we just need the input movie as the input stream
                    if os.path.abspath(input_files[0]) != \
                            os.path.abspath(output_file):
                        input_stream = ffmpeg.input(input_files[0])
                    else:
                        # no need to do copy because the output file is the
                        # input file.  return immediately
                        return

                # execute ffmpeg command
                input_stream. \
                    output(temp_output_file, vcodec='copy', acodec='copy'). \
                    run()

            else:
                # otherwise do transcoding
                if len(input_files) > 1:
                    input_stream = ffmpeg.concat(
                        *chain(*[
                            (ffmpeg.input(input_file), ffmpeg.input(input_file))
                            for input_file in input_files
                        ]),
                        a=1,
                        v=1,
                    )
                else:
                    input_stream = ffmpeg.input(input_files[0])
                input_stream.output(temp_output_file).run()

        # rename the file to the final output
        if os.path.exists(output_file):
            os.rename(output_file, temp_output_file2)
            try:
                os.rename(temp_output_file, output_file)
            except:
                os.rename(temp_output_file2, output_file)
                raise
            else:
                os.remove(temp_output_file2)
        else:
            os.rename(temp_output_file, output_file)

    finally:
        if os.path.exists(temp_output_file):
            os.remove(temp_output_file)
