from __future__ import print_function, unicode_literals
from argparse import ArgumentParser
from datetime import datetime
from itertools import chain
import sys

from .kinesis_logs_reader import KinesisLogsReader


def print_stream(stream_name, start_time, stop_after):
    reader = KinesisLogsReader(stream_name, start_time=start_time)

    # Peek into the first row to determine the header
    first_row = next(reader)
    keys = sorted(first_row.keys())
    print(*keys, sep='\t')

    # Join the first row with the rest of the rows and print them
    iterable = chain([first_row], reader)
    for i, fields in enumerate(iterable, 1):
        print(*[fields[k] for k in keys], sep='\t')
        if i == stop_after:
            break


def main(argv=None):
    argv = argv or sys.argv[1:]

    argument_parser = ArgumentParser('Read logs from AWS Kinesis')
    argument_parser.add_argument(
        'stream_name', type=str, help='Name of the Kinesis stream to read'
    )
    argument_parser.add_argument(
        '--start-time',
        type=str,
        help='Time from which to start reading (default: beginning of stream)'
    )
    argument_parser.add_argument(
        '--count',
        type=int,
        default=0,
        help='Number of records to return (default: 0, which is "no limit")'
    )
    argument_parser.add_argument(
        '--time-format',
        type=str,
        default='%Y-%m-%d %H:%M:%S',
        help='Format string for the --start-time argument',
    )
    args = argument_parser.parse_args(argv)

    if args.start_time:
        start_time = datetime.strptime(args.start_time, args.time_format)
    else:
        start_time = None

    print_stream(args.stream_name, start_time, args.count)


if __name__ == '__main__':
    main()
