#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: arguments.py
## desc: Argument parsing and handling.

import argparse
import logging
from argparse import ArgumentParser

from . import globe

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _register_args(parser: ArgumentParser) -> ArgumentParser:
    """

    :param parser:
    :return:
    """

    ## Options common to all (sub)parsers
    parser = ArgumentParser(add_help=False)

    parser.add_argument(
        'populations',
        nargs='?',
        help='comma separated list of populations to filter on'
    )

    parser.add_argument(
        'rsids',
        nargs='?',
        help='file containing list of refSNP IDs'
    )

    parser.add_argument(
        '-s',
        '--super',
        action='store_true',
        dest='super_population',
        help='the given population list is a super population'
    )

    parser.add_argument(
        '--r2',
        action='store',
        default=0.7,
        dest='rsquared',
        type=float,
        help='r2 LD threshold'
    )

    parser.add_argument(
        '--skip-download',
        action='store_true',
        dest='no_download',
        help='skip retrieving external tools and 1KGP variant calls'
    )

    perf_group = parser.add_argument_group('performance')

    perf_group.add_argument(
        '-l',
        '--local',
        action='store_true',
        dest='local',
        help='run a local cluster instead of using an HPC/PBS cluster'
    )

    perf_group.add_argument(
        '-c',
        '--cores',
        action='store',
        default=9,
        dest='cores',
        metavar='N',
        type=int,
        help='number of cores to use per job'
    )

    perf_group.add_argument(
        '-p',
        '--processes',
        action='store',
        default=3,
        dest='processes',
        metavar='N',
        type=int,
        help='number of processes to use per job'
    )

    perf_group.add_argument(
        '-j',
        '--jobs',
        action='store',
        default=90,
        dest='jobs',
        metavar='N',
        type=int,
        help='number of jobs to submit to an HPC system'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        dest='verbose',
        help='clutter your screen with output'
    )

    return parser


def _validate_arguments(parser: ArgumentParser) -> None:
    """
    Ensures arguemnts passed to the CLI script are valid. Informs the user of any missing
    or invalid arguments.

    :param args:
    :return:
    """

    args = parser.parse_args()

    if not args.populations:
        print('ERROR: You need to provide a population list')
        parser.print_help()
        exit(1)

    if not args.rsids:
        print('ERROR: You need to provide a file containing a list of refSNPs')
        parser.print_help()
        exit(1)

    return args


def setup_arguments(exe):
    """

    :param exe:
    :return:
    """

    parser = ArgumentParser()
    parser = _register_args(parser)

    return _validate_arguments(parser)

