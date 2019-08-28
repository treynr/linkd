#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: arguments.py
## desc: Argument parsing and handling.

import argparse
import logging
import click
from argparse import ArgumentParser

from . import globe

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _register_args() -> ArgumentParser:
    """

    :param common:
    :return:
    """

    ## Options common to all (sub)parsers
    common = ArgumentParser(add_help=False)

    perf_group = common.add_argument_group('performance')

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
        default=5,
        dest='cores',
        metavar='N',
        type=int,
        help='number of cores to use per job'
    )

    perf_group.add_argument(
        '-p',
        '--processes',
        action='store',
        default=5,
        dest='processes',
        metavar='N',
        type=int,
        help='number of processes to use per job'
    )

    perf_group.add_argument(
        '-j',
        '--jobs',
        action='store',
        default=50,
        dest='jobs',
        metavar='N',
        type=int,
        help='number of jobs to submit to an HPC system'
    )

    ## Subcommands
    subparser = common.add_subparsers(title='subcommand', dest='subcommand')
    retrieve_parser = subparser.add_parser(
        'retrieve',
        parents=[common],
        help='retrieve 1KGP datasets'
    )
    filter_parser = subparser.add_parser(
        'filter',
        parents=[common],
        help='filter 1KGP datasets based on population structure'
    )
    ld_parser = subparser.add_parser(
        'ld',
        parents=[common],
        help='calculate linkage disequilibrium for a given population and SNP list'
    )

    ## Retrieve arguments
    retrieve_parser.add_argument(
        'output',
        default=globe._dir_1k_variants,
        nargs='?',
        help='output directory to store raw 1KGP variant calls'
    )

    ## Filter arguments
    filter_parser.add_argument(
        'populations',
        nargs='?',
        help='comma separated list of population identifiers to filter on'
    )

    filter_parser.add_argument(
        'input',
        nargs='?',
        help='input directory containing raw 1KGP variant calls'
    )

    filter_parser.add_argument(
        'output',
        nargs='?',
        help='output directory to store raw 1KGP variant calls'
    )

    filter_parser.add_argument(
        '-s',
        '--super',
        action='store_true',
        dest='super_population',
        help='the given population list contains only super populations'
    )

    ## LD arguments
    ld_parser.add_argument(
        'snps',
        nargs='?',
        help='file containing a list of refSNP IDs to calculate LD for'
    )

    ld_parser.add_argument(
        'input',
        nargs='?',
        help='input directory containing processed 1KGP variant calls'
    )

    ld_parser.add_argument(
        '-r',
        '--r2',
        action='store',
        default=0.7,
        dest='rsquared',
        type=float,
        help='r2 LD threshold'
    )


    #common.add_argument(
    #    '--verbose',
    #    action='store_true',
    #    dest='verbose',
    #    help='clutter your screen with output'
    #)

    #common.add_argument(
    #    'populations',
    #    nargs='?',
    #    help='comma separated list of populations to filter on'
    #)

    #common.add_argument(
    #    'rsids',
    #    nargs='?',
    #    help='file containing list of refSNP IDs'
    #)

    #common.add_argument(
    #    'output',
    #    nargs='?',
    #    help='output LD file'
    #)


    #common.add_argument(
    #    '--skip-download',
    #    action='store_true',
    #    dest='no_download',
    #    help='skip retrieving external tools and 1KGP variant calls'
    #)

    #common.add_argument(
    #    '--skip-filtering',
    #    action='store_true',
    #    dest='no_filter',
    #    help='skip population filtering'
    #)

    #common.add_argument(
    #    '--skip-ld',
    #    action='store_true',
    #    dest='no_ld',
    #    help='skip LD calculations'
    #)


    return common


def _validate_arguments(parser: ArgumentParser) -> None:
    """
    Ensures arguemnts passed to the CLI script are valid. Informs the user of any missing
    or invalid arguments.

    :param args:
    :return:
    """

    args = parser.parse_args()

    print(args.subcommand)
    if not args.subcommand:
        print('')
        print('ERROR: You need to provide a subcommand')
        parser.print_help()
        exit(1)

    if not args.populations:
        print('')
        print('ERROR: You need to provide a population list')
        parser.print_help()
        exit(1)

    if not args.rsids:
        print('')
        print('ERROR: You need to provide a file containing a list of refSNPs')
        parser.print_help()
        exit(1)

    exit()
    return args


def setup_arguments(exe):
    """

    :param exe:
    :return:
    """

    parser = _register_args()

    return _validate_arguments(parser)

