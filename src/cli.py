#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: cli.py
## desc: CLI functions and most of the application logic.

from dask.distributed import get_client
from dask.distributed import Client
from pathlib import Path
from time import sleep
from typing import Dict
from typing import List
import click
import logging
import pandas as pd
import re
import tempfile as tf

from . import arguments
from . import calculate_ld as ld
from . import cluster
from . import filter_populations as filtpop
from . import globe
from . import log
from . import retrieve

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _parse_rsid_list(input: str) -> pd.Series:
    """
    Parses the input refSNP list and attempt to find the refSNP field.

    arguments
        input: input filepath

    returns
        the list of refSNPs
    """

    df = pd.read_csv(input, sep='\t')

    ## If it's a single field file, assume no header, reread all the IDs
    if len(df.columns) == 1:
        return pd.read_csv(input, sep='\t', header=None).iloc[:, 0]

    ## First lowercase the columns for string matching
    df.columns = [c.lower() for c in df.columns]

    ## Attempt to find the refSNP column
    snp_col = [c for c in df.columns if re.match(r'(snpid|rsid|snp)', c)]

    if not snp_col:
        raise ValueError(
            'Could not find the refSNP ID column in the SNP file you provided'
        )

    snp_col = snp_col[0]

    return df.loc[:, snp_col]


def _make_rsid_file(snps: pd.Series, base: str = globe._dir_work) -> str:
    """

    :param snps:
    :param base:
    :return:
    """

    output = tf.NamedTemporaryFile(dir=base, delete=True)

    snps.to_csv(output.name, header=False, index=False, sep='\t')

    return output


def _separate_population_list(s: str) -> List[str]:
    """
    Splits the population string into a list of populations.
    Converts to uppercase since all population labels in the 1KGP pop. list are upper'd.

    arguments
        s: string

    returns
        a list of populations
    """

    return [ss.strip().upper() for ss in s.split(',')]


def _make_population_path(
    populations: List[str],
    base: str = globe._dir_1k_processed
) -> Path:
    """

    :param populations:
    :param base:
    :return:
    """

    return Path(base, '-'.join(populations).lower())


def _wait_for_workers(n, min_n=0, limit=30, sleepy=5):

    client = get_client()
    waits = 0
    num_started = 0

    while True:
        if limit > 0 and waits > limit:
            log._logger.warning(
                'Waiting time limit has been reached, starting without additional workers'
            )
            break

        workers =  client.scheduler_info()["workers"].keys()

        sleep(sleepy)

        if len(workers) > 0 and len(workers) != num_started:
            num_started = len(workers)

            log._logger.info(f'Started {len(workers)} workers...')

        if len(workers) >= n:
            log._logger.info(f'All {len(workers)} workers have started...')
            break

        if min_n > 0 and len(workers) >= min_n:
            break

        waits += 1


def main():
    """

    :return:
    """

    args = arguments.setup_arguments('linkd')

    log._initialize_logging(verbose=args.verbose)

    ## Start the cluster
    if args.local:
        client = cluster.initialize_cluster(
            hpc=False, workers=args.processes, verbose=args.verbose
        )
    else:
        ## The first part doesn't need multiple cores, we'll re-init the cluster with
        ## the correct amount later on
        client = cluster.initialize_cluster(
            hpc=True,
            #cores=8,
            #procs=2,
            #jobs=30,
            #jobs=4,
            walltime='72:00:00',
            cores=10,
            procs=10,
            jobs=100,
            tmp='/var/tmp',
            verbose=args.verbose
        )

    if not args.no_download:
        log._logger.info('Downloading datasets and tools...')

        calls = retrieve.retrieve_variants(client, force=False)
        plink = retrieve.retrieve_plink(client, force=False)
        merge = retrieve.retrieve_dbsnp_merge_table(client, force=False)

        client.gather(calls + [plink, merge])

    else:
        log._logger.info('Skipping dataset retrieval...')

    populations = _separate_population_list(args.populations)
    pop_path = _make_population_path(populations)
    snps = _parse_rsid_list(args.rsids)
    snps_file = _make_rsid_file(snps)

    if not args.no_filter:
        log._logger.info('Filtering 1KGP variant calls using the following populations:')
        log._logger.info(', '.join(populations))
        _wait_for_workers(100, min_n=60, limit=0)

        filtered = filtpop.filter_populations(populations, super_pop=args.super_population)

        client.gather(filtered)

    else:
        log._logger.info('Skipping population filtering...')

    if not pop_path.exists():
        log._logger.error('Filtered population datasets are missing.')
        exit(1)

    if not args.no_ld:
        log._logger.info('Waiting for workers to start...')

        _wait_for_workers(40)

        log._logger.info('Calculating LD...')

        ld_scores = ld.calculate_ld2(
            pop_path.as_posix(),
            snps_file.name,
            r2=args.rsquared,
            threads=(args.cores // args.processes)
        )

        ld_scores = client.gather(ld_scores)

        client.close()

        log._logger.info('Formatting output...')

        ld.format_merge_files(ld_scores, args.output)

    else:
        log._logger.info('Skipping LD calculations...')

    log._logger.info('Done...')


def _initialize_cluster(args: Dict) -> Client:
    """
    Initialize the distributed cluster.

    arguments
        args: CLI args and options

    returns
        the dask Client object connected to the cluster
    """

    if args['local']:
        log._logger.info('Initializing local cluster...')
    else:
        log._logger.info('Initializing cluster for an HPC environment...')

    client = cluster.initialize_cluster(
        hpc=(not args['local']), **args
    )

    return client


def run_retrieve(args: Dict) -> None:
    """
    Run the retrieve command.

    arguments
        args: CLI args and options
    """

    client = _initialize_cluster(args)

    log._logger.info('Retrieving datasets...')

    futures = retrieve.retrieve_all_datasets(client, args['force'])

    client.gather(futures)


@click.group()
@click.option(
    '-l',
    '--local',
    is_flag=True,
    default=False,
    help='run a local cluster instead of using an HPC/PBS cluster'
)
@click.option(
    '-c',
    '--cores',
    default=5,
    type=int,
    help='number of cores to use per job'
)
@click.option(
    '-p',
    '--procs',
    default=5,
    type=int,
    help='number of processes to use per job'
)
@click.option(
    '-j',
    '--jobs',
    default=50,
    type=int,
    help='number of jobs to submit to an HPC system'
)
@click.option(
    '-t',
    '--temp',
    default=tf.gettempdir(),
    help='temp directory'
)
@click.option(
    '-w',
    '--walltime',
    default='72:00:00',
    help='walltime limit if using an HPC system'
)
@click.option(
    '--verbose',
    default=False,
    is_flag=True,
    help='clutter your screen with output'
)
@click.pass_context
def cli(ctx, local, cores, procs, jobs, walltime, temp, verbose):

    ctx.ensure_object(dict)

    ctx.obj['local'] = local
    ctx.obj['cores'] = cores
    ctx.obj['procs'] = procs
    ctx.obj['jobs'] = jobs
    ctx.obj['temp'] = temp
    ctx.obj['walltime'] = walltime
    ctx.obj['verbose'] = verbose

    log._initialize_logging(verbose=verbose)
    #try:
    #    Path(temp).mkdir(parents=True, exist_ok=True)
    #except Exception as e:
    #    print('ERROR: Could not make temp directory')
    #    print(e)
    #    exit(1)


@cli.command('retrieve')
@click.argument('output', required=False)
@click.option(
    '-f',
    '--force',
    default=False,
    is_flag=True,
    help='retrieve datasets even if local copies already exist'
)
@click.pass_context
def retrieve_cmd(ctx, output, force):
    """
    Retrieve and store variant calls from the 1K Genome Project.

    OUTPUT is the optional output filepath to store the variants.
    """

    ctx.ensure_object(dict)

    if not output:
        output = globe._dir_1k_variants

    try:
        Path(output).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log._logger.error('Could not make output directory')
        print(e)
        exit(1)

    ctx.obj['subcommand'] = 'retrieve'
    ctx.obj['output'] = output
    ctx.obj['force'] = force

    #print(ctx.obj)
    run_retrieve(ctx.obj)


@cli.command()
@click.argument('populations', required=True)
@click.argument('input', required=False)
@click.argument('output', required=False)
@click.pass_context
def filter(ctx, populations, input, output):
    """
    Filter 1KGP datasets based on population structure.

    \b
    POPULATIONS is a comma separated list of population identifiers to filter on.
    INPUT is a directory containing raw 1KGP variant calls.
    OUTPUT is the output filepath of processed and filtered variants.
    """

    ctx.ensure_object(dict)

    populations = _separate_population_list(populations)

    if not populations:
        log._logger.error('You must provide at least on population to filter on')
        exit(1)

    if not input:
        input = globe._dir_1k_variants

    if not output:
        output = globe._dir_1k_processed

    try:
        Path(output).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log._logger.error('Could not make output directory')
        print(e)
        exit(1)

    ctx.obj['subcommand'] = 'filter'
    ctx.obj['populations'] = populations
    ctx.obj['input'] = input
    ctx.obj['output'] = output


@cli.command()
@click.argument('snps', required=True)
@click.argument('input', required=True)
@click.argument('output', required=True)
@click.pass_context
def ld(ctx, snps, input, output):
    """
    Calculate linkage disequilibrium for a given population and SNP list.

    \b
    SNPS is a file containing a list of refSNP IDs to calculate LD for.
    INPUT is a directory containing processed 1KGP variant calls.
    OUTPUT is the output of the LD calculations.
    """

    ctx.ensure_object(dict)

    snps = _parse_rsid_list(snps)

    try:
        Path(output).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        log._logger.error('Could not make output directory')
        print(e)
        exit(1)

    ctx.obj['subcommand'] = 'ld'
    ctx.obj['snps'] = snps
    ctx.obj['input'] = input
    ctx.obj['output'] = output


if __name__ == '__main__':
    cli()
