#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: cli.py
## desc: CLI functions and most of the application logic.

from dask.distributed import get_client
from pathlib import Path
from time import sleep
from typing import List
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


def _wait_for_workers(n, limit=30):

    client = get_client()
    waits = 0

    while True:
        if waits > limit:
            log._logger.warning(
                'Waiting time limit has been reached, starting without additional workers'
            )
            break

        workers =  client.scheduler_info()["workers"].keys()

        sleep(3)

        if len(workers) > 0:
            log._logger.info(f'Started {len(workers)} workers...')

        if len(workers) >= n:
            log._logger.info(f'All {len(workers)} workers have started...')
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
            cores=6,
            procs=6,
            jobs=60,
            #jobs=4,
            walltime='08:00:00',
            #cores=5,
            #procs=5,
            #jobs=100,
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

if __name__ == '__main__':
    main()
