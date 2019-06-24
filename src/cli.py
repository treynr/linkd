#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: cli.py
## desc: CLI functions and most of the application logic.

import argparse
import logging
import pandas as pd
from pathlib import Path
from typing import List

from . import arguments
from . import cluster
from . import filter_populations as filtpop
from . import globe
from . import log
from . import retrieve

logging.getLogger(__name__).addHandler(logging.NullHandler())


def manipulate_variants(args: argparse.Namespace):
    """

    :param args:
    :return:
    """

    if args.directory:
        variant_files = Path(args.input).iterdir()
    else:
        variant_files = [Path(args.input).resolve().as_posix()]

    for fl in variant_files:

        if not Path(fl).exists():
            log._logger.warning(f'Variant filepath ({fl}) does not exist, skipping...')
            continue

        log._logger.info(f'Loading variant metadata from {fl}...')

        loaded = db.load_variants2(fl, args.build)

        log._logger.info(f'Loaded {loaded} nodes...')


def handle_subcommand(args: argparse.Namespace):
    """

    :return:
    """

    if args.subcommand == globe._subcmd_variation:
        manipulate_variants(args)


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

    return df.iloc[:, snp_col]


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
        ## The first part doesn't need multiple cores, we'll reinit the cluster with
        ## the correct amount later on
        client = cluster.initialize_cluster(
            hpc=True,
            cores=3,
            procs=3,
            jobs=args.jobs,
            verbose=args.verbose
        )

    if not args.no_download:
        log._logger.info('Downloading datasets and tools...')

        calls = retrieve.retrieve_variants(client, force=False)
        plink = retrieve.retrieve_plink(client, force=False)
        merge = retrieve.retrieve_dbsnp_merge_table(client, force=False)

        client.gather(calls + [plink, merge])

    populations = _separate_population_list(args.populations)
    snps = _parse_rsid_list(args.rsids)

    log._logger.info('Filtering 1KGP variant calls using the following populations:')
    log._logger.info(', '.join(populations))

    filtered = filtpop.filter_populations(populations, super_pop=args.super_population)

    client.gather(filtered)

    client.close()


if __name__ == '__main__':
    main()
