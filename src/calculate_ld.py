#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: calculate_ld.py
## desc: Function wrappers for calculating LD using plink.

from collections import defaultdict as dd
from dask.distributed import Client
from dask.distributed import get_client
from glob import glob
from pathlib import Path
from typing import List
import dask
import dask.dataframe as ddf
import logging
import pandas as pd
import re
import shutil
import subprocess as sub
import tempfile as tf

from . import cluster
from . import exceptions
from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _calculate_ld(
    input: str,
    output: str,
    snps: str,
    r2: float = 0.7,
    threads: int = 3,
    plink: str = globe._exe_plink
) -> bool:
    """
    Calculate linkage disequilibrium (LD) for all combinations of SNPs in the given VCF
    file.

    arguments
        vcf: filepath to vcf file
        r2:  LD r2 value to use for thresholding
    """

    ## Run plink
    try:
        sub.run([
            plink,
            '--threads',
            str(threads),
            '--vcf',
            input,
            '--ld-snp-list',
            snps,
            '--ld-window-r2',
            str(r2),
            '--r2',
            'inter-chr',
            '--out',
            output
        ], stderr=sub.DEVNULL)

    except Exception as e:
        log._logger.error('There was a problem running plink as a subprocess: %s', e)
        raise

    ## Plink auto appends an '.ld' suffix to the output which we add here
    return Path(output).with_suffix(f'{Path(output).suffix}.ld')


def calculate_ld(
    vcf_dir: str,
    snps: str,
    out_dir: str = globe._dir_work,
    r2: float = 0.7,
    threads: int = 3
):
    """

    :param vcf_dir:
    :param out_dir:
    :return:
    """

    client = get_client()
    futures = []

    ## Get the list of distributed workers
    workers = [w for w in client.scheduler_info()['workers'].keys()]

    ## Iterate through all the VCF files
    for fp in Path(vcf_dir).iterdir():

        ## Construct the output path
        output = Path(out_dir, '-'.join([fp.stem, Path(snps).stem]))

        ## Get a worker
        #worker = workers.pop()
        ### Send him to the back of the worker queue
        #workers.append(worker)

        ## Calculate LD on the worker
        future = client.submit(
            _calculate_ld,
            fp.as_posix(),
            output.as_posix(),
            snps,
            r2=r2,
            threads=threads,
            #workers=[worker]
        )

        futures.append(future)

    return futures


def format_merge_files(files: List[str], output: str, delete: bool = True) -> str:
    """
    """

    if not files:
        return output

    first = True

    ## Open the single concatenated output file
    with open(output, 'w') as outfl:

        ## Loop through input files...
        for fpath in sorted(files):

            ## Read each input file and format line x line
            with open(fpath, 'r') as infl:

                if not first:
                    ## Skip the header
                    next(infl)

                else:
                    first = False

                for ln in infl:
                    ln = re.sub(r'^ +', '', ln)
                    ln = re.sub(r' +', '\t', ln)

                    outfl.write(ln)

            ## Remove the file once we're done
            if delete:
                Path(fpath).unlink()

    return output


def format_merge_separate_files(out_dir: str = globe._dir_1k_ld):
    """
    """

    client = get_client()
    groups = dd(list)
    futures = []

    ## Glob all the separate linkage disequilibrium files in the output directory
    for fl in glob(Path(out_dir, '*.ld.*').as_posix()):

        ## Group all the same chromosome files together
        groups[Path(fl).stem].append(fl)

    for files in groups.values():
        future = client.submit(_format_merge_files, files)

        futures.append(future)

    return futures


if __name__ == '__main__':

    #client = Client(LocalCluster(
    #    n_workers=2,
    #    processes=True
    #))

    log._initialize_logging(verbose=True)

    ## Each job utilizes 2 python processes (1 core each, 25GB limit of RAM each)
    client = cluster.initialize_cluster(
        hpc=True,
        #hpc=False,
        name='linkage-disequilibrium',
        jobs=90,
        cores=9,
        procs=3,
        #workers=1,
        memory='120GB',
        walltime='48:00:00',
        tmp='/var/tmp',
        log_dir='logs'
    )

    #print([w for w in client.scheduler_info()['workers'].keys()] * 2)
    outdir = Path(globe._dir_1k_ld, 'eur')

    outdir.mkdir(exist_ok=True, parents=True)

    futures = calculate_ld(
        Path(globe._dir_1k_processed, 'eur').as_posix(),
        outdir.as_posix()
        #globe._dir_1k_ld
    )

    #format_merge_separate_files()

    log._logger.info('Waiting...')

    client.gather(futures)

    saved = format_merge_separate_files()

    client.gather(saved)

    client.close()

