#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: filter_populations.py
## desc: Functions for filtering variant calls based on population structure.

from dask.distributed import Client
from dask.distributed import as_completed
from dask_jobqueue import PBSCluster
from dask.distributed import Future
from dask.distributed import LocalCluster
from functools import partial
from pathlib import Path
from typing import List
from zipfile import ZipFile
import dask
import dask.dataframe as ddf
import gzip
import logging
import pandas as pd
import shutil
import tempfile as tf

from . import exceptions
from . import globe
from . import log
from . import merge

logging.getLogger(__name__).addHandler(logging.NullHandler())


def read_population_map(fp: str = globe._fp_population_map) -> pd.DataFrame:
    """
    Parse the 1KGP sample -> population map. The file associates individual sample IDs
    with their population and superpopulation.

    arguments
        fp: filepath to the mapping

    returns
        a dataframe containing sample IDs and populations
    """

    return pd.read_csv(fp, sep='\t')[['sample', 'pop', 'super_pop']]


def show_population_groups(df: pd.DataFrame) -> None:
    """
    Display population and superpopulation groups.

    arguments
        df: dataframe containing sample -> population map
    """

    table = df[['pop', 'super_pop']].drop_duplicates(subset='pop')
    table = table.sort_values(by=['super_pop', 'pop']).set_index('super_pop')

    print(table)


def extract_header(fp: str) -> List[str]:
    """
    Find and attempt to extract the header from the given VCF file.

    arguments
        fp: filepath to the VCF file

    returns
        a list of header fields
    """

    header = []

    with open(fp, 'r') as fl:
        for ln in fl:
            if not ln:
                continue

            ## Field rows should be prefix by '##'
            if ln[:2] == '##':
                continue

            ## The header row should be prefix by a single '#'
            if ln[0] == '#':
                header = ln[1:].strip().split('\t')

            break

    if not header:
        raise exceptions.VCFHeaderParsingFailed(
            f'Could not discern VCF header from {fp}'
        )

    return header


def retain_samples(pops: List[str], popmap: pd.DataFrame, super_pop=False) -> List[str]:
    """
    Determine what variant call samples to keep based on population structure.
    Returns a list of samples that should be EXCLUDED from further analysis.

    arguments
        pops:      a list of population IDs
        popmap:    the sample -> population mapping dataframe
        super_pop: if True, the population list contains a list of super population IDs

    returns
        a list of samples to retain
    """

    ## Get sample IDs associated with the given population
    if super_pop:
        samples = popmap[popmap.super_pop.isin(pops)]['sample']
    else:
        samples = popmap[popmap['pop'].isin(pops)]['sample']

    ## Now get all other samples that should be excluded
    samples = popmap[~popmap['sample'].isin(samples)]['sample']

    return samples.tolist()


def read_vcf(fp: str) -> ddf.DataFrame:
    """
    Read in a VCF file.

    arguments
        fp: filepath to the VCF file

    returns
        a dask dataframe
    """

    header = extract_header(fp)

    return (
        header,
        ddf.read_csv(fp, sep='\t', comment='#', header=None, names=header)
    )


def filter_vcf_populations(
    df: ddf.DataFrame,
    pops: List[str],
    super_pop: bool = False
) -> ddf.DataFrame:
    """
    Filter variant call samples based on population structure.

    arguments
        df:
        pops:

    returns
    """

    #df = read_vcf(fp)
    popmap = read_population_map()
    samps = retain_samples(pops, popmap, super_pop=super_pop)

    return df.drop(labels=samps, axis=1)


def filter_vcf_populations_serial(
    fp: str,
    pops: List[str],
    super_pop: bool = False
) -> pd.DataFrame:
    """
    Filter variant call samples based on population structure.

    arguments
        df:
        pops:

    returns
    """

    popmap = read_population_map()
    samps = retain_samples(pops, popmap, super_pop=super_pop)
    header = extract_header(fp)

    for vcf in pd.read_csv(
        fp,
        sep='\t',
        header=None,
        names=header,
        comment='#',
        chunksize=14096
    ):

        ## Filter out samples not in our selected populations
        #vcf = vcf.filter(items=samps)
        vcf.drop(labels=samps, axis=1)

        ## Save as output
        vcf.to_csv('data/shit.vcf', sep='\t', header=False, index=False, mode='a')


def write_dataframe(df: pd.DataFrame, fp: str, first: bool = False, **kwargs) -> None:
    """

    arguments
        df:
        fp:
        first:

    returns
    """

    mode = 'w' if first else 'a'

    df.to_csv(fp, sep='\t', index=False, header=first, mode=mode)


def save_variant_dataframe(df: ddf.DataFrame, fp: str) -> None:
    """

    arguments
        df:
    """

    dels = df.to_delayed()

    log._logger.info(f'Saving variants to {fp}')

    last_delayed = dask.delayed(write_dataframe)(dels[0], fp, first=True)

    for d in dels[1:]:
        last_delayed = dask.delayed(write_dataframe)(
            d, fp, first=False, depends=last_delayed
        )

    return last_delayed


def save_variant_dataframe2(
    df: ddf.DataFrame,
    out_fp: str,
    header: List[str] = None
) -> None:
    """

    arguments
        df:
    """

    log._logger.info(f'Saving variants to {out_fp}')
    log._logger.info(df.head())

    ## Write all dataframe partitions to a temp dir
    #with tf.TemporaryDirectory(dir=globe._dir_1k_processed) as tmpdir:
    tmpdir = tf.mkdtemp(dir=globe._dir_1k_processed)

    if header:
        df.to_csv(tmpdir, sep='\t', index=False, columns=header)
    else:
        df.to_csv(tmpdir, sep='\t', index=False)

    log._logger.info(f'Finished writing the variants to {tmpdir}')

    first = True

    ## Concat all frames into a single file
    with open(out_fp, 'w') as ofl:

        for tmpfp in Path(tmpdir).iterdir():

            with open(tmpfp, 'r') as tmpfl:

                ## If it's the first file we include the header
                if first:
                    ofl.write(tmpfl.read())

                ## Otherwise skip the header so it isn't repeated
                else:
                    next(tmpfl)

                    ofl.write(tmpfl.read())

                first = False

    ## Remove the temp directory we made and it's contents
    shutil.rmtree(tmpdir)

    return True


"""
def filter_populations(client: Client, pops: List[str], super_pop: bool = False):

    ## List of delayed objects that will be written to files
    saved = []

    for chrom in range(1, 7):
        vcf_path = globe._fp_1k_variant_autosome % chrom
        out_path = globe._fp_1k_processed_autosome % chrom

        df = filter_vcf_populations(vcf_path, pops, super_pop=super_pop)
        df_save = save_variant_dataframe(df, out_path)

        saved.append(df_save)

    #x_df = filter_vcf_populations(globe._fp_1k_variant_x, pops, super_pop=super_pop)
    #x_df_save = save_variant_dataframe(x_df, globe._fp_1k_processed_x)

    #y_df = filter_vcf_populations(globe._fp_1k_variant_y, pops, super_pop=super_pop)
    #y_df = save_variant_dataframe(y_df, globe._fp_1k_processed_y)

    #saved.append(x_df_save)
    #saved.append(y_df)

    ## Wait for computations, i.e., file writing as delayed objects, to finish
    #client.compute(saved)
    return saved
"""

def filter_populations2(
    client: Client,
    pops: List[str],
    super_pop: bool = False,
    merge_table: ddf.DataFrame = ddf.from_pandas(pd.DataFrame([]), npartitions=1),
) -> List[Future]:
    """
    """

    ## List of delayed objects that will be written to files
    saved = []

    ## Generate the output directory based on filtered populations
    outdir = Path(globe._dir_1k_processed, '-'.join([p.lower() for p in pops]))

    ## Make sure it exists
    outdir.mkdir(exist_ok=True)

    for chrom in range(1, 23):
    #for chrom in range(1, 2):
        vcf_path = globe._fp_1k_variant_autosome % chrom
        out_path = Path(outdir, f'chromosome-{chrom}.vcf')
        #vcf_path = Path(globe._dir_1k_variants, 'sample2.vcf').as_posix()

        header, df = read_vcf(vcf_path)
        #df = client.submit(filter_vcf_populations, vcf_path, pops, super_pop=super_pop)
        filt_df = client.submit(filter_vcf_populations, df, pops, super_pop=super_pop)

        ## Update SNP identifiers if a merge table exists
        if len(merge_table.index) != 0:
            merge_df = client.submit(merge.merge_snp_identifiers, filt_df, header, merge_table)
        else:
            merge_df = df

        #df = filter_vcf_populations(vcf_path, pops, super_pop=super_pop)
        #dfs = client.scatter(df)
        #df = filter_vcf_populations(vcf_path, pops, super_pop=super_pop)
        #future = save_variant_dataframe(df, out_path)
        future = client.submit(save_variant_dataframe2, merge_df, out_path, header=header)
        #future = client.submit(save_variant_dataframe2, dfs, out_path)

        saved.append(future)

    x_out = Path(outdir, 'chromosome-x.vcf')
    x_head, x_df = read_vcf(vcf_path)
    x_filt_df = client.submit(
        filter_vcf_populations, x_df, pops, super_pop=super_pop
    )
    x_fut = client.submit(save_variant_dataframe2, x_filt_df, x_out, header=x_head)

    saved.append(x_fut)

    #y_df = filter_vcf_populations(globe._fp_1k_variant_y, pops, super_pop=super_pop)
    #y_df = save_variant_dataframe(y_df, globe._fp_1k_processed_y)

    #saved.append(x_df)
    #saved.append(y_df)

    ## Wait for computations, i.e., file writing as delayed objects, to finish
    #client.compute(saved)
    return saved


def filter_populations_serial(client: Client, pops: List[str], super_pop: bool = False):
    """
    """

    #for chrom in range(1, 23):
    for chrom in range(1, 7):
        vcf_path = globe._fp_1k_variant_autosome % chrom

        #df = filter_vcf_populations(vcf_path, pops, super_pop=super_pop)
        filter_vcf_populations_serial(vcf_path, pops, super_pop=super_pop)

        #Path('data/shit').mkdir(exist_ok=True)
        #df.to_csv('data/shit', sep='\t', index=False)
        break


if __name__ == '__main__':

    #client = Client(LocalCluster(
    #    n_workers=2,
    #    processes=True
    #))

    ## Each job utilizes 2 python processes (1 core each, 35GB limit of RAM each)
    cluster = PBSCluster(
        name='linkage-disequilibrium',
        queue='batch',
        interface='ib0',
        cores=2,
        processes=2,
        memory='70GB',
        walltime='02:00:00',
        job_extra=['-e logs', '-o logs'],
        env_extra=['cd $PBS_O_WORKDIR']
    )

    cluster.adapt(minimum=10, maximum=150)
    """
    cluster = LocalCluster(n_workers=1, processes=True)
    """

    client = Client(cluster)

    #log._initialize_logging(verbose=True)
    init_logging_partial = partial(log._initialize_logging, verbose=True)

    init_logging_partial()

    ## Newly added workers should initialize logging
    client.register_worker_callbacks(setup=init_logging_partial)

    ## List of populations to filter on
    populations = ['ACB']

    ## dbSNP merge table
    merge_table = merge.parse_merge_table()

    #dels = filter_populations(client, pops=['ACB'])
    dels = filter_populations2(client, pops=['ACB'], merge_table=merge_table)

    client.gather(dels)

    #wait(dels)
    #client.compute(dels)
    #filter_populations_serial(client, pops=['ACB'])

    client.close()

