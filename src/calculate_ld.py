#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: calculate_ld.py
## desc: Function wrappers for calculating LD using plink.

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


def read_vcf(fp: str):
    """
    Read in a VCF file.
    """

    header = extract_header(fp)

    return ddf.read_csv(fp, sep='\t', comment='#', header=None, names=header)


def filter_vcf_populations(
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

    df = read_vcf(fp)
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


def save_variant_dataframe2(df: ddf.DataFrame, out_fp: str) -> None:
    """

    arguments
        df:
    """

    log._logger.info(f'Saving variants to {out_fp}')
    log._logger.info(df.head())

    ## Write all dataframe partitions to a temp dir
    #with tf.TemporaryDirectory(dir=globe._dir_1k_processed) as tmpdir:
    tmpdir = tf.mkdtemp(dir=globe._dir_1k_processed)

    df.to_csv(tmpdir, sep='\t', index=False)

    log._logger.info(f'Finished writing the variants to {tmpdir}')

    first = True

    ## Concat all frames into a single file
    with open(out_fp, 'w') as ofl:

        for tmpfp in Path(tmpdir).iterdir():

            log._logger.info(f'Reading/writing {tmpfp}')
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

def filter_populations2(client: Client, pops: List[str], super_pop: bool = False):
    """
    """

    ## List of delayed objects that will be written to files
    saved = []

    #for chrom in range(21, 22):
    for chrom in range(1, 10):
        vcf_path = globe._fp_1k_variant_autosome % chrom
        out_path = globe._fp_1k_processed_autosome % chrom

        df = client.submit(filter_vcf_populations, vcf_path, pops, super_pop=super_pop)
        #df = filter_vcf_populations(vcf_path, pops, super_pop=super_pop)
        #dfs = client.scatter(df)
        #df = filter_vcf_populations(vcf_path, pops, super_pop=super_pop)
        #future = save_variant_dataframe(df, out_path)
        future = client.submit(save_variant_dataframe2, df, out_path)
        #future = client.submit(save_variant_dataframe2, dfs, out_path)

        saved.append(future)

    #x_df = filter_vcf_populations(globe._fp_1k_variant_x, pops, super_pop=super_pop)
    #x_df = save_variant_dataframe(x_df, globe._fp_1k_processed_x)

    #y_df = filter_vcf_populations(globe._fp_1k_variant_y, pops, super_pop=super_pop)
    #y_df = save_variant_dataframe(y_df, globe._fp_1k_processed_y)

    #saved.append(x_df)
    #saved.append(y_df)

    ## Wait for computations, i.e., file writing as delayed objects, to finish
    #client.compute(saved)
    return saved


def calculate_ld(vcf: str, r2: float = 0.7) -> str:
    """
    Calculate linkage disequilibrium (LD) for all combinations of SNPs in the given VCF
    file.

    arguments
        vcf: filepath to vcf file
        r2:  LD r2 value to use for thresholding
    """

    ## liftOver is really fucking picky about input and will refuse to do anything if
    ## something is out of place (e.g. a header exists or there's an extra column), so
    ## start by adding a row UID (index) to the frame
    df = df.reset_index(drop=False)

    ## We have to generate two separate files for lifting coordinates, one for the
    ## right side coords and another for the left side
    left_df = df[['chrom_left', 'start_left', 'end_left', 'index']]
    right_df = df[['chrom_right', 'start_right', 'end_right', 'index']]

    ## Temporary files for the liftOver input
    left_in = tf.NamedTemporaryFile(delete=True).name
    right_in = tf.NamedTemporaryFile(delete=True).name

    ## Files for any unmapped coordinates
    left_unmap = '%s.left.unmapped' % os.path.splitext(fout)[0]
    right_unmap = '%s.right.unmapped' % os.path.splitext(fout)[0]

    ## liftOver output
    left_out = tf.NamedTemporaryFile(delete=True).name
    right_out = tf.NamedTemporaryFile(delete=True).name

    ## Save the temp input
    left_df.to_csv(
        left_in,
        sep='\t',
        header=False,
        index=False,
        columns=['chrom_left', 'start_left', 'end_left', 'index']
    )

    right_df.to_csv(
        right_in,
        sep='\t',
        header=False,
        index=False,
        columns=['chrom_right', 'start_right', 'end_right', 'index']
    )

    ## Run liftover, usage: liftOver oldFile map.chain newFile unMapped
    try:
        sub.run([liftover, left_in, chain, left_out, left_unmap], stderr=sub.DEVNULL)
        sub.run([liftover, right_in, chain, right_out, right_unmap], stderr=sub.DEVNULL)

    except Exception as e:
        log._logger.error('There was a problem running liftOver as a subprocess: %s', e)

    ## Now we need to read in the lifted coordinates
    left_df = pd.read_csv(
        left_out,
        sep='\t',
        header=None,
        names=['chrom_left', 'start_left', 'end_left', 'index']
    )

    right_df = pd.read_csv(
        right_out,
        sep='\t',
        header=None,
        names=['chrom_right', 'start_right', 'end_right', 'index']
    )

    ## Now we need to rejoin the halves
    joined = left_df.join(right_df.set_index('index'), on='index', how='inner')

    ## Add back the cell type, group, and target
    joined = joined.join(
        df[['cell_type', 'cell_group', 'assay_target', 'index']].set_index('index'),
        on='index',
        how='left'
    )

    ## Save the output
    joined.drop(columns=['index']).to_csv(
        fout,
        sep='\t',
        index=False,
        columns=[
            'chrom_left',
            'start_left',
            'end_left',
            'chrom_right',
            'start_right',
            'end_right',
            'cell_type',
            'cell_group',
            'assay_target'
        ]
    )

    ## Delete temp files
    try:
        for tfl in [left_in, right_in, left_out, right_out]:
            os.remove(tfl)

    except Exception as e:
        log._logger.error('There was a problem deleting the temp files: %s', e)

    return fout


if __name__ == '__main__':

    #client = Client(LocalCluster(
    #    n_workers=2,
    #    processes=True
    #))

    ## Each job utilizes 2 python processes (1 core each, 25GB limit of RAM each)
    cluster = PBSCluster(
        name='linkage-disequilibrium',
        queue='batch',
        interface='ib0',
        cores=2,
        processes=2,
        memory='80GB',
        walltime='02:00:00',
        job_extra=['-e logs', '-o logs'],
        env_extra=['cd $PBS_O_WORKDIR']
    )

    cluster.adapt(minimum=10, maximum=50)

    client = Client(cluster)

    #log._initialize_logging(verbose=True)
    init_logging_partial = partial(log._initialize_logging, verbose=True)

    init_logging_partial()

    ## Init logging on each worker
    #client.run(log._initialize_logging, verbose=True)

    ## Newly added workers should initialize logging
    client.register_worker_callbacks(setup=init_logging_partial)

    #dels = filter_populations(client, pops=['ACB'])
    dels = filter_populations2(client, pops=['ACB'])

    #client.gather(dels)
    for fut in as_completed(dels):
        del fut

    #wait(dels)
    #client.compute(dels)
    #filter_populations_serial(client, pops=['ACB'])

    client.close()

