#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: filter_populations.py
## desc: Functions for filtering variant calls based on population structure.

from dask.distributed import Client
from dask.distributed import get_client
from dask_jobqueue import PBSCluster

from dask.distributed import Future
from dask.distributed import LocalCluster
from functools import partial
from pathlib import Path
from typing import List
from zipfile import ZipFile
from glob import glob
import dask
import dask.dataframe as ddf
import logging
import numpy as np
import pandas as pd
import shutil
import tempfile as tf

from . import cluster
from . import dfio
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


def read_merge_table(fp: str = globe._fp_dbsnp_table) -> pd.DataFrame:
    """
    Parse the NCBI dbSNP merge table. A description of the table can be found here:
    https://www.ncbi.nlm.nih.gov/projects/SNP/snp_db_table_description.cgi?t=RsMergeArch

    arguments
        fp: filepath to the merge table

    returns
        a dataframe of the merge table
    """

    df = ddf.read_csv(
        fp,
        sep='\t',
        header=None,
        assume_missing=True,
        names=[
            'high',
            'low',
            'build',
            'orien',
            'created',
            'updated',
            'current',
            'o2c',
            'comment'
        ]
    )

    return df[['high', 'current']]


def merge_snps(merge: ddf.DataFrame, snps: ddf.DataFrame) -> pd.DataFrame:
    """
    Update SNP identifiers, where possible, for the given SNP dataset using
    the dbSNP merge table.

    arguments
        merge: dataframe containing the dbSNP merge table
        eqtls: dataframe containing processed SNPs

    returns
        a dataframe with updated SNP IDs
    """

    ## Strip out the rs prefix from the SNP identifier and convert the ID to an integer
    snps['ID'] = snps.ID.str.strip(to_strip='rs')
    snps['ID'] = snps.ID.astype(np.int64)

    ## Join the dataframes on the old SNP identifiers (i.e. 'high' in the merge table)
    ## and set the refSNP ID to the new version if one exists
    snps = snps.merge(merge, how='left', left_on='ID', right_on='high')
    snps['ID'] = snps.ID.mask(snps.current.notnull(), snps.current)
    snps['ID'] = 'rs' + snps.ID.astype(str)

    return snps.drop(labels=['high', 'current'], axis=1)


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

    #if not header:
    #    raise exceptions.VCFHeaderParsingFailed(
    #        f'Could not discern VCF header from {fp}'
    #    )

    return header


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


def _find_vcf_header(vcf_dir: str) -> List[str]:
    """
    Given a directory of VCF files (all of which should have the same header), this
    grabs a random file from the directory and attempts to extract the header from
    that file.
    """

    header = []

    for fp in glob(Path(vcf_dir, '*.vcf').as_posix()):
        header = extract_header(fp)

        if header:
            break

    if not header:
        raise exceptions.VCFHeaderParsingFailed(
            f'No usable header files could be extracted from {vcf_dir}'
        )

    return header


def _read_vcf(fp: str, header: List[str], is_dir: bool = True) -> ddf.DataFrame:
    """
    Read in a VCF file.

    arguments
        fp: filepath to the VCF file
        header: filepath to the VCF file

    returns
        a dask dataframe
    """

    if is_dir:
        fp = Path(fp, '*.vcf').as_posix()

    return ddf.read_csv(
        fp,
        blocksize='500MB',
        sep='\t',
        comment='#',
        header=None,
        names=header,
        dtype={'CHROM': 'object'}
    )


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
    samps = identify_excluded_samples(pops, popmap, super_pop=super_pop)
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

def identify_excluded_samples(
    pops: List[str],
    popmap: pd.DataFrame,
    super_pop=False
) -> List[str]:
    """
    Determine what variant call samples to exclude based on population structure.
    Using an input list of populations a user want to INCLUDE in an analysis, this
    returns a list of samples that should be EXCLUDED from further analysis.

    arguments
        pops:      a list of population IDs we want to retain
        popmap:    the sample -> population mapping dataframe
        super_pop: if True, the population list contains a list of super population IDs

    returns
        a list of samples to exclude (the inverse of the given pops argument)
    """

    ## Get sample IDs associated with the given population
    if super_pop:
        samples = popmap[popmap.super_pop.isin(pops)]['sample']
    else:
        samples = popmap[popmap['pop'].isin(pops)]['sample']

    ## Now get all other samples that should be excluded
    samples = popmap[~popmap['sample'].isin(samples)]['sample']

    return samples.tolist()


def filter_vcf_populations(
    df: ddf.DataFrame,
    popmap: pd.DataFrame,
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

    #popmap = read_population_map()
    samples = identify_excluded_samples(pops, popmap, super_pop=super_pop)

    ## There are duplicate refSNP IDs which fucks plink up. We repartition after the drop
    ## since data removal may unbalance the data spread
    #return (
    #    df.drop(labels=samples, axis=1)
    #        #.repartition(npartitions=500)
    #        .drop_duplicates(subset=['ID'])
    #        .repartition(npartitions=300)
    #)
    #log._logger.info('Dropping samples...')

    ## Remove samples from populations we're not interested in
    df = df.drop(labels=samples, axis=1)

    return df


def separate_joined_snps(df):
    """

    :param df:
    :return:
    """

    return df.map_partitions(
        lambda d: d.drop('ID', axis=1).join(
            d.ID.str.split(';', expand=True)
                .stack()
                .reset_index(drop=True, level=1)
                .rename('ID')
        )
    )


def filter_on_refsnps(df):
    """
    Removes anything lacking a refSNP ID.

    :param df:
    :return:
    """

    return df[df.ID.str.contains('rs\d+')]


def remove_duplicate_refsnps(df):
    """

    :param df:
    :return:
    """

    ## Dask's drop_duplicates isn't always the greatest (at least in my experience) for
    ## massive datasets, so we set the index to be the rsid (expensive) then
    ## deduplicate using map_partitions
    df = df.set_index('ID', drop=False)
    df = df.map_partitions(lambda d: d.drop_duplicates(subset='ID'))
    df = df.reset_index(drop=True)
    df = df.repartition(npartitions=200)

    return df


def _save_distributed_variants(df: pd.DataFrame, out_dir: str = globe._dir_1k_processed):
    """
    Designed to be called from a dask dataframe groupby apply operation. Saves the grouped
    variant dataframe to a file.

    :param df:
    :return:
    """

    output = Path(out_dir, f'chromosome-{df.name}.vcf').as_posix()

    df.to_csv(output, sep='\t', index=None, header=None)

    return output


def _filter_populations(
    vcf_dir,
    populations: List[str],
    popmap,
    merge,
    super_pop: bool = False,
    #vcf_dir: str = globe._dir_1k_variants,
    #map_path: str = globe._fp_population_map
) -> ddf.DataFrame:
    """

    :param populations:
    :param super_pop:
    :param vcf_dir:
    :return:
    """

    ## Read in the population mapping file
    #popmap = read_population_map(map_path)

    ## Determine the header fields for the given set of VCF files
    #header = _find_vcf_header(vcf_dir)
    header = extract_header(vcf_dir)

    ## Read in all ~800GB of variants
    #variants = _read_vcf(vcf_dir, header, is_dir=True)
    variants = _read_vcf(vcf_dir, header, is_dir=False)
    #variants = _read_vcf('data/1k-variants/chromosome-22.vcf', header, is_dir=False)
    #variants = _read_vcf('data/1k-variants/samples/chromosome-*.vcf', header, is_dir=False)

    ## Repartition cause the initial ones kinda suck
    #variants = variants.repartition(npartitions=500)
    #print(f'Partitions {Path(vcf_dir).name}: {variants.npartitions}')

    ## Filter based on population structure
    variants = filter_vcf_populations(variants, popmap, populations, super_pop=super_pop)

    ## Store the new header after samples have been removed
    header = variants.columns

    ## Some variant calls actually contain multiple SNPs in a single row, so we
    ## separate these out
    variants = separate_joined_snps(variants)

    ## Filter things that don't have a refSNP ID
    variants = filter_on_refsnps(variants)

    ## Update SNP IDs
    variants = merge_snps(merge, variants)

    ## Get rid of potential dups
    variants = remove_duplicate_refsnps(variants)

    ## Restore the original header ordering which is disrupted after our merge
    variants = variants[header]

    return variants


def _filter_populations2(
    vcf_dir,
    populations: List[str],
    popmap,
    super_pop: bool = False,
    #vcf_dir: str = globe._dir_1k_variants,
    #map_path: str = globe._fp_population_map
) -> ddf.DataFrame:
    """

    :param populations:
    :param super_pop:
    :param vcf_dir:
    :return:
    """

    ## Read in the population mapping file
    #popmap = read_population_map(map_path)

    ## Determine the header fields for the given set of VCF files
    header = _find_vcf_header(vcf_dir)

    ## Read in all ~800GB of variants
    #variants = _read_vcf(vcf_dir, header, is_dir=True)
    variants = _read_vcf(vcf_dir, header, is_dir=True)
    #variants = _read_vcf('data/1k-variants/chromosome-22.vcf', header, is_dir=False)
    #variants = _read_vcf('data/1k-variants/samples/chromosome-*.vcf', header, is_dir=False)

    ## Repartition cause the initial ones kinda suck
    #variants = variants.repartition(npartitions=500)
    print(f'Partitions {Path(vcf_dir).name}: {variants.npartitions}')

    ## Filter based on population structure
    variants = filter_vcf_populations(variants, popmap, populations, super_pop=super_pop)

    return variants


def filter_populations_d(
    populations: List[str],
    super_pop: bool = False,
    vcf_dir: str = globe._dir_1k_variants,
    out_dir: str = globe._dir_1k_processed,
    map_path: str = globe._fp_population_map
) -> ddf.DataFrame:
    """

    :param populations:
    :param super_pop:
    :param vcf_dir:
    :param map_path:
    :return:
    """

    ## Read in the population mapping file
    popmap = read_population_map(map_path)
    ## Generate an output path based on the populations used for filtering
    out_dir = Path(out_dir, '-'.join(populations).lower())

    ## Make the directory if it doesn't exist
    out_dir.mkdir(parents=True, exist_ok=True)

    futures = []

    #for vcf in Path(vcf_dir).iterdir():

    output = Path(out_dir, 'population.vcf')

    df = _filter_populations2(vcf_dir, populations, popmap, super_pop=super_pop)
    df = client.persist(df)
    #log._logger.info('Saving dataframe...')
    futures = dfio.save_distributed_dataframe_async(df, output.as_posix())

    #futures.append(future)

    return futures


def filter_populations(
    populations: List[str],
    super_pop: bool = False,
    #merge: bool = True,
    vcf_dir: str = globe._dir_1k_variants,
    out_dir: str = globe._dir_1k_processed,
    map_path: str = globe._fp_population_map,
    merge_path: str = globe._fp_dbsnp_table
) -> ddf.DataFrame:
    """

    :param populations:
    :param super_pop:
    :param vcf_dir:
    :param map_path:
    :return:
    """

    ## Read in the population mapping file
    popmap = read_population_map(map_path)
    ## Read in the merge table
    merge = read_merge_table(merge_path).persist()

    ## Generate an output path based on the populations used for filtering
    out_dir = Path(out_dir, '-'.join(populations).lower())

    ## Make the directory if it doesn't exist
    out_dir.mkdir(parents=True, exist_ok=True)

    futures = []
    client = get_client()

    chroms = ['chromosome-2.vcf', 'chromosome-8.vcf', 'chromosome-10.vcf', 'chromosome-15.vcf', 'chromosome-22.vcf', ]

    #for vcf in Path(vcf_dir).iterdir():
    for vcf in glob(Path(vcf_dir, '*.vcf').as_posix()):

        output = Path(out_dir, Path(vcf).name)

        df = _filter_populations(vcf, populations, popmap, merge, super_pop=super_pop)
        df = client.persist(df)
        #log._logger.info('Saving dataframe...')
        future = dfio.save_distributed_dataframe_async(df, output.as_posix())

        futures.append(future)

    return futures


def _save_chromosomal_variants(
    df: ddf.DataFrame,
    populations: List[str],
    out_dir: str = globe._dir_1k_processed
) -> None:
    """
    Partition the dataframe by chromosome and save variants to individual files.

    :param df:
    :return:
    """

    ## Generate an output path based on the populations used for filtering
    output = Path(out_dir, '-'.join(populations).lower())

    ## Make the directory if it doesn't exist
    output.mkdir(exist_ok=True)

    ## Provide meta so dask stops bitching
    df.groupby('CHROM').apply(
        _save_distributed_variants, output.as_posix(), meta=('CHROM', 'object')
    ).compute()


def _save_chromosomal_variants2(
    df: ddf.DataFrame,
    populations: List[str],
    out_dir: str = globe._dir_1k_processed
) -> None:
    """
    Partition the dataframe by chromosome and save variants to individual files.

    :param df:
    :return:
    """

    ## Generate an output path based on the populations used for filtering
    out_dir = Path(out_dir, '-'.join(populations).lower())

    ## Make the directory if it doesn't exist
    out_dir.mkdir(parents=True, exist_ok=True)

    futures = []

    log._logger.info('Saving variants')

    ## Faster to loop, filter by chromosome number, and save than using a groupby
    ## statement
    for chrom in (list(range(1, 23)) + ['X']):

        chrom = str(chrom)
        output = Path(out_dir, f'chromosome-{chrom}.vcf')
        future = dfio.save_distributed_dataframe_async(df[df.CHROM == chrom], output)

        futures.append(future)

        #log._logger.info(f'Saving to {output}')

    log._logger.info('Waiting for completion...')

    return futures


def filter_populations2(
    client: Client,
    pops: List[str],
    chromosomes: List[int] = range(0, 23),
    super_pop: bool = False,
    merge_table: ddf.DataFrame = ddf.from_pandas(pd.DataFrame([]), npartitions=1),
) -> List[Future]:
    """
    Unless you have a couple TB of RAM (e.g., Summit or Titan), this needs to be done
    in chunks.
    """

    ## List of delayed objects that will be written to files
    saved = []

    ## Generate the output directory based on filtered populations
    outdir = Path(globe._dir_1k_processed, '-'.join([p.lower() for p in pops]))

    ## Make sure it exists
    outdir.mkdir(exist_ok=True)

    for chrom in chromosomes:
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
            merge_df = filt_df

        #df = filter_vcf_populations(vcf_path, pops, super_pop=super_pop)
        #dfs = client.scatter(df)
        #df = filter_vcf_populations(vcf_path, pops, super_pop=super_pop)
        #future = save_variant_dataframe(df, out_path)
        future = client.submit(save_variant_dataframe2, merge_df, out_path, header=header)
        #future = client.submit(save_variant_dataframe2, dfs, out_path)

        saved.append(future)

    #x_out = Path(outdir, 'chromosome-x.vcf')
    #x_head, x_df = read_vcf(vcf_path)
    #x_filt_df = client.submit(
    #    filter_vcf_populations, x_df, pops, super_pop=super_pop
    #)
    #x_fut = client.submit(save_variant_dataframe2, x_filt_df, x_out, header=x_head)

    #saved.append(x_fut)

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

    client = cluster.initialize_cluster(
        hpc=True,
        #hpc=False,
        name='linkage-disequilibrium',
        jobs=90,
        cores=3,
        procs=3,
        memory='160GB',
        walltime='03:00:00',
        tmp='/var/tmp',
        log_dir='logs'
    )

    log._initialize_logging(verbose=True)

    ## List of populations to filter on
    #populations = ['ACB']
    populations = ['EUR']

    #variants = _filter_populations(populations)
    #variants = client.persist(variants)

    #futures = _save_chromosomal_variants2(variants, populations)
    futures = filter_populations(populations, super_pop=True)

    client.gather(futures)
    #dfio.save_distributed_dataframe_sync(variants, 'chr22-filtered-variants.tsv')
    ## dbSNP merge table
    #merge_table = merge.parse_merge_table()

    ##dels = filter_populations(client, pops=['ACB'])
    #dels = filter_populations2(
    #    client, pops=['ACB'], chromosomes=range(1, 10), merge_table=merge_table
    #)

    #client.gather(dels)

    #wait(dels)
    #client.compute(dels)
    #filter_populations_serial(client, pops=['ACB'])

    client.close()

