#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: merge.py
## desc: Functions parsing the dbSNP merge table and updating refSNP identifiers.

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
import numpy as np
import pandas as pd
import shutil
import tempfile as tf

from . import exceptions
from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def parse_merge_table(fp: str = globe._fp_dbsnp_table) -> ddf.DataFrame:
    """
    Parse the NCBI dbSNP merge table. A description of the table can be found here:
    https://www.ncbi.nlm.nih.gov/projects/SNP/snp_db_table_description.cgi?t=RsMergeArch

    arguments
        fp: filepath to the merge table

    returns
        a dask dataframe of the merge table
    """

    mtable = ddf.read_csv(
        fp,
        assume_missing=True,
        sep='\t',
        header=None,
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

    ## We only need high/low SNP ID columns (high = old, low = merged)
    mtable = mtable[['high', 'low']]

    ## Force as ints
    mtable['high'] = mtable.high.astype(np.int64)
    mtable['low'] = mtable.low.astype(np.int64)

    return mtable


def merge_snp_identifiers(
    df: ddf.DataFrame,
    header,
    merge: ddf.DataFrame
) -> ddf.DataFrame:
    """
    Update old refSNP identifiers to their most recent version using the given NCBI
    merge table.

    arguments
        df:    dataframe with refSNP IDs
        merge: merge table

    returns
        a dask dataframe with updated IDs
    """

    #df[['uid', 'features']] = df.loc[:, 'features'].str.extract(
    #    r'(?P<uid>\d+)\|(?P<features>.*)', expand=False
    #)
    #df['ids'] = df.loc[:, 'ID'].str.split(';', expand=False)
    #df['ids'] = df['ID'].str.split(';').apply(pd.Series)
    ## The ID field for some rows actually have multiple IDs separated by ';'
    ## So we split these and assign them to individual rows.
    df = df.map_partitions(
        lambda d: d.drop('ID', axis=1).join(
            d.ID.str.split(';', expand=True)
                .stack()
                .reset_index(drop=True, level=1)
                .rename('ID')
        )
    )

    ## Split each feature into their own column
    #df = ddf.concat(
    #    #[df, df.ID.str.split(';').apply(pd.Series)],
    #    #[df, df.ID.str.split(';')],
    #    [df, df.ids],
    #    axis=1
    #)

    ## Drop the features column
    #df = df.drop(columns=[3, 'features'])

    ## Transform each feature column into their own row per UID
    #df = pd.melt(
    #    df, id_vars=['chrom', 'start', 'end', 'uid'], value_name='feature'
    #)
    #df = df.drop(['variable'], axis=1)
    #df = ddf.melt(
    #    #df, id_vars=['CHROM', 'POS', 'ID', 'REF', 'ALT'], value_name='feature'
    #    df, value_vars=['CHROM', 'POS', 'ID', 'REF', 'ALT'], value_name='feature'
    #)
    #df = df.drop(['variable'], axis=1)

    ## Have to reset the index since any items that were split have the same index
    df = df.reset_index(drop=True)

    ## Ensure each ID is a refSNP identifier
    df['ID'] = df.ID.str.extract(r'^(rs\d+)', expand=False)

    ## Remove anything without a valid rsid
    df = df.dropna(subset=['ID'])

    ## Remove the 'rs' prefix
    df['ID'] = df.ID.str.strip(to_strip='rs')

    ## Convert to int
    df['ID'] = df.ID.astype(np.int64)

    df = df.merge(merge, how='left', left_on='ID', right_on='high')

    ## Update the refSNP IDs to the latest build given by the merge table
    df['ID'] = df.ID.where(df.low.isnull(), df.low)

    ## Add the 'rs' prefix back to the refSNP ID
    df['ID'] = 'rs' + df.ID.astype(str)

    return df.drop(['low', 'high'], axis=1)

