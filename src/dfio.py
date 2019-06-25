#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: dfio.py
## desc: DataFrame IO functions used by more than one module.

from dask.base import tokenize
from dask.distributed import Client
from dask.distributed import as_completed
from dask.distributed import get_client
from pathlib import Path
import dask.dataframe as ddf
import logging
import pandas as pd
import random
import shutil
import tempfile as tf

from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _write_dataframe_partition(
    df: pd.DataFrame,
    output: str = None,
    header: bool = True
) -> str:
    """
    Write dask dataframe partitions or a regular pandas dataframe to an output
    folder (dask) or file (pandas).

    arguments
        df:     a dask dataframe
        output: output directory or file to write the dataframe to
        header: bool indicating if the header should be saved

    returns
        the output directory/file path
    """

    ## If no output is provided, assume we're writing a series of dask dataframe
    ## partitions to a folder
    if not output:
        output = tf.mkdtemp(dir=globe._dir_data)

    df.to_csv(output, sep='\t', index=False, header=header, na_rep='NA')

    return output


def _write_dataframe_partitions(df: ddf.DataFrame) -> str:
    """
    Convert the partitions from a distributed dataframe to a list of futures, then use
    those futures to write the partition contents to files asynchronously.

    arguments
        df:     a dask dataframe

    returns
        the output directory path
    """

    client = get_client()
    outdir = tf.mkdtemp(dir=globe._dir_data)
    paths = []

    for i, partition in enumerate(df.to_delayed()):
        #log._logger.info(f'Tokenizing partition {i}')
        path = Path(outdir, tokenize(partition.key)).resolve().as_posix()

        #log._logger.info(f'Futurizing partition {i}')
        #fut = client.compute(partition)

        #log._logger.info(f'Submitting partition {i}')
        #p = client.submit(_write_dataframe_partition, fut, path)

        #log._logger.info(f'Appending partition {i}')
        #paths.append(p)

        paths.append(client.submit(
            _write_dataframe_partition, client.compute(partition), path
        ))

    return paths


def _consolidate_separate_partitions(partitions, output: str) -> str:
    """
    Read separate dask dataframe partition files from a single folder and
    concatenate them into a single file.

    arguments
        indir:  input directory filepath
        output: output filepath

    returns
        the output filepath
    """

    log._logger.info(f'Finalizing {output}')

    first = True

    with open(output, 'w') as ofl:
        for part in partitions:
            with open(part, 'r') as ifl:

                ## If this is the first file being read then we include the header
                if first:

                    ## VCF files require the header to be prefix by a '#'
                    header = next(ifl)
                    header = '#' + header

                    ofl.write(header)
                    ofl.write(ifl.read())

                    first = False

                ## Otherwise skip the header so it isn't repeated throughout
                else:
                    next(ifl)

                    ofl.write(ifl.read())

    ## Assume the input directory is a temp one and remove it since it's no longer needed
    shutil.rmtree(Path(partitions[0]).parent)

    return output



def save_distributed_dataframe_sync(
    df: ddf.DataFrame,
    output: str,
    header: bool = True
) -> str:
    """
    Save a distributed dask dataframe synchronously (i.e., this function blocks).
    Basically a wrapper for _save_distributed_dataframe.
    If you're calling this function using client.submit (not recommended, use the async
    version instead), you should scatter the dataframe beforehand or horrible shit is
    gonna happen to you.

    arguments
        df:     dask dataframe
        output: output filepath

    returns
        the output filepath
    """


    client = get_client()
    paths = client.gather(_write_dataframe_partition(df, header=header))

    return _consolidate_separate_partitions(paths, output)


def save_distributed_dataframe_async(
    df: ddf.DataFrame,
    output: str,
    client: Client = None,
) -> str:
    """
    Terrible ass generic name but idk what else to name it and it's used throughout by
    different parts of the pipeline. Basically a wrapper for save_distributed_dataframe
    but will autogenerate an output path if one isn't given.
    If you're calling this function using client.submit, you should scatter the dataframe
    beforehand or horrible shit is gonna happen to you.

    arguments
        df:     dask dataframe
        name:   an input path or filename used to generate a filename for the output path
        ext:    file extension
        outdir: output directory
        output: output filepath

    returns
        the output filepath
    """

    client = get_client() if client is None else client

    path_futures = _write_dataframe_partitions(df)

    return client.submit(_consolidate_separate_partitions, path_futures, output)
