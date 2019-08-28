#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: retrieve.py
## desc: Functions for retrieving variant calls from the 1K Genome Project and
##       the plink toolset.

from dask.distributed import Client
from dask.distributed import Future
from dask.distributed import LocalCluster
from pathlib import Path
from typing import List
from zipfile import ZipFile
import gzip
import logging
import os
import requests as req
import shutil
import stat
import tempfile as tf

from . import cluster
from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _download(url: str, output: str) -> str:
    """
    Download a file at the given URL and save it to disk.

    arguments
        url:    file URL
        output: output filepath

    returns
        the output filepath
    """

    try:
        response = req.get(url, stream=True)

        ## Throw exception for any HTTP errors
        response.raise_for_status()

        with open(output, 'wb') as fl:
            for chunk in response.iter_content(chunk_size=1024):
                fl.write(chunk)

    except Exception as e:
        log._logger.error('Request exception occurred: %s', e)
        raise

    return output


def _unzip(fp: str, output: str = None, force: bool = False, **kwargs) -> str:
    """
    Unzip a gzipped file to the given output path.

    arguments
        fp:     zipped input filepath
        output: output path
        force:  if true and the output already exists, will overwrite the output file
                with the decompressed input
        kwargs: used to trick dask into creating dataset dependencies for this function

    returns
        the output filepath
    """

    log._logger.info('Decompressing: %s', fp)

    if output is None:
        output = Path(Path(fp).parent, Path(fp).stem).as_posix()

    if Path(output).exists() and not force:
        log._logger.warning(
            f'Uncompressed data ({output}) already exists, use force=True to overwrite'
        )

        return output

    with gzip.open(fp, 'rb') as gfl, open(output, 'wb') as ufl:
        shutil.copyfileobj(gfl, ufl)

    return output


def _download_autosome_calls(
    chromosome: int,
    output: str = globe._fp_1k_variant_autosome_gz,
    force: bool = False
) -> str:
    """
    Download compressed variant calls for the given chromosome.

    arguments
        chromosome: chromosome number
        output:     output filepath of the downloaded, compressed build
        force:      if true, retrieve the dataset even if it already exists locally

    returns
        the output filepath
    """

    if chromosome < 1 or chromosome > 22:
        log._logger.error(
            f"Can't download autosomes with an invalid chromsome number ({chromosome})"
        )

        return output

    log._logger.info(f'Downloading chr{chromosome} variant calls')

    if Path(output).exists() and not force:
        log._logger.warning(
            f'The chr{chromosome} VCF file already exist, use force=True to retrieve it'
        )

        return output

    ## Make the url and output
    url = globe._url_1k_autosome % chromosome

    try:
        output = output % chromosome
    except Exception:
        pass

    return _download(url, output)


def _download_x_calls(output: str = globe._fp_1k_variant_x_gz, force: bool = False) -> str:
    """
    Download compressed variant calls for the X chromosome.

    arguments
        output: output filepath of the downloaded, compressed build
        force:  if true, retrieve the dataset even if it already exists locally

    returns
        the output filepath
    """

    log._logger.info(f'Downloading chrX variant calls')

    if Path(output).exists() and not force:
        log._logger.warning(
            'The chrX VCF file already exist, use force=True to retrieve it'
        )

        return output

    ## Make the url
    url = globe._url_1k_x_chromosome

    return _download(url, output)


def _download_y_calls(output: str = globe._fp_1k_variant_y_gz, force: bool = False):
    """
    Download compressed variant calls for the Y chromosome.

    arguments
        output: output filepath of the downloaded, compressed build
        force:  if true, retrieve the dataset even if it already exists locally
    """

    log._logger.info(f'Downloading chrY variant calls')

    if Path(output).exists() and not force:
        log._logger.warning(
            'The chrY VCF file already exist, use force=True to retrieve it'
        )

        return output

    ## Make the url
    url = globe._url_1k_y_chromosome

    return _download(url, output)


def _download_dbsnp_merge_table(
    url: str = globe._url_dbsnp150,
    output: str = globe._fp_compressed_dbsnp_table,
    force: bool = False
) -> None:
    """
    Retrieve the dbSNP merge table. Retrieves v150 by default.

    arguments
        url:    optional dbSNP merge table URL
        output: optional filepath where the compressed dataset would be stored
        force:  force data retrieval even if the dataset exists locally
    """

    if Path(output).exists() and not force:
        log._logger.warning('dbSNP merge table exists, skipping retrieval')
        return

    log._logger.info('Retrieving NCBI dbSNP merge table')

    return _download(url, output)


def _download_plink(
    url: str = globe._url_plink,
    output: str = globe._dir_plink_bin
) -> str:
    """
    Retrieves the plink toolset and decompresses it.

    arguments
        url:    optional url to the archive containing the bedops toolkit
        output: optional output directory path
    """

    log._logger.info('Downloading plink')

    ## Zipped form is put in a temp file
    temp_zip = tf.NamedTemporaryFile(delete=True).name

    _download(url, temp_zip)

    log._logger.info('Extracting plink')

    with ZipFile(temp_zip) as zfl:
        zfl.extractall(path=output)

    ## Make the binary executable
    os.chmod(globe._exe_plink, os.stat(globe._exe_plink).st_mode | stat.S_IEXEC)

    return globe._exe_plink


def retrieve_variants(client: Client, force: bool = True) -> List[Future]:
    """
    Retrieve variant calls from the 1K Genome Project and decompress them.

    arguments
        client: a dask Client object
        force:  if true, datasets will be downloaded even if they exist locally

    returns
        a list of Futures
    """

    calls = []

    for chrom in range(1, 23):
        call = client.submit(_download_autosome_calls, chrom, force=force)
        call = client.submit(_unzip, call, force=force)

        calls.append(call)

    x_call = client.submit(_download_x_calls, force=force)
    x_call = client.submit(_unzip, x_call, force=force)

    y_call = client.submit(_download_y_calls, force=force)
    y_call = client.submit(_unzip, y_call, force=force)

    calls.append(x_call)
    calls.append(y_call)

    return calls


def retrieve_plink(client: Client, force: bool = True) -> Future:
    """
    Retrieve the plink toolkit.

    arguments
        client: a dask Client object
        force:  if true, datasets will be downloaded even if they exist locally

    returns
        a future
    """

    return client.submit(_download_plink)


def retrieve_dbsnp_merge_table(client: Client, force: bool = True) -> Future:
    """
    Retrieve the dbSNP merge table.

    arguments
        client: a dask Client object
        force:  if true, datasets will be downloaded even if they exist locally

    returns
        a future containing the filepath to the merge table
    """

    table = client.submit(_download_dbsnp_merge_table, force=force)

    return client.submit(
        _unzip, globe._fp_compressed_dbsnp_table, force=force, depends=table
    )


def retrieve_all_datasets(client: Client, force: bool = False) -> List[Future]:
    """
    Retrieve all datasets: 1KGP variant calls, the dbSNP merge table, and plink.

    arguments
        client: a dask Client object
        force:  if true, datasets will be retrieved even if they exist locally

    returns
        a list of futures
    """

    calls = retrieve_variants(client, force=False)
    plink = retrieve_plink(client, force=False)
    merge = retrieve_dbsnp_merge_table(client, force=False)

    return (calls + [plink, merge])


if __name__ == '__main__':

    client = Client(LocalCluster(
        n_workers=8,
        processes=True
    ))

    log._initialize_logging(verbose=True)

    ## Init logging on each worker
    client.run(log._initialize_logging, verbose=True)

    calls = retrieve_variants(client, force=False)
    plink = retrieve_plink(client, force=False)
    merge = retrieve_dbsnp_merge_table(client, force=False)

    client.gather(calls + [plink, merge])

    client.close()

