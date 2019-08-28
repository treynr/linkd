#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: cluster.py
## desc: Functions for dask cluster initialization.

from dask.distributed import Client
from dask.distributed import LocalCluster
from dask_jobqueue import PBSCluster
from functools import partial
from pathlib import Path
from typing import List
import dask
import logging

from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def initialize_local_cluster(
    procs: int = 6,
    temp: str = '/tmp',
    **kwargs
) -> LocalCluster:
    """
    Initialize a dask LocalCluster.

    arguments
        workers: number of workers (processes) to use
        temp:     location of the local working, or temp, directory

    returns
        a LocalCluster
    """

    return LocalCluster(
        n_workers=procs,
        processes=True,
        local_directory=temp
    )


def initialize_pbs_cluster(
    name: str = 'linkage-disequilibrium',
    queue: str = 'batch',
    interface: str = 'ib0',
    cores: int = 2,
    procs: int = 2,
    workers: int = 40,
    memory: str = '240GB',
    walltime: str = '03:00:00',
    env_extra: List[str] = ['cd $PBS_O_WORKDIR'],
    log_dir: str = 'linkd-logs',
    temp: str = '/tmp',
    **kwargs
) -> LocalCluster:
    """
    Initialize a dask distributed cluster for submission on an HPC system running
    PBS/TORQUE.

    arguments
        name:      job name
        queue:     queue used for submission
        interface: interconnect interface (e.g. ib0 = Infiniband)
        cores:     number of cores per job
        procs:     number of processes per job
        workers:   number of workers (jobs) which takes into account the number of cores.
                   So if you specify that each job should use cores = 2, and workers = 40,
                   a total of 20 jobs will be submitted (40 / 2).
        memory:    total memory per job, so memory = 120GB and cores = 2, means each
                   process will have 60GB of usable memory
        walltime:  max. runtime for each job
        env_extra: extra arguments to use with the submission shell script
        temp:       location of the local working, or temp, directory

    returns
        a LocalCluster
    """

    ## Ensure the log directory exists
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    return PBSCluster(
        name=name,
        queue=queue,
        interface=interface,
        cores=cores,
        processes=procs,
        memory=memory,
        walltime=walltime,
        local_directory=temp,
        job_extra=[f'-e {log_dir}', f'-o {log_dir}'],
        env_extra=env_extra
    )


def initialize_cluster(hpc: bool = True, verbose: bool = True, jobs: int = 10, **kwargs):
    """

    :param hpc:
    :param kwargs:
    :return:
    """

    if hpc:
        cluster = initialize_pbs_cluster(**kwargs)
        cluster.scale_up(jobs)

    else:
        cluster = initialize_local_cluster(**kwargs)

    client = Client(cluster)

    temp = kwargs['temp'] if 'temp' in kwargs else '/tmp'

    Path(temp).mkdir(parents=True, exist_ok=True)

    dask.config.set({'temporary_directory': temp})
    dask.config.set({'local_directory': temp})

    ## Run the logging init function on each worker and register the callback so
    ## future workers also run the function
    init_logging_partial = partial(log._initialize_logging, verbose=verbose)
    client.register_worker_callbacks(setup=init_logging_partial)

    return client


if __name__ == '__main__':
    pass

