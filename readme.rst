
linkd
=====

A collection of scripts for calculating population-specific `linkage disequilibrium`__
(LD) using variant calls from the `1000 Genomes Project`__.
Each script can be executed by itself or submitted to an HPC cluster for faster
processing.

.. __: https://en.wikipedia.org/wiki/Linkage_disequilibrium
.. __: http://www.internationalgenome.org

Usage
-----

.. pull-quote::

    Using datasets from the 1K Genome Project requires a *lot* of disk space.
    Uncompressed variant calls are roughly 1TB in size.

Start by retrieving variant calls for all 22 autosomoes and both sex chromosomes.
By default, all data (raw and processed) is saved to the :code:`data/` directory.

.. code:: bash

    $ ./src/retrieve-1k-genomes.sh

We may also wish to update old SNP identifiers prior to calculating LD.
If this is the case, also retrieve the latest identifier merge table (currently set to 
dbSNP v. 150) from NCBI:

.. code:: bash

    $ ./src/retrieve-merged-snps.sh


Next we filter out variant call samples based on population structure.
For example, if we wanted to only include chromosome 21 samples from the European 
superpopulation we would use the :code:`-s` option along with the population code 
:code:`EUR`:

.. code:: bash

    $ ./src/filter-samples.sh -s EUR data/chr21.vcf data/chr21-filtered.vcf

We could also select individual populations.
For example, if we wanted to only include individuals of Han Chinese descent:

.. code:: bash

    $ ./src/filter-samples.sh CHB,CHS data/chr21.vcf data/chr21-filtered.vcf

The sample-population map can be found at `data/1k-genomes-sample-populations.tsv`__.
A list of populations, population codes, and metadata is available on the 1K Genomes
Project website__. 

.. __: data/1k-genomes-sample-populations.tsv
.. __: http://www.internationalgenome.org/data-portal/population

Updating SNP identifiers to their more recent versions can now be performed on filtered
variant calls:

.. code:: bash

    $ ./src/update-snp-identifiers.sh data/merged-snps.tsv data/chr21-filtered.vcf data/chr21-merged.vcf

Finally, given a list of SNPs, we can calculate LD scores (:math:`D'`) between all pairwise
combinations of SNPs on that list and SNPs residing on the same chromosome.

.. code:: bash

    $ ./src/calculate-ld.sh snp-list.txt data/chr21-merged.vcf data/chr21-ld


HPC Usage
'''''''''

All scripts can be submitted to an HPC cluster running PBS/TORQUE.
Submission scripts can be found in the :code:`hpc/` directory.
These scripts will process all chromosomes simultaneously.
Their performance (node and process per node utilization) can be tweaked by editing the
submission or :code:`src/` scripts.
The HPC version of the previous usage examples:

.. code:: bash

    $ ./hpc/submit-retrieve-1k-genomes.sh

    ## Only keep samples from the European superpop...
    $ ./hpc/submit-filter-samples.sh -s EUR

    ## ...or only keep samples of Han Chinese descent
    $ ./hpc/submit-filter-samples.sh CHB,CHS

    $ ./hpc/submit-calculate-ld.sh snp-list.txt


Requirements and installation
-----------------------------

The following dependencies are required:

- Python 2.7/3.5/3.6
- pandas__
- miller__

.. __: https://pandas.pydata.org/
.. __: https://github.com/johnkerl/miller

Make sure all dependencies are available on your :code:`$PATH`.

