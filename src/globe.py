#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: globe.py
## desc: Contains global variables, mostly output directories and files used by
##       the pipeline scripts.

from pathlib import Path
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())

## URLs ##

## The 1K Genome Project
_url_1k_base = 'http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502'

## Partial file URLs for chromosomes 1 - 22
_url_1k_autosome = f'{_url_1k_base}/ALL.chr%s.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz'

## X chromosome
_url_1k_x_chromosome = f'{_url_1k_base}/ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf.gz'

## Y chromosome
_url_1k_y_chromosome = f'{_url_1k_base}/ALL.chrY.phase3_integrated_v2a.20130502.genotypes.vcf.gz'

## URLs for the NCBI dbSNP merge table. NCBI now reports merged SNPs in JSON format for
## all dbSNP versions >= 152. So write your own parser for that.
_url_dbsnp150 = "http://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b150_GRCh38p7/database/data/organism_data/RsMergeArch.bcp.gz"
_url_dbsnp151 = "http://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b151_GRCh38p7/database/organism_data/RsMergeArch.bcp.gz"

## Plink executeable
_url_plink = 'http://s3.amazonaws.com/plink1-assets/plink_linux_x86_64_20190304.zip'



## Output directories ##
_dir_data = 'data'

## 1K Genome Project datasets
_dir_1k_variants = Path(_dir_data, '1k-variants')

## Semi-processed variant calls
_dir_1k_processed = Path(_dir_data, '1k-processed')

## LD calls
_dir_1k_ld = Path(_dir_data, '1k-ld')

## External tools
_dir_bin = Path(_dir_data, 'bin')
_dir_plink_bin = Path(_dir_bin, 'plink')

## Work directory
_dir_work = Path(_dir_data, 'work')


## Output files ##

## Compressed VCF files
_fp_1k_variant_autosome_gz = Path(_dir_1k_variants, 'chromosome-%s.vcf.gz').as_posix()
_fp_1k_variant_x_gz = Path(_dir_1k_variants, 'chromosome-x.vcf.gz').as_posix()
_fp_1k_variant_y_gz = Path(_dir_1k_variants, 'chromosome-y.vcf.gz').as_posix()

## Decompressed VCF files
_fp_1k_variant_autosome = Path(_dir_1k_variants, 'chromosome-%s.vcf').as_posix()
_fp_1k_variant_x = Path(_dir_1k_variants, 'chromosome-x.vcf').as_posix()
_fp_1k_variant_y = Path(_dir_1k_variants, 'chromosome-y.vcf').as_posix()

## Semi-processed VCF files
_fp_1k_processed_autosome = Path(_dir_1k_processed, 'chromosome-%s.vcf').as_posix()
_fp_1k_processed_x = Path(_dir_1k_processed, 'chromosome-x.vcf').as_posix()
_fp_1k_processed_y = Path(_dir_1k_processed, 'chromosome-y.vcf').as_posix()

## Sample -> population map that's part of the repo
_fp_population_map = 'data/1k-genomes-sample-populations.tsv'

## NCBI dbSNP merge table
_fp_compressed_dbsnp_table = Path('data/dbsnp-merge-table.tsv.gz').as_posix()
_fp_dbsnp_table = Path('data/dbsnp-merge-table.tsv').as_posix()

## Plink executable
_exe_plink = Path(_dir_plink_bin, 'plink').as_posix()



## Bedops executables
#_exe_bedops_bedmap = Path(_dir_bedops_bin, 'bedmap').as_posix()
#_exe_bedops_bedops = Path(_dir_bedops_bin, 'bedops').as_posix()
#_exe_bedops_sortbed = Path(_dir_bedops_bin, 'sort-bed').as_posix()

## In case these don't exist
try:
    Path(_dir_1k_variants).mkdir(parents=True, exist_ok=True)
    Path(_dir_1k_processed).mkdir(parents=True, exist_ok=True)
    Path(_dir_1k_ld).mkdir(parents=True, exist_ok=True)
    Path(_dir_work).mkdir(parents=True, exist_ok=True)
    Path(_dir_plink_bin).mkdir(parents=True, exist_ok=True)

except OSError as e:
    logging.getLogger(__name__).error('Could not make data directories: %s', e)

