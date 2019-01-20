#!/usr/bin/env bash
#PBS -N retrieve-1k-genomes
#PBS -l nodes=1:ppn=24

## file: retrieve-1k-genomes.sh
## desc: Retrieves variant calls for all chromosomes from the 1K genome project.
## auth: TR

## Check if this was submitted to HPC cluster
if [[ -n "$PBS_JOBID" ]]; then

    cd "$PBS_O_WORKDIR"
fi

## Config file searching
if [[ -r "$HOME/.linkd.sh" ]]; then
    source "$HOME/.linkd.sh"
elif [[ -r "../.linkd.sh" ]]; then
    source "../.linkd.sh"
elif [[ -r "./.linkd.sh" ]]; then
    source "./.linkd.sh"
else
    echo "ERROR: the .linkd.sh configuration file is missing"
    exit 1
fi

url="ftp://ftp.ncbi.nlm.nih.gov/snp/organisms/human_9606_b150_GRCh38p7/database/data/organism_data/RsMergeArch.bcp.gz"
gz_out="$DATA_DIR/merged-snps-raw.tsv.gz"
output="$DATA_DIR/merged-snps-raw.tsv"
final_output="$DATA_DIR/merged-snps.tsv"

## Retrieve merged SNP table
wget "$url" -O "$gz_out"

## Decompress
gunzip -c "$gz_out" > "$output"

## DSL to add the rs prefix to all rsIDs
put_dsl='$old_rsid = "rs" . string($old_rsid); $new_rsid = "rs" . string($new_rsid);'

## Format: first strip out old and merged rsid fields
mlr --tsv --implicit-csv-header cut -f '1,2' "$output" |
## Label the columns
mlr --tsv label 'old_rsid,new_rsid' |
## Add the 'rs' prefix to each ID
mlr --tsv put "$put_dsl" > "$final_output"
