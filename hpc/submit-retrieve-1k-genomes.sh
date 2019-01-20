#!/usr/bin/env bash

## file: submit-retrieve-1k-genomes.sh
## desc: Submit the retrieve-1k-genomes.sh job to an HPC cluster.
## auth: TR

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

## Submit to the batch queue using 1 node and 24 processes w/ a run time limit
## of 45 min.
qsub -q batch -l nodes=1:ppn=24 -l walltime=00:45:00 "$SRC_DIR/retrieve-1k-genomes.sh"

## Check to see what version of PBS/TORQUE is running.
## I have access to two clusters which run wildly different versions and this
## affects the job submission syntax.
#version=$(qstat --version 2>&1 | sed -r -e ':a;N;$!ba;s/.*([0-9]+)\.[0-9]+\.[0-9]+.*/\1/')
#
### Old and busted
#if [[ $version -lt 14 ]]; then
#
#    qsub -q batch -l walltime=00:45:00 -t 1-24 "$SRC_DIR/update-1k-snps-job.sh"
#
### New hotness
#else
#
#    qsub -q batch -l walltime=00:45:00 -t 1-24 "$SRC_DIR/update-1k-snps-job.sh"
#fi
#echo "$version"
