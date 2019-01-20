#!/usr/bin/env bash

## file: submit-update-snp-identifiers.sh
## desc: Submit the update-snp-identifiers.sh script to an HPC cluster.
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

## Check to see what version of PBS/TORQUE is running.
## I have access to two clusters which run wildly different versions and this
## affects the job submission syntax.
version=$(
    qstat --version 2>&1 | 
    sed -r -e ':a;$!{N;ba};s/.*([0-9]+)\.[0-9]+\.[0-9]+.*/\1/'
)

## Old and busted
if [[ $version -lt 14 ]]; then

    qsub -q batch -t 1-24 -l walltime=01:45:00  "$SRC_DIR/update-snp-identifiers.sh"

## New hotness
else

    qsub -q batch -J 1-24 "$SRC_DIR/update-snp-identifiers.sh"
fi

