#!/usr/bin/env bash

## file: submit-filter-populations.sh
## desc: Submit the filter-populations.sh script to an HPC cluster.
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

if [[ "$#" -lt 1 ]]; then
    
    echo "ERROR: You need to supply a comma delimited list of populations"
    exit 1
fi

population="$1"

## Check to see what version of PBS/TORQUE is running.
## I have access to two clusters which run wildly different versions and this
## affects the job submission syntax.
version=$(qstat --version 2>&1 | sed -r -e ':a;N;$!ba;s/.*([0-9]+)\.[0-9]+\.[0-9]+.*/\1/')

## Old and busted
if [[ $version -lt 14 ]]; then

    qsub -q batch -l walltime=01:30:00 -t 1-24 -v population="$population" "$SRC_DIR/filter-populations.sh"

## New hotness
else

    qsub -q batch -J 1-24 -v population="$population" "$SRC_DIR/filter-populations.sh"
fi

