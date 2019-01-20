#!/usr/bin/env bash

## file: submit-filter-samples.sh
## desc: Submit the filter-samples.sh script to an HPC cluster.
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

super=""

## cmd line processing
while :; do
    case $1 in

        -s | --super)
            super=1
            ;;

        -h | -\? | --help)
            usage
            exit
            ;;

        --)
            shift
            break
            ;;

        -?*)
            echo "WARN: unknown option (ignored): $1" >&2
            ;;

        *)
            break
    esac

    shift
done

if [[ "$#" -lt 1 ]]; then
    
    echo "ERROR: You need to supply a comma delimited list of populations"
    exit 1
fi

## Comma separated lists have to be enclosed in single quotes when passing
## variables to qsub
population="'$1'"
## Script being submitted to the HPC cluster
script="$SRC_DIR/filter-samples.sh"

## Check to see what version of PBS/TORQUE is running.
## I have access to two clusters which run wildly different versions and this
## affects the job submission syntax.
version=$(
	qstat --version 2>&1 | 
	sed -r -e ':a;$!{N;ba};s/[^0-9]*([0-9]+)\.([0-9]+)\.([0-9]+).*/\1/g'
)

## Old and busted
if [[ $version -lt 14 ]]; then

    qsub -q batch -l walltime=03:00:00 -t 1-24 -v population="$population",super="$super" "$script"

## New hotness
else

    qsub -q batch -J 1-24 -v population="$population",super="$super" "$script"
fi

