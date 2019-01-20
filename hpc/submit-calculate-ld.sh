#!/usr/bin/env bash

## file: submit-calculate-ld.sh
## desc: Submit the calculate-ld.sh script to an HPC cluster.
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
    
    echo "ERROR: You need to supply a file containing a list of SNPs"
    exit 1
fi

## SNP list. Each SNP in the list will be compared to all other SNPs on the
## same chromosome and LD calculated between each pairwise comparison.
snps="'$1'"
## Script being submitted to the HPC cluster
script="$SRC_DIR/calculate-ld.sh"

## Check to see what version of PBS/TORQUE is running.
## I have access to two clusters which run wildly different versions and this
## affects the job submission syntax.
version=$(
	qstat --version 2>&1 | 
	sed -r -e ':a;$!{N;ba};s/[^0-9]*([0-9]+)\.([0-9]+)\.([0-9]+).*/\1/g'
)

## Old and busted
if [[ $version -lt 14 ]]; then

    qsub -q batch  -t 1-24 -v snps="$snps" "$script"

## New hotness
else

    qsub -q batch -J 1-24 -v snps="$snps" "$script"
fi

