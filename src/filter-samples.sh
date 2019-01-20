#!/usr/bin/env bash
#PBS -N filter-samples
#PBS -l nodes=1:ppn=1

## file: filter-samples.sh
## desc: Perform population-based sample filtering on 1K Genome Project variant
##       calls.
## auth: TR

## Check if this was submitted to HPC cluster
if [[ -n "$PBS_JOBID" ]]; then

    cd "$PBS_O_WORKDIR"

    ## Submission to an HPC cluster requires the population variable to be set
    if [[ -z "$population" ]]; then

        echo "ERROR: Script requires the population variable to be set"
        exit 1
    fi

    ## Also requires submission as a job array
    if [[ -n "$PBS_ARRAY_INDEX" ]]; then

        chr="$PBS_ARRAY_INDEX"

    elif [[ -n "$PBS_ARRAYID" ]]; then

        chr="$PBS_ARRAYID"

    elif [[ -n "$chr" ]]; then

        :
    else
        echo "ERROR: Script should be submitted as a job array or the 'chr' "
        echo "       variable should be set"
        exit 1
    fi

    if [[ "$chr" -eq 23 ]]; then
        chr="X"
    elif [[ "$chr" -eq 24 ]]; then
        chr="Y"
    fi
else

	## cmd line processing if this script was not submitted to an HPC cluster
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

    if [[ $# -lt 3 ]]; then

        echo "ERROR: $0 requires three arguments. <input> and <output> arguments"
        echo "       <population>  a comma delimited list of populations to include"
        echo "       <input>       an input VCF file"
        echo "       <output>      an output VCF file"
        exit 1
    fi

    if [[ -z "$population" ]]; then

        echo "ERROR: Script requires the --population option"
        exit 1
    fi

    ## Population list
    population="$1"
    ## File being processed
    input="$2"
    ## Output
    output="$3"
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

## If job submission, then we construct the filenames
if [[ -n "$PBS_JOBID" ]]; then

    input="$DATA_DIR/chr${chr}.vcf"
    output="${input%.vcf}-filtered.vcf"
fi

popmap="$DATA_DIR/1k-genomes-sample-populations.tsv"

## Remove any quotes from the population variable.
## Quoted if this is an HPC submission.
population="$(echo "$population" | sed -e s/\'//g)"

## Use a super population
if [[ -n "$super" ]]; then
    python "$SRC_DIR/filter-samples.py" -s "$population" "$popmap" "$input" "$output"
else
    python "$SRC_DIR/filter-samples.py" -p "$population" "$popmap" "$input" "$output"
fi

