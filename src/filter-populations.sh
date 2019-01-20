#!/usr/bin/env bash
#PBS -N filter-populations
#PBS -l nodes=1:ppn=1

## file: filter-populations.sh
## desc: Filter out specific populations from 1K Genome Project variant calls.
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

			-p | --population)
				if [ "$2" ]; then
					population="$2"
					shift
				else
					echo "ERROR: --population requires an argument"
					echo "       e.g. 'GBR,FIN', 'GIH'"
					exit 1
				fi
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

    if [[ $# -lt 2 ]]; then

        echo "ERROR: Script requires <input> and <output> arguments"
        exit 1
    fi

    if [[ -z "$population" ]]; then

        echo "ERROR: Script requires the --population option"
        exit 1
    fi

    ## File being processed
    input="$1"
    ## Output
    output="$2"
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

python "$SRC_DIR/filter-populations.py" -i "$population" "$popmap" "$input" "$output"

