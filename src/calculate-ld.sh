#!/usr/bin/env bash
#PBS -N calculate-ld
#PBS -l nodes=1:ppn=8

## file: calculate-ld.sh
## desc: Calculate linkage disequilibrium for a given set of SNPs.
## auth: TR

## Check if this was submitted to HPC cluster
if [[ -n "$PBS_JOBID" ]]; then

    cd "$PBS_O_WORKDIR"

    ## Submission to an HPC cluster requires the snps variable to be set
    if [[ -z "$snps" ]]; then

        echo "ERROR: Script requires the snps variable to be set"
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

        echo "ERROR: Script requires three arguments:"
        echo "       <snps>    a file containing a list of snps"
        echo "       <input>   an input VCF file"
        echo "       <output>  an output file containing LD scores"
        exit 1
    fi

    ## Input SNP list
    snps="$1"
    ## File VCF being processed
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

    input="$DATA_DIR/chr${chr}-filtered-merged.vcf"
    output="$DATA_DIR/chr${chr}-ld"
fi

plink --threads 8 --vcf "$input" --ld-window-r2 0.5 --ld-snp-list "$snps" --r2 inter-chr --out "$output"

## Fix shitty plink output
sed -i -e 's/^\s\+//g' "$output.ld"
sed -i -e 's/\s\+$//g' "$output.ld"
sed -i -e 's/\s\+/\t/g' "$output.ld"

