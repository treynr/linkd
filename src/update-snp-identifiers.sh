#!/usr/bin/env bash
#PBS -N update-snp-identifiers.sh
#PBS -l nodes=1:ppn=2

## file: update-snp-identifiers.sh
## desc: Update reference SNP identifiers to their most recent version.
## auth: TR

usage() {

    echo "usage: $0 [options] <merged> <input> <output>"
    echo ""
    echo "Update old reference SNP identifiers using the NCBI rsMerge table"
    echo ""
    echo "Misc. options:"
    echo "  -i, --interactive  the script is running in an interactive HPC session and"
    echo "                     should not be treated as an HPC job"
    echo "  -h, --help         print this help message and exit"
    echo ""
    echo "Arguments:"
    echo "  <merged>           a TSV file containing SNP IDs to update"
    echo "  <input>            an input VCF file"
    echo "  <output>           an output VCF file"
}

## cmd line processing if this script was not submitted to an HPC cluster
while :; do
    case $1 in

        -i | --interactive)
            interactive=1
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

## Check if this was submitted to HPC cluster and the interactive argument wasn't given
if [[ -n "$PBS_JOBID" && -z "$interactive" ]]; then

    cd "$PBS_O_WORKDIR"

    ## HPC cluster submission should be doneas a job array
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

    if [[ $# -lt 3 ]]; then

        echo "ERROR: Script requires three arguments"
        echo ""
        usage
        exit 1
    fi

    ## Merged SNPs
    merged="$1"
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
if [[ -n "$PBS_JOBID" && -z "$interactive" ]]; then

    if [[ -z "$merged" ]]; then
        merged="$DATA_DIR/merged-snps.tsv"
    fi

    input="$DATA_DIR/chr${chr}-filtered.vcf"
    output="${input%.vcf}-merged.vcf"
fi

## Label some of the columns
mlr --tsvlite --pass-comments --implicit-csv-header label "chr,pos,rsid" "$input" |
## Join the merged SNP table and VCF file to update SNP reference IDs
mlr --tsvlite --pass-comments join -f "$merged" -l 'old_rsid' -r 'rsid' -j 'rsid' --ur |
## If the rsID was updated we need to get rid of the old one and replace the field name
mlr --tsvlite --pass-comments put 'if (haskey($*, "new_rsid")) { $rsid = $new_rsid; unset $new_rsid; }' |
## Reorder the first three columns which may be out of order due to the update
mlr --tsvlite --pass-comments --headerless-csv-output reorder -f 'chr,pos,rsid' > "$output"

