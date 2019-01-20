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

base_url="ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502"
vcf="ALL.chr!x.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz"
xvcf="ALL.chrX.phase3_shapeit2_mvncall_integrated_v1b.20130502.genotypes.vcf.gz"
yvcf="ALL.chrY.phase3_integrated_v2a.20130502.genotypes.vcf.gz"

for i in {1..22}
do
    ## Substitute !x with the chromosome number
    fl="${vcf/!x/$i}"
    ## Complete URL
    url="$base_url/$fl"
    ## Output file
    out="$DATA_DIR/chr${i}.vcf.gz"

    ## Don't retrieve anything if the gzipped or unzipped copies of these data
    ## files already exist
    [[ -f "$out" || -f "${out%.gz}" ]] || (wget "$url" -O $out &>/dev/null) &
done

xout="$DATA_DIR/chrX.vcf.gz"
yout="$DATA_DIR/chrY.vcf.gz"

## Get X and Y chromosome data if it doesn't already exist
[[ -f "$xout" || -f "${xout%.gz}" ]] || (wget "$base_url/$xvcf" -O "$xout" ) &
[[ -f "$yout" || -f "${yout%.gz}" ]] || (wget "$base_url/$yvcf" -O "$yout" ) &

wait

## Extract the data (it's a ton of data)
for fl in "$DATA_DIR/"*.vcf.gz
do
    out="${fl%.gz}"

    (gunzip -c "$fl" > "$out") &
done

wait

