#!/usr/bin/env python
# -*- coding: utf-8 -*-

## file: filter-samples.py
## desc: Perform population-based sample filtering on the given set of variant calls.

from __future__ import print_function
from sys import argv
import pandas as pd

if __name__ == '__main__':
    from argparse import ArgumentParser

    usage = '%s <population-map> <vcf> <output>' % argv[0]
    description = 'Filter out variant calls based on population structure'

    arg_parser = ArgumentParser(usage=usage, description=description)

    arg_parser.add_argument(
        'popmap',
        metavar='population',
        help='sample and population mapping'
    )

    arg_parser.add_argument(
        'vcf',
        help='variant call file'
    )

    arg_parser.add_argument(
        'output',
        help='output VCF'
    )

    arg_parser.add_argument(
        '-c',
        '--chunksize',
        dest='chunksize',
        action='store',
        default=10000,
        type=int,
        help='process the VCF file N rows at a time'
    )

    arg_parser.add_argument(
        '-p',
        '--populations',
        dest='population',
        action='store',
        type=str,
        help='only include samples from the given list of populations'
    )

    arg_parser.add_argument(
        '-s',
        '--superpopulations',
        dest='superpop',
        action='store',
        type=str,
        help='only include samples from the given list of super populations'
    )

    args = arg_parser.parse_args()

    if not args.population and not args.superpop:
        print('ERROR: You need to use the --populations or --superpopulations options')
        exit(1)

    ## Format the list of populations into a dict
    if args.superpop:
        pops = dict([(p.strip(), 0) for p in args.superpop.split(',')])
    else:
        pops = dict([(p.strip(), 0) for p in args.population.split(',')])

    ## Read in the sample-population map
    popdf = pd.read_csv(args.popmap, sep='\t')

    ## The VCF files don't have a header row (well they do but it's commented
    ## out) so we construct our own.
    ## Plink will shit the bed if VCF header columns aren't capitalized 
    header = [
        'CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT'
    ]
    ## Incldue the pop. samples
    headerpop = header + popdf['sample'].tolist()

    ## Only keep samples originating from the given list of populations
    if args.superpop:
        popdf = popdf[popdf['super_pop'].isin(pops)]
    else:
        popdf = popdf[popdf['pop'].isin(pops)]

    vcf_header = []

    ## We have to read in the VCF header and save it for output.
    with open(args.vcf, 'r') as fl:
        for ln in fl:

            if not ln:
                continue
            elif ln[0] == '#':
                vcf_header.append(ln.strip())
            else:
                break

    ## Columns we're keeping
    columns = header + popdf['sample'].tolist()

    ## Clear out the VCF output file prior to writing new data and save the
    ## header
    with open(args.output, 'w') as fl:
        print('\n'.join(vcf_header[:-1]), file=fl)

        ## The stupid VCF specification requires header columns to be prefixed w/ '#'
        print('#' + '\t'.join(columns), file=fl)

    ## Read in the VCF. Must be done one chunk at a time since often these
    ## files are ~50GB
    for vcf in pd.read_csv(
        args.vcf,
        sep='\t',
        header=None,
        names=headerpop,
        comment='#', 
        chunksize=args.chunksize
    ):

        ## Filter out samples not in our selected populations
        vcf = vcf.filter(items=columns)

        ## Save as output
        vcf.to_csv(args.output, sep='\t', header=False, index=False, mode='a')

