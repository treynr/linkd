#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from sys import argv
import pandas as pd

if __name__ == '__main__':
    from argparse import ArgumentParser

    usage = '%s <population-map> <vcf> <output>' % argv[0]

    arg_parser = ArgumentParser(usage=usage)

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
        '-i',
        '--include',
        dest='include',
        action='store',
        type=str,
        help='only include samples from the given list of populations'
    )

    args = arg_parser.parse_args()

    if not args.include:
        print('ERROR: You should probably supply some populations to filter')
        exit(1)

    ## Format the list of populations into a dict
    pops = dict([(p.strip(), 0) for p in args.include.split(',')])

    ## Read in the sample-population map
    popdf = pd.read_csv(args.popmap, sep='\t')

    ## The VCF files don't have a header row (well they do but it's commented
    ## out) so we construct our own.
    header = [
        'chrom', 'pos', 'id', 'ref', 'alt', 'qual', 'filter', 'info', 'format'
    ]
    ## Incldue the pop. samples
    headerpop = header + popdf['sample'].tolist()

    ## Only keep samples originating from the given list of populations
    popdf = popdf[popdf['pop'].isin(pops)]

    ## Read in the VCF
    vcf = pd.read_csv(args.vcf, sep='\t', header=None, names=headerpop, comment='#')

    #print(vcf.head())
    #print(popdf.head())
    #print(popdf['sample'])

    ## Filter out samples not in our selected populations
    #vcf = vcf.filter(items=(header + popdf['sample'].tolist()))
    vcf = vcf.filter(items=(header + popdf['sample'].tolist()))

    #print(vcf.head())
    ## Save as output
    vcf.to_csv(args.output, sep='\t', header=False, index=False)

