#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: exceptions.py
## desc: Custom exceptions.

class VCFHeaderParsingFailed(Exception):
    """
    Raised when the VCF header could not be extracted from a VCF file.
    """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

