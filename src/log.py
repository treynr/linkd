#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: log.py
## desc: Logging functions and objects.

import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())

_logger = None

def _initialize_logging(verbose=False):
    """
    """
    global _logger

    ## Add a console logger
    conlog = logging.StreamHandler()
    conlog.setLevel(logging.INFO if verbose else logging.ERROR)
    conlog.setFormatter(logging.Formatter('[%(levelname)-7s] %(message)s'))

    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.INFO if verbose else logging.ERROR)
    _logger.addHandler(conlog)

