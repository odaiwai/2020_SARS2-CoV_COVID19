#!/usr/bin/env python3
"""
    Read in the filenames in the DATADIR directory and rename any with a 
    2-digit year to have a 4 digit one.
"""

import os, sys, re
import datetime

if __name__ == '__main__':
    DATADIR = '01_download_data'
    shortdate1_re = re.compile(r'^([0-9]{2})([0-9]{2})([0-9]{2})_(.*)$')
    shortdate2_re = re.compile(r'^(3g_dxy_cn)_([0-9]{2})([0-9]{2})([0-9]{2})_(.*)$')
    filenames = os.listdir(DATADIR)
    for filename in filenames:
        match = shortdate1_re.match(filename)
        if match:
            print(match)
            oldfile = '{}/{}'.format(DATADIR, filename)
            newfile = '{}/20{}{}{}_{}'.format(DATADIR, match[1], match[2], match[3], match[4])
            print('\t', oldfile, '->', newfile)
            os.rename(oldfile, newfile)

        match = shortdate2_re.match(filename)
        if match:
            print(match)
            oldfile = '{}/{}'.format(DATADIR, filename)
            newfile = '{}/{}_20{}{}{}_{}'.format(DATADIR, match[1], match[2], match[3], match[4], match[5])
            print('\t', oldfile, '->', newfile)
            os.rename(oldfile, newfile)

