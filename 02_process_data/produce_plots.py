#!/usr/bin/env python3
"""Read in the NID data files and populate the 
   historic database."""

import matplotlib as plt
import sqlite

conn = sqlite3.connect('health_data.sqlite')
c = conn.cursor()

#Constants
DATADIR = "../01_download_data"

# The Main Loop
if __name__ == '__main__':
    for dirpath, dirnames, filenames in os.walk(DATADIR):
        if filenames

conn.commit()
conn.close()
