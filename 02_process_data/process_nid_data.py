#!/usr/bin/env python3
"""Read in the NID data files and populate the 
   historic database."""

import sqlite

import re
conn = sqlite3.connect('health_data.sqlite')
c = conn.cursor()
c.execute('''DROP TABLE IF EXISTS nid_monthly''')
c.execute('''CREATE TABLE nid_monthly (TimeStamp Integer Primary Key, read Integer)''')

#Constants
DATADIR = "../01_download_data"

# The Main Loop
if __name__ == '__main__':
    for dirpath, dirnames, filenames in os.walk(DATADIR):
        if filenames

conn.commit()
conn.close()
