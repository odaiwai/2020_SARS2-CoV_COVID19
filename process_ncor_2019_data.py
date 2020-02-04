#!/usr/bin/env python3
"""
Gather all of the saved data from the NCorV 2019 outbreak and
get them into an SQLITE3 database som we can plot them.

dave o'brien (c) 2020

CC: BY-SA
"""
import sys, os, re
import sqlite3
#import openpyxl
import datetime

VERBOSE = 1
FIRSTRUN = 1
DATADIR = '01_download_data'



def array_from_query(query_str):
     """
     Return an array from the database.
     only uses the first column returned by the query.
     """
     results = []
     for row in dbc.execute(query_str):
         results.append(row[0])

     return results

def dict_from_query(query_str):
     """
     Return a query as a dict of two elements:
     e.g. select name, rank from staff
     staff['Fred'] = 'Boss'
     Only uses the first two columns of the query.
     """
     results = {}
     for row in dbc.execute(query_str):
         results[row[0]] = row[1]

     return results

def rows_from_query(query_str):
     """
     Return a list of lists from a database query.
     each list contains a list of all the rows.
     """
     results = []
     for row in dbc.execute(query_str):
         results.append(row)

     return results

def make_tables():
    # Make the Database tables
    print ('Dropping Tables')
    for table in array_from_query('select name from sqlite_master where type like \'table\';'):
        result = dbc.execute('DROP TABLE IF EXISTS [{}]'.format(table))

    tabledefs = {
        'hksarg_pr': 'timestamp text Unique Primary Key, New Int, Total Int, Cured Int, Remain Int, Stable Int, Serious Int, Critical Int, Confirmed Int, Dead Int',
        '3g_dxy_cn': 'timestamp text unique Primary Key, Province Text, City Text, Confirmed Int, Suspected Int, Dead Int, Cured Int',
        'jhu': 'timestamp text unique Primary Key, Country Text, Province Text, Confirmed Int, Dead Int, Cured Int'
            }
    print ('Building Tables...')
    for table in tabledefs.keys():
        result = dbc.execute('CREATE TABLE [{}] ({});'.format(table, tabledefs[table]))

def escaped_list(list):
    """
    Given a list, return it as a comma-separated list, quoted if necessary
    """
    escaped_list = []
    for item in list:
        match = re.match(r'^[0-9]+\/[0-9]+\/[0-9]+ [0-9]+\:[0-9]+$', item)
        if match:
            print(match)
            escaped_list.append('\\\"{}\\\"'.format(item))
    
        match = re.match(r'^[0-9]+$', item)
        if match:
            escaped_list.append(item)

        match = re.match(r'^$', item)
        if match:
            escaped_list.append('0')
       
    print (escaped_list)
    return ', '.join(escaped_list)

def read_hksarg_pr():
    # read in the HK SARG Press Releases
    tab = re.compile(r'\t')
    datesplit = re.compile(r'[/: ]')
    newline = re.compile(r'\n')
    filename = DATADIR + '/hksarg_pr.csv'
    print (filename)
    with open(filename, 'r') as file:
        lines = list(file)

    dbc.execute('BEGIN')
    for line in lines:
        values = tab.split(line)
        date_str = values.pop(0)
        # convert the components into escaped 
        escaped = escaped_list(values)

        # make a datetime object of the date
        date_list = datesplit.split(date_str)
        date = datetime.datetime(int(date_list[2]), int(date_list[1]), 
                                 int(date_list[0]), int(date_list[3]), 
                                 int(date_list[4]))
        
        sqlcmd = 'INSERT OR IGNORE INTO [hksarg_pr] (Timestamp, New, Total, Cured, Remain, Stable, Serious, Critical, Confirmed, Dead) Values (\"{}\", {});'.format(date, escaped)
        dbc.execute(sqlcmd)
        print(sqlcmd)
    
    dbc.execute('COMMIT')
    return 1 

def read_3g_dxy_cn_json():
    """
    Read in the JSON data from 3G.DXY.CN and add it to the database.
    we're mainly interested in provincial growth.
    """
    files = os.listfiles(DATADIR)
    json = re.compile(r'^([0-9]{8}[0-9]{6})$')
    for filename in files:




    return 1

def main():
    # main body
    """
    Go through the download dir and collect all of the various data sources:
    """
    db_connect = sqlite3.connect('ncorv2019.sqlite')
    dbc = db_connect.cursor()

    if (FIRSTRUN):
        make_tables()
        read_hksarg_pr()
        read_3g_dxy_cn_json()

if __name__ == '__main__':

    main()
    #tidy up and shut down
    dbc.close()
