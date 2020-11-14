#!/usr/bin/env python3
"""
    Track the Immigration data from Immd
    Typical page: https://www.immd.gov.hk/eng/stat_20200124.html
"""
import requests
import datetime
import sqlite3
import re
import sys
sys.path.append('/home/odaiwai/src/dob_DBHelper')
import db_helper as dbdo # This is a library of my own database routines - it just 
                         # wraps sqlite commands into handier methods I like

def get_all_data_in_range(from_date, to_date):
    this_date = from_date
    while this_date <= to_date:
        get_all_data_for_day(this_date)
        this_date += datetime.timedelta(days = 1)
        
    return None

def column_headings():
    columns = ['arr.Hong Kong Residents',
               'arr.Mainland Visitors',
               'arr.Other Visitors',
               'arr.Total',
               'dep.Hong Kong Residents',
               'dep.Mainland Visitors',
               'dep.Other Visitors',
               'dep.Total']
    return columns

def make_database():
    table_defs = {
        'Crossings': 'Name Text Unique Primary Key', 
        'data': 'date text, port Text, direction Text, pax integer'
    }
    
    dbdo.make_tables_from_dict(dbc, table_defs, VERBOSE)
    return None
    
def get_all_data_for_day(date):
    date_str = date.strftime('%Y%m%d')
    # URIs are of the form: https://www.immd.gov.hk/eng/stat_20200124.html
    baseuri = 'https://www.immd.gov.hk/eng/stat'
    ext = 'html'
    url = '{}_{}.{}'.format(baseuri, date_str, ext)
    response = requests.get(url)
    html_source = response.content.decode()
    lines = response.content.decode().split('\n')

    # Setup our Regular Expressions
    crossing = re.compile(r'^\s+<td>([A-Za-z -]+)\s*<\/td>')
    count    = re.compile(r'^\s+<td class="hRight">([0-9, ]+)\s*<\/td>')
    data = {'date': date_str}
    crossings = []

    dbdo.dbdo(dbc, "BEGIN", VERBOSE)
    # parse the file
    current_crossing = ''
    for line in lines:
        #print('LINE:', line)
        match = crossing.match(line)
        if match:
            #print(match, match[1])
            current_crossing = match[1]
            crossings.append(current_crossing)
            columns = column_headings()
            #print(current_crossing, columns)
        
        match = count.match(line)
        if match:
            #print(match, match[1])
            pax = match[1]
            column = columns.pop(0)
            direction, port = column.split('.')
            uuid = '{}.{}'.format(current_crossing, column)
            data[uuid] = pax
            #print(current_crossing, column, port, direction, pax)
            sql_cmd = ('INSERT OR IGNORE INTO [data] (date, port, direction, pax) '
                       'Values (:date, :port, :dirn, :pax) ')
            params = {'date': date_str, 'port': port,
                      'dirn': direction, 'pax': pax} 
            dbdo.dbdo_params(dbc, sql_cmd, params, VERBOSE)

    print(data)
    dbdo.dbdo(dbc, "COMMIT", VERBOSE)


def main():
    start_date = datetime.datetime.strptime('20200124', '%Y%m%d')
    today = datetime.datetime.now()
    yday = today + datetime.timedelta(days = -1)
    if FIRSTRUN:
        make_database()
        get_all_data_in_range(start_date, yday)
    else:
        get_all_data_for_day(yday)

    return None

if __name__ == '__main__':
    # constants
    FIRSTRUN = 0
    VERBOSE = 0

    db_connect = sqlite3.connect(r'immd.sqlite')
    dbc = db_connect.cursor()
    
    results = main()

    dbc.close()
