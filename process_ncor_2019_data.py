#!/usr/bin/env python3
"""
Gather all of the saved data from the NCorV 2019 outbreak and
get them into an SQLITE3 database som we can plot them.

dave o'brien (c) 2020

CC: BY-SA
"""
import sys, os, re
import sqlite3
import json
#import openpyxl
import datetime
from db_helper import *
import matplotlib.pyplot as plt
#import pandas as pd
#import numpy as np

def make_tables():
    # Make the Database tables
    tabledefs = {
        'hksarg_pr': 'timestamp text Unique Primary Key, New Int, Total Int, Cured Int, Remain Int, Stable Int, Serious Int, Critical Int, Confirmed Int, Dead Int',
        '3g_dxy_cn_province': 'UUID text unique Primary Key, Timestamp Int, ISO_Date Text, Province_ZH Text, Province_EN Text, Confirmed Int, Suspected Int, Dead Int, Cured Int, Comment Text',
        '3g_dxy_cn_city': 'UUID text unique Primary Key, Timestamp Int, ISO_Date Text, Province_ZH Text, Province_EN Text, City_ZH Text, City_EN, Confirmed Int, Suspected Int, Dead Int, Cured Int',
        'jhu': 'UUID text unique Primary Key, Timestamp Int, Country Text, Province Text, Confirmed Int, Dead Int, Cured Int',
        'china_places': 'OBJECTID Int UNIQUE Primary Key, ADMIN_TYPE Text, ADM2_CAP Text, ADM2_EN Text, ADM2_ZH Text, ADM2_PCODE Text, ADM1_EN Text, ADM1_ZH Text, ADM1_PCODE Text, ADM0_EN Text, ADM0_ZH Text, ADM0_PCODE Text'
            }

    make_tables_from_dict(dbc, tabledefs)

def escaped_list(list):
    """
    Given a list, return it as a comma-separated list, quoted if necessary
    """
    escaped_list = []
    for item in list:
        match = re.match(r'^[0-9]+\/[0-9]+\/[0-9]+ [0-9]+\:[0-9]+$', item)
        if match:
            #print(match)
            escaped_list.append('\\\"{}\\\"'.format(item))

        match = re.match(r'[A-Za-z\u4e00-\u9fff]+', item)
        if match:
            escaped_list.append('\\\"{}\\\"'.format(item))

        match = re.match(r'^[0-9]+$', item)
        if match:
            escaped_list.append(item)

        match = re.match(r'^$', item)
        if match:
            escaped_list.append('0')
       
    #print (escaped_list)
    return ', '.join(escaped_list)

def read_hksarg_pr():
    # read in the HK SARG Press Releases
    tab = re.compile(r'\t')
    datesplit = re.compile(r'[/: ]')
    newline = re.compile(r'\n')
    filename = DATADIR + '/hksarg_pr.csv'
    print (filename)
    with open(filename, 'r') as infh:
        lines = list(infh)

    dbdo(dbc, 'BEGIN', VERBOSE)
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
        dbdo(dbc, sqlcmd, VERBOSE)
    
    dbdo(dbc, 'COMMIT', VERBOSE)
    return 1 

def read_china_places():
    """
    Read in a list of China municipalities.
    """
    with open('./gis/chn_admbnda_adm2_ocha/chn_admbnda_adm2_ocha.csv', 'r') as infh:
        lines = list(infh)

    fields =  'OBJECTID, ADMIN_TYPE, ADM2_CAP, ADM2_EN, ADM2_ZH, ADM2_PCODE, ADM1_EN, ADM1_ZH, ADM1_PCODE, ADM0_EN, ADM0_ZH, ADM0_PCODE'
    dbdo(dbc, 'BEGIN', VERBOSE)
    for line in lines:
        components = line.rstrip('\n').split(';')
        print (components)
        values = '{}'.format(components.pop(0))
        for component in components:
            values += r', "{}"'.format(component)

        dbdo(dbc, 'INSERT into [china_places] ({}) Values ({})'.format(fields, values), VERBOSE)

    dbdo(dbc, 'COMMIT', VERBOSE)
    return 1

def read_3g_dxy_cn_json():
    """
    Read in the JSON data from 3G.DXY.CN and add it to the database.
    we're mainly interested in provincial growth.
    """
    files = os.listdir(DATADIR)
    areastat = re.compile(r'^([0-9]{8})_([0-9]{6})_getAreaStat.json$')
    for filename in files:
        match = areastat.match(filename)
        if match:
            date = match[1]
            time = match[2]
            timestamp = '{}{}'.format(date, time)
            #print(int(date[0:4]), int(date[4:6]), int(date[6:]), int(time[0:2]), int(time[2:4]), int(time[4:]))
            iso_date = datetime.datetime(int(date[0:4]), int(date[4:6]), int(date[6:]), int(time[0:2]), int(time[2:4]), int(time[4:]))
            print (timestamp, filename, iso_date)
            with open('{}/{}'.format(DATADIR, filename), 'r') as infile:
                areastats = json.loads(infile.read())

            print (len(areastats))
            # Walk the tree
            dbdo(dbc, 'BEGIN', VERBOSE)
            pfields = 'UUID, Timestamp, ISO_Date, Province_ZH, Province_EN, Confirmed, Suspected, Cured, Dead, Comment'
            cfields = 'UUID, Timestamp, ISO_Date, Province_ZH, Province_EN, City_ZH, City_EN, Confirmed, Suspected, Cured, Dead'
            for province in areastats:
                uuid = '{}_{}'.format(timestamp, province['provinceName'])
                province_en = value_from_query(dbc, 'select distinct(ADM1_EN) from china_places where ADM1_ZH like \'{}%\';'.format(province['provinceName']))
                values = '"{}", {}, "{}", "{}", "{}", {}, {}, {}, {}, "{}"'.format(
                        uuid,
                        int(timestamp),
                        iso_date,
                        province['provinceName'],
                        province_en,
                        province['confirmedCount'],
                        province['suspectedCount'],
                        province['curedCount'],
                        province['deadCount'],
                        province['comment'])
                sql_cmd = 'INSERT into [3g_dxy_cn_province] ({}) Values ({})'.format(pfields, values)
                dbdo(dbc, sql_cmd, VERBOSE)

                #printlog (case_count)
                
                for city in province['cities']:
                    uuid = '{}_{}_{}'.format(timestamp, 
                            province['provinceName'], 
                            city['cityName'])
                    city_en = value_from_query(dbc, 'select distinct(ADM2_EN) from china_places where ADM2_ZH like \'{}%\';'.format(city['cityName']))
                    values = '"{}", {}, "{}", "{}", "{}", "{}", "{}", {}, {}, {}, {}'.format(
                            uuid,
                            int(timestamp),
                            iso_date,
                            province['provinceName'],
                            province_en,
                            city['cityName'],
                            city_en,
                            city['confirmedCount'],
                            city['suspectedCount'],
                            city['curedCount'],
                            city['deadCount'])
                    sql_cmd = 'INSERT into [3g_dxy_cn_city] ({}) Values ({})'.format(cfields, values)
                    dbdo(dbc, sql_cmd, VERBOSE)

            dbdo(dbc, 'COMMIT', VERBOSE)

    return 1

def make_plots():
    """ 
    Make a plot by province:
    """
    axis_range = [datetime.datetime(2020,1,29), datetime.datetime.now()]
    provinces = array_from_query(dbc, 'select distinct(province_ZH) from [3g_dxy_cn_province];')
    for province in provinces:
        province_en = value_from_query(dbc, 'select distinct(ADM1_EN) from china_places where ADM1_ZH like \'{}%\';'.format(province))
        confirmed = dict_from_query(dbc, 'select iso_date, confirmed from [3g_dxy_cn_province] where province_ZH like \'{}\' order by timestamp;'.format(province))
        dead = dict_from_query(dbc, 'select iso_date, dead from [3g_dxy_cn_province] where province_ZH like \'{}\' order by timestamp;'.format(province))
        cured = dict_from_query(dbc, 'select iso_date, cured from [3g_dxy_cn_province] where province_ZH like \'{}\' order by timestamp;'.format(province))

        print(province, province_en)
        dates = []
        for date_str in confirmed.keys():
            date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            dates.append(date)

        #print (dates)
        fig, ax = plt.subplots()
        fig.suptitle('NovelCoronaVirus Cases for {}'.format(province))
        ax.set_title('Date')
        ax.plot(dates, confirmed.values(), label='confirmed')
        ax.plot(dates, dead.values(), label='dead')
        ax.plot(dates, cured.values(), label='cured')
        ax.set(xlabel='Date', xlim = axis_range, ylabel='Reported Cases')
        ax.legend()
        fig.savefig('plots/{}.png'.format(province_en), format = 'png')

        plt.close()
        exit()

    return 0


def main():
    # main body
    """
    Go through the download dir and collect all of the various data sources:
    """

    if (FIRSTRUN):
        make_tables()
        read_hksarg_pr()
        read_china_places()
        read_3g_dxy_cn_json()
    else:
        make_plots()

    return 0

if __name__ == '__main__':
    VERBOSE = 1
    FIRSTRUN = 0
    DATADIR = '01_download_data'
    
    db_connect = sqlite3.connect('ncorv2019.sqlite')
    dbc = db_connect.cursor()

    main()

    #tidy up and shut down
    dbc.close()

