#!/usr/bin/env python3
"""
Gather all of the saved data from the NCorV 2019 outbreak and
get them into an SQLITE3 database som we can plot them.

dave o'brien (c) 2020

CC: BY-SA
"""
import sys
import os
import re
import sqlite3
import json
import datetime
from db_helper import * # This is a library of my own database routines - it just 
                        # wraps sqlite commands into handier methods I like
import matplotlib.pyplot as plt
import matplotlib as mpl
#import pandas as pd
#import numpy as np

def make_tables():
    # Make the Database tables from the JSON
    """
        A province looks like this:
            "provinceShortName":"湖北",
            "comment":"湖北省卫健委尚未发布昨日确诊数据，武汉确诊数据及治愈死亡数据来自国家卫健委。",
            "deadCount":2250,
            "confirmedCount":63454,
            "provinceName":"湖北省",
            "curedCount":13557,
            "suspectedCount":0,
            "locationId":420000"
        A City looks like this:
            "curedCount":222,
            "suspectedCount":0,
            "cityName":"深圳",
            "locationId":440300,
            "deadCount":2,
            "confirmedCount":417,       # Total of Confirmed, Dead, Cured
            "currentConfirmedCount":193
    """

    tabledefs = {
        'hksarg': ('timestamp text Unique Primary Key, New Integer, Total Integer, '
                   'Cured Integer, Remain Integer, Stable Integer, Serious Integer, '
                   'Critical Integer, Confirmed Integer, Dead Integer'),
        'cn_prov': ('Timestamp Integer, ISO_Date Text, '
                    'ProvinceName Text, Province_EN Text, confirmedCount Integer, '
                    'suspectedCount Integer, deadCount Integer, curedCount Integer, '
                    'LocationID Integer, Comment Text, provinceShortName Text, '
                    'currentConfirmedCount Integer, statisticsData Text'),
        'cn_city': ('Timestamp Integer, ISO_Date Text, '
                    'ProvinceName Text, Province_EN Text, CityName Text, City_EN Text, '
                    'currentConfirmedCount Integer, suspectedCount Integer, '
                    'deadCount Integer, LocationID Integer, confirmedCount Integer, '
                    'curedCount Integer'),
        'jhu_git': ('Timestamp Integer, Date Text, '
                    'Country Text, Province Text, Last_Update Text, '
                    'Confirmed Integer, Deaths Integer, Recovered Integer, '
                    'Latitude Real, Longitude Real'),
        'places': ('OBJECTID Int UNIQUE Primary Key, ADMIN_TYPE Text, ADM2_CAP Text, '
                   'ADM2_EN Text, ADM2_ZH Text, ADM2_PCODE Text, ADM1_EN Text, '
                   'ADM1_ZH Text, ADM1_PCODE Text, ADM0_EN Text, ADM0_ZH Text, '
                   'ADM0_PCODE Text'),
        'files': ('filename Text, dateProcessed Text')
            }

    make_tables_from_dict(dbc, tabledefs, VERBOSE)

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
        
        sqlcmd = 'INSERT OR IGNORE INTO [hksarg] (Timestamp, New, Total, Cured, Remain, Stable, Serious, Critical, Confirmed, Dead) Values (\"{}\", {});'.format(date, escaped)
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

        dbdo(dbc, 'INSERT into [places] ({}) Values ({})'.format(fields, values), VERBOSE)

    dbdo(dbc, 'COMMIT', VERBOSE)
    return 1

def read_3g_dxy_cn_json():
    """
    Read in the JSON data from 3G.DXY.CN and add it to the database.
    we're mainly interested in provincial growth.
    """
    files = os.listdir(DATADIR)
    areastat = re.compile(r'^([0-9]{8})_([0-9]{6})_getAreaStat.json$')
    already_processed = list_from_query(dbc, 'select filename from files;') 
    for filename in files:
        match = areastat.match(filename)

        if match and not(filename in already_processed):
            dbdo(dbc, 'BEGIN', VERBOSE)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dbdo(dbc, 'INSERT INTO [files] (Filename, DateProcessed) Values ("{}", "{}")'.format(filename, now), VERBOSE)
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
            pfields_base = 'Timestamp, ISO_Date, ProvinceName, Province_EN'
            cfields_base = 'Timestamp, ISO_Date, ProvinceName, Province_EN, City_EN'
            #cfields = 'Timestamp, ISO_Date, Province_ZH, Province_EN, CityEN, CityName, Confirmed, Suspected, Cured, Dead, AllConfirmed, LocationID'
            for province in areastats:
                province_en = value_from_query(dbc, 'select distinct(ADM1_EN) from places where ADM1_ZH like \'{}%\';'.format(province['provinceName']))
                #print ('Province:', province.keys())
                values = '{}, "{}", "{}", "{}"'.format(
                            int(timestamp), iso_date, 
                            province['provinceName'], province_en)

                # Build up the string of Columns and values depending on what's in the JSON
                pfields = pfields_base
                print ('Province:', province.keys(), province)
                for key in province.keys():
                    if key != 'cities':
                        pfields += ', {}'.format(key)
                        if type(key) is str:
                            values += ', "{}"'.format(province[key])
                        else:
                            values += ', {}'.format(province[key])

                sql_cmd = 'INSERT into [cn_prov] ({}) Values ({})'.format(pfields, values)
                dbdo(dbc, sql_cmd, VERBOSE)
                #printlog (case_count)
                
                # Now do the same for every city in the province
                for city in province['cities']:
                    city_en = value_from_query(dbc, 'select distinct(ADM2_EN) from [places] where ADM2_ZH like \'{}%\';'.format(city['cityName']))
                    print ('City:', city.keys(), city)
                
                    # Build up the string of Columns and values depending on what's in the JSON
                    cfields = cfields_base
                    values = '{}, "{}", "{}", "{}", "{}"'.format(
                            int(timestamp), iso_date, province['provinceName'],
                            province_en, city_en)
                    for key in city.keys():
                        cfields += ', {}'.format(key)
                        if type(city[key]) is str:
                            values += ', "{}"'.format(city[key])
                        else: 
                            values += ', {}'.format(city[key])

                    sql_cmd = 'INSERT into [cn_city] ({}) Values ({})'.format(cfields, values)
                    dbdo(dbc, sql_cmd, VERBOSE)

            dbdo(dbc, 'COMMIT', VERBOSE)

    return 1

def read_jhu_data():
    datadir = r'./JHU_data/2019-nCoV/csse_covid_19_data/csse_covid_19_daily_reports'
    already_processed = list_from_query(dbc, 'select filename from files;') 
    files = os.listdir(datadir)
    #print (files)
    
    # precompile some regular expressions...
    namedate = re.compile(r'^([0-9]{2})-([0-9]{2})-([0-9]{4}).csv$')
    yankdate = re.compile(r'^([0-9]+)/([0-9]+)/([0-9]{2,4}) ([0-9]+):([0-9]+)$')
    altdate  = re.compile(r'^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})$')
    fixstate = re.compile(r'^"(.*?),\s+(.*)"') # replaces '"Tempe, AZ", a, b, c' with 'Tempe_AZ, a, b, c'
    fixprov  = re.compile(r'([A-Za-z]+)/([A-Za-z]+)')
    fixspcs  = re.compile(r'(\s+)')
    # There's been a bit of inconsistency with naming of countries
    # Make a dict to keep things consistent.
    # On March 10th, the naming has started to get a bit political
    normalise_countries ={'Republic of Ireland': 'Ireland', # Claiming Cases in the north for Ireland
                          'North Ireland': 'Ireland',       # makes a bit more sense - travel
                          ' Azerbaijan': 'Azerbaijan', # Spurious Spacing issue
                          'US': 'USA',
                          'Hong Kong': 'Hong Kong SAR', # This is actually a reasonable change
                          'Macau': 'Macau SAR',         # This is actually a reasonable change
                          'Macao SAR': 'Macau SAR',     # This is not
                          'Taipei and environs': 'Taiwan', # Taiwan is *not* a province of China
                          'occupied Palestinian territory': 'Palestine', # Pointless change
                          'Iran (Islamic Republic of)': 'Iran' # Are there multiple Irans?
                          }


    for filename in files:
        match = namedate.match(filename)
        
        if match and (filename not in already_processed):
            dbdo(dbc, 'BEGIN', VERBOSE)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            month, day, year = match[1], match[2], match[3]
            dbdo(dbc, 'INSERT INTO [files] (Filename, DateProcessed) Values ("{}", "{}")'.format(filename, now), VERBOSE)
            filedate = '{:04d}-{:02d}-{:02d}'.format(int(year), int(month), int(day))
            print (filedate, filename)
            with open('{}/{}'.format(datadir, filename), mode='r', encoding='utf-8-sig') as infile:
                lines=list(infile)
                # First Line - gives us the fieldnames
                line = fixprov.sub(r'\1', lines.pop(0))
                line = fixspcs.sub(r'_', line.rstrip())
                fields = ['timestamp', 'date']
                fields.extend(line.rstrip().split(r','))
                print (fields)

                for line in lines:
                    #print (line)
                    # American Provinces are "City, State" because Yanks, so that needs to be fixed
                    line = fixstate.sub(r'\1_\2', line.rstrip())
                    components = line.split(',')

                    # Normalise some of the inputs
                    for normalise in normalise_countries.keys():
                        if components[1] == normalise:
                            components[1] = normalise_countries[normalise]
                    # Date of last update:
                    # check which form the date is in: There's a MDY format and there's a proper ISO
                    update   = components[2] # always third
                    last_update = 'NULL' # So we can catch it if it falls 
                    match = yankdate.match(update)
                    if match:
                        #print(match)
                        year = int(match[3])
                        if year < 100:
                            year = 2000 + year
                            
                        last_update = datetime.datetime(year, int(match[1]), int(match[2]), int(match[4]), int(match[5]), 0)
                    else:
                        match = altdate.match(update)
                        if match:
                            #print (match)
                            last_update = datetime.datetime(int(match[1]), int(match[2]), int(match[3]), int(match[4]), int(match[5]), int(match[6]))
                    timestamp = last_update.strftime('%Y%m%d%H%M%S')
                    if last_update == 'NULL':    
                        print ('BARF!', isodate)
                        exit()
                    
                    #build up the values list
                    values = [timestamp,
                              '"{}"'.format(filedate),      # Date of Report file
                              '"{}"'.format(components[0]), # Region always first 
                              '"{}"'.format(components[1]), # Country always second
                              '"{}"'.format(last_update)    # Date of last report
                              ]

                    # if there's no coordinates, don't add them
                    for index in range(3,len(components)):
                        if components[index] == '':
                            components[index] = 0
                        else:
                            components[index] = float(components[index])
                            
                        values.append('{}'.format(components[index]))
                    
                        
                    #print (fields, values)
                    dbdo(dbc, 'insert into [jhu_git] ({}) Values ({});'.
                         format(','.join(fields), 
                                ','.join(values)), VERBOSE)
                        
            dbdo(dbc, 'COMMIT', VERBOSE)
            


    return len(files)

def make_plot(title, dates, confirmed, dead, cured):
    axis_range = [datetime.datetime(2020,1,29), datetime.datetime.now()]
    fig, ax = plt.subplots()
    fig.suptitle('NovelCoronaVirus Cases for {}'.format(title))
    ax.set_title('Date')
    ax.plot(dates, list(confirmed.values()), label='confirmed')
    ax.plot(dates, list(dead.values()), label='dead')
    ax.plot(dates, list(cured.values()), label='cured')
    ax.set(xlabel='Date', xlim = axis_range, ylabel='Reported Cases')
    ax.legend()
    fig.savefig('plots/{}.png'.format(title), format = 'png')

    plt.close()
    return 0;

def make_plots_from_dxy():
    """ 
    Make a plot by province:
    """
    provinces = list_from_query(dbc, 'select distinct(provinceName) from [cn_prov];')
    china_total_conf = {}
    china_total_date = {}
    china_total_cure = {}
    china_total_dead = {}
    chinadates = []
    for province in provinces:
        province_en = value_from_query(dbc, 'select distinct(ADM1_EN) from china_places where ADM1_ZH like \'{}%\';'.format(province))
        print(province, province_en)

        confirmed = dict_from_query(dbc, 'select iso_date, confirmedCount from [cn_prov] where provinceName like \'{}\' order by timestamp;'.format(province))
        dead = dict_from_query(dbc, 'select iso_date, deadCount from [cn_prov] where provinceName like \'{}\' order by timestamp;'.format(province))
        cured = dict_from_query(dbc, 'select iso_date, curedCount from [cn_prov] where provinceName like \'{}\' order by timestamp;'.format(province))
        dates = []

        #add each province to the China total
        for key in confirmed.keys():
            #print (key, confirmed[key], cured[key], dead[key], '\n')
            china_total_conf[key] = china_total_conf.setdefault(key, 0) + confirmed[key]
            china_total_cure[key] = china_total_cure.setdefault(key, 0) + cured[key]
            china_total_dead[key] = china_total_dead.setdefault(key, 0) + dead[key]
            china_total_date[key] = china_total_date.setdefault(key, 0) + 1
            isodate = datetime.datetime.strptime(key, '%Y-%m-%d %H:%M:%S')
            dates.append(isodate)
            if isodate not in chinadates:
                chinadates.append(isodate)

            #time = datetime.datetime.strptime(date_str, '%H:%M:%S')

        #print (type(dates), dates)
        make_plot(province_en, dates, confirmed, dead, cured)


    #print (type(chinadates), chinadates)

    make_plot('GreaterChina', chinadates, china_total_conf, china_total_dead, china_total_cure)
    return 0

def make_plots_from_jhu():
    """ 
    Make a plot by Country:
    """
    countries = list_from_query(dbc, 'select distinct(country) from [jhu_git];')
    countries.append('World')
    style_list = ['default', 'classic'] + sorted(style for style in plt.style.available if style != 'classic')
    print (style_list)

    plt.style.use('seaborn-paper')
    plot_style_index = 10
    plot_style_index_max = len(style_list)

    #Some Generics
    FIGSIZE=[9,6]
    FACTOR = 0.001 # 0.1% of World Values
    start_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date ASC  limit 1')
    final_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date DESC limit 1')
    max_cases = value_from_query(dbc, 'SELECT confirmed from [world] order by Date DESC limit 1')
    axis_range = [datetime.datetime.strptime(start_date_str + ' 00:00', '%Y-%m-%d %H:%M'), 
                  datetime.datetime.strptime(final_date_str + ' 23:59', '%Y-%m-%d %H:%M')]
    
    all_fig, all_ax = plt.subplots(figsize=FIGSIZE)
    all_fig.suptitle('SARS2/2019_NCorV (COVID19) for All Reporting Countries')
    all_ax.set(title = 'All Countries')
    all_ax.set(xlabel='Reporting Date', xlim = axis_range, ylabel='Reported Cases')

    
    for country in countries:
        date_strs = list_from_query(dbc, 'SELECT Date from [{}] order by Date'.format(country))
        conf = list_from_query(dbc, 'SELECT Confirmed from [{}] order by Date'.format(country))
        sick = list_from_query(dbc, 'SELECT (Confirmed-Dead-Cured) from [{}] order by Date'.format(country))
        dead = list_from_query(dbc, 'SELECT Dead from [{}] order by Date'.format(country))
        cure = list_from_query(dbc, 'SELECT Cured from [{}] order by Date'.format(country))
        cfr = list_from_query(dbc, 'SELECT Cfr from [{}] order by Date'.format(country))
        crr = list_from_query(dbc, 'SELECT Crr from [{}] order by Date'.format(country))

        print (country, conf[-1], style_list[plot_style_index])
        
        # Get datetime objects for the dates
        dates = []
        for date in date_strs:
            dates.append(datetime.datetime.strptime(date, '%Y-%m-%d %H:%M'))

        #plt.style.use(style_list[plot_style_index])
        #plot_style_index += 1
        #if plot_style_index >= plot_style_index_max:
        #    plot_style_index = 0

        fig, ax = plt.subplots(figsize=FIGSIZE)
        fig.suptitle('SARS2/2019_NCorV (COVID 2019) for {}'.format(country))
        # Primary Axis
        ax.set(title = 'Reported Cases (JHU CSSE Data.)'.format(style_list[plot_style_index]))
        ax.set(xlabel='Date', xlim = axis_range, ylabel='Reported Cases')
        ax.stackplot(dates, sick, cure, dead, labels=['Still Sick', 'cured', 'dead'])
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.legend(loc='upper left')
        
        # Secondary Axis
        ax2 = ax.twinx()
        ax2.plot(dates, cfr, label='Case Fatality Rate', color='red')
        ax2.plot(dates, crr, label='Case Recovery Rate', color='blue')
        ax2.set(ylim=(0.0,100.0), ylabel='Percentage')
        ax2.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.1f}%'))
        ax2.legend(loc='center left')

        fig.savefig('plots/{}.png'.format(country), format = 'png')
        
         # Add to the all countries plot
        if (country != 'World' and conf[-1] >= FACTOR * max_cases):
            all_ax.plot(dates, conf, label=country)
            
        
        plt.close()
    all_ax.legend(loc='upper left')
    all_fig.savefig('plots/All_Confirmed.png', format = 'png')
    return 0

class Report:
    def __init__(self, confirmed, dead, cured):
        """ confirmed is all confirmed cases, it *includes* cured and dead."""
        self.confirmed = confirmed
        self.dead      = dead
        self.cured     = cured
        if confirmed > 0:
            self.cfr = float(dead / confirmed)  # Case Fatality Rate
            self.crr = float(cured / confirmed) # Case Recovery Rate
        else:
            self.cfr = 0.00
            self.crr = 0.00

    def update(self, confirmed, dead, cured):
        self.confirmed = confirmed
        self.dead      = dead
        self.cured     = cured
        if confirmed > 0:
            self.cfr = float(dead / confirmed)  # Case Fatality Rate
            self.crr = float(cured / confirmed) # Case Recovery Rate
        else:
            self.cfr = 0.00
            self.crr = 0.00
        return list(self.confirmed, self.dead, self.cured, self.cfr, self.crr)

    def get_all(self):
        return list(self.confirmed, self.dead, self.cured, self.cfr, self.crr)
    
    def __str__(self):
        return '{}, {}, {}'.format(self.confirmed, self.dead, self.cured)

def make_summary_tables():
    """
        Make Tables from JHU data of:
            1. All confirmed, dead, recovered by date
            2. Each Country by Date (sum up provinces)
            3. Each WHO Region by date
    """
    tablespec = ('Date Text, '
                 'Confirmed Integer, '
                 'Dead Integer, '
                 'Cured Integer, '
                 'CFR Real, CRR Real')
    countries = list_from_query(dbc, 'select distinct(country) from [jhu_git];')
    for country in countries:
        dbdo(dbc, "BEGIN", VERBOSE)
        dbdo(dbc, 'DROP TABLE IF EXISTS [{}]'.format(country), VERBOSE)
        dbdo(dbc, 'CREATE TABLE [{}] ({})'.format(country, tablespec), VERBOSE)
        provinces = list_from_query(dbc, 'select distinct(province) from [jhu_git] where country = \"{}\"'.format(country))
        # Make a Table of all the provinces
        country_dates = {}
        for province in provinces:
            dbdo(dbc, 'DROP TABLE IF EXISTS [{}.{}]'.format(country, province), VERBOSE)
            # Create the Provincial Tables (also need to split the ISO date into Date and Time)
            dbdo(dbc, 
                 ('CREATE TABLE [{C}.{P}] AS '
                  '  SELECT date || \' 17:00\' AS Date, last_update, confirmed, deaths, recovered '
                  '  FROM [jhu_git] where country = \'{C}\' and province = \'{P}\'').format(C=country, P=province), VERBOSE)
            province_dates = list_from_query(dbc, 'Select date from [{}.{}]'.format(country, province))
            for date in province_dates:
                country_dates[date] = country_dates.setdefault(date, 0) + 1
            #print (country, provinces, province_iso_dates)

        for date in country_dates:
            country_conf = 0
            country_dead = 0
            country_cure = 0
            for province in provinces:
                row = row_from_query(
                    dbc, 
                    ('select date, confirmed, deaths, recovered from [{}.{}] '
                     'where date like \'{}%\'').format(country, province, date))
                #print (country, province, date, row)
                if row is not None:
                    date = row[0]
                    country_conf += row[1]
                    country_dead += row[2]
                    country_cure += row[3]

            if country_conf > 0:        
                country_cfr = 100 * float(country_dead / country_conf) # Case Fatality Rate
                country_crr = 100 * float(country_cure / country_conf) # Case Recovery Rate
            else:
                country_cfr = 0.00
                country_crr = 0.00

            #print (date, country, provinces, len(provinces), country_conf, country_dead, country_cure, country_cfr, country_crr)
            dbdo(dbc, 
                 ('INSERT into [{}] (Date, confirmed, dead, cured, CFR, CRR) '
                  'Values(\'{}\', {}, {}, {}, {}, {})').format(country, date, country_conf, country_dead, country_cure, country_cfr, country_crr), VERBOSE)
        if (len(provinces)) == 1:
            dbdo(dbc, 'DROP TABLE IF EXISTS [{}.{}]'.format(country, provinces[0]), VERBOSE)
            
        dbdo(dbc, 'COMMIT', VERBOSE)
    
    # Make the master Table of all Countries
    dbdo(dbc, 'BEGIN', VERBOSE)
    dbdo(dbc, 'DROP TABLE IF EXISTS [World]', VERBOSE)
    dbdo(dbc, 'CREATE TABLE [World] ({})'.format(tablespec), VERBOSE)
    dates = list_from_query(dbc, 'select distinct(Date) from [jhu_git]')
    for date in dates:
        world_conf = 0
        world_dead = 0
        world_cure = 0
        for country in countries:
            row = row_from_query(
                dbc, 
                ('select date, confirmed, dead, cured from [{}] '
                    'where date like \'{}%\'').format(country, date))
            #print (country, date, row)
            if row is not None:
                date = row[0]
                world_conf += row[1]
                world_dead += row[2]
                world_cure += row[3]

        if world_conf > 0:        
            world_cfr = 100 * float(world_dead / world_conf)
            world_crr = 100 * float(world_cure / world_conf)
        else:
            world_cfr = 0.00
            world_crr = 0.00

        #print (date, country,  world_conf, world_dead, world_cure, world_cfr, world_crr)
        dbdo(dbc, 
                ('INSERT into [World] (Date, confirmed, dead, cured, CFR, CRR) '
                'Values(\'{}\', {}, {}, {}, {}, {})').format(date, world_conf, world_dead, world_cure, world_cfr, world_crr), VERBOSE)
    
    #dbdo(dbc, 'DROP TABLE IF EXISTS [World without China]', VERBOSE)
    #dbdo(dbc, 'CREATE TABLE [World without China] ({})'.format(tablespec), VERBOSE)
    
    dbdo(dbc, 'COMMIT', VERBOSE)
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
        read_jhu_data()
    else:
        read_jhu_data()
        read_3g_dxy_cn_json()
        make_summary_tables()
        #make_plots_from_dxy()
        make_plots_from_jhu()

    return 0

if __name__ == '__main__':
    VERBOSE = 1
    FIRSTRUN = 0
    DATADIR = '01_download_data'
    for arg in sys.argv:
        if arg == 'VERBOSE':
            VERBOSE = 1
        if arg == 'FIRSTRUN':
            FIRSTRUN = 1
    
    db_connect = sqlite3.connect('ncorv2019.sqlite')
    dbc = db_connect.cursor()

    main()

    #tidy up and shut down
    dbc.close()

