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
import matplotlib.dates as mdates # for date formatting
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
                    'FIPS Integer, Admin2 Text, Country Text, Province Text, '
                    'Last_Update Text, '
                    'Confirmed Integer, Deaths Integer, Recovered Integer, Active Integer, '
                    'Latitude Real, Longitude Real, Combined_Key Text'),
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
    print ('Reading 3G+DXY.CN data')
    for filename in files:
        match = areastat.match(filename)

        if match and not(filename in already_processed):
            dbdo(dbc, 'BEGIN', VERBOSE)
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
                #print ('Province:', province.keys(), province)
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
                    #print ('City:', city.keys(), city)
                
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

            # Only add to the database on success addition
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dbdo(dbc, 'INSERT INTO [files] (Filename, DateProcessed) Values ("{}", "{}")'.format(filename, now), VERBOSE)
            dbdo(dbc, 'COMMIT', VERBOSE)

    return 1

def read_jhu_data():
    datadir = r'./JHU_data/2019-nCoV/csse_covid_19_data/csse_covid_19_daily_reports'
    already_processed = list_from_query(dbc, 'select filename from files;') 
    files = os.listdir(datadir)
    #print (files)
    
    print ('Reading JHU CSSE data')
    # precompile some regular expressions...
    namedate = re.compile(r'^([0-9]{2})-([0-9]{2})-([0-9]{4}).csv$')
    yankdate = re.compile(r'^([0-9]+)/([0-9]+)/([0-9]{2,4}) ([0-9]+):([0-9]+)$')
    altdate  = re.compile(r'^([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{2}):([0-9]{2}):([0-9]{2})$')
    fixstate = re.compile(r'"(.*?),\s*(.*?)"') # replaces '"Tempe, AZ", a, b, c' with 'Tempe_AZ, a, b, c'
    fixcckey = re.compile(r'"([A-Za-z. ]+?),\s*([A-Za-z. ]+?),\s*([A-Za-z. ]+?)"') # replaces '""Alleghany, North Carolina, US", a, b, c' with 'Alleghany.North.Carolina.US, a, b, c'
    fixprov  = re.compile(r'([A-Za-z]+)/([A-Za-z]+)')
    fixspcs  = re.compile(r'(\s+)')
    # There's been a bit of inconsistency with naming of countries
    # Make a dict to keep things consistent.
    # On March 10th, the naming has started to get a bit political
    normalise_countries ={'Republic of Ireland': 'Ireland', # Now that the UK has had such a bad response
                          'North Ireland': 'United Kingdom',# they can own the north too.
                          ' Azerbaijan': 'Azerbaijan', # Spurious Spacing issue
                          'US': 'USA',
                          'UK': 'United Kingdom',
                          'Mainland China': 'China', 
                          'Hong Kong SAR': 'Hong Kong', # stick with initial usage
                          'Macao SAR': 'Macau',         # Stick with initial usage
                          'Taipei and environs': 'Taiwan', # Taiwan, just Taiwan 
                          'Taiwan*': 'Taiwan',          # Taiwan 
                          'occupied Palestinian territory': 'Palestine', # Pointless change
                          'Iran (Islamic Republic of)': 'Iran',# Are there multiple Irans?
                          'Holy See': 'Vatican City',
                          'Viet Nam': 'Vietnam',
                          'Korea_South': 'South Korea',
                          'Cote d\'Ivoire': 'Ivory Coast'
                          }
    normalise_fields ={'Country_Region': 'Country',
                       'Province_State': 'Province',
                       'Lat': 'Latitude',
                       'Long_': 'Longitude'
                          }

    for filename in files:
        match = namedate.match(filename)
        
        if match and (filename not in already_processed):
            dbdo(dbc, 'BEGIN', VERBOSE)
            month, day, year = match[1], match[2], match[3]
            filedate = '{:04d}-{:02d}-{:02d}'.format(int(year), int(month), int(day))
            print (filedate, filename)
            with open('{}/{}'.format(datadir, filename), mode='r', encoding='utf-8-sig') as infile:
                lines=list(infile)
                # First Line - gives us the fieldnames
                line = fixprov.sub(r'\1', lines.pop(0))
                line = fixspcs.sub(r'_', line.rstrip())
                fields = ['timestamp', 'date']
                line_fields = line.rstrip().split(r',')
                # normalise the fields
                nfields = []
                for field in line_fields:
                    if field in normalise_fields.keys():
                        nfields.append(normalise_fields[field])
                    else:
                        nfields.append(field)
                
                # The fields can change (23 March 2020), so need to have a more robust way of
                # handling them
                fields.extend(nfields)
                print (line_fields, fields)

                for line in lines:
                    print (line)
                    # American Provinces are "City, State" because Yanks, so that needs to be fixed
                    line = fixcckey.sub(r'\1_\2_\3', line.rstrip())
                    print (line)
                    line = fixstate.sub(r'\1_\2', line.rstrip())
                    print (line)
                    line_data = line.split(',')
                    line_dict = {}
                    for key, value in zip(nfields, line_data):
                        line_dict[key] = value
                    print (line_dict)
                    
                    # Normalise some of the inputs
                    for normalise in normalise_countries.keys():
                        if line_dict['Country'] == normalise:
                            line_dict['Country'] = normalise_countries[normalise]

                    if line_dict['Country'] == 'China' and line_dict['Province'] == 'Hong Kong':
                            line_dict['Country'] = 'Hong Kong'
                    if line_dict['Country'] == 'China' and line_dict['Province'] == 'Macau':
                            line_dict['Country'] = 'Macau'
                    # Date of last update:
                    # check which form the date is in: There's a MDY format and there's a proper ISO
                    update   = line_dict['Last_Update']
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
                            print (match)
                            last_update = datetime.datetime(int(match[1]), int(match[2]), int(match[3]), int(match[4]), int(match[5]), int(match[6]))
                    timestamp = last_update.strftime('%Y%m%d%H%M%S')
                    if last_update == 'NULL':    
                        print ('BARF!', update)
                        exit()
                    
                    # if there's empty fields, set them to zero
                    for key in line_dict.keys():
                        if line_dict[key] == '':
                            line_dict[key] = 0
                    
                    #build up the values list
                    values = [timestamp, '"{}"'.format(filedate)]
                    for key in line_dict.keys():
                        if key in ['Lat, Long_']:
                            line_dict[key] = float(line_dict[key])
                        
                        if type(line_dict[key]) == str:
                            values.append('"{}"'.format(line_dict[key]))
                        else:
                            values.append('{}'.format(line_dict[key]))
                        
                    
                        
                    #print (fields, values)
                    dbdo(dbc, 'insert into [jhu_git] ({}) Values ({});'.
                         format(','.join(fields), 
                                ','.join(values)), VERBOSE)
                        
            # Only add to the database on success addition
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dbdo(dbc, 'INSERT INTO [files] (Filename, DateProcessed) Values ("{}", "{}")'.format(filename, now), VERBOSE)
            dbdo(dbc, 'COMMIT', VERBOSE)

    return len(files)

def make_plot(title, dates, confirmed, dead, cured):
    axis_range = [datetime.datetime(2020,1,29), datetime.datetime.now()]
    fig, ax = plt.subplots()
    fig.suptitle('NovelCoronaVirus Cases for {}'.format(title))
    ax.set_title('Date')
    ax.plot(dates, list(cured.values()), label='Recovered')
    ax.plot(dates, list(confirmed.values()), label='Confirmed')
    ax.plot(dates, list(dead.values()), label='Deaths')
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

def list_of_countries_by_confirmed(final_date_str):
    countries = list_from_query(dbc, 'select distinct(country) from [jhu_git] where date like \'{}%\';'.format(final_date_str))
    country_count = {}
    for country in countries:
        country_count[country] = value_from_query(dbc, 'SELECT max(confirmed) from [{}]'.format(country))
    # Create a list of tuples sorted by index 1 i.e. value field     
    country_tuples = sorted(country_count.items() , reverse=True, key=lambda x: x[1])
    # Iterate over the sorted sequence
    countries_by_confirmed = []
    for elem in country_tuples :
        countries_by_confirmed.append(elem[0])
    countries_by_confirmed.append('World')
    return countries_by_confirmed


def keys_values_as_list_from_dict(dict):    
    keys = [key for key in dict.keys()]
    values = []
    keys.sort()
    for key in keys:
        values.append(dict[key])
    return keys, values

def make_days_since_start_plot():
    #Make the rate of increase since 100 cases, plot
    box = dict(boxstyle = 'square', fc='#ffffff80')
    attrib_str = r'plot produced by @odaiwai using MatPlotLib, Python and SQLITE3. Data from JHU CSSE. https://www.diaspoir.net/'
    attrib_box = dict(boxstyle = 'square', fc='#ffffff80', pad = 0.25)

    limitc = 16#MINCASES
    limitd = 1
    limitr = 1
    
    max_cases = value_from_query(dbc, 'SELECT confirmed from [world] order by Date DESC limit 1')
    final_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date DESC limit 1')
    countries = list_of_countries_by_confirmed(final_date_str)

    max_days = value_from_query(dbc, 'SELECT max(days_since_start) from [China] order by Date DESC limit 1')
    plt.style.use('seaborn-paper')
    
    axis_range = [limitd,max_days+7] # fixme later
    since100c_fig, since100c_ax = plt.subplots(figsize=FIGSIZE)
    since100c_fig.suptitle('SARS2-CoV / COVID19 for Major Reporting Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
    since100c_ax.set(title = 'Confirmed cases since no. {}'.format(limitc))
    since100c_ax.set(xlabel='Days since {} confirmed cases'.format(limitc), xlim = axis_range, ylabel='Reported Cases (includes Recoveries and Deaths)')
    since100c_ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    since100c_ax.set_yscale('log', basey = 10)
    since100c_fig.autofmt_xdate()
    
    since100d_fig, since100d_ax = plt.subplots(figsize=FIGSIZE)
    since100d_fig.suptitle('SARS2-CoV / COVID19 for Major Reporting Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
    since100d_ax.set(title = 'Fatalities since no. {}'.format(limitd))
    since100d_ax.set(xlabel='Days since {} Deaths'.format(limitd), xlim = axis_range, ylabel='Deaths')
    since100d_ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    since100d_ax.set_yscale('log', basey = 10)
    since100d_fig.autofmt_xdate()

    since100r_fig, since100r_ax = plt.subplots(figsize=FIGSIZE)
    since100r_fig.suptitle('SARS2-CoV / COVID19 for Major Reporting Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
    since100r_ax.set(title = 'Recoveries since no. {}'.format(limitr))
    since100r_ax.set(xlabel='Days since {} Recoveries'.format(limitr), xlim = axis_range, ylabel='Deaths')
    since100r_ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    since100r_ax.set_yscale('log', basey = 10)
    since100r_fig.autofmt_xdate()
    
    for country in countries:
        conf_d = dict_from_query(dbc, 
                                 'SELECT ROW_NUMBER() OVER (PARTITION BY confirmed > {L} order by date) as days, '
                                 'confirmed from [{C}] where confirmed > {L} order by Date'.format(C = country, L = limitc))
        dead_d = dict_from_query(dbc, 
                                 'SELECT ROW_NUMBER() OVER (PARTITION BY deaths > {L} order by date) as days, '
                                 ' deaths from [{C}] where deaths > {L} and days_since_start > 0 order by Date'.format(C=country, L=limitd))
        reco_d = dict_from_query(dbc, 
                                 'SELECT ROW_NUMBER() OVER (PARTITION BY Recovered > {L} order by date) as days, '
                                 ' Recovered from [{C}] where Recovered > {L} and days_since_start > 0 order by Date'.format(C=country, L=limitr))

        daysc, conf = keys_values_as_list_from_dict(conf_d)
        daysd, dead = keys_values_as_list_from_dict(dead_d)
        daysr, reco = keys_values_as_list_from_dict(reco_d)
        if len(daysd) > 0:
            since100c_ax.plot(daysc, conf)
            since100c_ax.plot([daysc[-1]], [conf[-1]], marker='o', markersize=3)
            since100d_ax.plot(daysd, dead)
            since100d_ax.plot([daysd[-1]], [dead[-1]], marker='o', markersize=3)
        if len(daysr) > 0:
            since100r_ax.plot(daysr, reco)
            since100r_ax.plot([daysr[-1]], [reco[-1]], marker='o', markersize=3)
        if country in ['Hong Kong', 'Singapore', 'China', 'Italy', 'South Korea', 'USA', 'Germany', 'United Kingdom', 'Ireland', 'France', 'Poland', 'Japan', 'Spain', 'Taiwan', 'Vietnam', 'Thailand', 'Australia', 'Malaysia']:
            if len(daysd) > 0:
                since100c_ax.annotate('{}: {:,.0f}'.format(country, conf[-1]), (daysc[-1]+1, conf[-1]), fontsize = 8, ha='left', bbox = box)
                since100d_ax.annotate('{}: {:,.0f}'.format(country, dead[-1]), (daysd[-1]+1, dead[-1]), fontsize = 8, ha='left', bbox = box)
            if len(daysr) > 0:
                since100r_ax.annotate('{}: {:,.0f}'.format(country, reco[-1]), (daysr[-1]+1, reco[-1]), fontsize = 8, ha='left', bbox = box)
            
    since100c_fig.text(0.5, 0.01, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
    since100d_fig.text(0.5, 0.01, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
    since100r_fig.text(0.5, 0.01, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
    
    since100c_fig.savefig('plots/Confirmed_since_start.png', format = 'png')
    since100d_fig.savefig('plots/Dead_since_start.png', format = 'png')
    since100r_fig.savefig('plots/Recovered_since_start.png', format = 'png')
    return 0

def make_plots_from_jhu():
    """ 
    Make a plot by Country:
    """
    start_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date ASC  limit 1')
    final_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date DESC limit 1')
    countries = list_of_countries_by_confirmed(final_date_str)
    plt.style.use('seaborn-paper')
    # styles
    box = dict(boxstyle = 'square', fc='#ffffff40')
    attrib_str = r'plot produced by @odaiwai using MatPlotLib, Python and SQLITE3. Data from JHU CSSE. https://www.diaspoir.net/'
    attrib_box = dict(boxstyle = 'square', fc='#ffffff80', pad = 0.25)
    
    max_cases = value_from_query(dbc, 'SELECT confirmed from [world] order by Date DESC limit 1')
    axis_range = [datetime.datetime.strptime(start_date_str + ' 00:00', '%Y-%m-%d %H:%M'), 
                  datetime.datetime.strptime(final_date_str + ' 17:00', '%Y-%m-%d %H:%M')]
    
    allconf_fig, allconf_ax = plt.subplots(figsize=FIGSIZE)
    allconf_fig.suptitle('SARS2-CoV /COVID 19 for Major Reporting Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
    allconf_ax.set(title = 'All Countries')
    allconf_ax.set(xlabel='Reporting Date', xlim = axis_range, ylabel='Reported Cases (includes Recoveries and Deaths)')
    allconf_ax.format_data = mdates.DateFormatter('%Y-%m-%d')
    allconf_ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    allconf_fig.autofmt_xdate()

    allcfr_fig, allcfr_ax = plt.subplots(figsize=FIGSIZE)
    allcfr_fig.suptitle('SARS2-CoV / COVID19 for Major Reporting Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
    allcfr_ax.set(title = 'All Countries')
    allcfr_ax.set(xlabel='Reporting Date', xlim = axis_range, ylabel='Case Fatality Rates', ylim=(0,20))
    allcfr_ax.format_data = mdates.DateFormatter('%Y-%m-%d')
    allcfr_ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.2f}%'))
    allcfr_fig.autofmt_xdate()

    for country in countries:
        date_strs = list_from_query(dbc, 'SELECT Date from [{}] order by Date'.format(country))
        conf = list_from_query(dbc, 'SELECT Confirmed from [{}] order by Date'.format(country))
        sick = list_from_query(dbc, 'SELECT (Confirmed-deaths-recovered) from [{}] order by Date'.format(country))
        dead = list_from_query(dbc, 'SELECT deaths from [{}] order by Date'.format(country))
        cure = list_from_query(dbc, 'SELECT Recovered from [{}] order by Date'.format(country))
        cfr  = list_from_query(dbc, 'SELECT CFR from [{}] order by Date'.format(country))
        crr  = list_from_query(dbc, 'SELECT CFR from [{}] order by Date'.format(country))

        print (country, conf[-1], cure[-1], sick[-1], dead[-1], cfr[-1])
        
        # Get datetime objects for the dates
        dates = []
        for date in date_strs:
            dates.append(datetime.datetime.strptime(date, '%Y-%m-%d %H:%M'))
        this_axis_range = [dates[0], dates[-1]]
        if this_axis_range[0] == this_axis_range[1]:
            this_axis_range[0] = this_axis_range[1] - datetime.timedelta(days = 1)
        #plt.style.use(style_list[plot_style_index])
        #plot_style_index += 1
        #if plot_style_index >= plot_style_index_max:
        #    plot_style_index = 0

        fig, ax = plt.subplots(figsize=FIGSIZE)
        fig.suptitle('SARS2-CoV / COVID 19 for {}'.format(country))
        # Primary Axis
        ax.set(title = '{:,.0f} Reported Cases (JHU CSSE Data.)'.format(conf[-1]))
        ax.set(xlabel='Date', xlim = this_axis_range, ylabel='Reported Cases')
        ax.format_data = mdates.DateFormatter('%Y-%m-%d')
        fig.autofmt_xdate()
        ax.stackplot(dates, cure, sick, dead, labels=['Recovered', 'Sick', 'Deaths'], colors=['green', 'orange', 'black'])
        labelx = dates[len(dates)-2]
        ax.annotate('Recovered {:,.0f}'.format(cure[-1]), (labelx, cure[-1]/2), fontsize = 8, ha='right', bbox = box)
        ax.annotate('Sick {:,.0f}'.format(sick[-1]), (labelx, cure[-1] + conf[-1]/2 - dead[-1]), fontsize = 8, ha='right', bbox = box)
        ax.annotate('deaths {:,.0f}'.format(dead[-1]), (labelx, conf[-1] - dead[-1]/2), fontsize = 8, ha='right', bbox = box)
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.legend(loc='upper left')
        
        # Secondary Axis
        ax2 = ax.twinx()
        ax2.plot(dates, cfr, label='Case Fatality Rate', color='red')
        #ax2.plot(dates, crr, label='Case Recovery Rate', color='blue')
        ax2.annotate('CFR {:,.1f}%'.format(cfr[-1]), (labelx, cfr[-1]), fontsize = 8, ha='left', bbox = box)
        #ax2.annotate('CRR {:,.1f}%'.format(crr[-1]), (labelx, crr[-1]), ha='right', bbox = box)
        ax2.set(ylim=(0.0,15.0), ylabel='Percentage')
        ax2.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.1f}%'))
        ax2.legend(loc='lower left')
        fig.text(0.5, 0.01, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
    
        fig.savefig('plots/{}.png'.format(country), format = 'png')
        
         # Add to the all countries plot
        if (country != 'World' and conf[-1] >= FACTOR * max_cases):
            allconf_ax.plot(dates, conf, label='{}: {:,.0f}'.format(country, conf[-1]))
            allconf_ax.annotate('{}'.format(country), (labelx, conf[-1]), fontsize = 8, ha='left', bbox = box)
            allcfr_ax.plot(dates, cfr, label='{}: {:,.2f}%'.format(country, cfr[-1]))
            allcfr_ax.annotate('{}'.format(country), (labelx, cfr[-1]), fontsize = 8, ha='left', bbox = box)
            
        
        plt.close()
    allconf_ax.legend(loc='best', ncol = 2, fontsize = 8)
    allconf_fig.text(0.5, 0.01, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
    allconf_fig.savefig('plots/All_Confirmed.png', format = 'png')

    allcfr_ax.legend(loc='best', ncol = 2, fontsize = 8)
    allcfr_fig.text(0.5, 0.01, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
    allcfr_fig.savefig('plots/All_CFR.png', format = 'png')
    return 0

class Report:
    def __init__(self, confirmed, deaths, cured):
        """ confirmed is all confirmed cases, it *includes* cured and deaths."""
        self.confirmed = confirmed
        self.deaths      = deaths
        self.cured     = cured
        if confirmed > 0:
            self.cfr = float(deaths / confirmed)  # Case Fatality Rate
            self.crr = float(cured / confirmed) # Case Recovery Rate
        else:
            self.cfr = 0.00
            self.crr = 0.00

    def update(self, confirmed, deaths, cured):
        self.confirmed = confirmed
        self.deaths      = deaths
        self.cured     = cured
        if confirmed > 0:
            self.cfr = float(deaths / confirmed)  # Case Fatality Rate
            self.crr = float(cured / confirmed) # Case Recovery Rate
        else:
            self.cfr = 0.00
            self.crr = 0.00
        return list(self.confirmed, self.deaths, self.cured, self.cfr, self.crr)

    def get_all(self):
        return list(self.confirmed, self.deaths, self.cured, self.cfr, self.crr)
    
    def __str__(self):
        return '{}, {}, {}'.format(self.confirmed, self.deaths, self.cured)

def make_summary_tables():
    """
        Make Tables from JHU data of:
            1. All confirmed, deaths, recovered by date
            2. Each Country by Date (sum up provinces)
            3. Each WHO Region by date
    """
    print ('Make Summary Tables')
    tablespec = ('Date Text, '
                 'Confirmed Integer, '
                 'Deaths Integer, '
                 'Recovered Integer, '
                 'Active Integer, '
                 'CFR Real, CRR Real, '
                 'days_since_start Integer, '
                 'new_cases_rate_1day Real, '
                 'new_cases_rate_7day Real')
    countries = list_from_query(dbc, 'select distinct(country) from [jhu_git];')
    for country in countries:
        dbdo(dbc, "BEGIN", VERBOSE)
        dbdo(dbc, 'DROP TABLE IF EXISTS [{}]'.format(country), VERBOSE)
        #dbdo(dbc, 'CREATE TABLE [{}] ({})'.format(country, tablespec), VERBOSE)
        provinces = list_from_query(dbc, 'select distinct(province) from [jhu_git] where country = \"{}\"'.format(country))
        # Make a Table of all the provinces
        country_dates = {}
        dbdo(dbc, 
                ('CREATE TABLE [{C}] AS '
                '  SELECT distinct(date) || \' 17:00\' AS Date, '
                '   sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, sum(Recovered) as Recovered,  '
                '   sum(Active) as Active, '
                '   ROW_NUMBER() OVER (PARTITION BY confirmed > {M} order by date ) as days_since_start, '
                '   (100*(CAST (sum(Deaths) as REAL) / sum(confirmed))) as CFR, '
                '   (100*(CAST (sum(Recovered) as REAL) / sum(confirmed))) as CRR '
                '  FROM [jhu_git] where country = \'{C}\' group by date order by date').format(C=country, M=MINCASES), VERBOSE)
        for province in provinces:
            dbdo(dbc, 'DROP TABLE IF EXISTS [{}.{}]'.format(country, province), VERBOSE)
            # Create the Provincial Tables (also need to split the ISO date into Date and Time)
            dbdo(dbc, 
                 ('CREATE TABLE [{C}.{P}] AS '
                  '  SELECT distinct(date) || \' 17:00\' AS Date, '
                  '   sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, sum(Recovered) as Recovered, '
                  '   sum(Active) as Active, '
                  '   ROW_NUMBER() OVER (PARTITION BY confirmed > {M} order by Date) as days_since_start, '
                  '   (100*(CAST (sum(Deaths) as REAL) / sum(confirmed))) as CFR, '
                  '   (100*(CAST (sum(Recovered) as REAL) / sum(confirmed))) as CRR '
                  '  FROM [jhu_git] where country = \'{C}\' and province = \'{P}\' group by date order by date').format(C=country, P=province, M=MINCASES), VERBOSE)

        dbdo(dbc, 'COMMIT', VERBOSE)

    # Make the master Table of all Countries
    dbdo(dbc, 'BEGIN', VERBOSE)
    dbdo(dbc, 'DROP TABLE IF EXISTS [World]', VERBOSE)
    dbdo(dbc, 'CREATE TABLE [World] ({})'.format(tablespec), VERBOSE)
    dates = list_from_query(dbc, 'select distinct(Date) from [jhu_git] order by date')
    for date in dates:
        world = [ 0, 0, 0, 0, 0.0, 0.0]
        for country in countries:
            row = row_from_query(
                dbc, 
                ('select date, Confirmed, Deaths, Recovered, Active from [{}] '
                    'where date like \'{}%\' and confirmed > 0').format(country, date))
            print (country, date, row)
            if row is not None:
                date = row[0]
                for idx in range(1,len(row)):
                    #print (idx)
                    if row[idx] is not None:
                        world[idx-1] += row[idx]
                
                world[4] = 100 * float(world[1] / world[0]) # CFR
                world[5] = 100 * float(world[2] / world[0]) # CRR
        world_str = ', '.join(['{}'.format(w) for w in world])
        
        #print (country, world, world_str)
        dbdo(dbc,
                ('INSERT into [World] (Date, confirmed, deaths, Recovered, Active, CFR, CRR) '
                 'Values (\'{}\', {})'.format(date, world_str)), VERBOSE)

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

    if (UPDATE or FIRSTRUN):
        read_3g_dxy_cn_json()
        read_jhu_data()
        make_summary_tables()

    if PLOTS:
        #make_plots_from_dxy()
        make_plots_from_jhu()
        make_days_since_start_plot()

    return 0

if __name__ == '__main__':
    #Some Generics
    FIGSIZE=[9,6]
    FACTOR = 0.001 # 0.1% of World Values
    VERBOSE = 0
    FIRSTRUN = 0
    UPDATE = 0
    PLOTS = 1
    MINCASES = 8
    
    DATADIR = '01_download_data'
    for arg in sys.argv:
        if arg == 'VERBOSE':
            VERBOSE = 1
        if arg == 'FIRSTRUN':
            FIRSTRUN = 1
        if arg == 'UPDATE':
            UPDATE = 1
        if arg == 'PLOTS':
            PLOTS = 1
    
    db_connect = sqlite3.connect('ncorv2019.sqlite')
    dbc = db_connect.cursor()

    main()

    #tidy up and shut down
    dbc.close()
