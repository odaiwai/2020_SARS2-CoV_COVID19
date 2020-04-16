#!/usr/bin/env python3
"""
Gather all of the saved data from the NCorV 2019 outbreak and
get them into an SQLITE3 database so we can plot them.

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
        
        Provinces contain cities
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
                    'Last_Update Text, incident_rate Real, People_tested Integer, People_hospitalized Integer, '
                    'Confirmed Integer, Deaths Integer, Recovered Integer, Active Integer, '
                    'Latitude Real, Longitude Real, Combined_Key Text'),
        'places': ('OBJECTID Int UNIQUE Primary Key, ADMIN_TYPE Text, ADM2_CAP Text, '
                   'ADM2_EN Text, ADM2_ZH Text, ADM2_PCODE Text, ADM1_EN Text, '
                   'ADM1_ZH Text, ADM1_PCODE Text, ADM0_EN Text, ADM0_ZH Text, '
                   'ADM0_PCODE Text'),
        'un_places': ('id Integer Unique Primary Key, hrinfo_id Integer, fts_api_id Integer, '
                      'reliefweb_id Integer, m49 Integer, admin_level Integer, dgacm_list Text, '
                      'lat Real, long Real, iso2 Text, iso3 Text, arabic_short Text, '
                      'chinese_short Text, french_short Text, Default_form Text, fts Text, '
                      'russian_short Text, spanish_short Text'),
        'populations': ('id Integer Unique Primary Key, Country Text, alt_name text, Population Integer, '
                        'Yearly_Change Real, Net_Change Integer, Density Real, Land_Area Integer, '
                        'Migrants Integer, Fert_rate Real, median_age Integer, Urban_pct Real, '
                        'world_pct Real'),
        'files': ('filename Text, dateProcessed Text')
            }
    make_tables_from_dict(dbc, tabledefs, VERBOSE)

def cagr(value1, value2, interval):
    """ Calculate the CAGR (Compound Average Growth Rate) between value1 and value 2 over interval
        e.g. Value1 is today's data, Value2 is 7 days ago
    """
    value1 = float(value1)
    value2 = float(value2)
    if value2 == 0 or interval == 0:
        cagr = -1
    else:
        cagr = ((value1/value2) ** (1/interval))-1
    return cagr

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
    return None 

def quoted_if_required(string):
    if type(string) == str:
        return '"{}"'.format(string)
    elif string is None:
        return '""'
    else:
        return '{}'.format(string)

def normalise_countries(country):
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
                          'West Bank and Gaza': 'Palestine', # Pointless change
                          'Russian Federation': 'Russia', # Pointless change
                          'The Bahamas': 'Bahamas', # Pointless change
                          'Czech Republic': 'Czechia',
                          'Iran (Islamic Republic of)': 'Iran',# Are there multiple Irans?
                          'Holy See': 'Vatican City',
                          'Viet Nam': 'Vietnam',
                          'Korea_South': 'South Korea',
                          'Cote d\'Ivoire': 'Ivory Coast'
                          }
    if country in normalise_countries.keys():
        return normalise_countries[country]
    else:
        return country

def read_populations():
    """Read in the list of places from the UN list and fix it.
    """
    with open('./01_download_data/world_population.csv', 'r') as infile:
        lines = list(infile)
        fields = ('id, Country, Population, Yearly_Change, Net_Change , Density, '
                  'Land_Area, Migrants, Fert_rate, median_age, Urban_pct, '
                  'world_pct, alt_name')
        # first line is fieldnames
        fixcomma = re.compile(r'([0-9]),([0-9])')
        descriptions = lines.pop(0)
        print (descriptions)
        dbdo(dbc, 'BEGIN', VERBOSE)
        for line in lines:
            values = line.rstrip('\n').split(';')
            print(values)
            country = values[1]
            alt_name = normalise_countries(country)
            print(alt_name)
            values.append(alt_name)
            
            value_list = []
            print(values)
            for value in values:
                value = fixcomma.sub(r'\1\2', value)
                print (value, type(value))
                if value[-1:] == '%':
                    value = value[0:-2]
                
                    
                value_list.append(quoted_if_required(value))
            values = ', '.join(value_list)
            dbdo(dbc,'INSERT INTO [populations] ({F}) Values ({V})'.format(F=fields, V=values), VERBOSE)

        dbdo(dbc, 'COMMIT', VERBOSE)
    return None


def read_un_places():
    """Read in the list of places from the UN list and fix it.
    """
    with open('./countries.json', 'r') as infile:
        un_countries = json.loads(infile.read())
        fields = ('id, hrinfo_id, fts_api_id, reliefweb_id, m49, admin_level, dgacm_list, '
                  'iso2, iso3, lat, long, arabic_short, chinese_short, french_short, '
                  'default_form, fts, russian_short, spanish_short')
        dbdo(dbc, 'BEGIN', VERBOSE)
        for entity in un_countries['data']:
            value_list = []
            for object in ['id', 'hrinfo_id', 'fts_api_id', 'reliefweb_id', 'm49', 'admin_level', 'dgacm-list', 'iso2', 'iso3']:
                value_list.append(quoted_if_required(entity[object]))
                
            # Add the gelocation data
            geo = entity['geolocation']
            value_list.append(quoted_if_required(geo['lat']))
            value_list.append(quoted_if_required(geo['lon']))
            
            # Add the Label Data
            labels = entity['label']
            for label in ['arabic-short', 'chinese-short', 'french-short', 'default', 'fts', 'russian-short', 'spanish-short']:
                value_list.append(quoted_if_required(labels[label]))
            
            values = ', '.join(value_list)
            dbdo(dbc,'INSERT INTO [un_places] ({F}) Values ({V})'.format(F=fields, V=values), VERBOSE)

        dbdo(dbc, 'COMMIT', VERBOSE)
    return None

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
        #print (components)
        values = '{}'.format(components.pop(0))
        for component in components:
            values += r', "{}"'.format(component)

        dbdo(dbc, 'INSERT into [places] ({}) Values ({})'.format(fields, values), VERBOSE)

    dbdo(dbc, 'COMMIT', VERBOSE)
    return None

def read_3g_dxy_cn_json():
    """
    Read in the JSON data from 3G.DXY.CN and add it to the database.
    we're mainly interested in provincial growth.
    """
    files = os.listdir(DATADIR)
    areastat = re.compile(r'^([0-9]{8})_([0-9]{6})_getAreaStat.json$')
    already_processed = list_from_query(dbc, 'select filename from files;') 
    print ('Reading 3G_DXY.CN data')
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

    return None

       
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
    # Keep the field names consistent
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
                    line_dict['Country'] = normalise_countries(line_dict['Country'])

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

def make_summary_tables():
    """
        Make Tables from JHU data of:
            1. All confirmed, deaths, recovered by date
            2. Each Country by Date (sum up provinces)
            3. Each WHO Region by date TODO
    """
    print ('Make Summary Tables')
    tablespec = ('Date Text, '
                 'Confirmed Integer, '
                 'Deaths Integer, '
                 'Recovered Integer, '
                 'Active Integer')
    countries = list_from_query(dbc, 'select distinct(country) from [jhu_git];')
    rounding = 4
    for country in countries:
        dbdo(dbc, "BEGIN", VERBOSE)
        dbdo(dbc, 'DROP TABLE IF EXISTS [{}]'.format(country), VERBOSE)
        dbdo(dbc, 'DROP TABLE IF EXISTS [{}.temp]'.format(country), VERBOSE)
        #dbdo(dbc, 'CREATE TABLE [{}] ({})'.format(country, tablespec), VERBOSE)
        provinces = list_from_query(dbc, 'select distinct(province) from [jhu_git] where country = \"{}\"'.format(country))
        # Make a Table of all the provinces
        country_dates = {}
        dbdo(dbc, 
             ('CREATE TABLE [{C}.temp] AS '
              '  SELECT distinct(date) || \' 17:00\' AS Date, '
              '   sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, '
              '   sum(Recovered) as Recovered, sum(Active) as Active '
              '  FROM [jhu_git] where country = \'{C}\' group by date order by date'
             ).format(C=country), VERBOSE)
        dbdo(dbc, 
             ('CREATE TABLE [{C}] AS '
              '   SELECT Date, Confirmed, Deaths, Recovered, Active, '
              '   ROUND(CAST(Deaths as REAL) / Confirmed, {R}) as CFR, '
              '   ROUND(CAST(Recovered as REAL) / Confirmed, {R}) as CRR, '
              '   ROUND(Cast(Confirmed as REAL)/(LAG (Confirmed, 1, 0) OVER (order by date))-1, {R}) as C1day, '
              '   ROUND(Cast(Deaths    as REAL)/(LAG (Deaths,    1, 0) OVER (order by date))-1, {R}) as D1day, '
              '   ROUND(Cast(Recovered as REAL)/(LAG (Recovered, 1, 0) OVER (order by date))-1, {R}) as R1day, '
              '   ROUND(CAGR(Confirmed, LAG (Confirmed, 7, 0) OVER (order by date), 7), {R}) as C7day, '
              '   ROUND(CAGR(Deaths,    LAG (Deaths,    7, 0) OVER (order by date), 7), {R}) as D7day, '
              '   ROUND(CAGR(Recovered, LAG (Recovered, 7, 0) OVER (order by date), 7), {R}) as R7day '
              '  FROM [{C}.temp] order by date').format(C=country, R = rounding), VERBOSE)
        for province in provinces:
            dbdo(dbc, 'DROP TABLE IF EXISTS [{}.{}]'.format(country, province), VERBOSE)
            dbdo(dbc, 'DROP TABLE IF EXISTS [{}.{}.temp]'.format(country, province), VERBOSE)
            # Create the Provincial Tables (also need to split the ISO date into Date and Time)
            dbdo(dbc, 
                 ('CREATE TABLE [{C}.{P}.temp] AS '
                  '  SELECT distinct(date) || \' 17:00\' AS Date, '
                  '   sum(Confirmed) as Confirmed, sum(Deaths) as Deaths, '
                  '   sum(Recovered) as Recovered, sum(Active) as Active '
                  '  FROM [jhu_git] where country = \'{C}\' and province = \'{P}\' group by date order by date'
                  ).format(C=country, P=province, R = rounding), VERBOSE)
            dbdo(dbc, 
                 ('CREATE TABLE [{C}.{P}] AS '
                  '   SELECT Date, Confirmed, Deaths, Recovered, Active, '
                  '   ROUND(CAST(Deaths as REAL) / Confirmed, {R}) as CFR, '
                  '   ROUND(CAST(Recovered as REAL) / Confirmed, {R}) as CRR, '
                  '   ROUND(Cast(Confirmed as REAL)/(LAG (Confirmed, 1, 0) OVER (order by date))-1, {R}) as C1day, '
                  '   ROUND(Cast(Deaths    as REAL)/(LAG (Deaths,    1, 0) OVER (order by date))-1, {R}) as D1day, '
                  '   ROUND(Cast(Recovered as REAL)/(LAG (Recovered, 1, 0) OVER (order by date))-1, {R}) as R1day, '
                  '   ROUND(CAGR(Confirmed, LAG (Confirmed, 7, 0) OVER (order by date), 7), {R}) as C7day, '
                  '   ROUND(CAGR(Deaths,    LAG (Deaths,    7, 0) OVER (order by date), 7), {R}) as D7day, '
                  '   ROUND(CAGR(Recovered, LAG (Recovered, 7, 0) OVER (order by date), 7), {R}) as R7day '
                  '  FROM [{C}.{P}.temp] order by date').format(C=country, P=province, R = rounding), VERBOSE)

        dbdo(dbc, 'COMMIT', VERBOSE)

    # Make the master Table of all Countries
    dbdo(dbc, 'DROP TABLE IF EXISTS [World]', VERBOSE)
    dbdo(dbc, 'DROP TABLE IF EXISTS [World.temp]', VERBOSE)
    dbdo(dbc, 'BEGIN', VERBOSE)
    dbdo(dbc, 'CREATE TABLE [World.temp] ({})'.format(tablespec), VERBOSE)
    dates = list_from_query(dbc, 'select distinct(Date) from [jhu_git] order by date')
    for date in dates:
        world = [0, 0, 0, 0]
        for country in countries:
            row = row_from_query(
                dbc, 
                ('select date, Confirmed, Deaths, Recovered, Active from [{}] '
                    'where date like \'{}%\' and confirmed > 0').format(country, date))
            if row is not None:
                date = row[0]
                for idx in range(1,len(row)):
                    #print (idx)
                    if row[idx] is not None:
                        world[idx-1] += row[idx]
                
        world_str = ', '.join(['{}'.format(w) for w in world])
        
        #print (country, world, world_str)
        dbdo(dbc,
                ('INSERT into [World.temp] (Date, confirmed, deaths, Recovered, Active) '
                 'Values (\'{}\', {})'.format(date, world_str)), VERBOSE)
    dbdo(dbc, 
         ('CREATE TABLE [World] AS '
          '   SELECT Date, Confirmed, Deaths, Recovered, Active, '
          '   ROUND(CAST(Deaths as REAL) / Confirmed, {R}) as CFR, '
          '   ROUND(CAST(Recovered as REAL) / Confirmed, {R}) as CRR, '
          '   ROUND(Cast(Confirmed as REAL)/(LAG (Confirmed, 1, 0) OVER (order by date))-1, {R}) as C1day, '
          '   ROUND(Cast(Deaths    as REAL)/(LAG (Deaths,    1, 0) OVER (order by date))-1, {R}) as D1day, '
          '   ROUND(Cast(Recovered as REAL)/(LAG (Recovered, 1, 0) OVER (order by date))-1, {R}) as R1day, '
          '   ROUND(CAGR(Confirmed, LAG (Confirmed, 7, 0) OVER (order by date), 7), {R}) as C7day, '
          '   ROUND(CAGR(Deaths,    LAG (Deaths,    7, 0) OVER (order by date), 7), {R}) as D7day, '
          '   ROUND(CAGR(Recovered, LAG (Recovered, 7, 0) OVER (order by date), 7), {R}) as R7day '
          '  FROM [World.temp] order by date').format(R = rounding), 
         VERBOSE)

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
        read_un_places()
        read_populations()

    if (UPDATE or FIRSTRUN):
        read_3g_dxy_cn_json()
        read_jhu_data()
        make_summary_tables()

    return None

if __name__ == '__main__':
    #Some Generics
    VERBOSE = 1
    FIRSTRUN = 0
    UPDATE = 0
    
    DATADIR = '01_download_data'
    for arg in sys.argv:
        if arg == 'VERBOSE':
            VERBOSE = 1
        if arg == 'FIRSTRUN':
            FIRSTRUN = 1
        if arg == 'UPDATE':
            UPDATE = 1
    
    db_connect = sqlite3.connect('ncorv2019.sqlite')
    db_connect.create_function('CAGR', 3, cagr)
    dbc = db_connect.cursor()

    main()

    #tidy up and shut down
    dbc.close()
