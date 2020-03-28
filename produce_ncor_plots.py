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
    
    # Style and Attributions text
    box = dict(boxstyle = 'square', fc='#ffffff80')
    attrib_str = r'plot produced by @odaiwai using MatPlotLib, Python and SQLITE3. Data from JHU CSSE. https://www.diaspoir.net/'
    attrib_box = dict(boxstyle = 'square', fc='#ffffff80', pad = 0.25)
    plt.style.use('seaborn-paper')

    # General Parameters
    max_cases = value_from_query(dbc, 'SELECT confirmed from [world] order by Date DESC limit 1')
    start_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date ASC limit 1')
    final_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date DESC limit 1')
    start_date = datetime.datetime.strptime(start_date_str + ' 00:00', '%Y-%m-%d %H:%M')
    final_date = datetime.datetime.strptime(final_date_str + ' 17:00', '%Y-%m-%d %H:%M')
    max_days = (final_date - start_date).days
    axis_range = [1, max_days+7]
    countries = list_of_countries_by_confirmed(final_date_str)
    countries.remove('World')
    countries_of_interest = ['Hong Kong', 'Singapore', 'China', 'Italy', 'South Korea', 'USA', 'Germany', 'United Kingdom', 'Ireland', 'France', 'Poland', 'Japan', 'Spain', 'Taiwan', 'Vietnam', 'Thailand', 'Australia', 'Malaysia', 'Macau', 'World']
    
    # Setup the parameters for each graph
    FACTOR = 0.00001
    #
    graphs = []
    # Contains a list of the parameters
    graphs.append(['Confirmed', 10, 'Confirmed Cases (includes Deaths, Recoveries)', 'log', 2])
    graphs.append(['Recovered', 10, 'Recoveries', 'log', 2])
    graphs.append(['Deaths', 1, 'Deaths', 'log', 2])
    
    for graph, limit, description, scale, base in graphs:
        fig, ax = plt.subplots(figsize=FIGSIZE)
        fig.suptitle('SARS2-CoV / COVID19 for Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
        ax.set(title = '{} cases since no. {}'.format(graph, limit))
        ax.set(xlabel='Days since {} {}'.format(limit, graph), xlim = axis_range, ylabel=description)
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.set_yscale(scale, basey = base)
        fig.autofmt_xdate()
        
        for country in countries:
            cmd = ('SELECT ROW_NUMBER() OVER (PARTITION BY {G} >= {L} order by date) as days, '
                '{G} from [{C}] where {G} >= {L} order by Date'.format(C = country, G=graph, L = limit))
            results = dict_from_query(dbc, cmd)
            days, cases = keys_values_as_list_from_dict(results)
            ax.plot(days, cases)
            
            if len(days) > 0:
                ax.plot([days[-1]], [cases[-1]], marker='o', markersize=3)
                if country in countries_of_interest:
                    # Add a label
                    ax.annotate('{}: {:,.0f}'.format(country, cases[-1]), (days[-1]+1, cases[-1]), fontsize = 8, ha='left', bbox = box)

        # Attribution on the canvas
        fig.text(0.5, 0.01, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
        # save it out
        fig.savefig('plots/{G}_since_start.png'.format(G=graph), format = 'png')
        
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
    #make_plots_from_dxy()
    make_plots_from_jhu()
    make_days_since_start_plot()

    return 0

if __name__ == '__main__':
    #Some Generics
    FIGSIZE=[9,6]
    FACTOR = 0.001 # 0.1% of World Values
    VERBOSE = 0
    PLOTS = 1
    MINCASES = 8
    
    DATADIR = '01_download_data'
    for arg in sys.argv:
        if arg == 'VERBOSE':
            VERBOSE = 1
    
    db_connect = sqlite3.connect('ncorv2019.sqlite')
    dbc = db_connect.cursor()

    main()

    #tidy up and shut down
    dbc.close()
