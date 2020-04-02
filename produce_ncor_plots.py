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

def keys_values_as_lists_from_dict(dict):    
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
    attrib_str = 'plot inspired by the work of https://twitter.com/jburnmurdoch/\nproduced by https://twitter.com/odaiwai using MatPlotLib, Python and SQLITE3. Data from JHU CSSE. https://www.diaspoir.net/'
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
    countries_of_interest = ['Hong Kong', 'Singapore', 'China', 'Italy', 'South Korea', 'USA', 'Germany', 'United Kingdom', 'Ireland', 'France', 'Poland', 'Japan', 'Spain', 'Taiwan', 'Vietnam', 'Thailand', 'Australia', 'Malaysia', 'Macau', 'World', 'Philippines', 'Turkey', 'Iran', 'Switzerland']
    
    # Setup the parameters for each graph
    FACTOR = 0.00001
    #
    graphs = []
    # Contains a list of the parameters
    graphs.append(['Confirmed', 10, 'Confirmed Cases (includes Deaths, Recoveries)', 'log', 2])
    graphs.append(['Recovered', 10, 'Recoveries', 'log', 2])
    graphs.append(['Deaths', 1, 'Deaths', 'log', 2])
    
    for graph, limit, description, scale, base in graphs:
        fig = plt.figure(figsize=FIGSIZE)
        ax = plt.axes([0.1, 0.15, 0.85, 0.75])
        fig.suptitle('SARS2-CoV / COVID19 for Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
        ax.set(title = '{} cases since no. {}'.format(graph, limit))
        ax.set(xlabel='Days since {} {}'.format(limit, graph), xlim = axis_range, ylabel=description)
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        ax.set_yscale(scale, basey = base)
        ax.set(ylim = (limit, max_cases))
        fig.autofmt_xdate()
        
        for country in countries:
            cmd = ('SELECT ROW_NUMBER() OVER (PARTITION BY {G} >= {L} order by date) as days, '
                   '{G} from [{C}] where {G} >= {L} order by Date'.format(C = country, G=graph, L = limit))
            results = dict_from_query(dbc, cmd)
            days, cases = keys_values_as_lists_from_dict(results)
            ax.plot(days, cases)
            # Add a marker and optionaly an annotation for the last point
            if len(days) > 0:
                ax.plot([days[-1]], [cases[-1]], marker='o', markersize=6)
                if country in countries_of_interest:
                    # Add a label
                    ax.annotate('{}: {:,.0f}'.format(country, cases[-1]), (days[-1]+1, cases[-1]), fontsize = 8, ha='left', bbox = box)
                #else:
                #    ax.annotate('{}: {:,.0f}'.format(country, cases[-1]), (days[-1]+1, cases[-1]), fontsize = 5, ha='left', bbox = box)
        #TODO - add dashed lines for 'doubles every (1..7) days
        for ddays in [1,2,3,4,5,7,14,21]:
            rate = ((2/1) ** (1/ddays))-1
            days = [1]
            double = [limit]
            for day in range(1, max_days):
                days.append(day)
                double.append(double[-1] * (1 + rate))
                if double[-1] >= 2**18:
                    break

            print (days, double)
            ax.plot(days, double, linestyle = 'dashed', linewidth = 0.5)
            ax.annotate('doubles in {} days'.format(ddays), (days[-1]+1, double[-1]), fontsize = 8, ha='left', bbox = box)
        # Attribution on the canvas
        fig.text(0.5, 0.025, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
        # save it out
        fig.savefig('plots/{G}_since_start.png'.format(G=graph), format = 'png')
        plt.close()
        
    return 0

def make_world_stackplots_from_jhu():
    """ 
    Make a World Stackplot :
    """
    start_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date ASC  limit 1')
    final_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date DESC limit 1')
    max_cases = value_from_query(dbc, 'SELECT confirmed from [world] order by Date DESC limit 1')
    axis_range = [datetime.datetime.strptime(start_date_str + ' 00:00', '%Y-%m-%d %H:%M'), 
                  datetime.datetime.strptime(final_date_str + ' 17:00', '%Y-%m-%d %H:%M')]
    countries = list_of_countries_by_confirmed(final_date_str)
    plt.style.use('seaborn-paper')

    graphs = []
    # Contains a list of the parameters for the stackplots
    # Graph, Description, scale, base
    graphs.append(['Confirmed', 'All Confirmed Cases (includes Deaths, Recoveries)', 'linear', 10]) 
    graphs.append(['Recovered', 'All Recoveries', 'linear', 10])
    graphs.append(['Deaths', 'AllDeaths', 'linear', 10])
    all_cure = []
    all_sick = []
    all_dead = []
        
    for graph, description, scale, base in graphs:
        fig = plt.figure(figsize=FIGSIZE)
        ax = plt.axes()
        fig.suptitle('SARS2-CoV /COVID 19 for Major Reporting Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
        #ax.set(title = 'All Countries')
        #ax.set(xlabel='Reporting Date', xlim = axis_range, ylabel='Reported Cases (includes Recoveries and Deaths)')
        #ax.format_data = mdates.DateFormatter('%Y-%m-%d')
        #ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        #fig.autofmt_xdate()
        
    # List to hold the data for the combined stackplots
    
    return None

def make_country_plots_from_jhu():
    """ 
    Make a plot by Country:
    """
    plt.style.use('seaborn-paper')
    # styles
    box = dict(boxstyle = 'square', fc='#ffffff40')
    attrib_str = r'plot produced by @odaiwai using MatPlotLib, Python and SQLITE3. Data from JHU CSSE. https://www.diaspoir.net/'
    attrib_box = dict(boxstyle = 'square', fc='#ffffff80', pad = 0.25)
    
    final_date_str = value_from_query(dbc, 'SELECT Date from [jhu_git] order by Date DESC limit 1')
    countries = list_of_countries_by_confirmed(final_date_str)
    for country in countries:
        date_strs = list_from_query(dbc, 'SELECT Date from [{}] order by Date'.format(country))
        conf = list_from_query(dbc, 'SELECT Confirmed from [{}] order by Date'.format(country))
        sick = list_from_query(dbc, 'SELECT (Confirmed-deaths-recovered) from [{}] order by Date'.format(country))
        dead = list_from_query(dbc, 'SELECT deaths from [{}] order by Date'.format(country))
        cure = list_from_query(dbc, 'SELECT Recovered from [{}] order by Date'.format(country))
        cfr  = list_from_query(dbc, 'SELECT CFR from [{}] order by Date'.format(country))
        c7d  = list_from_query(dbc, 'SELECT C7Day from [{}] order by Date'.format(country))
        d7d  = list_from_query(dbc, 'SELECT D7Day from [{}] order by Date'.format(country))
        d1d  = list_from_query(dbc, 'SELECT D1Day from [{}] order by Date'.format(country))
        print (country, conf[-1], cure[-1], sick[-1], dead[-1], cfr[-1])
        for index in range(0,len(d1d)):
            if type(d1d[index]) == None:
                d1d[index] = 0 
        # Get datetime objects for the dates and the axis_range
        dates = []
        for date in date_strs:
            dates.append(datetime.datetime.strptime(date, '%Y-%m-%d %H:%M'))
        axis_range = [dates[0], dates[-1]]
        if axis_range[0] == axis_range[1]:
            axis_range[0] = axis_range[1] - datetime.timedelta(days = 1)

        # Build the Plot
        fig = plt.figure(figsize=FIGSIZE)
        ax = plt.axes([0.1, 0.175, 0.80, 0.725])
        fig.suptitle('SARS2-CoV / COVID 19 for {}'.format(country))
        
        # Primary Axis for C/S/D
        ax.set(title = '{:,.0f} Confirmed Cases (JHU CSSE Data)'.format(conf[-1]),
               xlabel='Date', xlim = axis_range, ylabel='Reported Cases')
        ax.format_data = mdates.DateFormatter('%Y-%m-%d')
        fig.autofmt_xdate()
        ax.stackplot(dates, cure, sick, dead, 
                     labels=['Recovered', 'Sick', 'Deaths'], 
                     colors=['green', 'orange', 'black'])
        ax.legend(loc='upper left')
        
        # Annotate the final numbers
        labelx = dates[len(dates)-2]
        ax.annotate('Recovered {:,.0f}'.format(cure[-1]), (labelx, cure[-1]/2), fontsize = 8, ha='right', bbox = box)
        ax.annotate('Sick {:,.0f}'.format(sick[-1]), (labelx, cure[-1] + conf[-1]/2 - dead[-1]), fontsize = 8, ha='right', bbox = box)
        ax.annotate('deaths {:,.0f}'.format(dead[-1]), (labelx, conf[-1] - dead[-1]/2), fontsize = 8, ha='right', bbox = box)
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        
        # Secondary Axis for CFR
        ax2 = ax.twinx()
        ax2.plot(dates, cfr, label='Case Fatality Rate', color='red')
        ax2.annotate('CFR {:,.1f}%'.format(cfr[-1]*100), (labelx, cfr[-1]), fontsize = 8, ha='left', bbox = box)
        
        # Death Rates in aggregate only
        if country == 'World':
            ax2.plot(dates, c7d, label='Weekly Growth Rate', linestyle = 'dashed')
            ax2.annotate('C7D {:,.1f}%'.format(c7d[-1]*100), (labelx, c7d[-1]), fontsize = 8, ha='left', bbox = box)
            ax2.plot(dates, d7d, label='Weekly Growth Rate (Deaths)', linestyle = 'dashed')
            ax2.annotate('D7D {:,.1f}%'.format(d7d[-1]*100), (labelx, d7d[-1]), fontsize = 8, ha='left', bbox = box)
        
        ax2.set(ylim=(0.0,0.25), ylabel='Percentage')
        ax2.yaxis.set_major_formatter(mpl.ticker.PercentFormatter(xmax = 1, decimals = 1, symbol='%'))
        ax2.legend(loc='lower left')
        
        # Attribution and save
        fig.text(0.5, 0.025, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
        fig.savefig('plots/{}.png'.format(country), format = 'png')
        
        plt.close('all')
    return 0

def main():
    # main body
    """
    Go through the download dir and collect all of the various data sources:
    """
    #make_plots_from_dxy()
    #make_country_plots_from_jhu()
    make_days_since_start_plot()

    return 0

if __name__ == '__main__':
    #Some Generics
    FIGSIZE=[9,6] # 900 x 600 (3:2)
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
