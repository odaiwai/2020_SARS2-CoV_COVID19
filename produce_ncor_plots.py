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
import math
sys.path.append('/home/odaiwai/src/dob_DBHelper')
import db_helper as dbdo # This is a library of my own database routines - it just 
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
    provinces = dbdo.list_from_query(dbc, 'select distinct(provinceName) from [cn_prov];')
    china_total_conf = {}
    china_total_date = {}
    china_total_cure = {}
    china_total_dead = {}
    chinadates = []
    for province in provinces:
        province_en = dbdo.value_from_query(dbc, 'select distinct(ADM1_EN) from china_places where ADM1_ZH like \'{}%\';'.format(province))
        print(province, province_en)

        confirmed = dbdo.dict_from_query(dbc, 'select iso_date, confirmedCount from [cn_prov] where provinceName like \'{}\' order by timestamp;'.format(province))
        dead = dbdo.dict_from_query(dbc, 'select iso_date, deadCount from [cn_prov] where provinceName like \'{}\' order by timestamp;'.format(province))
        cured = dbdo.dict_from_query(dbc, 'select iso_date, curedCount from [cn_prov] where provinceName like \'{}\' order by timestamp;'.format(province))
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
        make_plot(provincesce_en, dates, confirmed, dead, cured)


    #print (type(chinadates), chinadates)

    make_plot('GreaterChina', chinadates, china_total_conf, china_total_dead, china_total_cure)
    return 0

def list_of_countries_by_confirmed(final_date_str):
    countries = dbdo.list_from_query(dbc, 'select distinct(country) from [jhu_data] where date like \'{}%\' and confirmed > 1;'.format(final_date_str))
    country_count = {}
    for country in countries:
        country_count[country] = dbdo.value_from_query(dbc, 'SELECT max(confirmed) from [{}]'.format(country))
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

def graph_definitions():
    graphs = []
    # Contains a list of the parameters for each graph
    # in order, these are: [graph_name, limit, description, scale, base, lag, doubling]:
    # Should add a Title, a Per Capita BOOL
    graphs.append(['Confirmed', 10, 'Confirmed Cases (includes Deaths, Recoveries)', 'log', 2, 0, 1])
    graphs.append(['Confirmed', 10, 'Confirmed New Cases (includes Deaths, Recoveries)', 'linear', 10, 7, 0])
    graphs.append(['Recovered', 10, 'Recoveries', 'log', 2, 0, 1])
    graphs.append(['Recovered', 10, 'New Recoveries', 'linear', 10, 7, 0])
    graphs.append(['Deaths', 1, 'Deaths', 'log', 2, 0, 1])
    graphs.append(['Deaths', 1, 'New Deaths', 'linear', 10, 7, 0])
    #graphs = {'Confirmed': 'limit', 'description'
    
    return graphs

    
def graph_definitions_as_dict():
    graphs = []
    graph_uuid = 0
    """
        TODO: add per population graph_definitions_as_dict
    """
    for column in 'Confirmed Recovered Deaths'.split():
        if column == 'Deaths':
            limit = 1
        else:
            limit = 10
        
        for per_capita in (True, False):
            graph = {'by_pop': per_capita}
            for lag in [0, 7]:
                graph['uuid'] = graph_uuid
                graph['column'] = column
                graph['limit'] = limit
                graph['lag'] = lag
                if lag == 0:
                    graph['description'] = ' '.join([column, 'Cases'])
                    graph['scale'] = 'log'
                    graph['base'] = 2
                    graph['doubling'] = True
                else:
                    graph['description'] = ' '.join([column, 'New Cases'])
                    graph['scale'] = 'linear'
                    graph['base'] = 10
                    graph['doubling'] = False
        
                graph_uuid += 1
                print(graph)

                graphs.append(graph)
    print()
    return graphs

def make_days_since_start_plot():
    #Make the rate of increase since N cases plot
    # with all the countries
        
    # Style and Attributions text
    box = dict(boxstyle = 'round', fc='#ffffffff')
    attrib_str = ('plot inspired by the work of https://twitter.com/jburnmurdoch/\n'
                  'produced by https://github.com/odaiwai using MatPlotLib, Python '
                  'and SQLITE3. Data from JHU CSSE. https://www.diaspoir.net/')
    attrib_box = dict(boxstyle = 'square', fc='#ffffff80', pad = 0.25)
    plt.style.use('seaborn-paper')
    
    # General Parameters
    max_cases = dbdo.value_from_query(dbc, 'SELECT confirmed from [world] order by Date DESC limit 1')
    start_date_str = dbdo.value_from_query(dbc, 'SELECT Date from [jhu_data] order by Date ASC limit 1')
    final_date_str = dbdo.value_from_query(dbc, 'SELECT Date from [jhu_data] order by Date DESC limit 1')
    start_date = datetime.datetime.strptime(start_date_str + ' 00:00', '%Y-%m-%d %H:%M')
    final_date = datetime.datetime.strptime(final_date_str + ' 17:00', '%Y-%m-%d %H:%M') + datetime.timedelta(days = 7)
    max_days = (final_date - start_date).days
    axis_range = [1, max_days] # x-axis
    countries = list_of_countries_by_confirmed(final_date_str)
    countries.remove('World')
    countries_of_interest = ['Hong Kong', 'Singapore', 'China', 'Italy', 
            'South Korea', 'USA', 'Germany', 'United Kingdom', 'Ireland', 'France', 
            'Poland', 'Japan', 'Spain', 'Taiwan', 'Vietnam', 'Thailand', 'Australia', 
            'Malaysia', 'Macau', 'World', 'Philippines', 'Turkey', 'Iran', 
            'Switzerland', 'Brazil', 'Russia', 'India']
    
    # Setup the parameters for each graph
    FACTOR = 0.00001
    graphs = graph_definitions_as_dict()

    ANNOTATE_ALL = False
    
    for graph in graphs:
        #for graph, limit, description, scale, base, lag, doubling in graphs:
        fig = plt.figure(figsize=FIGSIZE)
        ax = plt.axes([0.1, 0.15, 0.85, 0.75])
        fig.suptitle('SARS2-CoV / COVID 19 for Countries (last data: {})'.format(final_date_str))
        if graph['lag'] > 0:
            ax.set(title = '{} new cases per day (over {} days) since no. {}'.format(graph['column'], graph['lag'], graph['limit']))
        else:
            ax.set(title = '{} cases since no. {}'.format(graph['column'], graph['limit']))
        ax.set(xlabel='Days since {} {}'.format(graph['limit'], graph['column']), 
               xlim = axis_range, ylabel=graph['description'])
        fig.autofmt_xdate()
        # configure the Y-Axis
        ax.set_yscale(graph['scale'], basey = graph['base'])
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        #ax.set(ylim = (graph['limit'], max_cases))
        max_cases = 0
        for country in countries:
            zord = 500 - countries.index(country)
            population = dbdo.value_from_query(dbc, 'SELECT population from [populations] where country like \'{}\''.format(country))
            if graph['lag'] > 0:
                cmd = ('SELECT ROW_NUMBER() OVER (PARTITION BY {G} >= {L} order by date) as days, '
                       'cast({G} - LAG ({G}, {D}, 0) OVER (order by date) as REAL)/{D} as {G} '
                       'from [{C}] where {G} >= {L} order by Date'.format(C = country, G=graph['column'], L = graph['limit'], D=graph['lag']))
            else:
                cmd = ('SELECT ROW_NUMBER() OVER (PARTITION BY {G} >= {L} order by date) as days, '
                       '{G} from [{C}] where {G} >= {L} order by Date'.format(C = country, G=graph['column'], L = graph['limit']))
 
            #print(cmd)
            results = dbdo.dict_from_query(dbc, cmd)
            days, cases = keys_values_as_lists_from_dict(results)
            
            # Add a marker and optionly an annotation for the last point
            if len(days) > 0:
                # Keep track of the largest number
                if max(cases) > max_cases:
                    max_cases = max(cases)
                if country in countries_of_interest or ANNOTATE_ALL:
                    ax.plot(days, cases, lw = 2.5, zorder = zord)
                    ax.plot([days[-1]], [cases[-1]], marker='o', markersize=6, 
                            zorder = zord)
                    # Add a label
                    ax.annotate('{}: {:,.0f}'.format(country, cases[-1]), 
                                (days[-1]+1, cases[-1]), fontsize = 8, ha='left', 
                                bbox = box, zorder = zord)
                else:
                    #ax.plot(days, cases, color = '#80808080', lw = 1, zorder = zord)
                    ax.plot(days, cases, lw = 1, zorder = zord)
                    ax.plot([days[-1]], [cases[-1]], marker='o', markersize=3, 
                            color = '#80808080', zorder = zord)

        if graph['doubling']:
            # add dashed lines for 'doubles every (1..7) days
            axis_limit = 2 ** int(math.log2(max_cases)-1)
            for ddays in [1,2,3,4,5,7,14,21]:
                rate = ((2/1) ** (1/ddays))-1
                days = [0]
                double = [graph['limit']]
                for day in range(0, max_days):
                    days.append(day)
                    double.append(double[-1] * (1 + rate))
                    if double[-1] >= axis_limit:
                        break
                ax.plot(days, double, linestyle = 'dashed', linewidth = 0.5, zorder = 2)
                ax.annotate('doubles in {} days'.format(ddays), (days[-1]+1, double[-1]), 
                            fontsize = 8, ha='left', bbox = box, zorder = 2)
        
        ax.set(ylim = (graph['limit'], max_cases))
        # Attribution on the canvas
        fig.text(0.5, 0.025, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
        # save it out
        if graph['lag'] > 0:
            fig.savefig('plots/{G}_new_since_start.png'.format(G=graph['column']), format = 'png')
        else:
            fig.savefig('plots/{G}_since_start.png'.format(G=graph['column']), format = 'png')
        plt.close()
        
    return None

def make_world_gridplots_from_jhu():
    """ 
    Make a grid plot (nxn) of the top countries by number of cases
    Each graph should be the rolling average of new confirmed cases over the last N days
    """
    start_date_str = dbdo.value_from_query(dbc, 'SELECT Date from [jhu_data] order by Date ASC  limit 1')
    final_date_str = dbdo.value_from_query(dbc, 'SELECT Date from [jhu_data] order by Date DESC limit 1')
    max_cases = dbdo.value_from_query(dbc, 'SELECT confirmed from [world] order by Date DESC limit 1')
    axis_range = [datetime.datetime.strptime(start_date_str + ' 00:00', '%Y-%m-%d %H:%M'), 
                  datetime.datetime.strptime(final_date_str + ' 17:00', '%Y-%m-%d %H:%M')]
    countries = list_of_countries_by_confirmed(final_date_str)
    plt.style.use('seaborn-paper')

    graphs = graph_definitions_as_dict()
    num_graphx = 9 # Number of graphs in a row
    num_graphy = 6 # Number of graphs in a row
    FIGSIZE = [num_graphx * 1.5, num_graphy * 1.5]
    DAYS = True
        
    for graph in graphs:
        fig, axes  = plt.subplots(num_graphx, num_graphy, figsize=FIGSIZE)
        fig.suptitle('SARS-CoV2 /COVID-19 in order of total confirmed cases')
        for row in range(0, num_graphx):
            for col in range(0, num_graphy):
                country = countries[row*num_graphx + col]
                if DAYS:
                    cmd = 'SELECT ROW_NUMBER() OVER (PARTITION BY {G} >= {L} order by date) as days, '.format(G = graph['column'], L = graph['limit'])
                else:
                    cmd = 'SELECT date as days, '
                if graph['lag'] > 0:
                    cmd += ('cast({G} - LAG ({G}, {D}, 0) OVER (order by date) as REAL)/{D} as {G} '
                            'from [{C}] where {G} >= {L} order by Date'.format(C = country, G=graph['column'], L = graph['limit'], D=graph['lag']))
                else:
                    cmd += ('{G} from [{C}] where {G} >= {L} order by Date'.format(C = country, G=graph['column'], L = graph['limit']))
                #print(cmd)
                results = dbdo.dict_from_query(dbc, cmd)
                days, cases = keys_values_as_lists_from_dict(results)
            
                # Determine the colour
                max_cases = max(cases)
                colour = 'tab:red'
                if cases[-1] <= 0.75 * max_cases:
                    colour = 'tab:orange'
                if cases[-1] <= 0.50 * max_cases:
                    colour = 'tab:cyan'
                if cases[-1] <= 0.25 * max_cases:
                    colour = 'tab:green'

                # Add the plot to the canvas
                axes[row, col].plot(days, cases, colour, label = '{}'.format(country))
                axes[row, col].legend(loc = 'upper left')
                #axes[row, col].set_title('{}'.format(country))

        # iterate through the axes nump array
        for ax in axes.flat:
            ax.set(xlabel = 'days', ylabel = 'cases')
            ax.label_outer()


        if graph['lag'] > 0:
            fig.savefig('plots/{G}_new_grid_since_start.png'.format(G=graph['column']), format = 'png')
        else:
            fig.savefig('plots/{G}_grid_since_start.png'.format(G=graph['column']), format = 'png')
        plt.close()
    
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
    
    final_date_str = dbdo.value_from_query(dbc, 'SELECT Date from [jhu_data] order by Date DESC limit 1')
    countries = list_of_countries_by_confirmed(final_date_str)
    for country in countries:
        date_strs = dbdo.list_from_query(dbc, 'SELECT Date from [{}] order by Date'.format(country))
        conf = dbdo.list_from_query(dbc, 'SELECT Confirmed from [{}] order by Date'.format(country))
        sick = dbdo.list_from_query(dbc, 'SELECT (Confirmed-deaths-recovered) from [{}] order by Date'.format(country))
        dead = dbdo.list_from_query(dbc, 'SELECT deaths from [{}] order by Date'.format(country))
        cure = dbdo.list_from_query(dbc, 'SELECT Recovered from [{}] order by Date'.format(country))
        cfr  = dbdo.list_from_query(dbc, 'SELECT CFR from [{}] order by Date'.format(country))
        c7d  = dbdo.list_from_query(dbc, 'SELECT C7Day from [{}] order by Date'.format(country))
        d7d  = dbdo.list_from_query(dbc, 'SELECT D7Day from [{}] order by Date'.format(country))
        d1d  = dbdo.list_from_query(dbc, 'SELECT D1Day from [{}] order by Date'.format(country))
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

def make_days_since_start_plot_by_country():
    #Make the rate of increase since N cases plot
    # with all the countries
        
    # Style and Attributions text
    box = dict(boxstyle = 'round', fc='#ffffffff')
    attrib_str = 'plot inspired by the work of https://twitter.com/jburnmurdoch/\nproduced by https://twitter.com/odaiwai using MatPlotLib, Python and SQLITE3. Data from JHU CSSE. https://www.diaspoir.net/'
    attrib_box = dict(boxstyle = 'square', fc='#ffffff80', pad = 0.25)
    plt.style.use('seaborn-paper')
    
    # General Parameters
    max_cases = dbdo.value_from_query(dbc, 'SELECT confirmed from [world] order by Date DESC limit 1')
    start_date_str = dbdo.value_from_query(dbc, 'SELECT Date from [jhu_data] order by Date ASC limit 1')
    final_date_str = dbdo.value_from_query(dbc, 'SELECT Date from [jhu_data] order by Date DESC limit 1')
    start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d')
    final_date = datetime.datetime.strptime(final_date_str, '%Y-%m-%d') + datetime.timedelta(days = 7)
    max_days = (final_date - start_date).days
    axis_range = [1, max_days]
    #axis_range = [start_date, final_date]
    countries = list_of_countries_by_confirmed(final_date_str)
    countries_of_interest = ['Hong Kong', 'Singapore', 'China', 'Italy', 'South Korea', 'USA', 'Germany', 'United Kingdom', 'Ireland', 'France', 'Poland', 'Japan', 'Spain', 'Taiwan', 'Vietnam', 'Thailand', 'Australia', 'Malaysia', 'Macau', 'World', 'Philippines', 'Turkey', 'Iran', 'Switzerland']
    
    # Setup the parameters for each graph
    FACTOR = 0.00001
    #

    graphs = graph_definitions_as_dict()
    colours = {'Confirmed': 'orange', 'Deaths': 'black', 'Recovered': 'green'}
    
    for country in countries:
        fig = plt.figure(figsize=FIGSIZE)
        ax = plt.axes([0.1, 0.15, 0.85, 0.75])
        fig.suptitle('SARS2-CoV / COVID 19 for Countries (with {} cases or more)'.format(int(max_cases * FACTOR)))
        ax.set(title = '{}: cases since Reporting started'.format(country))
        ax.set(xlabel='Days since reporting Started', xlim = axis_range, ylabel='Cases')
        fig.autofmt_xdate()
        # configure the Y-Axis
        ax.set_yscale('log', basey = 2)
        ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
        #max_cases = 0
        zord = 10 #
        print (country)
        for graph in graphs:
            col = colours[graph['column']]
            # we want to have dates in here...
            if graph['lag'] > 0:
                cmd = ('SELECT Date, ROW_NUMBER() OVER (PARTITION BY Confirmed >= 10 order by date) as days, '
                       'cast({G} - LAG ({G}, {D}, 0) OVER (order by date) as REAL)/{D} as {G} '
                       'from [{C}] where Confirmed >= 10 order by Date'.format(C = country, G=graph['column'], L = graph['limit'], D=graph['lag']))
            else:
                cmd = ('SELECT Date, ROW_NUMBER() OVER (PARTITION BY Confirmed >= 10 order by date) as days, '
                       '{G} from [{C}] where Confirmed >= 10 order by Date'.format(C = country, G=graph['column'], L = graph['limit']))
            results = dbdo.rows_from_query(dbc, cmd) # 3 x n
            
            if len(results) > 0:
                dates = []
                days = []
                cases = []
                for result in results:
                    dates.append(result[0])
                    days.append(result[1])
                    cases.append(result[2])
        
                #print (graph, results)
                #print (graph, '\n\t', dates, '\n\t', days, '\n\t', cases)
                # Add a marker and optionly an annotation for the last point
                label = '{} cases since no. {} ({})'.format(graph['column'], graph['limit'], dates[0])
                style = 'solid'
                if graph['lag'] > 0:
                    label = '{} new cases per day (over {} day) since no. {} ({})'.format(graph['column'], graph['lag'], graph['limit'], dates[0])
                    style = 'dashed'
                        
                final_note = '{:,.0f}'.format(cases[-1])
                if graph['lag'] > 0:
                    final_note = '{:,.0f} per day'.format(cases[-1])
                max_cases = max(max_cases, cases[-1])
                ax.plot(days[graph['lag']:], cases[graph['lag']:], lw = 2.5, zorder = zord, color = col, 
                        linestyle = style, label = label)
                ax.plot([days[-1]], [cases[-1]], marker='o', markersize=6, zorder = zord)
                # Add a label
                ax.annotate(final_note, (days[-1], cases[-1]), 
                            fontsize = 8, ha='left', bbox = box, zorder = zord)
        
        if graph['doubling']:
            # add dashed lines for 'doubles every (1..7) days
            ax.set(ylim = (1, max_cases))
            axis_limit = 2 ** int(math.log2(max_cases)-1)
            for ddays in [1,2,3,4,5,7,14]:
                rate = ((2/1) ** (1/ddays))-1
                days = [0]
                double = [1]
                for day in range(0, max_days):
                    days.append(day)
                    double.append(double[-1] * (1 + rate))
                    if double[-1] >= axis_limit:
                        break
                ax.plot(days, double, linestyle = 'dashed', linewidth = 0.5, zorder = 2)
                ax.annotate('doubles in {} days'.format(ddays), (days[-1]+1, double[-1]), fontsize = 8, ha='left', bbox = box, zorder = 2)
        
        # Attribution on the canvas
        ax.legend()
        fig.text(0.5, 0.025, attrib_str, ha = 'center', fontsize = 8, bbox = attrib_box, transform=plt.gcf().transFigure)
        # save it out
        fig.savefig('plots/{C}_since_start.png'.format(C=country), format = 'png')
        plt.close()
        
    return None

def main():
    # main body
    """
    Go through the download dir and collect all of the various data sources:
    """
    make_days_since_start_plot()
    make_days_since_start_plot_by_country()
    make_country_plots_from_jhu()
    make_world_gridplots_from_jhu()
    #make_plots_from_dxy()
    
    # TODO
    # Assign a Region to Countries, also a consistent colour, and flag emoji?
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
