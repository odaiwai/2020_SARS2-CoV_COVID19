#!/usr/bin/env python3
"""Read in the NID data files and populate the 
   historic database."""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import sqlite3
import datetime
import re

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

def make_table_of_disease_by_month():
    # drop the combined table and build it again
    dbc.execute('DROP TABLE IF EXISTS [disease_by_month];')

    # get a list of the diseases
    diseases = array_from_query('select distinct(ref) from [diseases];')
    table_spec = 'Date Text UNIQUE PRIMARY KEY'
    for disease in diseases:
        table_spec += ', {} Int'.format(disease)

    print (table_spec)
    dbc.execute('CREATE TABLE [disease_by_month] ({})'.format(table_spec))
    dbc.execute('BEGIN')

    #print (diseases, type(diseases))
    for disease in diseases:
        print('Disease: ', disease)
        all_cases = rows_from_query('select * from  [{}] order by year;'.format(disease))
        for year_data in all_cases:
            print (year_data)
            year = year_data[0]
            for month in range(1, 13):
                date = '{:04d}/{:02d}/{:02d}'.format(year, month, 15)
                cases = year_data[month]
                if type(cases) is str:
                    cases = int(0)

                # Build the SQL command to make the row.
                cmd = 'INSERT INTO [disease_by_month] (Date, {}) Values (\"{}\", {}) '.format(disease, date, cases)
                cmd += 'ON CONFLICT(Date) DO UPDATE SET {} = {} where date = \"{}\";'.format(disease, cases, date)
                print (cmd)
                dbc.execute(cmd)

    dbc.execute('COMMIT')

def hk_population():
    pop_data = """
        1986,-,-,5524600,-
        1987,-,-,5580500,-
        1988,-,-,5627600,-
        1989,-,-,5686200,-
        1990,-,-,5704500,-
        1991,-,-,5752000,-
        1992,-,-,5800500,-
        1993,-,-,5901000,-
        1994,-,-,6035400,-
        1995,-,-,6156100,-
        1996,-,-,6435500,-
        1997,-,-,6489300,-
        1998,-,-,6543700,-
        1999,-,-,6606500,-
        2000,-,-,6665000,-
        2001,-,-,6714300,-
        2002,-,-,6744100,-
        2003,-,-,6730800,-
        2004,-,-,6783500,-
        2005,-,-,6813200,-
        2006,-,-,6857100,-
        2007,-,-,6916300,-
        2008,-,-,6957800,-
        2009,-,-,6972800,-
        2010,-,-,7024200,-
        2011,-,-,7071600,-
        2012,-,-,7150100,-
        2013,-,-,7178900,-
        2014,-,-,7229500,-
        2015,-,-,7291300,-
        2016,-,-,7336600,-
        2017,-,-,7409800,-
        2018,-,-,7486400,-
        2019,-,-,7524100,-
    """
    annual_pop = {}
    matchglob = re.compile(r'([0-9]{4}),\-,\-,([0-9]+),\-')
    for match in matchglob.finditer(pop_data):
        annual_pop[int(match[1])] = int(match[2])

    # Now we have the annual data, interpolate to get the monthly totals
    monthly_pop = {}
    for year in range(1997, 2020):
        last_year_pop = annual_pop[year - 1]
        this_year_pop = annual_pop[year]
        growth_rate = ((this_year_pop/last_year_pop)**(1/12))-1
        for month in range(1, 13):
            date = datetime.datetime(year, month, 15, 12, 0, 0)
            this_month_pop = last_year_pop * ( 1 + growth_rate) ** month
            monthly_pop[date] = int(this_month_pop)

    #print (monthly_pop)

    return monthly_pop
#Constants

# The Main Loop
if __name__ == '__main__':
    db_connect = sqlite3.connect('notifiable_infections_diseases.sqlite')
    dbc = db_connect.cursor()
    FIRSTRUN = 0

    if FIRSTRUN:
        make_table_of_disease_by_month()

    # get the Data in Pandas Dataframe
    #df = pd.read_sql_query('select * from [disease_by_month];', db_connect)
    #print(df)
    #print(type(df))

    # get the list of dates and convert to date objects
    date_strs = array_from_query('select Date from [disease_by_month] order by date;')
    dates = []
    for date_str in date_strs:
        year, month, day = date_str.split('/')
        #print (date_str, year, month, day)
        date = datetime.datetime(int(year), int(month), int(day), 12, 0)
        dates.append(date)

    #print(dates)
    hk_pop = hk_population()

    axis_range = [datetime.datetime(1997,1,1), datetime.datetime(2020,1,1)]
    diseases = array_from_query('select distinct(ref) from diseases;')
    disease_full_names = dict_from_query('select distinct(ref), name from [diseases];')

    for disease in diseases:
        cases = array_from_query('select {} from  [disease_by_month] order by date;'.format(disease))
        print('Plotting {}...'.format(disease))
        fig, ax = plt.subplots()
        fig.suptitle('Notifiable Infections and Diseases in HK')
        ax.set_title(disease_full_names[disease])
        ax.scatter(dates, cases, label=disease, )
        ax.set(xlabel='Date', xlim=axis_range, ylabel='Reported Cases per month')
        
        # population on the second axis
        ax2 = ax.twinx()
        ax2.plot(hk_pop.keys(), hk_pop.values(), label='Population', color='red')
        ax2.set(ylabel='Population')

        ax.legend()
        ax2.legend()
        #fig, ax = plt.subplots()
        #ax.set(ylim=[0,max(cases)])

        fig.savefig('plots/' + disease + '.png', format = 'png')

        plt.close()

    #plot = plt.plot(dataframe)
    #plot.show()
    # Tidy up and close.
    dbc.close()


