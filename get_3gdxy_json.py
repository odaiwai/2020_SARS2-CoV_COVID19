#!/usr/bin/env python3
"""
split the HTML from 3G_DGY.CN into JSON Blobs and Decode them
into a CSV file

if you get codepage errors running under Windows you'll need to update the code page:
   
https://stackoverflow.com/questions/6812031/how-to-make-unicode-string-with-python3

Tested on a Mac running Mojave, and a Linux box running Fedora 30, works on Windows (as 
noted above for CMD), and with no problems in a Cygwin shell.

dave o'brien (c) 2020/01/29
"""
import os, json, re, sys
import requests
import datetime
import sqlite3

def top_and_tail(string, width):
    """
    Show the start and end of a long string - helpful for debugging without clutter.
    """
    if len(string) < width:
        return string
    else:
        return (string[0:width] + '...' + string[-width:])

def printlog(*arguments):
    # printlog to a logfile or stdout
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    logentries = str(arguments)
    #concatenate all of the logentries to one string
    logentry = now + ' ' + logentries

    with open(LOGFILE, 'a+') as outfh:
        outfh.write(logentry + '\n')

    if VERBOSE:
        trunclog = now + ' ' + top_and_tail(logentries, 50)
        print (trunclog)

def walk_province_data(outfile, area_stats):
    # Walk the province data given a JSON data struct
    header = "Province, City, Confirmed, Suspected, Cured, Dead, comment"
    #printlog (header)
    outfile.write(header + '\n')
    for province in area_stats:
        case_count = '"{}", "{}", {}, {}, {}, {}, "{}"'.format(
            province["provinceName"],
            '(total)',
            province["confirmedCount"],
            province["suspectedCount"],
            province["curedCount"],
            province["deadCount"],
            province["comment"])
            #printlog (case_count)
        outfile.write(case_count + '\n')
    
        for city in province["cities"]:
            #printlog ('\tCity', type(city), city)
            case_count = '"{}", "{}", {}, {}, {}, {}'.format(
            province["provinceName"],
            city["cityName"],
            city["confirmedCount"],
            city["suspectedCount"],
            city["curedCount"],
            city["deadCount"])
            #printlog (case_count)
            outfile.write(case_count + '\n')

    outfile.close()
 
def main():
    html_source = ''
    timestamp = ''
    overwrite = 1

    if len(sys.argv) > 1:
        for index in range(1, len(sys.argv)):
            # parse an existing file
            if sys.argv[index] == 'silent':
                VERBOSE = 0
            elif sys.argv[index] == 'verbose':
                VERBOSE = 1
            else: 
                with open(sys.argv[index], "r") as infh:
                    html_source = infh.read()
                    overwrite = 0

    if len(html_source) == 0:
        # Go to the URL and retrieve it
        #response = requests.get(r'https://3g.dxy.cn/newh5/view/pneumonia')
        response = requests.get(r'https://ncov.dxy.cn/ncovh5/view/pneumonia')
        html_source = response.content.decode()

    if len(html_source) == 0:
        print('BARF! no data!', len(html_source))
        exit()
    #print (len(html_source))

    # try to figure out when this file was made so we can timestamp it.
    # The overall strategy is to find the latest time reference in the html
    # ASSUMPTION: time only moves in a forward direction.
    timestamp_match = re.compile(r'Time\"\:([0-9]+)\,')
    latest_timestamp = datetime.datetime.fromtimestamp(0)
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for match in timestamp_match.finditer(html_source):
        #printlog (match, type(match), match[1], type(match[1]))
        found_time = datetime.datetime.fromtimestamp(int(float(match[1])/1000))
        if found_time > latest_timestamp:
            latest_timestamp = found_time
            #printlog (found_time, latest_timestamp)
    
    timestamp = latest_timestamp.strftime("%Y%m%d_%H%M%S")
    printlog (latest_timestamp, timestamp)

    # Don't overwrite the source if we're working from a file
    if overwrite:
        with open(DATADIR + timestamp + '_3g_dxy_cn.html', "w") as outfile:
            outfile.write(html_source)
    
     # Gather all of the jsons into a dict
    printlog ('Looking for all blobs...')
    json_blobs = {}
    fileroot = '{}/{}'.format(DATADIR, timestamp)
    with open(fileroot + '_json_blobs.json', "w") as outfile:
        outfile.write('# JSON BLOBS for ' + timestamp + '\n')

    json_general   = re.compile(r'try \{ (.*?) = (\[\{.*?)\}catch\(e\)')
    for match in json_general.finditer(html_source):
        name = match[1]
        json_blob = match[2]
        printlog (name, '=', json_blob, len(json_blob))
        json_blobs[name] = json.loads(json_blob)
        with open(fileroot + '_json_blobs.json', "a") as outfile:
            outfile.write(name + '=')
            json.dump(json_blobs[name], outfile, ensure_ascii=False)
            outfile.write('\n')
        #printlog (json.dumps(area_stats, ensure_ascii=False))

        # dump a nicely formatted JSON blob
        if name == 'window.getAreaStat':
            outfile = open(fileroot + '_3g_dxy_cn.csv', "w")
            walk_province_data(outfile, json_blobs[name])
            with open(fileroot + '_getAreaStat.json', "w") as outfile:
                json.dump(json_blobs[name], outfile, ensure_ascii=False)
   
    #printlog(json_blobs.keys())


if __name__ == '__main__':
    DATADIR = '01_download_data/'
    VERBOSE = 0
    LOGFILE = os.getcwd() + '/get_3gdxy_cn.log'

    main()
 

