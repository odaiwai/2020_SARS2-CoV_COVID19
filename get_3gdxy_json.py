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

def main():
    html_source = ''
    timestamp = ''
    overwrite = 1

    if len(sys.argv) > 1:
        # parse an existing file
        infh = open(sys.argv[1], "r")
        html_source = infh.read()
        infh.close()
        overwrite = 0
    else:
        # Go to the URL and retrieve it
        response = requests.get(r'https://3g.dxy.cn/newh5/view/pneumonia')
        #response = requests.get(r'https://ncov.dxy.cn/ncovh5/view/pneumonia')
        html_source = response.content.decode()


    # try to figure out when this file was made so we can timestamp it.
    # The overall strategy is to find the latest time reference in the html
    # ASSUMPTION: time only moves in a forward direction.
    timestamp_match = re.compile(r'Time\"\:([0-9]+)\,')
    latest_timestamp = datetime.datetime.fromtimestamp(0)
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
    
    #compile a regexp to match a script
    json_areastats = re.compile(r'\.get(AreaStat) = (\[\{\"provinceName\"\:.*?\}\])\}catch\(e\)')
    json_general   = re.compile(r'\.get(.*) = (\[\{\"provinceName\"\:.*?\}\])\}catch\(e\)')
    #printlog (jsonareamatch, type(jsonareamatch))
    for match in json_areastats.finditer(html_source): # list of scripts
        printlog (match, type(match))
        printlog (type(match[0]), match[0])
        printlog (type(match[1]), match[1])
        printlog (type(match[2]), match[2])
        area_stats = json.loads(match[2])
        printlog (json.dumps(area_stats, indent=4, ensure_ascii=False))

        # dump a nicely formatted JSON blob
        with open('01_download_data/' + timestamp + '_getAreaStat.json', "w") as outfile:
            json.dump(area_stats, outfile, indent=4, ensure_ascii=False)

        # Walk the tree...
        header = "Province, City, Confirmed, Suspected, Cured, Dead, comment"
        outfh = open('01_download_data/' + timestamp + '_3g_dxy_cn.csv', "w")
        printlog (header)
        outfh.write(header + '\n')
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
            outfh.write(case_count + '\n')

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
                outfh.write(case_count + '\n')

        outfh.close()

if __name__ == '__main__':
    DATADIR = '01_download_data/'
    VERBOSE = 0
    LOGFILE = os.getcwd() + '/get_3gdxy_cn.log'
 
    main()


