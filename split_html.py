#!/usr/bin/env python3
"""
split the HTML from 3G_DGY.CN into JSON Blobs and Decode them
into a CSV file

if you get codepage errors running under Windows you'll need to update the code page:
   
https://stackoverflow.com/questions/6812031/how-to-make-unicode-string-with-python3

Tested on a Mac running Mojave, and a Linux box runnng Fedora 30, works on Windows (as 
noted above for CMD, and with no problems in a Cygwin shell.)

dave o'brien (c) 2020
"""
import os, json, re, sys
import requests
import datetime

def top_and_tail(string, width):
    return (string[0:width] + '...' + string[-width:])

if __name__ == '__main__':
    WIDTH = 50
    html_source = ''
    timestamp = ''

    print (len(sys.argv))
    if len(sys.argv) > 1:
        # parse an existing file
        INFILE = sys.argv[1] # r'./01_download_data/全国新型肺炎疫情实时动态 - 丁香园·丁香医生.html'
        infh = open(INFILE, "r")
        html_source = infh.read()
        infh.close()
    else:
        # Go to the URL and retrieve it
        response = requests.get(r'https://3g.dxy.cn/newh5/view/pneumonia')
        html_source = response.content.decode()

    # try to figure out when this file was made so we can timestamp it.
    #  overall strategy is to find the latest time reference in the html
    timestamp_match = re.compile(r'Time\"\:([0-9]+)\,')
    latest_timestamp = datetime.datetime.fromtimestamp(0)
    for match in timestamp_match.finditer(html_source):
        #print (match, type(match), match[1], type(match[1]))
        found_time = datetime.datetime.fromtimestamp(int(float(match[1])/1000))
        if found_time > latest_timestamp:
            latest_timestamp = found_time
            #print (found_time, latest_timestamp)
    
    timestamp = latest_timestamp.strftime("%y%m%d_%H%M%S")
    print (latest_timestamp, timestamp)
    
    #compile a regexp to match a script
    jsonareamatch   = re.compile(r'(\[\{\"provinceName\"\:.*\"cities\"\:\[\]\}\])')
    print(jsonareamatch, type(jsonareamatch))
    for match in jsonareamatch.finditer(html_source): # list of scripts
        print (match, type(match))
        #print (match[0])
        area_stats = json.loads(match[0])
        #print(json.dumps(area_stats, indent=4, ensure_ascii=False))

        with open('01_download_data/' + timestamp + '_getAreaStat.json', "w") as outfile:
            json.dump(area_stats, outfile, indent=4, ensure_ascii=False)

        # Walk the tree...
        header = "Province, City, Confirmed, Suspected, Cured, Dead, comment"
        outfh = open('01_download_data/' + timestamp + '_3g_dxy_cn.csv', "w")
        #print (header)
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
            #print(case_count)
            outfh.write(case_count + '\n')

            for city in province["cities"]:
                #print('\tCity', type(city), city)
                case_count = '"{}", "{}", {}, {}, {}, {}'.format(
                    province["provinceName"],
                    city["cityName"],
                    city["confirmedCount"],
                    city["suspectedCount"],
                    city["curedCount"],
                    city["deadCount"])
                #print(case_count)
                outfh.write(case_count + '\n')

        outfh.close()

