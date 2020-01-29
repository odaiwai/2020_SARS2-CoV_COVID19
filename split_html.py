#!/usr/bin/env python3
"""
split the HTML from 3G_DGY.CN into JSON Blobs and Decode them
into a CSV file

dave o'brien (c) 2020
"""
import os, json, re, sys


def top_and_tail(string, width):
    return (string[0:width] + '...' + string[-width:])

if __name__ == '__main__':
    WIDTH = 50

    INFILE = sys.argv[1] # r'./01_download_data/全国新型肺炎疫情实时动态 - 丁香园·丁香医生.html'
    infh = open(INFILE, "r")
    html_source = infh.read()
    infh.close()

    print(top_and_tail, html_source, 100)

    #compile a regexp to match a script
    jsonareamatch   = re.compile(r'(\[\{\"provinceName\"\:.*\"cities\"\:\[\]\}\])')

    print(jsonareamatch, type(jsonareamatch))

    for match in jsonareamatch.finditer(html_source ): # list of scripts
        print (match, type(match))
        #print (match[0])
        area_stats = json.loads(match[0])
        #print(json.dumps(area_stats, indent=4, ensure_ascii=False))

        with open('getAreaStat.json', "w") as outfile:
            json.dump(area_stats, outfile, indent=4, ensure_ascii=False)

        # Walk the tree...
        header = "Province, City, Confirmed, Suspected, Cured, Dead, comment"
        outfh = open('3g_dxy_cn.csv', "w")
        print (header)
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
            print(case_count)
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
                print(case_count)
                outfh.write(case_count + '\n')

        outfh.close()

