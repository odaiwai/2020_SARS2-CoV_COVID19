#!/usr/bin/env python3
""" Download and plot the Apple Mobility Data"
"""
import os
import json
import requests
import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.dates as mdates # for date formatting

def main():
    # get the index json
    uri_list = ['www.apple.com' , 'local.apple.com' , 'covid19-static.cdn-apple.com']
    cdn_uri = r'https://{}'.format(uri_list[2])
    idx_path = r'/covid19-mobility-data/current/v1/index.json'
    response = requests.get('{}{}'.format(cdn_uri, idx_path))
    # Walk it and get the mobility files
    idx_json = json.loads(response.text)
    basePath = idx_json['basePath']
    regions = idx_json['regions']
    for locale in regions.keys():
        jsonPath = idx_json['regions'][locale]['jsonPath']
        csvPath =  idx_json['regions'][locale]['csvPath']
        version =  idx_json['regions'][locale]['mobilityDataVersion']
        print (idx_json, '\n', locale, jsonPath, csvPath, version)
        # Download them and build some graphs
        response = requests.get('{}/{}/{}'.format(cdn_uri, basePath, jsonPath))
        print(response)#, response.text)
        mob_data = json.loads(response.text)
        places = {}
        names = {}
        titles = {}
        for place in mob_data['data']:
            print (place)
            places[place] = places.setdefault(place, 0) + 1
            place_data = mob_data['data'][place]
            for dataset in place_data:
                print(dataset)
                for key in ['name', 'title', 'values']:
                    if key == 'name':
                        names[dataset[key]] = names.setdefault(dataset[key], 0) + 1
                    if key == 'title':
                        titles[dataset[key]] = titles.setdefault(dataset[key], 0) + 1
                    print(place, ':', key, ':', dataset[key])
        
        print (places)
        print (names)
        print (titles)
    return None
if __name__ == '__main__':
    LOCALE = 'en-us'
    main()
