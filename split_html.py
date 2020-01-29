#!/usr/bin/env python3
"""
split the HTML from 3G_DGY.CN into JSON Blobs and Decode them

dave o'brien
"""
import os, json, re

def top_and_tail(string, width):
    return (string[0:width] + '...' + string[-width:])

if __name__ == '__main__':
    WIDTH = 50

    INFILE = r'./01_download_data/全国新型肺炎疫情实时动态 - 丁香园·丁香医生.html'
    infh = open(INFILE, "r")
    html_source = infh.read()
    print(top_and_tail(html_source, 100))
    infh.close()

    #File is one packed line, but we only want the JSON in the <script></script> blocks
    #compile a regexp to match a script
    scriptmatch = re.compile(r'script.*>(.*?)</script')       # Script in the header
    jsonmatch   = re.compile(r'try \{ (.*) = \{.*\})\}catch')   # Java Script to load the JSON
    jlistmatch   = re.compile(r'try \{ (.*) = \[(\{.*\})\]\}catch')   # Java Script to load the JSON
    idblock   = re.compile(r'(,\{"id":.*\})$')          # Split the JSONs from the end of the line

    print(scriptmatch, type(scriptmatch))
    print(jsonmatch, type(jsonmatch))
    print(jlistmatch, type(jlistmatch))

    scripts = scriptmatch.findall(html_source ) # list of scripts
    print (len(scripts))

    for script in scripts:
        print('\tSCR:', len(script), top_and_tail(script, WIDTH))
        matches = jsonmatch.findall(html_source)
        for match in matches:
            #print('\tJSON: ', match[0], '=', top_and_tail(match[1], WIDTH))
            #name = match[0]
            #print(match[1]) # This can be more than one JSON Blob
            #data = json.loads(match[1])
            #with open(name + '.json', "w") as outfile:
                #json.dump(data, outfile)

            #print(json.dumps(data, indent=4))





