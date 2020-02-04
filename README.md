# 2020_wuhan_coronavirus
Some analysis on the Wuhan Corona Virus outbreak of early 2020.

A bunch of scripts to get data files from a few sources, and also to process them.

This is a work in progress!

dave

## 3G.DXY.CN
This webpage seems to be aggregating content from Weibo.
 - get_3gdxy_data.py - Return the Body Text in text form
 - get_3gdxy_json.py - Return the JSON Data in CSV/JSON/HTML

## WHO Reports
The WHO is usually behind the times. Trailing indicator.
- get_disease_outbreak_news.pl - Retrieve the relevant WHO

## Hong Kong Government Information Office Press Releases
 - get_press_releases.pl - Get the HK Government Press Releases that might be
 - process_press_releases.pl - Attempt to estimate relevant data from the GIS
   PR feed

## Centre for Health Protection (HK) data
 - get_historic.pl - Get historic NID data 
 - process_nid_data.pl - Parse the Non-Communicable Infectionous Diseases data
