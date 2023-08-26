# COVID 19 Data tracker and plotter

Some analysis on the COVID 19 /  Corona Virus outbreak of early 2020.
This will produce images like below:

![Confirmed Cases since reporting started](./plots/Confirmed_since_start.png)

and 

![Confirmed New Cases since reporting started](./plots/Confirmed_new_since_start.png)

## Data Sources
The Data sources and related files are shown in the sections below.  Note that the 
`run_everything.pl` script runs all of these in the appropriate order.

### 3G.DXY.CN
Earlier Versions of the script took China Data from this webpage that seemed to be 
aggregating content from Weibo. Later versions of the script used the JHU Data
 globally.
 - process_ncor_2019_data.py- Get the data from the various online sources
 and populate the database with the latest numbers
 - produce_ncor_plots.py - Produce the plots

### Johns Hopkins University
 - get_jhu_data.sh - Very simple utility to run `git pull` on the JHU data.

### WHO Reports
The WHO is usually behind the times. Trailing indicator.
 - get_disease_outbreak_news.pl - Retrieve the relevant WHO documents. Not parsed
  yet
 - get_WHO_data.py
### Hong Kong Government Information Office Press Releases
 - get_press_releases.pl - Get the HK Government Press Releases that might be
 - process_press_releases.pl - Attempt to estimate relevant data from the GIS
   PR feed

### Centre for Health Protection (HK) data
 - get_historic.pl - Get historic NID data 
 - process_nid_data.pl - Parse the Non-Communicable Infectionous Diseases data
 - I was going to compare to the SARS outbreak, but...

## Other Notes
### 2020/03/11 - NOTES
 - This was started on the 5th of January, when initial reports of some sort of
   illness in Wuhan were started to appear in Hong Kong.  Hence the original name of the
   repository, and many references to Wuhan Coronvirus, etc.  

### 2020/03/27 - NOTES
 - Added separate routines to download and process the data and to produce the
   graphs.

### 2023/04/23 - Hibernation
 - The JHU repository is no longer updated, I could switch to the WHO data, but
   it doesn't seem urgent.
### 2023/08/26 - Using WHO Data
   - Switched to downloading the WHO data, only to find that that's not being
   updated anymore either - stopped on 2023/08/23
