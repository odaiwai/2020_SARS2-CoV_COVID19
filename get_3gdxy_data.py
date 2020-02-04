#!/usr/bin/env python3
"""
    Go to the 3g.dxy.cn page about the nCoV-2019 outbreak and scrape it for useful
    information.


   This page is in simplified Chinese.

"""
# Selenium
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.expected_conditions import presence_of_element_located

import os, sys, re, datetime


def show_me_all(webelements_list):
    for result in webelements_list:
        print ("Object", result)
        print ("Type", type(result))
        print ("Tags", result.tag_name)
        print ("Dict", result.__dict__)
        print ("text", result.text)

# Main()
if __name__ == '__main__':
    # Go into Headless Mode
    opts = Options()
    opts.set_headless()
    assert opts.headless
    os.environ["PATH"] += os.pathsep + '/usr/local/bin'
 
    # Get the time and open the outfile
    timestamp = datetime.datetime.now() 
    outfile = "01_download_data/3g_dxy_cn_" + timestamp.strftime("%Y%m%d_%H%M%S") + ".txt"
    fh = open(outfile, "w")
    fh.write(timestamp.strftime("%Y-%m-%d %h:%m:%s"))
    
    # This page is in simplified Chinese.
    #baseurl= "https://3g.dxy.cn/newh5/view/pneumonia?scene=2"
    #baseurl= "https://3g.dxy.cn/newh5/view/pneumonia"
    baseurl= "https://ncov.dxy.cn/ncovh5/view/pneumonia" # new URL
    fh.write("Opening: " + baseurl)
    browser = webdriver.Firefox(options=opts)
    wait = WebDriverWait(browser, 30)
    browser.get(baseurl)


    fh.write("Getting page_body...")
    page_body = browser.find_element_by_tag_name('body')
    fh.write(page_body.text)
 

    #print ("Source")
    #print(source.text)

    #show_me_all(tables)
    """
    截至 2020-01-27 16:50（北京时间）数据统计   ; Date and Time
    确诊 2823 例，疑似 5794 例                  ; Confirmed and Suspected Cases
    死亡 81 例，治愈 58 例                      ; Dead, cured
    """
    mdatetime = re.search(r'截至 ([0-9-]+ [0-9:]+) ', page_body.text)          # Date and Time:wq
    mdate = re.search(r'截至 ([0-9-]+) ', page_body.text)          # Date and Time:wq
    mtime = re.search(r' ([0-9:]+) \(北京时间\）', page_body.text) # Date and Time
    mconf = re.search(r'确诊 ([0-9]+) 例，', page_body.text)       # Confirmed and Suspected Cases
    msusp = re.search(r'，疑似 ([0-9]+) 例', page_body.text)       # Confirmed and Suspected Cases
    mdead = re.search(r'死亡 ([0-9]+) 例，', page_body.text)       # Dead, cured
    mcure = re.search(r'治愈 ([0-9]+) 例', page_body.text)         # Dead, cured

    #print (mdatetime)
    #print (mdate, mtime)
    #print (mconf, msusp)
    #print (mdead, mcure)

    # write the data to a database?
    #conn = sqlite3.connect('health_data.sqlite')
    #c = conn.cursor()
    #conn.commit()
    #conn.close()


    # tidy up:
    fh.close()
    browser.quit()  # Quits the running browser

