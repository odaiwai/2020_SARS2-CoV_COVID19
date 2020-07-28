#!/usr/bin/env python3
"""
    Retrieve the List of Covid Buildings from the CHP
"""
import subprocess
import datetime

def get_all():
    for year in range(2020, 2021):
        print(year)
        for month in range(1, 8):
            for date in range(1, 32):
                timestamp = '{:04d}{:02d}{:02d}'.format(year, month, date)
                results = get_one(timestamp)
                print(results.stdout)
    return None

def get_one(timestamp):
    baseuri = r'https://www.chp.gov.hk/files/pdf/building_list_eng_'
    ext = r'.pdf'
    url = baseuri + timestamp + ext
    base_cmd = ['wget',
                '--no-clobber',
                '--directory-prefix', '01_download_data', 
                url
               ]
    print(base_cmd)
    results = subprocess.run(base_cmd, stdout = subprocess.PIPE, text = True)

    return results


def main():
    if FIRSTRUN:
        get_all()
    else:
        today = datetime.datetime.now()
        timestamp = today.strftime('%Y%m%d')
        results = get_one(timestamp)
        print(results.stdout)
    return None


if __name__ == '__main__':
    FIRSTRUN = False
    main()
