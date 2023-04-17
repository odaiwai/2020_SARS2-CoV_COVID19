#!/usr/bin/env python3
"""
    Script to do stuff in Python.

"""

# import os
# import sys
import json
import requests


def main():
    """
    Fetch the CSV data from the WHO site: https://covid19.who.int/data
    """
    outdir = 'WHO_data'
    with open('WHO_data_urls.json', 'r', encoding='utf-8') as infh:
        urls = json.loads(infh.read())
        print(urls)
        for url in urls:
            print(urls[url])
            result = requests.get(urls[url])
            print(result.content)
            with open(f'{outdir}/{url}.csv', 'w', encoding=result.encoding) as outfh:
                print(result.content, file=outfh)


if __name__ == '__main__':
    main()
