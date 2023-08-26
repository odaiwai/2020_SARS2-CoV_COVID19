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
    with open(f'{outdir}/WHO_data_urls.json', 'r', encoding='utf-8') as infh:
        urls = json.loads(infh.read())
        print(urls)
    for outfile, url in urls.items():
        print(f'{url} encoding=', end='')
        result = requests.get(url, timeout=30)
        # decode from the
        print(result.apparent_encoding[:50])
        print('\t', result.content[:50])
        output = result.content.decode(result.apparent_encoding)
        # output = output.encode('utf-8')
        print('\t', output[:100])
        # print(result.content)
        with open(f'{outdir}/{outfile}.csv', 'w',
                  encoding=result.apparent_encoding) as outfh:
            print(output, file=outfh)


if __name__ == '__main__':
    main()
