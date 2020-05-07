#!/bin/bash

# Script to refresh the JHU data

cd HGIS_UW_data/
DATE=`date "+%Y-%m-%d %H:%M:%S"`
# -N stops the creation of .1, etc
wget -q -N http://hgis.uw.edu/virus/assets/virus.csv 1>> ../HGIS_data.log 2>&1
git add virus.csv
#git add HGIS_UW_data/virus.csv
git commit -m "Added HGIS data on $DATE" virus.csv



