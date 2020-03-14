#!/bin/bash

# Script to refresh the JHU data

cd HGIS_UW_data/
wget http://hgis.uw.edu/virus/assets/virus.csv
git add virus.csv
git commit -m "added HGIS data on $DATE" 1> ../JHU_data.log 2>&1


