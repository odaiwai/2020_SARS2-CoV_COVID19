#!/bin/bash

# Script to refresh the JHU data
# This repository stopped updating after March 23, 2023
#
BASEDIR=`pwd`
cd "$BASEDIR/JHU_data/COVID-19/"
git pull > "$BASEDIR/JHU_data.log" 2>&1

cd "$BASEDIR"
cat ./JHU_data.log

