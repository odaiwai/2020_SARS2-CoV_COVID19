#!/bin/bash

echo "show the latest few rows of data in the database..."
sqlite3 ncorv2019.sqlite "select * from JHU_data order by date desc limit 10"

echo "Show the latest JHU pull:"
head -10 ./JHU_data/JHU_data.log

echo "Check for merge conflicts:"
cd JHU_data/COVID-19/; git status
