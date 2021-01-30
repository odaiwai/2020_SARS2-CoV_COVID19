#!/bin/bash

# Script to show the latest few rows of data in the database
#
sqlite3 ncorv2019.sqlite "select * from JHU_data order by date desc limit 10"
