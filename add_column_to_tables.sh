#!/bin/bash

tables="cn_city cn_prov"
column=$1
ctype=$2

for table in $tables
do
	echo "adding column $column ($ctype) to [$table]"
	result=`sqlite3 ncorv2019.sqlite "alter table [$table] add column $column $ctype"`
	echo "$result"
done
