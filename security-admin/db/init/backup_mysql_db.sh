#!/bin/bash
# 
# Script to reset mysql database
#

if [ $# -lt 3 ]; then
	echo "Usage: $0 <db_user> <db_password> <db_database> <output file>"
	exit 1
fi

db_user=$1
db_password=$2
db_database=$3
outfile=$4

echo "Exporting $db_database ...  "
mysqldump -u $db_user  --password=$db_password --add-drop-database --database $db_database > $outfile
echo "Check output file $outfile"
