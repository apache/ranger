#!/bin/bash
# 
# Script to reset mysql database
#


db_user=xaadmin
db_password=xaadmin
db_database=xa_db
outfile=../xa_core_db.sql

echo "Exporting $db_database ...  "
mysqldump -u $db_user  --password=$db_password  $db_database > $outfile
echo "Check output file $outfile"
