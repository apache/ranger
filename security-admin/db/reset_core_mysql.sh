#!/bin/bash
#
# Script to reset mysql database
#

#if [ $# -lt 1 ]; then
#       echo "Usage: $0 <db_root_password> [db_host] <db_name>"
#       exit 1
#fi
#
#db_root_password=$1
#db_host="localhost"
#if [ "$2" != "" ]; then
#    db_host="$2"
#fi
#db_name="xa_db"
#if [ "$3" != "" ]; then
#    db_name="$3"
#fi

db_user=xaadmin
db_password=xaadmin
db_file=xa_core_db.sql
db_name=xa_db

echo "Importing database file $db_file.sql ...  "
set -x
mysql -u $db_user  --password=$db_password --database=$db_name < $db_file
