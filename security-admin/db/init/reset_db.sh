#!/bin/bash
if [ $# -ne 3 ]; then
	echo "Usage: $0 <db_user> <db_password> <db_database> [db_host]"
	exit 1
fi

db_user=$1
db_password=$2
db_database=$3

set -x
#First drop the database and recreate i
echo "y" | mysqladmin -u $db_user -p$db_password drop $db_database
mysqladmin -u $db_user -p$db_password create $db_database

#Create the schema
mysql -u $db_user -p$db_password $db_database < schema_mysql.sql

