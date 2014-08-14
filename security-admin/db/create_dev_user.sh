#!/bin/bash
# 
# Script to reset mysql database
#

if [ $# -lt 1 ]; then
	echo "Usage: $0 <db_root_password> [db_host]"
	exit 1
fi

db_root_password=$1
db_host="localhost"
if [ "$2" != "" ]; then
    db_host="$2"
fi

echo "Creating user  ...  "
set -x
mysql -u root  --password=$db_root_password < create_dev_user.sql
