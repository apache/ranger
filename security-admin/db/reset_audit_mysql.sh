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
#db_name="xa_logger"
#if [ "$3" != "" ]; then
#    db_name="$3"
#fi

audit_db_user=xaadmin
audit_db_password=xaadmin
audit_db_file=xa_audit_db.sql
audit_db_name=xa_logger
echo "Importing database file $audit_db_file.sql ...  "
set -x
mysql -u $audit_db_user  --password=$audit_db_password  -e "create database IF NOT EXISTS $audit_db_name"
mysql -u $audit_db_user  --password=$audit_db_password --database=$audit_db_name < $audit_db_file
