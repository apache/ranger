#!/bin/bash
cp="./dist/*:./conf/:.:./lib/hadoop/*:./lib/hive/*:./lib/hbase/*"

case $# in
2 )
java ${JOPTS} -cp "${cp}" com.xasecure.hbase.client.HBaseClientTester  "${1}" "${2}" ;;
3 )
java  ${JOPTS} -cp "${cp}" com.xasecure.hbase.client.HBaseClientTester  "${1}" "${2}" "${3}" ;;
4 )
java ${JOPTS} -cp "${cp}" com.xasecure.hbase.client.HBaseClientTester  "${1}" "${2}" "${3}" "${4}" ;;
* )
java ${JOPTS} -cp "${cp}" com.xasecure.hbase.client.HBaseClientTester;;
esac