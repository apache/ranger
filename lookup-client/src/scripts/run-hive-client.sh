#!/bin/bash
case $# in
2 )
	java -cp "./dist/*:./lib/hadoop/*:./lib/hive/*:./conf:."  com.xasecure.hive.client.HiveClientTester "$1" "${2}"  ;;
3 )
	java -cp "./dist/*:./lib/hadoop/*:./lib/hive/*:./conf:."  com.xasecure.hive.client.HiveClientTester "$1" "${2}" "${3}" ;;
4 )
	java -cp "./dist/*:./lib/hadoop/*:./lib/hive/*:./conf:."  com.xasecure.hive.client.HiveClientTester "$1" "${2}" "${3}" "${4}" ;;
5 )
	java -cp "./dist/*:./lib/hadoop/*:./lib/hive/*:./conf:."  com.xasecure.hive.client.HiveClientTester "$1" "${2}" "${3}" "${4}" "${5}" ;;
* )
	java -cp "./dist/*:./lib/hadoop/*:./lib/hive/*:./conf:."  com.xasecure.hive.client.HiveClientTester  ;;
esac
