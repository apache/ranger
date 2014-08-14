#!/bin/bash
case $# in
4 )
    java -cp "./dist/*:./lib/hadoop/*:./conf:." com.xasecure.hadoop.client.HadoopFSTester  "${1}" "${2}" "${3}" "${4}" ;;
* )
    java -cp "./dist/*:./lib/hadoop/*:./conf:." com.xasecure.hadoop.client.HadoopFSTester   ;;
esac