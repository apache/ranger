#!/bin/bash
if [ "$HADOOP_HOME" == "" ]; then
	export HADOOP_HOME=/usr/hdp/current/hadoop-client
fi