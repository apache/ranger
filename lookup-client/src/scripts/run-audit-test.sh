#!/bin/bash

HADOOP_DIR=/usr/hdp/current/hadoop
HADOOP_CONF_DIR=/etc/hadoop/conf

cp=
for jar in $HADOOP_CONF_DIR $HADOOP_DIR/lib/* $HADOOP_DIR/client/*
do
  cp=${cp}:${jar}
done

export cp

java -Xmx1024M -Xms1024M -cp "${cp}" com.xasecure.audit.test.TestEvents $*
