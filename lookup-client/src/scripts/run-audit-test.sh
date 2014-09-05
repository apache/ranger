#!/bin/bash

HADOOP_DIR=/usr/lib/hadoop
HADOOP_CONF_DIR=/etc/hadoop/conf

cp="$HADOOP_CONF_DIR:$HADOOP_DIR/lib/*:$HADOOP_DIR/client/hadoop-common.jar"
export cp

java -Xmx1024M -Xms1024M -cp "${cp}" com.xasecure.audit.test.TestEvents $*
