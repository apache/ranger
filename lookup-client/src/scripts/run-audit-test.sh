#!/bin/bash

XA_AGENTS_DIR=$HOME/Hortonworks/git/xa-agents-2.1

CONF_DIR=$XA_AGENTS_DIR/conf/hadoop
HADOOP_LIB_DIR=$XA_AGENTS_DIR/lib/hadoop-hdp-2.0
JPA_LIB_DIR=$XA_AGENTS_DIR/lib/jpa
MYSQL_LIB_DIR=$XA_AGENTS_DIR/lib/mysql

cp="$CONF_DIR:$HADOOP_LIB_DIR/*:$JPA_LIB_DIR/*:$MYSQL_LIB_DIR/*:$XA_AGENTS_DIR/dist/xasecure-audit.jar"
export cp

java -Xmx1024M -Xms1024M -cp "${cp}" com.xasecure.audit.test.TestEvents $*
