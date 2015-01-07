#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


HADOOP_DIR=/usr/hdp/current/hadoop-client
HADOOP_LIB_DIR=/usr/hdp/current/hadoop-client/lib
HADOOP_CONF_DIR=/etc/hadoop/conf

cp=./ranger-plugins-audit-0.4.0.jar
for jar in $HADOOP_CONF_DIR $HADOOP_LIB_DIR/commons-logging-1.1.3.jar $HADOOP_LIB_DIR/log4j-1.2.17.jar $HADOOP_LIB_DIR/eclipselink-2.5.2-M1.jar $HADOOP_LIB_DIR/gson-2.2.4.jar $HADOOP_LIB_DIR/javax.persistence-2.1.0.jar $HADOOP_LIB_DIR/mysql-connector-java.jar $HADOOP_DIR/hadoop-common.jar
do
  cp=${cp}:${jar}
done

export cp

java -Xmx1024M -Xms1024M -cp "${cp}" org.apache.ranger.audit.test.TestEvents $*
