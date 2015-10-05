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

#Usage: Run this script as user hdfs. 
#Creating folders required for Apache Ranger auditing to HDFS. 
#Note 1: Use this script only for non-secure/kerberos environment
#

set -x
hdfs dfs -mkdir -p /ranger/audit
hdfs dfs -chown hdfs:hdfs /ranger/audit
hdfs dfs -chmod 755 /ranger
hdfs dfs -chmod 755 /ranger/audit

hdfs dfs -mkdir -p /ranger/audit/hbaseMaster
hdfs dfs -chown hbase:hbase /ranger/audit/hbaseMaster
hdfs dfs -chmod -R 0700 /ranger/audit/hbaseMaster

hdfs dfs -mkdir -p /ranger/audit/hbaseRegional
hdfs dfs -chown hbase:hbase /ranger/audit/hbaseRegional
hdfs dfs -chmod -R 0700 /ranger/audit/hbaseRegional

hdfs dfs -mkdir -p /ranger/audit/hdfs
hdfs dfs -chown hdfs:hdfs /ranger/audit/hdfs
hdfs dfs -chmod -R 0700 /ranger/audit/hdfs

hdfs dfs -mkdir -p /ranger/audit/hiveServer2
hdfs dfs -chown hive:hive /ranger/audit/hiveServer2
hdfs dfs -chmod -R 0700 /ranger/audit/hiveServer2

hdfs dfs -mkdir -p /ranger/audit/kafka
hdfs dfs -chown kafka:kafka /ranger/audit/kafka
hdfs dfs -chmod -R 0700 /ranger/audit/kafka

hdfs dfs -mkdir -p /ranger/audit/kms
hdfs dfs -chown kms:kms /ranger/audit/kms
hdfs dfs -chmod -R 0700 /ranger/audit/kms

hdfs dfs -mkdir -p /ranger/audit/knox
hdfs dfs -chown knox:knox /ranger/audit/knox
hdfs dfs -chmod -R 0700 /ranger/audit/knox

hdfs dfs -mkdir -p /ranger/audit/solr
hdfs dfs -chown solr:solr /ranger/audit/solr
hdfs dfs -chmod -R 0700 /ranger/audit/solr

hdfs dfs -mkdir -p /ranger/audit/storm
hdfs dfs -chown storm:storm /ranger/audit/storm
hdfs dfs -chmod -R 0700 /ranger/audit/storm

hdfs dfs -mkdir -p /ranger/audit/yarn
hdfs dfs -chown yarn:yarn /ranger/audit/yarn
hdfs dfs -chmod -R 0700 /ranger/audit/yarn
