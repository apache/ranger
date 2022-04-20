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
# limitations under the License

mvn install:install-file -DgroupId=org.apache.atlas -DartifactId=atlas-authorization -Dversion=3.0.0-SNAPSHOT -Dpackaging=jar -Dfile=plugin-atlas/external-libs/atlas-authorization-3.0.0-SNAPSHOT.jar
mvn install:install-file -DgroupId=org.apache.atlas -DartifactId=atlas-intg -Dversion=3.0.0-SNAPSHOT -Dpackaging=jar -Dfile=plugin-atlas/external-libs/atlas-intg-3.0.0-SNAPSHOT.jar

echo "Maven Building"
mvn -pl '!plugin-kylin,!ranger-kylin-plugin-shim, !hdfs-agent, !ranger-hdfs-plugin-shim, !plugin-elasticsearch, !ranger-elasticsearch-plugin-shim, !plugin-kafka, !ranger-kafka-plugin-shim, !kms, !plugin-kms, !ranger-kms-plugin-shim, !plugin-kudu, !plugin-kylin, !ranger-kylin-plugin-shim, !plugin-nifi, !plugin-nifi-registry, !plugin-ozone, !ranger-ozone-plugin-shim, !plugin-presto, !ranger-presto-plugin-shim, !plugin-solr, !ranger-solr-plugin-shim, !plugin-schema-registry, !plugin-sqoop, !ranger-sqoop-plugin-shim, !plugin-yarn, !ranger-yarn-plugin-shim, !hbase-agent, !ranger-hbase-plugin-shim, !hive-agent, !ranger-hive-plugin-shim, !knox-agent, !ranger-knox-plugin-shim, !ranger-storm-plugin-shim' -DskipJSTests -DskipTests=true -Drat.skip=true clean package -Pall -Denforcer.skip=true

echo "[DEBUG] listing /home/runner/work/ranger-policy/ranger-policy/target"
ls /home/runner/work/ranger-policy/ranger-policy/target

echo "[DEBUG] listing target directory"
ls target
