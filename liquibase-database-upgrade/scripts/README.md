<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->
# KMS
## Sample exports and commands
```
export JAVA_HOME=/usr/java/jdk1.8.0
export RANGER_KMS_HOME=/var/lib/ranger-kms
#get process folder for kms: ps -ef | grep ranger-kms
export RANGER_KMS_CONF=/var/run/process/1546336381-ranger_kms-RANGER_KMS_SERVER/conf
export SQL_CONNECTOR_JAR=/usr/share/java/postgresql-connector-java.jar
export LIQUI_SEARCH_PATH=/root/ranger-3.0.0-SNAPSHOT-liquibase-upgrade/lib
#get HADOOP_CREDSTORE_PASSWORD: cat /var/run/process/1546336381-ranger_kms-RANGER_KMS_SERVER/proc.json | grep HADOOP
export HADOOP_CREDSTORE_PASSWORD=eo7im023ga7kv6ttbaj7y7a41
export LOG4J_PROPERTIES_FILE_PATH=/root/ranger-3.0.0-SNAPSHOT-liquibase-upgrade/sample_resources/kms_log4j.properties
=====
bash upgrade_kms_db.sh kms update 3.x
=====
bash upgrade_kms_db.sh kms finalize 3.x
=====
bash upgrade_kms_db.sh kms rollback 3.x
=====
This prints 2 variables OP_STATUS and IS_UPGRADE_COMPLETE
bash kms_liquibase_utils.sh kms isUpgradeComplete 7.3.2
=====
This prints 2 variables OP_STATUS and IS_FINALIZE_COMPLETE
bash kms_liquibase_utils.sh kms isFinalizeComplete 3.x
=====

```

# Ranger
## Sample exports and commands
```
#FOR RANGER

export JAVA_HOME_RANGER=/usr/java/jdk1.8.0
export RANGER_HOME=/var/lib/ranger-admin
export RANGER_CONF=/var/process/1546336029-ranger-RANGER_ADMIN/conf
export SQL_CONNECTOR_JAR_RANGER=/usr/share/java/postgresql-connector-java.jar
export LIQUI_SEARCH_PATH_RANGER=/root/ranger-3.0.0-SNAPSHOT-liquibase-upgrade
#export HADOOP_CREDSTORE_PASSWORD=<Value from /var/run/process/1546336029-ranger-RANGER_ADMIN/proc.json>
export HADOOP_CREDSTORE_PASSWORD=8ezax7470bysu6ng0a3qghfzd
export LOG4J_PROPERTIES_FILE_PATH_RANGER=/root/ranger-3.0.0-SNAPSHOT-liquibase-upgrade/sample_resources/ranger_log4j.properties

=====
bash upgrade_ranger_db.sh ranger update 3.x
=====
bash upgrade_ranger_db.sh ranger finalize 3.x
=====
bash upgrade_ranger_db.sh ranger rollback 3.x
=====
```
