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

if [ -z "$1" ]
then
	echo "Invalid argument [$1];"
	echo "Usage: Only start | stop | restart | version, are supported."
	exit;
fi
action=$1

realScriptPath=`readlink -f $0`
realScriptDir=`dirname $realScriptPath`
RANGER_KMS_DIR=`(cd $realScriptDir; pwd)`
RANGER_KMS_EWS_DIR=${RANGER_KMS_DIR}/ews
RANGER_KMS_EWS_CONF_DIR="${RANGER_KMS_EWS_DIR}/conf"
RANGER_KMS_EWS_LIB_DIR="${RANGER_KMS_EWS_DIR}/lib"

JAVA_OPTS=" ${JAVA_OPTS} -XX:MaxPermSize=256m -Xmx1024m -Xms1024m "

for custom_env_script in `find ${RANGER_KMS_DIR}/ews/conf/ -name "ranger-kms-env*"`; do
        if [ -f $custom_env_script ]; then
                . $custom_env_script
        fi
done

if [ "$JAVA_HOME" != "" ]; then
        export PATH=$JAVA_HOME/bin:$PATH
fi

cd ${RANGER_KMS_EWS_DIR}

if [ ! -d logs ]
then
        mkdir logs
fi


PROC_NAME=proc_rangerkms
export PROC_NAME

START_CLASS_NAME="org.apache.ranger.server.tomcat.EmbeddedServer"

STOP_CLASS_NAME="org.apache.ranger.server.tomcat.StopEmbeddedServer"

KMS_CONFIG_FILENAME=kms_webserver.properties

TOMCAT_LOG_DIR=/var/log/ranger/kms

TOMCAT_LOG_FILE=${TOMCAT_LOG_DIR}/catalina.out
TOMCAT_STOP_LOG_FILE=${TOMCAT_LOG_DIR}/stop_catalina.out

if [ ! -d ${TOMCAT_LOG_DIR} ]
then
	mkdir -p ${TOMCAT_LOG_DIR}
fi

KMS_CONF_DIR=${RANGER_KMS_EWS_DIR}/webapp/config/

JAVA_OPTS="${JAVA_OPTS} -Dcatalina.base=${RANGER_KMS_EWS_DIR} -Dkms.config.dir=${KMS_CONF_DIR} -Dkms.log.dir=${TOMCAT_LOG_DIR} -cp ${RANGER_KMS_EWS_CONF_DIR}:${RANGER_KMS_EWS_LIB_DIR}/*:${RANGER_KMS_EWS_DIR}/webapp/lib/*:${JAVA_HOME}/lib/* "

if [ "${action^^}" == "START" ]; then
	echo "+ java -D${PROC_NAME} ${JAVA_OPTS} ${START_CLASS_NAME} ${KMS_CONFIG_FILENAME} "
	java -D${PROC_NAME} ${JAVA_OPTS} ${START_CLASS_NAME} ${KMS_CONFIG_FILENAME} > ${TOMCAT_LOG_FILE} 2>&1 &
	echo "Apache Ranger KMS has started."
	exit
elif [ "${action^^}" == "STOP" ]; then
	java ${JAVA_OPTS} ${STOP_CLASS_NAME} ${KMS_CONFIG_FILENAME} > ${TOMCAT_STOP_LOG_FILE} 2>&1
	echo "Apache Ranger KMS has been stopped."
	exit
elif [ "${action^^}" == "RESTART" ]; then
	echo "Restarting Apache Ranger KMS"
	java ${JAVA_OPTS} ${STOP_CLASS_NAME} ${KMS_CONFIG_FILENAME} > ${TOMCAT_STOP_LOG_FILE} 2>&1
	echo "Apache Ranger KMS has been stopped."
	echo "Starting Apache Ranger KMS."
	java -D${PROC_NAME} ${JAVA_OPTS} ${START_CLASS_NAME} ${KMS_CONFIG_FILENAME} > ${TOMCAT_LOG_FILE} 2>&1 &
	echo "Apache Ranger KMS has started successfully."
	exit
elif [ "${action^^}" == "VERSION" ]; then
	( cd ${RANGER_KMS_LIB_DIR} ; java -cp ranger-util-*.jar org.apache.ranger.common.RangerVersionInfo )
	exit
else 
        echo "Invalid argument [$1];"
        echo "Usage: Only start | stop | restart | version, are supported."
        exit;
fi
