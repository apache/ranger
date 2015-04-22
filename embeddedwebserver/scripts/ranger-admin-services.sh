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

if [[ -z $1 ]]; then
	echo "Invalid argument [$1];"
	echo "Usage: Only start | stop | restart | version, are supported."
	exit;
fi
action=$1

realScriptPath=`readlink -f $0`
realScriptDir=`dirname $realScriptPath`
XAPOLICYMGR_DIR=`(cd $realScriptDir/..; pwd)`

XAPOLICYMGR_EWS_DIR=${XAPOLICYMGR_DIR}/ews
RANGER_JAAS_LIB_DIR="${XAPOLICYMGR_EWS_DIR}/ranger_jaas"
RANGER_JAAS_CONF_DIR="${XAPOLICYMGR_EWS_DIR}/webapp/WEB-INF/classes/conf/ranger_jaas"

JAVA_OPTS=" ${JAVA_OPTS} -XX:MaxPermSize=256m -Xmx1024m -Xms1024m "

if [ -f ${XAPOLICYMGR_DIR}/ews/webapp/WEB-INF/classes/conf/java_home.sh ]; then
        . ${XAPOLICYMGR_DIR}/ews/webapp/WEB-INF/classes/conf/java_home.sh
fi

for custom_env_script in `find ${XAPOLICYMGR_DIR}/ews/webapp/WEB-INF/classes/conf/ -name "ranger-admin-env*"`; do
        if [ -f $custom_env_script ]; then
                . $custom_env_script
        fi
done

if [ "$JAVA_HOME" != "" ]; then
        export PATH=$JAVA_HOME/bin:$PATH
fi

cd ${XAPOLICYMGR_EWS_DIR}
if [ ! -d logs ]
then
        mkdir logs
fi

start() {
	java -Dproc_rangeradmin ${JAVA_OPTS} -Dlogdir=${XAPOLICYMGR_EWS_DIR}/logs/ -Dcatalina.base=${XAPOLICYMGR_EWS_DIR} -cp "${XAPOLICYMGR_EWS_DIR}/webapp/WEB-INF/classes/conf:${XAPOLICYMGR_EWS_DIR}/lib/*:${RANGER_JAAS_LIB_DIR}/*:${RANGER_JAAS_CONF_DIR}:${JAVA_HOME}/lib/*" org.apache.ranger.server.tomcat.EmbeddedServer > logs/catalina.out 2>&1 &
	echo "Apache Ranger Admin has started."
}

stop(){
	java ${JAVA_OPTS} -Dcatalina.base=${XAPOLICYMGR_EWS_DIR} -cp "${XAPOLICYMGR_EWS_DIR}/webapp/WEB-INF/classes/conf:${XAPOLICYMGR_EWS_DIR}/lib/*:${RANGER_JAAS_LIB_DIR}/*:${RANGER_JAAS_CONF_DIR}" org.apache.ranger.server.tomcat.StopEmbeddedServer > logs/catalina.out 2>&1
	echo "Apache Ranger Admin has been stopped."

}

if [ ${action^^} == "START" ]; then
	start;
	exit;
elif [ ${action^^} == "STOP" ]; then
	stop;
	exit;
elif [ ${action^^} == "RESTART" ]; then
	echo "Restarting Apache Ranger Admin"
	stop;
	sleep 2
	start;
	exit;
elif [ ${action^^} == "VERSION" ]; then
	cd ${XAPOLICYMGR_EWS_DIR}/webapp/WEB-INF/lib
	java -cp ranger-util-*.jar org.apache.ranger.common.RangerVersionInfo
	exit;
else
    echo "Invalid argument [$1];"
    echo "Usage: Only start | stop | restart | version, are supported."
    exit;
fi
