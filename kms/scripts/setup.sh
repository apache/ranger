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
# -------------------------------------------------------------------------------------
#
# Ranger KMS Setup Script
#
# This script will install ranger kms webapplication under tomcat and also, initialize the database with ranger kms users/tables.

realScriptPath=`readlink -f $0`
realScriptDir=`dirname $realScriptPath`
RANGER_KMS_DIR=`(cd $realScriptDir/..; pwd)`
RANGER_KMS_EWS_DIR=${RANGER_KMS_DIR}/ews
RANGER_KMS_EWS_CONF_DIR="${RANGER_KMS_EWS_DIR}/conf"
RANGER_KMS_EWS_LIB_DIR="${RANGER_KMS_EWS_DIR}/lib"

PROPFILE=${RANGER_KMS_EWS_CONF_DIR}/kms_webserver.properties
propertyValue=''

. $PROPFILE 1>/dev/null 2>&1
if [ ! $? = "0" ];then
	log "$PROPFILE file not found....!!";
	exit 1;
fi

usage() {
  [ "$*" ] && echo "$0: $*"
  sed -n '/^##/,/^$/s/^## \{0,1\}//p' "$0"
  exit 2
} 2>/dev/null

log() {
   local prefix="[$(date +%Y/%m/%d\ %H:%M:%S)]: "
   echo "${prefix} $@" >> $LOGFILE
   echo "${prefix} $@"
}

check_ret_status(){
	if [ $1 -ne 0 ]; then
		log "[E] $2";
		exit 1;
	fi
}

is_command () {
    log "[I] check if command $1 exists"
    type "$1" >/dev/null
}

get_distro(){
	log "[I] Checking distribution name.."
	ver=$(cat /etc/*{issues,release,version} 2> /dev/null)
	if [[ $(echo $ver | grep DISTRIB_ID) ]]; then
	    DIST_NAME=$(lsb_release -si)
	else
	    DIST_NAME=$(echo $ver | cut -d ' ' -f 1 | sort -u | head -1)
	fi
	export $DIST_NAME
	log "[I] Found distribution : $DIST_NAME"

}

init_logfiles () {
    for f in $LOGFILES; do
        touch $f
    done
}

init_variables(){
	curDt=`date '+%Y%m%d%H%M%S'`

	INSTALL_DIR=${RANGER_KMS_DIR}

	DB_FLAVOR=`echo $DB_FLAVOR | tr '[:lower:]' '[:upper:]'`
	if [ "${DB_FLAVOR}" == "" ]
	then
		DB_FLAVOR="MYSQL"
	fi
	log "[I] DB_FLAVOR=${DB_FLAVOR}"
}

check_python_command() {
		if is_command ${PYTHON_COMMAND_INVOKER} ; then
			log "[I] '${PYTHON_COMMAND_INVOKER}' command found"
		else
			log "[E] '${PYTHON_COMMAND_INVOKER}' command not found"
		exit 1;
		fi
}

check_java_version() {
	#Check for JAVA_HOME
	if [ "${JAVA_HOME}" == "" ]
	then
		log "[E] JAVA_HOME environment property not defined, aborting installation."
		exit 1
	fi

        export JAVA_BIN=${JAVA_HOME}/bin/java

	if is_command ${JAVA_BIN} ; then
		log "[I] '${JAVA_BIN}' command found"
	else
               log "[E] '${JAVA_BIN}' command not found"
               exit 1;
	fi

	$JAVA_BIN -version 2>&1 | grep -q $JAVA_VERSION_REQUIRED
	if [ $? != 0 ] ; then
		log "[E] Java 1.7 is required"
		exit 1;
	fi
}

sanity_check_files() {

	if test -d $app_home; then
		log "[I] $app_home folder found"
	else
		log "[E] $app_home does not exists" ; exit 1;
    fi
	if [ "${DB_FLAVOR}" == "MYSQL" ]
    then
		if test -f $mysql_core_file; then
			log "[I] $mysql_core_file file found"
		else
			log "[E] $mysql_core_file does not exists" ; exit 1;
		fi
	fi	
}

copy_db_connector(){
	log "[I] Copying ${DB_FLAVOR} Connector to $app_home/lib ";
    cp -f $SQL_CONNECTOR_JAR $app_home/lib
	check_ret_status $? "Copying ${DB_FLAVOR} Connector to $app_home/lib failed"
	log "[I] Copying ${DB_FLAVOR} Connector to $app_home/lib DONE";
}

setup_kms(){
        #copying ranger kms provider 
        cd ${RANGER_KMS_EWS_DIR}/webapp
        log "[I] Adding ranger kms provider as services in hadoop-common jar"
        jar -uf lib/hadoop-common*.jar META-INF/services/org.apache.hadoop.crypto.key.KeyProviderFactory
}


init_logfiles
log " --------- Running ranger kms Web Application Install Script --------- "
log "[I] uname=`uname`"
log "[I] hostname=`hostname`"
init_variables
get_distro
check_java_version
sanity_check_files
copy_db_connector
check_python_command
$PYTHON_COMMAND_INVOKER db_setup.py	
setup_kms

echo "Installation of ranger kms is completed."
