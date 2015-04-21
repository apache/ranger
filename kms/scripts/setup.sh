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
# Ranger Admin Setup Script
#
# This script will install policymanager webapplication under tomcat and also, initialize the database with ranger users/tables.

PROPFILE=$PWD/install.properties
propertyValue=''

if [ ! -f ${PROPFILE} ]
then
	echo "$PROPFILE file not found....!!";
	exit 1;
fi

eval `grep -v '^XAAUDIT.' ${PROPFILE} | grep -v '^$' | grep -v '^#'`

DB_HOST="${db_host}"

usage() {
  [ "$*" ] && echo "$0: $*"
  sed -n '/^##/,/^$/s/^## \{0,1\}//p' "$0"
  exit 2
} 2>/dev/null

log() {
   local prefix="$(date +%Y-%m-%d\ %H:%M:%S,%3N) "
   echo "${prefix} $@" >> $LOGFILE
   echo "${prefix} $@"
}

check_ret_status(){
	if [ $1 -ne 0 ]; then
		log "[E] $2";
		exit 1;
	fi
}

check_ret_status_for_groupadd(){
# 9 is the response if the group exists
    if [ $1 -ne 0 ] && [ $1 -ne 9 ]; then
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
#Get Properties from File without erroring out if property is not there
#$1 -> propertyName $2 -> fileName $3 -> variableName $4 -> failIfNotFound
getPropertyFromFileNoExit(){
	validateProperty=$(sed '/^\#/d' $2 | grep "^$1"  | tail -n 1) # for validation
	if  test -z "$validateProperty" ; then 
            log "[E] '$1' not found in $2 file while getting....!!"; 
            if [ $4 == "true" ] ; then
                exit 1; 
            else 
                value=""
            fi
        else
	    value=`sed '/^\#/d' $2 | grep "^$1"  | tail -n 1 | cut -d "=" -f2-`
        fi
	#echo 'value:'$value
	eval $3="'$value'"
}
#Get Properties from File
#$1 -> propertyName $2 -> fileName $3 -> variableName
getPropertyFromFile(){
	validateProperty=$(sed '/^\#/d' $2 | grep "^$1"  | tail -n 1) # for validation
	if  test -z "$validateProperty" ; then log "[E] '$1' not found in $2 file while getting....!!"; exit 1; fi
	value=`sed '/^\#/d' $2 | grep "^$1"  | tail -n 1 | cut -d "=" -f2-`
	#echo 'value:'$value
	#validate=$(sed '/^\#/d' $2 | grep "^$1"  | tail -n 1 | cut -d "=" -f2-) # for validation
	#if  test -z "$validate" ; then log "[E] '$1' not found in $2 file while getting....!!"; exit 1; fi
	eval $3="'$value'"
}

#Update Properties to File
#$1 -> propertyName $2 -> newPropertyValue $3 -> fileName
updatePropertyToFile(){
	sed -i 's@^'$1'=[^ ]*$@'$1'='$2'@g' $3
	#validate=`sed -i 's/^'$1'=[^ ]*$/'$1'='$2'/g' $3`	#for validation
	validate=$(sed '/^\#/d' $3 | grep "^$1"  | tail -n 1 | cut -d "=" -f2-) # for validation
	#echo 'V1:'$validate
	if test -z "$validate" ; then log "[E] '$1' not found in $3 file while Updating....!!"; exit 1; fi
	log "[I] File $3 Updated successfully : {'$1'}"
}

#Update Properties to File
#$1 -> propertyName $2 -> newPropertyValue $3 -> fileName
updatePropertyToFilePy(){
    python update_property.py $1 $2 $3
    check_ret_status $? "Update property failed for: " $1
}


init_logfiles () {
    for f in $LOGFILES; do
        touch $f
    done
}

init_variables(){
	curDt=`date '+%Y%m%d%H%M%S'`

	if [ -f ${PWD}/version ] 
	then
		VERSION=`cat ${PWD}/version`
	else
		VERSION="0.5.0"
	fi

	KMS_DIR=$PWD

	RANGER_KMS=ranger-kms

	INSTALL_DIR=${KMS_DIR}

	WEBAPP_ROOT=${INSTALL_DIR}/ews/webapp

	DB_FLAVOR=`echo $DB_FLAVOR | tr '[:lower:]' '[:upper:]'`
	if [ "${DB_FLAVOR}" == "" ]
	then
		DB_FLAVOR="MYSQL"
	fi
	log "[I] DB_FLAVOR=${DB_FLAVOR}"

	getPropertyFromFile 'db_root_user' $PROPFILE db_root_user
	getPropertyFromFile 'db_root_password' $PROPFILE db_user
	getPropertyFromFile 'db_user' $PROPFILE db_user
	getPropertyFromFile 'db_password' $PROPFILE db_password
}


check_python_command() {
		if is_command ${PYTHON_COMMAND_INVOKER} ; then
			log "[I] '${PYTHON_COMMAND_INVOKER}' command found"
		else
			log "[E] '${PYTHON_COMMAND_INVOKER}' command not found"
		exit 1;
		fi
}

run_dba_steps(){
	getPropertyFromFileNoExit 'setup_mode' $PROPFILE setup_mode false
	if [ "x${setup_mode}x" == "xSeparateDBAx" ]; then
		log "[I] Setup mode is set to SeparateDBA. Not Running DBA steps. Please run dba_script.py before running setup..!";
	else
		log "[I] Setup mode is not set. Running DBA steps..";
                python dba_script.py -q
        fi
}
check_db_connector() {
	log "[I] Checking ${DB_FLAVOR} CONNECTOR FILE : ${SQL_CONNECTOR_JAR}"
	if test -f "$SQL_CONNECTOR_JAR"; then
		log "[I] ${DB_FLAVOR} CONNECTOR FILE : $SQL_CONNECTOR_JAR file found"
	else
		log "[E] ${DB_FLAVOR} CONNECTOR FILE : $SQL_CONNECTOR_JAR does not exists" ; exit 1;
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

	version=$("$JAVA_BIN" -version 2>&1 | awk -F '"' '/version/ {print $2}')
	major=`echo ${version} | cut -d. -f1`
	minor=`echo ${version} | cut -d. -f2`
	if [[ "${major}" == 1 && "${minor}" < 7 ]] ; then
		log "[E] Java 1.7 is required, current java version is $version"
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
	if [ "${DB_FLAVOR}" == "ORACLE" ]
    then
        if test -f ${oracle_core_file}; then
			log "[I] ${oracle_core_file} file found"
        else
            log "[E] ${oracle_core_file} does not exists" ; exit 1;
        fi
    fi
    if [ "${DB_FLAVOR}" == "POSTGRES" ]
    then
        if test -f ${postgres_core_file}; then
			log "[I] ${postgres_core_file} file found"
        else
            log "[E] ${postgres_core_file} does not exists" ; exit 1;
        fi
    fi
    if [ "${DB_FLAVOR}" == "SQLSERVER" ]
    then
        if test -f ${sqlserver_core_file}; then
			log "[I] ${sqlserver_core_file} file found"
        else
            log "[E] ${sqlserver_core_file} does not exists" ; exit 1;
        fi
    fi
}

create_rollback_point() {
    DATE=`date`
    BAK_FILE=$APP-$VERSION.$DATE.bak
    log "Creating backup file : $BAK_FILE"
    cp "$APP" "$BAK_FILE"
}


copy_db_connector(){
	log "[I] Copying ${DB_FLAVOR} Connector to $app_home/WEB-INF/lib ";
    cp -f $SQL_CONNECTOR_JAR $app_home/WEB-INF/lib
	check_ret_status $? "Copying ${DB_FLAVOR} Connector to $app_home/WEB-INF/lib failed"
	log "[I] Copying ${DB_FLAVOR} Connector to $app_home/WEB-INF/lib DONE";
}

setup_kms(){
        #copying ranger kms provider 
	oldP=${PWD}
        cd $PWD/ews/webapp
        log "[I] Adding ranger kms provider as services in hadoop-common jar"
	for f in lib/hadoop-common*.jar
	do
        	jar -uf ${f}  META-INF/services/org.apache.hadoop.crypto.key.KeyProviderFactory
		chown ${unix_user}:${unix_group} ${f}
	done
        cd ${oldP}
}

update_properties() {
	newPropertyValue=''
	#echo "export JAVA_HOME=${JAVA_HOME}" > ${WEBAPP_ROOT}/WEB-INF/classes/conf/java_home.sh
	#chmod a+rx ${WEBAPP_ROOT}/WEB-INF/classes/conf/java_home.sh


	to_file=$app_home/config/dbks-site.xml
	if test -f $to_file; then
		log "[I] $to_file file found"
	else
		log "[E] $to_file does not exists" ; exit 1;
    fi


	propertyName=ranger.ks.jpa.jdbc.user
	newPropertyValue="${db_user}"
	updatePropertyToFilePy $propertyName $newPropertyValue $to_file

	if [ "${DB_FLAVOR}" == "MYSQL" ]
	then
		propertyName=ranger.ks.jpa.jdbc.url
		newPropertyValue="jdbc:log4jdbc:mysql://${DB_HOST}/${db_name}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.MySQLPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.driver
		newPropertyValue="net.sf.log4jdbc.DriverSpy"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

	fi
	if [ "${DB_FLAVOR}" == "ORACLE" ]
	then
		propertyName=ranger.ks.jpa.jdbc.url
		newPropertyValue="jdbc:oracle:thin:\@//${DB_HOST}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.OraclePlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.driver
		newPropertyValue="oracle.jdbc.OracleDriver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

	fi
	if [ "${DB_FLAVOR}" == "POSTGRES" ]
	then
		propertyName=ranger.ks.jpa.jdbc.url
		newPropertyValue="jdbc:postgresql://${DB_HOST}/${db_name}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.PostgreSQLPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.driver
		newPropertyValue="org.postgresql.Driver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

	fi
	if [ "${DB_FLAVOR}" == "SQLSERVER" ]
	then
		propertyName=ranger.ks.jpa.jdbc.url
		newPropertyValue="jdbc:sqlserver://${DB_HOST};databaseName=${db_name}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.SQLServerPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.driver
		newPropertyValue="com.microsoft.sqlserver.jdbc.SQLServerDriver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

	fi

	keystore="${cred_keystore_filename}"

	echo "Starting configuration for XA DB credentials:"

	MK_CREDENTIAL_ATTR="ranger.db.encrypt.key.password"
	DB_CREDENTIAL_ATTR="ranger.ks.jpa.jdbc.password" 

	MK_CREDENTIAL_ALIAS="ranger.ks.masterkey.password"
	DB_CREDENTIAL_ALIAS="ranger.ks.jpa.jdbc.credential.alias"

	if [ "${keystore}" != "" ]
	then
		mkdir -p `dirname "${keystore}"`

		$JAVA_HOME/bin/java -cp "cred/lib/*" org.apache.ranger.credentialapi.buildks create "${DB_CREDENTIAL_ALIAS}" -value "$db_password" -provider jceks://file$keystore
		$JAVA_HOME/bin/java -cp "cred/lib/*" org.apache.ranger.credentialapi.buildks create "${MK_CREDENTIAL_ALIAS}" -value "${KMS_MASTER_KEY_PASSWD}" -provider jceks://file$keystore

		propertyName=ranger.ks.jpa.jdbc.credential.alias
		newPropertyValue="${DB_CREDENTIAL_ALIAS}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.credential.provider.path
		newPropertyValue="${keystore}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName=ranger.ks.jpa.jdbc.password
		newPropertyValue="_"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file
	else
		propertyName="${DB_CREDENTIAL_ATTR}"
		newPropertyValue="${db_password}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName="${MK_CREDENTIAL_ATTR}"
		newPropertyValue="${KMS_MASTER_KEY_PASSWD}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file
	fi

	if test -f $keystore; then
		#echo "$keystore found."
		chown -R ${unix_user}:${unix_group} ${keystore}
		chmod 640 ${keystore}
	else
		#echo "$keystore not found. so clear text password"

		propertyName="${DB_CREDENTIAL_ATTR}"
		newPropertyValue="${db_password}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file

		propertyName="${MK_CREDENTIAL_ATTR}"
		newPropertyValue="${KMS_MASTER_KEY_PASSWD}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file
	fi

	###########
}

#=====================================================================

setup_unix_user_group(){

	log "[I] Setting up UNIX user : ${unix_user} and group: ${unix_group}";

    groupadd ${unix_group}
    check_ret_status_for_groupadd $? "Creating group ${unix_group} failed"

	id -u ${unix_user} > /dev/null 2>&1

	if [ $? -ne 0 ]
	then
	    log "[I] Creating new user and adding to group";
        useradd ${unix_user} -g ${unix_group} -m
		check_ret_status $? "useradd ${unix_user} failed"
	else
	    log "[I] User already exists, adding it to group";
	    usermod -g ${unix_group} ${unix_user}
	fi

	log "[I] Setting up UNIX user : ${unix_user} and group: ${unix_group} DONE";
}

setup_install_files(){

	log "[I] Setting up installation files and directory";

	#if [ ! -d ${WEBAPP_ROOT}/WEB-INF/classes/conf ]; then
	#    log "[I] Copying ${WEBAPP_ROOT}/WEB-INF/classes/conf.dist ${WEBAPP_ROOT}/WEB-INF/classes/conf"
	#    mkdir -p ${WEBAPP_ROOT}/WEB-INF/classes/conf
	#    cp ${WEBAPP_ROOT}/WEB-INF/classes/conf.dist/* ${WEBAPP_ROOT}/WEB-INF/classes/conf
	#	chown -R ${unix_user} ${WEBAPP_ROOT}/WEB-INF/classes/conf
	#fi

	if [ ! -d ${WEBAPP_ROOT}/WEB-INF/classes/lib ]; then
	    log "[I] Creating ${WEBAPP_ROOT}/WEB-INF/classes/lib"
	    mkdir -p ${WEBAPP_ROOT}/WEB-INF/classes/lib
		chown -R ${unix_user} ${WEBAPP_ROOT}/WEB-INF/classes/lib
	fi

	if [ -d /etc/init.d ]; then
	    log "[I] Setting up init.d"
	    cp ${INSTALL_DIR}/${RANGER_KMS}.initd /etc/init.d/${RANGER_KMS}

	    chmod ug+rx /etc/init.d/${RANGER_KMS}

	    if [ -d /etc/rc2.d ]
	    then
		RC_DIR=/etc/rc2.d
		log "[I] Creating script S88${RANGER_KMS}/K90${RANGER_KMS} in $RC_DIR directory .... "
		rm -f $RC_DIR/S88${RANGER_KMS}  $RC_DIR/K90${RANGER_KMS}
		ln -s /etc/init.d/${RANGER_KMS} $RC_DIR/S88${RANGER_KMS}
		ln -s /etc/init.d/${RANGER_KMS} $RC_DIR/K90${RANGER_KMS}
	    fi

	    if [ -d /etc/rc3.d ]
	    then
		RC_DIR=/etc/rc3.d
		log "[I] Creating script S88${RANGER_KMS}/K90${RANGER_KMS} in $RC_DIR directory .... "
		rm -f $RC_DIR/S88${RANGER_KMS}  $RC_DIR/K90${RANGER_KMS}
		ln -s /etc/init.d/${RANGER_KMS} $RC_DIR/S88${RANGER_KMS}
		ln -s /etc/init.d/${RANGER_KMS} $RC_DIR/K90${RANGER_KMS}
	    fi

	    # SUSE has rc2.d and rc3.d under /etc/rc.d
	    if [ -d /etc/rc.d/rc2.d ]
	    then
		RC_DIR=/etc/rc.d/rc2.d
		log "[I] Creating script S88${RANGER_KMS}/K90${RANGER_KMS} in $RC_DIR directory .... "
		rm -f $RC_DIR/S88${RANGER_KMS}  $RC_DIR/K90${RANGER_KMS}
		ln -s /etc/init.d/${RANGER_KMS} $RC_DIR/S88${RANGER_KMS}
		ln -s /etc/init.d/${RANGER_KMS} $RC_DIR/K90${RANGER_KMS}
	    fi
	    if [ -d /etc/rc.d/rc3.d ]
	    then
		RC_DIR=/etc/rc.d/rc3.d
		log "[I] Creating script S88${RANGER_KMS}/K90${RANGER_KMS} in $RC_DIR directory .... "
		rm -f $RC_DIR/S88${RANGER_KMS}  $RC_DIR/K90${RANGER_KMS}
		ln -s /etc/init.d/${RANGER_KMS} $RC_DIR/S88${RANGER_KMS}
		ln -s /etc/init.d/${RANGER_KMS} $RC_DIR/K90${RANGER_KMS}
	    fi
	fi

	if [ ! -d ${KMS_DIR}/ews/logs ]; then
	    log "[I] ${KMS_DIR}/ews/logs folder"
	    mkdir -p ${KMS_DIR}/ews/logs
	    chown -R ${unix_user} ${KMS_DIR}/ews/logs
	fi

	log "[I] Setting up installation files and directory DONE";

	if [ ! -f ${INSTALL_DIR}/rpm ]; then
	    if [ -d ${INSTALL_DIR} ]
	    then
		chown -R ${unix_user}:${unix_group} ${INSTALL_DIR}
		chown -R ${unix_user}:${unix_group} ${INSTALL_DIR}/*
	    fi
	fi

	# Copy ranger-admin-services to /usr/bin
	if [ ! \( -e /usr/bin/ranger-kms \) ]
	then
	  ln -sf ${INSTALL_DIR}/ranger-kms /usr/bin/ranger-kms
	  chmod ug+rx /usr/bin/ranger-kms	
	fi

	if [ ! -d /var/log/ranger/kms ]
	then
		mkdir -p /var/log/ranger/kms
	fi
	chgrp ${unix_group} /var/log/ranger/kms
	chmod g+rwx /var/log/ranger/kms
}

init_logfiles
log " --------- Running Ranger KMS Application Install Script --------- "
log "[I] uname=`uname`"
log "[I] hostname=`hostname`"
init_variables
get_distro
check_java_version
check_db_connector
setup_unix_user_group
setup_install_files
sanity_check_files
copy_db_connector
check_python_command
run_dba_steps
$PYTHON_COMMAND_INVOKER db_setup.py
if [ "$?" == "0" ]
then
	update_properties
	$PYTHON_COMMAND_INVOKER db_setup.py -javapatch
    setup_kms
else
	log "[E] DB schema setup failed! Please contact Administrator."
	exit 1
fi

./enable-kms-plugin.sh

echo "Installation of Ranger KMS is completed."
