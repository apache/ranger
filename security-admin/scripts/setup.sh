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
   local prefix="$(date +%Y-%m-%d\ %H:%M:%S,%3N) "
   echo "${prefix} $@" >> $LOGFILE
   echo "${prefix} $@"
}
get_prop(){
	validateProperty=$(sed '/^\#/d' $2 | grep "^$1\s*="  | tail -n 1) # for validation
	if  test -z "$validateProperty" ; then log "[E] '$1' not found in $2 file while getting....!!"; exit 1; fi
	value=$(echo $validateProperty | cut -d "=" -f2-)
	if [[ $1 == *password* ]]
        then
                echo $value
        else
                echo $value | tr -d \'\"
        fi
}

PYTHON_COMMAND_INVOKER=$(get_prop 'PYTHON_COMMAND_INVOKER' $PROPFILE)
DB_FLAVOR=$(get_prop 'DB_FLAVOR' $PROPFILE)
SQL_CONNECTOR_JAR=$(get_prop 'SQL_CONNECTOR_JAR' $PROPFILE)
db_root_user=$(get_prop 'db_root_user' $PROPFILE)
db_root_password=$(get_prop 'db_root_password' $PROPFILE)
db_host=$(get_prop 'db_host' $PROPFILE)
db_name=$(get_prop 'db_name' $PROPFILE)
db_user=$(get_prop 'db_user' $PROPFILE)
db_password=$(get_prop 'db_password' $PROPFILE)
audit_store=$(get_prop 'audit_store' $PROPFILE)
audit_solr_urls=$(get_prop 'audit_solr_urls' $PROPFILE)
audit_solr_user=$(get_prop 'audit_solr_user' $PROPFILE)
audit_solr_password=$(get_prop 'audit_solr_password' $PROPFILE)
audit_solr_zookeepers=$(get_prop 'audit_solr_zookeepers' $PROPFILE)
audit_db_name=$(get_prop 'audit_db_name' $PROPFILE)
audit_db_user=$(get_prop 'audit_db_user' $PROPFILE)
audit_db_password=$(get_prop 'audit_db_password' $PROPFILE)
policymgr_external_url=$(get_prop 'policymgr_external_url' $PROPFILE)
policymgr_http_enabled=$(get_prop 'policymgr_http_enabled' $PROPFILE)
unix_user=$(get_prop 'unix_user' $PROPFILE)
unix_group=$(get_prop 'unix_group' $PROPFILE)
authentication_method=$(get_prop 'authentication_method' $PROPFILE)
remoteLoginEnabled=$(get_prop 'remoteLoginEnabled' $PROPFILE)
authServiceHostName=$(get_prop 'authServiceHostName' $PROPFILE)
authServicePort=$(get_prop 'authServicePort' $PROPFILE)
xa_ldap_url=$(get_prop 'xa_ldap_url' $PROPFILE)
xa_ldap_userDNpattern=$(get_prop 'xa_ldap_userDNpattern' $PROPFILE)
xa_ldap_groupSearchBase=$(get_prop 'xa_ldap_groupSearchBase' $PROPFILE)
xa_ldap_groupSearchFilter=$(get_prop 'xa_ldap_groupSearchFilter' $PROPFILE)
xa_ldap_groupRoleAttribute=$(get_prop 'xa_ldap_groupRoleAttribute' $PROPFILE)
xa_ldap_base_dn=$(get_prop 'xa_ldap_base_dn' $PROPFILE)
xa_ldap_bind_dn=$(get_prop 'xa_ldap_bind_dn' $PROPFILE)
xa_ldap_bind_password=$(get_prop 'xa_ldap_bind_password' $PROPFILE)
xa_ldap_referral=$(get_prop 'xa_ldap_referral' $PROPFILE)
xa_ldap_userSearchFilter=$(get_prop 'xa_ldap_userSearchFilter' $PROPFILE)
xa_ldap_ad_domain=$(get_prop 'xa_ldap_ad_domain' $PROPFILE)
xa_ldap_ad_url=$(get_prop 'xa_ldap_ad_url' $PROPFILE)
xa_ldap_ad_base_dn=$(get_prop 'xa_ldap_ad_base_dn' $PROPFILE)
xa_ldap_ad_bind_dn=$(get_prop 'xa_ldap_ad_bind_dn' $PROPFILE)
xa_ldap_ad_bind_password=$(get_prop 'xa_ldap_ad_bind_password' $PROPFILE)
xa_ldap_ad_referral=$(get_prop 'xa_ldap_ad_referral' $PROPFILE)
xa_ldap_ad_userSearchFilter=$(get_prop 'xa_ldap_ad_userSearchFilter' $PROPFILE)
XAPOLICYMGR_DIR=$(eval echo "$(get_prop 'XAPOLICYMGR_DIR' $PROPFILE)")
app_home=$(eval echo "$(get_prop 'app_home' $PROPFILE)")
TMPFILE=$(eval echo "$(get_prop 'TMPFILE' $PROPFILE)")
LOGFILE=$(eval echo " $(get_prop 'LOGFILE' $PROPFILE)")
LOGFILES=$(eval echo "$(get_prop 'LOGFILES' $PROPFILE)")
JAVA_BIN=$(get_prop 'JAVA_BIN' $PROPFILE)
JAVA_VERSION_REQUIRED=$(get_prop 'JAVA_VERSION_REQUIRED' $PROPFILE)
JAVA_ORACLE=$(get_prop 'JAVA_ORACLE' $PROPFILE)
mysql_core_file=$(get_prop 'mysql_core_file' $PROPFILE)
mysql_audit_file=$(get_prop 'mysql_audit_file' $PROPFILE)
oracle_core_file=$(get_prop 'oracle_core_file' $PROPFILE)
oracle_audit_file=$(get_prop 'oracle_audit_file' $PROPFILE)
postgres_core_file=$(get_prop 'postgres_core_file' $PROPFILE)
postgres_audit_file=$(get_prop 'postgres_audit_file' $PROPFILE)
sqlserver_core_file=$(get_prop 'sqlserver_core_file' $PROPFILE)
sqlserver_audit_file=$(get_prop 'sqlserver_audit_file' $PROPFILE)
sqlanywhere_core_file=$(get_prop 'sqlanywhere_core_file' $PROPFILE)
sqlanywhere_audit_file=$(get_prop 'sqlanywhere_audit_file' $PROPFILE)
cred_keystore_filename=$(eval echo "$(get_prop 'cred_keystore_filename' $PROPFILE)")

DB_HOST="${db_host}"

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
	validateProperty=$(sed '/^\#/d' $2 | grep "^$1\s*="  | tail -n 1) # for validation
	if  test -z "$validateProperty" ; then 
		log "[E] '$1' not found in $2 file while getting....!!";
		if [ $4 == "true" ] ; then
		    exit 1;
		else
		    value=""
		fi
	else
		value=$(echo $validateProperty | cut -d "=" -f2-)
	fi
	eval $3="'$value'"
}
#Get Properties from File
#$1 -> propertyName $2 -> fileName $3 -> variableName
getPropertyFromFile(){
	validateProperty=$(sed '/^\#/d' $2 | grep "^$1\s*="  | tail -n 1) # for validation
	if  test -z "$validateProperty" ; then log "[E] '$1' not found in $2 file while getting....!!"; exit 1; fi
	value=$(echo $validateProperty | cut -d "=" -f2-)
	eval $3="'$value'"
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
    #log "start date for $0 = `date`"
}

init_variables(){
	curDt=`date '+%Y%m%d%H%M%S'`
	VERSION=`cat ${PWD}/version`
	XAPOLICYMGR_DIR=$PWD
	RANGER_ADMIN_INITD=ranger-admin-initd
	RANGER_ADMIN=ranger-admin
	INSTALL_DIR=${XAPOLICYMGR_DIR}
	WEBAPP_ROOT=${INSTALL_DIR}/ews/webapp
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
    if [ "${DB_FLAVOR}" == "MSSQL" ]
    then
        if test -f ${sqlserver_core_file}; then
			log "[I] ${sqlserver_core_file} file found"
        else
            log "[E] ${sqlserver_core_file} does not exists" ; exit 1;
        fi
    fi
	if [ "${DB_FLAVOR}" == "SQLA" ]
	then
		if [ "${LD_LIBRARY_PATH}" == "" ]
		then
			log "[E] LD_LIBRARY_PATH environment property not defined, aborting installation."
			exit 1
		fi
		if test -f ${sqlanywhere_core_file}; then
			log "[I] ${sqlanywhere_core_file} file found"
		else
			log "[E] ${sqlanywhere_core_file} does not exists" ; exit 1;
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

update_properties() {
	newPropertyValue=''
	echo "export JAVA_HOME=${JAVA_HOME}" > ${WEBAPP_ROOT}/WEB-INF/classes/conf/java_home.sh
	chmod a+rx ${WEBAPP_ROOT}/WEB-INF/classes/conf/java_home.sh

	to_file_ranger=$app_home/WEB-INF/classes/conf/ranger-admin-site.xml
	if test -f $to_file_ranger; then
		log "[I] $to_file_ranger file found"
	else
		log "[E] $to_file_ranger does not exists" ; exit 1;
    fi

	to_file_default=$app_home/WEB-INF/classes/conf/ranger-admin-default-site.xml
	if test -f $to_file_default; then
		log "[I] $to_file_default file found"
	else
		log "[E] $to_file_default does not exists" ; exit 1;
    fi

	if [ "${DB_FLAVOR}" == "MYSQL" ]
	then
		propertyName=ranger.jpa.jdbc.url
		newPropertyValue="jdbc:log4jdbc:mysql://${DB_HOST}/${db_name}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		if [ "${audit_store}" == "db" ]
		then
			propertyName=ranger.jpa.audit.jdbc.url
			newPropertyValue="jdbc:log4jdbc:mysql://${DB_HOST}/${audit_db_name}"
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
		fi

		propertyName=ranger.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.MySQLPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.audit.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.MySQLPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.jdbc.driver
		newPropertyValue="net.sf.log4jdbc.DriverSpy"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		propertyName=ranger.jpa.audit.jdbc.driver
		newPropertyValue="net.sf.log4jdbc.DriverSpy"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi
	if [ "${DB_FLAVOR}" == "ORACLE" ]
	then
		propertyName=ranger.jpa.jdbc.url
		count=$(grep -o ":" <<< "$DB_HOST" | wc -l)
		#if [[ ${count} -eq 2 ]] ; then
		if [ ${count} -eq 2 ] || [ ${count} -eq 0 ]; then
			#jdbc:oracle:thin:@[HOST][:PORT]:SID or #jdbc:oracle:thin:@GL
			newPropertyValue="jdbc:oracle:thin:@${DB_HOST}"
		else
			#jdbc:oracle:thin:@//[HOST][:PORT]/SERVICE
			newPropertyValue="jdbc:oracle:thin:@//${DB_HOST}"
		fi
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		if [ "${audit_store}" == "db" ]
		then
			propertyName=ranger.jpa.audit.jdbc.url
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
		fi

		propertyName=ranger.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.OraclePlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.audit.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.OraclePlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.jdbc.driver
		newPropertyValue="oracle.jdbc.OracleDriver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		propertyName=ranger.jpa.audit.jdbc.driver
		newPropertyValue="oracle.jdbc.OracleDriver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi
	if [ "${DB_FLAVOR}" == "POSTGRES" ]
	then
		db_name=`echo ${db_name} | tr '[:upper:]' '[:lower:]'`
		audit_db_name=`echo ${audit_db_name} | tr '[:upper:]' '[:lower:]'`
		db_user=`echo ${db_user} | tr '[:upper:]' '[:lower:]'`
		audit_db_user=`echo ${audit_db_user} | tr '[:upper:]' '[:lower:]'`

		propertyName=ranger.jpa.jdbc.url
		newPropertyValue="jdbc:postgresql://${DB_HOST}/${db_name}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		if [ "${audit_store}" == "db" ]
		then
			propertyName=ranger.jpa.audit.jdbc.url
			newPropertyValue="jdbc:postgresql://${DB_HOST}/${audit_db_name}"
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
		fi

		propertyName=ranger.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.PostgreSQLPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.audit.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.PostgreSQLPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.jdbc.driver
		newPropertyValue="org.postgresql.Driver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		propertyName=ranger.jpa.audit.jdbc.driver
		newPropertyValue="org.postgresql.Driver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi

	if [ "${DB_FLAVOR}" == "MSSQL" ]
	then
		propertyName=ranger.jpa.jdbc.url
		newPropertyValue="jdbc:sqlserver://${DB_HOST};databaseName=${db_name}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		if [ "${audit_store}" == "db" ]
		then
			propertyName=ranger.jpa.audit.jdbc.url
			newPropertyValue="jdbc:sqlserver://${DB_HOST};databaseName=${audit_db_name}"
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
		fi

		propertyName=ranger.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.SQLServerPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.audit.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.SQLServerPlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.jdbc.driver
		newPropertyValue="com.microsoft.sqlserver.jdbc.SQLServerDriver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		propertyName=ranger.jpa.audit.jdbc.driver
		newPropertyValue="com.microsoft.sqlserver.jdbc.SQLServerDriver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi

	if [ "${DB_FLAVOR}" == "SQLA" ]
	then
		propertyName=ranger.jpa.jdbc.url
		newPropertyValue="jdbc:sqlanywhere:database=${db_name};host=${DB_HOST}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		if [ "${audit_store}" == "db" ]
		then
			propertyName=ranger.jpa.audit.jdbc.url
			newPropertyValue="jdbc:sqlanywhere:database=${audit_db_name};host=${DB_HOST}"
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
		fi

		propertyName=ranger.jpa.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.SQLAnywherePlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.audit.jdbc.dialect
		newPropertyValue="org.eclipse.persistence.platform.database.SQLAnywherePlatform"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.jdbc.driver
		newPropertyValue="sap.jdbc4.sqlanywhere.IDriver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		propertyName=ranger.jpa.audit.jdbc.driver
		newPropertyValue="sap.jdbc4.sqlanywhere.IDriver"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi

	if [ "${audit_store}" == "solr" ]
	then
		propertyName=ranger.audit.solr.urls
		newPropertyValue=${audit_solr_urls}
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi

	propertyName=ranger.audit.source.type
        newPropertyValue=${audit_store}
	updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

	propertyName=ranger.externalurl
	newPropertyValue="${policymgr_external_url}"
	updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

	propertyName=ranger.service.http.enabled
	newPropertyValue="${policymgr_http_enabled}"
	updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

	propertyName=ranger.jpa.jdbc.user
	newPropertyValue="${db_user}"
	updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

	if [ "${audit_store}" == "db" ]
	then
		propertyName=ranger.jpa.audit.jdbc.user
		newPropertyValue="${audit_db_user}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi
	##########

	keystore="${cred_keystore_filename}"

	echo "Starting configuration for Ranger DB credentials:"

	db_password_alias=ranger.db.password

	if [ "${keystore}" != "" ]
	then
		mkdir -p `dirname "${keystore}"`
		$PYTHON_COMMAND_INVOKER ranger_credential_helper.py -l "cred/lib/*" -f "$keystore" -k "$db_password_alias" -v "$db_password" -c 1

		propertyName=ranger.credential.provider.path
		newPropertyValue="${keystore}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.jpa.jdbc.credential.alias
		newPropertyValue="${db_password_alias}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

		propertyName=ranger.credential.provider.path
		newPropertyValue="${keystore}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

		propertyName=ranger.jpa.jdbc.password
		newPropertyValue="_"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	else
		propertyName=ranger.jpa.jdbc.password
		newPropertyValue="${db_password}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi

	if test -f $keystore; then
		#echo "$keystore found."
		chown -R ${unix_user}:${unix_group} ${keystore}
		chmod 640 ${keystore}
	else
		propertyName=ranger.jpa.jdbc.password
		newPropertyValue="${db_password}"
		updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	fi

	###########
	if [ "${audit_store}" == "db" ]
	then
	    audit_db_password_alias=ranger.auditdb.password

	    echo "Starting configuration for Audit DB credentials:"

	    if [ "${keystore}" != "" ]
	    then
		$PYTHON_COMMAND_INVOKER ranger_credential_helper.py -l "cred/lib/*" -f "$keystore" -k "$audit_db_password_alias" -v "$audit_db_password" -c 1

			propertyName=ranger.jpa.audit.jdbc.credential.alias
		newPropertyValue="${audit_db_password_alias}"
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default
			propertyName=ranger.jpa.audit.jdbc.password
		newPropertyValue="_"
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	    else
			propertyName=ranger.jpa.audit.jdbc.password
		newPropertyValue="${audit_db_password}"
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	    fi

	    if test -f $keystore; then
		chown -R ${unix_user}:${unix_group} ${keystore}
		#echo "$keystore found."
	    else
		#echo "$keystore not found. so use clear text password"
			propertyName=ranger.jpa.audit.jdbc.password
		newPropertyValue="${audit_db_password}"
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
	    fi
	fi
	if [ "${audit_store}" == "solr" ]
	then
		if [ "${audit_solr_zookeepers}" != "" ]
		then
			propertyName=ranger.audit.solr.zookeepers
			newPropertyValue=${audit_solr_zookeepers}
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
		fi
		if [ "${audit_solr_user}" != "" ] && [ "${audit_solr_password}" != "" ]
		then
			propertyName=ranger.solr.audit.user
			newPropertyValue=${audit_solr_user}
			updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger

			if [ "${keystore}" != "" ]
			then
				echo "Starting configuration for solr credentials:"
				mkdir -p `dirname "${keystore}"`
				audit_solr_password_alias=ranger.solr.password

				$PYTHON_COMMAND_INVOKER ranger_credential_helper.py -l "cred/lib/*" -f "$keystore" -k "$audit_solr_password_alias" -v "$audit_solr_password" -c 1

				propertyName=ranger.solr.audit.credential.alias
				newPropertyValue="${audit_solr_password_alias}"
				updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

				propertyName=ranger.solr.audit.user.password
				newPropertyValue="_"
				updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
			else
				propertyName=ranger.solr.audit.user.password
				newPropertyValue="${audit_solr_password}"
				updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
			fi

			if test -f $keystore; then
				chown -R ${unix_user}:${unix_group} ${keystore}
			else
				propertyName=ranger.solr.audit.user.password
				newPropertyValue="${audit_solr_password}"
				updatePropertyToFilePy $propertyName $newPropertyValue $to_file_ranger
			fi
		fi
	fi
}

do_unixauth_setup() {

    ldap_file=$app_home/WEB-INF/classes/conf/ranger-admin-site.xml
    if test -f $ldap_file; then
	log "[I] $ldap_file file found"
	
        propertyName=ranger.authentication.method
        newPropertyValue="${authentication_method}"
        updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

        propertyName=ranger.unixauth.remote.login.enabled
        newPropertyValue="${remoteLoginEnabled}"
        updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

        propertyName=ranger.unixauth.service.hostname
        newPropertyValue="${authServiceHostName}"
        updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

        propertyName=ranger.unixauth.service.port
        newPropertyValue="${authServicePort}"
        updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file
	else
		log "[E] $ldap_file does not exists" ; exit 1;
	fi
}

do_authentication_setup(){
	log "[I] Starting setup based on user authentication method=$authentication_method";
	./setup_authentication.sh $authentication_method $app_home

    if [ $authentication_method = "LDAP" ] ; then
	log "[I] Loading LDAP attributes and properties";
		newPropertyValue=''
		ldap_file=$app_home/WEB-INF/classes/conf/ranger-admin-site.xml
		if test -f $ldap_file; then
			log "[I] $ldap_file file found"
			propertyName=ranger.ldap.url
			newPropertyValue="${xa_ldap_url}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			propertyName=ranger.ldap.user.dnpattern
			newPropertyValue="${xa_ldap_userDNpattern}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			propertyName=ranger.ldap.group.searchbase
			newPropertyValue="${xa_ldap_groupSearchBase}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			propertyName=ranger.ldap.group.searchfilter
			newPropertyValue="${xa_ldap_groupSearchFilter}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			propertyName=ranger.ldap.group.roleattribute
			newPropertyValue="${xa_ldap_groupRoleAttribute}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			propertyName=ranger.authentication.method
			newPropertyValue="${authentication_method}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			if [ "${xa_ldap_base_dn}" != "" ] && [ "${xa_ldap_bind_dn}" != "" ]  && [ "${xa_ldap_bind_password}" != "" ]
			then
				$PYTHON_COMMAND_INVOKER dba_script.py ${xa_ldap_bind_password} 'LDAP' 'password_validation'
				if [ "$?" != "0" ]
				then
					exit 1
				fi

				propertyName=ranger.ldap.base.dn
				newPropertyValue="${xa_ldap_base_dn}"
				updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

				propertyName=ranger.ldap.bind.dn
				newPropertyValue="${xa_ldap_bind_dn}"
				updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

				propertyName=ranger.ldap.referral
				newPropertyValue="${xa_ldap_referral}"
				updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

				propertyName=ranger.ldap.user.searchfilter
				newPropertyValue="${xa_ldap_userSearchFilter}"
				updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

				keystore="${cred_keystore_filename}"

				if [ "${keystore}" != "" ]
				then
					mkdir -p `dirname "${keystore}"`

					ldap_password_alias=ranger.ldap.binddn.password
					$PYTHON_COMMAND_INVOKER ranger_credential_helper.py -l "cred/lib/*" -f "$keystore" -k "$ldap_password_alias" -v "$xa_ldap_bind_password" -c 1

					to_file_default=$app_home/WEB-INF/classes/conf/ranger-admin-default-site.xml

					if test -f $to_file_default; then
						propertyName=ranger.credential.provider.path
						newPropertyValue="${keystore}"
						updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

						propertyName=ranger.ldap.binddn.credential.alias
						newPropertyValue="${ldap_password_alias}"
						updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

						propertyName=ranger.ldap.bind.password
						newPropertyValue="_"
						updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file
					else
						log "[E] $to_file_default does not exists" ; exit 1;
					fi
				else
					propertyName=ranger.ldap.bind.password
					newPropertyValue="${xa_ldap_bind_password}"
					updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file
				fi
				if test -f $keystore; then
					#echo "$keystore found."
					chown -R ${unix_user}:${unix_group} ${keystore}
					chmod 640 ${keystore}
				else
					propertyName=ranger.ldap.bind.password
					newPropertyValue="${xa_ldap_bind_password}"
					updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file
				fi
			fi
		else
			log "[E] $ldap_file does not exists" ; exit 1;

	fi
    fi
    if [ $authentication_method = "ACTIVE_DIRECTORY" ] ; then
	log "[I] Loading ACTIVE DIRECTORY attributes and properties";
		newPropertyValue=''
		ldap_file=$app_home/WEB-INF/classes/conf/ranger-admin-site.xml
		if test -f $ldap_file; then
			log "[I] $ldap_file file found"
			propertyName=ranger.ldap.ad.url
			newPropertyValue="${xa_ldap_ad_url}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			propertyName=ranger.ldap.ad.domain
			newPropertyValue="${xa_ldap_ad_domain}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			propertyName=ranger.authentication.method
			newPropertyValue="${authentication_method}"
			updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

			if [ "${xa_ldap_ad_base_dn}" != "" ] && [ "${xa_ldap_ad_bind_dn}" != "" ]  && [ "${xa_ldap_ad_bind_password}" != "" ]
			then
				$PYTHON_COMMAND_INVOKER dba_script.py ${xa_ldap_ad_bind_password} 'AD' 'password_validation'
				if [ "$?" != "0" ]
				then
					exit 1
				fi
				propertyName=ranger.ldap.ad.base.dn
				newPropertyValue="${xa_ldap_ad_base_dn}"
				updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

				propertyName=ranger.ldap.ad.bind.dn
				newPropertyValue="${xa_ldap_ad_bind_dn}"
				updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

				propertyName=ranger.ldap.ad.referral
				newPropertyValue="${xa_ldap_ad_referral}"
				updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

				propertyName=ranger.ldap.ad.user.searchfilter
				newPropertyValue="${xa_ldap_ad_userSearchFilter}"
				updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file

				keystore="${cred_keystore_filename}"

				if [ "${keystore}" != "" ]
				then
					mkdir -p `dirname "${keystore}"`

					ad_password_alias=ranger.ad.binddn.password
					$PYTHON_COMMAND_INVOKER ranger_credential_helper.py -l "cred/lib/*" -f "$keystore" -k "$ad_password_alias" -v "$xa_ldap_ad_bind_password" -c 1

					to_file_default=$app_home/WEB-INF/classes/conf/ranger-admin-default-site.xml

					if test -f $to_file_default; then
						propertyName=ranger.credential.provider.path
						newPropertyValue="${keystore}"
						updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

						propertyName=ranger.ldap.ad.binddn.credential.alias
						newPropertyValue="${ad_password_alias}"
						updatePropertyToFilePy $propertyName $newPropertyValue $to_file_default

						propertyName=ranger.ldap.ad.bind.password
						newPropertyValue="_"
						updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file
					else
						log "[E] $to_file_default does not exists" ; exit 1;
					fi
				else
					propertyName=ranger.ldap.ad.bind.password
					newPropertyValue="${xa_ldap_ad_bind_password}"
					updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file
				fi
				if test -f $keystore; then
					#echo "$keystore found."
					chown -R ${unix_user}:${unix_group} ${keystore}
					chmod 640 ${keystore}
				else
					propertyName=ranger.ldap.ad.bind.password
					newPropertyValue="${xa_ldap_ad_bind_password}"
					updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file
				fi
			fi
		else
			log "[E] $ldap_file does not exists" ; exit 1;
		fi
    fi
    if [ $authentication_method = "UNIX" ] ; then
        do_unixauth_setup
    fi

    if [ $authentication_method = "NONE" ] ; then
         newPropertyValue='NONE'
         ldap_file=$app_home/WEB-INF/classes/conf/ranger-admin-site.xml
         if test -f $ldap_file; then
                 propertyName=ranger.authentication.method
                 newPropertyValue="${authentication_method}"
                 updatePropertyToFilePy $propertyName $newPropertyValue $ldap_file
         fi
    fi	
	
    log "[I] Finished setup based on user authentication method=$authentication_method";
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
	if [ ! -d ${WEBAPP_ROOT}/WEB-INF/classes/conf ]; then
	    log "[I] Copying ${WEBAPP_ROOT}/WEB-INF/classes/conf.dist ${WEBAPP_ROOT}/WEB-INF/classes/conf"
	    mkdir -p ${WEBAPP_ROOT}/WEB-INF/classes/conf
	    cp ${WEBAPP_ROOT}/WEB-INF/classes/conf.dist/* ${WEBAPP_ROOT}/WEB-INF/classes/conf
	fi
	if [ -d ${WEBAPP_ROOT}/WEB-INF/classes/conf ]; then
		chown -R ${unix_user} ${WEBAPP_ROOT}/WEB-INF/classes/conf
	fi

	if [ ! -d ${WEBAPP_ROOT}/WEB-INF/classes/lib ]; then
	    log "[I] Creating ${WEBAPP_ROOT}/WEB-INF/classes/lib"
	    mkdir -p ${WEBAPP_ROOT}/WEB-INF/classes/lib
	fi
	if [ -d ${WEBAPP_ROOT}/WEB-INF/classes/lib ]; then
		chown -R ${unix_user} ${WEBAPP_ROOT}/WEB-INF/classes/lib
	fi

	if [ -d /etc/init.d ]; then
	    log "[I] Setting up init.d"
	    cp ${INSTALL_DIR}/ews/${RANGER_ADMIN_INITD} /etc/init.d/${RANGER_ADMIN}
	    chmod ug+rx /etc/init.d/${RANGER_ADMIN}

	    if [ -d /etc/rc2.d ]
	    then
		RC_DIR=/etc/rc2.d
		log "[I] Creating script S88${RANGER_ADMIN}/K90${RANGER_ADMIN} in $RC_DIR directory .... "
		rm -f $RC_DIR/S88${RANGER_ADMIN}  $RC_DIR/K90${RANGER_ADMIN}
		ln -s /etc/init.d/${RANGER_ADMIN} $RC_DIR/S88${RANGER_ADMIN}
		ln -s /etc/init.d/${RANGER_ADMIN} $RC_DIR/K90${RANGER_ADMIN}
	    fi

	    if [ -d /etc/rc3.d ]
	    then
		RC_DIR=/etc/rc3.d
		log "[I] Creating script S88${RANGER_ADMIN}/K90${RANGER_ADMIN} in $RC_DIR directory .... "
		rm -f $RC_DIR/S88${RANGER_ADMIN}  $RC_DIR/K90${RANGER_ADMIN}
		ln -s /etc/init.d/${RANGER_ADMIN} $RC_DIR/S88${RANGER_ADMIN}
		ln -s /etc/init.d/${RANGER_ADMIN} $RC_DIR/K90${RANGER_ADMIN}
	    fi

	    # SUSE has rc2.d and rc3.d under /etc/rc.d
	    if [ -d /etc/rc.d/rc2.d ]
	    then
		RC_DIR=/etc/rc.d/rc2.d
		log "[I] Creating script S88${RANGER_ADMIN}/K90${RANGER_ADMIN} in $RC_DIR directory .... "
		rm -f $RC_DIR/S88${RANGER_ADMIN}  $RC_DIR/K90${RANGER_ADMIN}
		ln -s /etc/init.d/${RANGER_ADMIN} $RC_DIR/S88${RANGER_ADMIN}
		ln -s /etc/init.d/${RANGER_ADMIN} $RC_DIR/K90${RANGER_ADMIN}
	    fi
	    if [ -d /etc/rc.d/rc3.d ]
	    then
		RC_DIR=/etc/rc.d/rc3.d
		log "[I] Creating script S88${RANGER_ADMIN}/K90${RANGER_ADMIN} in $RC_DIR directory .... "
		rm -f $RC_DIR/S88${RANGER_ADMIN}  $RC_DIR/K90${RANGER_ADMIN}
		ln -s /etc/init.d/${RANGER_ADMIN} $RC_DIR/S88${RANGER_ADMIN}
		ln -s /etc/init.d/${RANGER_ADMIN} $RC_DIR/K90${RANGER_ADMIN}
	    fi
	fi
	if [  -f /etc/init.d/${RANGER_ADMIN} ]; then
		if [ "${unix_user}" != "" ]; then
			sed  's/^LINUX_USER=.*$/LINUX_USER='${unix_user}'/g' -i  /etc/init.d/${RANGER_ADMIN}
		fi
	fi

	if [ ! -d ${XAPOLICYMGR_DIR}/ews/logs ]; then
	    log "[I] ${XAPOLICYMGR_DIR}/ews/logs folder"
	    mkdir -p ${XAPOLICYMGR_DIR}/ews/logs
	fi
	if [ -d ${XAPOLICYMGR_DIR}/ews/logs ]; then
          chown -R ${unix_user} ${XAPOLICYMGR_DIR}/ews/logs
          chown -R ${unix_user} ${XAPOLICYMGR_DIR}/ews/logs/*
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
	if [ ! \( -e /usr/bin/ranger-admin \) ]
	then
		ln -sf ${INSTALL_DIR}/ews/ranger-admin-services.sh /usr/bin/ranger-admin
		chmod ug+rx /usr/bin/ranger-admin	
	fi
}

init_logfiles
log " --------- Running Ranger PolicyManager Web Application Install Script --------- "
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
if [ "$?" == "0" ]
then
$PYTHON_COMMAND_INVOKER db_setup.py
else
	exit 1
fi
if [ "$?" == "0" ]
then
update_properties
do_authentication_setup
else
	log "[E] DB schema setup failed! Please contact Administrator."
	exit 1
fi
$PYTHON_COMMAND_INVOKER db_setup.py -javapatch
if [ "$?" == "0" ]
then
echo "ln -sf ${WEBAPP_ROOT}/WEB-INF/classes/conf ${INSTALL_DIR}/conf"
ln -sf ${WEBAPP_ROOT}/WEB-INF/classes/conf ${INSTALL_DIR}/conf
echo "Installation of Ranger PolicyManager Web Application is completed."
else
	exit 1
fi
