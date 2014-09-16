#!/bin/bash

# -------------------------------------------------------------------------------------
#
# XASecure PolicyManager Installation Script
# 
# This script will install policymanager webapplication under tomcat and also, initialize the mysql database with xasecure users/tables.
#
# (c) 2013,2014 XASecure
#
# -------------------------------------------------------------------------------------

PROPFILE=$PWD/install.properties
propertyValue=''

. $PROPFILE
if [ ! $? = "0" ];then	
	log "$PROPFILE file not found....!!"; 
	exit 1; 
fi

MYSQL_HOST="${db_host}"

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


init_logfiles () {
    for f in $LOGFILES; do
        touch $f
    done
    #log "start date for $0 = `date`"
}

init_variables(){
	curDt=`date '+%Y%m%d%H%M%S'`

	VERSION=`cat ${PWD}/version`

	XAPOLICYMGR_DIR=/usr/lib/xapolicymgr

	if [ "${VERSION}" != "" ]
	then
 		INSTALL_DIR=${XAPOLICYMGR_DIR}-${VERSION}
	else
		INSTALL_DIR=${XAPOLICYMGR_DIR}
 	fi

	WEBAPP_ROOT=${INSTALL_DIR}/ews/webapp
	
	getPropertyFromFile 'db_root_password' $PROPFILE db_user
	getPropertyFromFile 'db_user' $PROPFILE db_user
	getPropertyFromFile 'db_password' $PROPFILE db_password
	getPropertyFromFile 'audit_db_user' $PROPFILE audit_db_user
	getPropertyFromFile 'audit_db_password' $PROPFILE audit_db_password
}

wait_for_tomcat_shutdown() {
	i=1
	touch $TMPFILE
	while [ $i -le 20 ]
	do
		ps -ef | grep catalina.startup.Bootstrap | grep -v grep > $TMPFILE
		if [ $? -eq 1 ]; then
			log "[I] Tomcat stopped"
			i=21
		else 
			log "[I] stopping Tomcat.."
			i=`expr $i + 1`
			sleep 1
		fi 
	done
}

check_mysql_version() {
	if is_command ${MYSQL_BIN} ; then
		log "[I] '${MYSQL_BIN}' command found"
	else
		log "[E] '${MYSQL_BIN}' command not found"
		exit 1;
	fi
}

check_mysql_connector() {
	log "[I] Checking MYSQL CONNECTOR FILE : $MYSQL_CONNECTOR_JAR" 
	if test -f "$MYSQL_CONNECTOR_JAR"; then
		log "[I] MYSQL CONNECTOR FILE : $MYSQL_CONNECTOR_JAR file found" 
	else
		log "[E] MYSQL CONNECTOR FILE : $MYSQL_CONNECTOR_JAR does not exists" ; exit 1;
	fi

}
check_java_version() {
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

	#Check for JAVA_HOME 
	if [ "${JAVA_HOME}" == "" ]
	then
 		log "[E] JAVA_HOME environment property not defined, aborting installation."
 		exit 1
 	fi

	#$JAVA_BIN -version 2>&1 | grep -q "$JAVA_ORACLE"
	#if [ $? != 0 ] ; then
		#log "[E] Oracle Java is required"
		#exit 1;
	#fi
}

sanity_check_files() {

	if test -f $war_file; then
		log "[I] $war_file file found" 
	else
		log "[E] $war_file does not exists" ; exit 1;
        fi

	if test -f $db_core_file; then
		log "[I] $db_core_file file found" 
	else
		log "[E] $db_core_file does not exists" ; exit 1;
        fi
}

create_rollback_point() {
    DATE=`date`
    BAK_FILE=$APP-$VERSION.$DATE.bak
    log "Creating backup file : $BAK_FILE"
    cp "$APP" "$BAK_FILE"
}

create_mysql_user(){
	check_mysql_password
	check_mysql_user_password

	log "[I] Creating MySQL user '$db_user' (using root priviledges)"
	
	for thost in '%' localhost
	do
		usercount=`$MYSQL_BIN -B -u root --password="$db_root_password" -h $MYSQL_HOST --skip-column-names -e "select count(*) from mysql.user where user = '$db_user' and host = '$thost';"`
		if  [ ${usercount} -eq 0 ]
		then
			$MYSQL_BIN -B -u root --password="$db_root_password" -h $MYSQL_HOST -e "create user '$db_user'@'$thost' identified by '$db_password';"
		fi
		
		mysqlquery="GRANT ALL ON *.* TO '$db_user'@'$thost' ; 
		grant all privileges on *.* to '$db_user'@'$thost' with grant option;
		FLUSH PRIVILEGES;"
		
		echo "${mysqlquery}" | $MYSQL_BIN -u root --password="$db_root_password" -h $MYSQL_HOST
		check_ret_status $? "MySQL create user failed"
		
	done
	log "[I] Creating MySQL user '$db_user' (using root priviledges) DONE"
}
check_mysql_password () {
	count=0
	log "[I] Checking MYSQL root password"
	
	msg=`$MYSQL_BIN -u root --password="$db_root_password" -h $MYSQL_HOST -s -e "select version();" 2>&1`
	cmdStatus=$?
	while :
	do	
		if  [  $cmdStatus != 0 ]; then
			if [ $count != 0 ]
			then
				log "[I] COMMAND: mysql -u root --password=..... -h $MYSQL_HOST : FAILED with error message:			      						\n*******************************************\n${msg}\n*******************************************\n"
			fi
			if [ $count -gt 2 ]
			then
				log "[E] Unable to continue as mysql connectivity fails."
				exit 1
			fi
		    trap 'stty echo; exit 1' 2 3 15
			printf "Please enter password for mysql user-id, root@${MYSQL_HOST} : "
			stty -echo
			read db_root_password
			stty echo
			printf "\n"
			trap '' 2 3 15
			count=`expr ${count} + 1`
			msg=`$MYSQL_BIN -u root --password="$db_root_password" -h $MYSQL_HOST -s -e "select version();" 2>&1`
			cmdStatus=$?
	   	else
			log "[I] Checking MYSQL root password DONE"
			break;
		fi
	done
	return 0;
}

check_mysql_user_password() {
	count=0
	muser=${db_user}@${MYSQL_HOST}
	while [ "${db_password}" = "" ]
	do
		if [ $count -gt 0 ]
		then
			log "[I] You can not have a empty password for user: (${muser})." 
		fi
		if [ ${count} -gt 2 ]
		then
			log "[E] Unable to continue as user, ${muser} does not have a non-empty password."
		fi
		printf "Please enter password for the XASecure schema owner (${muser}): "
		trap 'stty echo; exit 1' 2 3 15
		stty -echo
		read db_password
		stty echo
		printf "\n"
		trap ''  2 3 15
		count=`expr ${count} + 1`
	done
}


check_mysql_audit_user_password() {
	count=0
	muser=${audit_db_user}@${MYSQL_HOST}
	while [ "${audit_db_password}" = "" ]
	do
		if [ $count -gt 0 ]
		then
			log "[I] You can not have a empty password for user: (${muser})." 
		fi
		if [ ${count} -gt 2 ]
		then
			log "[E] Unable to continue as user, ${muser} does not have a non-empty password."
		fi
		printf "Please enter password for the XASecure Audit Table owner (${muser}): "
		trap 'stty echo; exit 1' 2 3 15
		stty -echo
		read audit_db_password
		stty echo
		printf "\n"
		trap ''  2 3 15
		count=`expr ${count} + 1`
	done
}

upgrade_db() {
	log "[I] - starting upgradedb ... "

	DBVERSION_CATALOG_CREATION=db/create_dbversion_catalog.sql

	#mysqlexec="${MYSQL_BIN} -u ${db_user} --password=${db_password} -h ${MYSQL_HOST} -D ${db_name}"
	
	if [ -f ${DBVERSION_CATALOG_CREATION} ]
	then
		`${MYSQL_BIN} -u "${db_user}" --password="${db_password}" -h ${MYSQL_HOST} -D ${db_name} < ${DBVERSION_CATALOG_CREATION}`
		check_ret_status $? "Verifying database version catalog table Failed."
	fi
		
	dt=`date '+%s'`
	tempFile=/tmp/sql_${dt}_$$.sql
	sqlfiles=`ls -1 db/patches/*.sql 2> /dev/null | awk -F/ '{ print $NF }' | awk -F- '{ print $1, $0 }' | sort -k1 -n | awk '{ printf("db/patches/%s\n",$2) ; }'`
	for sql in ${sqlfiles}
	do
		if [ -f ${sql} ]
		then
			bn=`basename ${sql}`
			version=`echo ${bn} | awk -F'-' '{ print $1 }'`
			if [ "${version}" != "" ]
			then
				c=`${MYSQL_BIN} -u "${db_user}" --password="${db_password}" -h ${MYSQL_HOST} -D ${db_name} -B --skip-column-names -e "select count(id) from x_db_version_h where version = '${version}' and active = 'Y'"`
				check_ret_status $? "DBVerionCheck - ${version} Failed."
				if [ ${c} -eq 0 ]
				then
					cat ${sql} > ${tempFile}
					echo >> ${tempFile}
					echo "insert into x_db_version_h (version, inst_at, inst_by, updated_at, updated_by) values ( '${version}', now(), user(), now(), user()) ;" >> ${tempFile}
					log "[I] - patch [${version}] is being applied."
					`${MYSQL_BIN} -u "${db_user}" --password="${db_password}" -h ${MYSQL_HOST} -D ${db_name} < ${tempFile}`
					check_ret_status $? "Update patch - ${version} Failed. See sql file : [${tempFile}]"
					rm -f ${tempFile}
				else
					log "[I] - patch [${version}] is already applied. Skipping ..."
				fi
			fi
		fi
	done
	log "[I] - upgradedb completed."
}

import_db () {

	log "[I] Verifying Database: $db_name";
	existdb=`${MYSQL_BIN} -u "${db_user}" --password="${db_password}" -h $MYSQL_HOST -B --skip-column-names -e  "show databases like '${db_name}' ;"`

	if [ "${existdb}" = "${db_name}" ]
	then
		log "[I] - database ${db_name} already exists. Ignoring import_db ..."
	else
		log "[I] Creating Database: $db_name";
		$MYSQL_BIN -u "$db_user" --password="$db_password" -h $MYSQL_HOST -e "create database $db_name"  
		check_ret_status $? "Creating database Failed.."
	
	
		log "[I] Importing Core Database file: $db_core_file "
    	$MYSQL_BIN -u "$db_user" --password="$db_password" -h $MYSQL_HOST $db_name < $db_core_file
    	check_ret_status $? "Importing Database Failed.."
	
		if [ -f "${db_asset_file}" ] 
		then
			$MYSQL_BIN -u "$db_user" --password="$db_password" -h $MYSQL_HOST ${db_name} < ${db_asset_file}
			check_ret_status $? "Reset of DB repositories failed"
		fi

		log "[I] Importing Database file : $db_core_file DONE";
	fi	
}

extract_war () {
	if [ ! -e $war_file ]
	then
		log "[E] $war_file file not found!"
	fi
	log "[I] Extract War file $war_file to $app_home" # 
	if [ -d $app_home ]
	then
		mv ${app_home} ${app_home}_archive_`date '+%s'`
	fi
	mkdir -p $app_home
	unzip -q $war_file -d $app_home 
	check_ret_status $? "Extraction of war file failed....!!"
	log "[I] Extract War file $war_file DONE" # 
}

copy_to_webapps (){
	log "[I] Copying to ${WEBAPP_ROOT} ";
	if [ -f $app_home/WEB-INF/log4j.xml.prod ]
    then
        mv -f $app_home/WEB-INF/log4j.xml.prod $app_home/WEB-INF/log4j.xml
    fi
    cp -rf $app_home/* ${WEBAPP_ROOT}
	check_ret_status $? "Copying to ${WEBAPP_ROOT} failed"
	
	#
	# the jar file, ${INSTALL_DIR}/webapps/ROOT/WEB-INF/lib/unixauthclient-*.jar should be accessed from external to have the parameter to work correctly
	#
	for f in  ${WEBAPP_ROOT}/WEB-INF/lib/unixauthclient-*.jar
    do
		if [ -f ${f} ]
		then
			mkdir -p ${INSTALL_DIR}/xasecure_jaas/
			mv ${f} ${INSTALL_DIR}/xasecure_jaas/
		fi
    done

	log "[I] Copying to ${WEBAPP_ROOT} DONE";
}

copy_mysql_connector(){
	log "[I] Copying MYSQL Connector to $app_home/WEB-INF/lib ";
    cp -f $MYSQL_CONNECTOR_JAR $app_home/WEB-INF/lib
	check_ret_status $? "Copying MYSQL Connector to $app_home/WEB-INF/lib failed"
	log "[I] Copying MYSQL Connector to $app_home/WEB-INF/lib DONE";
}

update_properties() {
	newPropertyValue=''
	to_file=$app_home/WEB-INF/classes/xa_system.properties

	if test -f $to_file; then
		log "[I] $to_file file found" 
	else
		log "[E] $to_file does not exists" ; exit 1;
    fi

	propertyName=jdbc.url
	newPropertyValue="jdbc:log4jdbc:mysql://${MYSQL_HOST}:3306/${db_name}"
	updatePropertyToFile $propertyName $newPropertyValue $to_file	

	propertyName=xa.webapp.url.root
	newPropertyValue="${policymgr_external_url}"
	updatePropertyToFile $propertyName $newPropertyValue $to_file

	propertyName=http.enabled
	newPropertyValue="${policymgr_http_enabled}"
	updatePropertyToFile $propertyName $newPropertyValue $to_file

	propertyName=auditDB.jdbc.url
	newPropertyValue="jdbc:log4jdbc:mysql://${MYSQL_HOST}:3306/${audit_db_name}"
	updatePropertyToFile $propertyName $newPropertyValue $to_file	
	
	propertyName=jdbc.user
	newPropertyValue="${db_user}"
	updatePropertyToFile $propertyName $newPropertyValue $to_file	
	
	propertyName=auditDB.jdbc.user
	newPropertyValue="${audit_db_user}"
	updatePropertyToFile $propertyName $newPropertyValue $to_file
	##########

	keystore="${cred_keystore_filename}"

	echo "Starting configuration for XA DB credentials:"

	db_password_alias=policyDB.jdbc.password
	
   	if [ "${keystore}" != "" ]
   	then
		mkdir -p `dirname "${keystore}"`

   		java -cp "cred/lib/*" com.hortonworks.credentialapi.buildks create "$db_password_alias" -value "$db_password" -provider jceks://file$keystore
   		
   		propertyName=xaDB.jdbc.credential.alias
		newPropertyValue="${db_password_alias}"
		updatePropertyToFile $propertyName $newPropertyValue $to_file
	
		propertyName=xaDB.jdbc.credential.provider.path
		newPropertyValue="${keystore}"
		updatePropertyToFile $propertyName $newPropertyValue $to_file

		propertyName=jdbc.password
		newPropertyValue="_"	
		updatePropertyToFile $propertyName $newPropertyValue $to_file
   	else  	
		propertyName=jdbc.password
		newPropertyValue="${db_password}"	
		updatePropertyToFile $propertyName $newPropertyValue $to_file
	fi	
	
	if test -f $keystore; then
		#echo "$keystore found."
		chown -R ${unix_user}:${unix_group} ${keystore}
	else
		#echo "$keystore not found. so clear text password"
		propertyName=jdbc.password
		newPropertyValue="${db_password}"
		updatePropertyToFile $propertyName $newPropertyValue $to_file
	fi
 
	###########
	audit_db_password_alias=auditDB.jdbc.password

	echo "Starting configuration for Audit DB credentials:"
	
   	if [ "${keystore}" != "" ]
   	then
	   	java -cp "cred/lib/*" com.hortonworks.credentialapi.buildks create "$audit_db_password_alias" -value "$audit_db_password" -provider jceks://file$keystore
	   	
		propertyName=auditDB.jdbc.credential.alias
		newPropertyValue="${audit_db_password_alias}"
		updatePropertyToFile $propertyName $newPropertyValue $to_file	
		
		propertyName=auditDB.jdbc.credential.provider.path
		newPropertyValue="${keystore}"
		updatePropertyToFile $propertyName $newPropertyValue $to_file

		propertyName=auditDB.jdbc.password
		newPropertyValue="_"	
		updatePropertyToFile $propertyName $newPropertyValue $to_file
   	else
		propertyName=auditDB.jdbc.password
		newPropertyValue="${audit_db_password}"	
		updatePropertyToFile $propertyName $newPropertyValue $to_file
	fi	
	
	if test -f $keystore; then
		chown -R ${unix_user}:${unix_group} ${keystore}
		#echo "$keystore found."
	else
		#echo "$keystore not found. so use clear text password"
		propertyName=auditDB.jdbc.password
		newPropertyValue="${audit_db_password}"	
		updatePropertyToFile $propertyName $newPropertyValue $to_file
	fi
	
}

create_audit_mysql_user(){

	check_mysql_audit_user_password

	AUDIT_DB="${audit_db_name}"
	AUDIT_USER="${audit_db_user}"
	AUDIT_PASSWORD="${audit_db_password}"

	log "[I] Verifying Database: $AUDIT_DB";
	existdb=`${MYSQL_BIN} -u root --password="$db_root_password" -h $MYSQL_HOST -B --skip-column-names -e  "show databases like '$AUDIT_DB' ;"`

	if [ "${existdb}" = "$AUDIT_DB" ]
	then
		log "[I] - database $AUDIT_DB already exists."
	else
		log "[I] Creating Database: $audit_db_name";
		$MYSQL_BIN -u root --password="$db_root_password" -h $MYSQL_HOST -e "create database $AUDIT_DB"  
		check_ret_status $? "Creating database $AUDIT_DB Failed.."
	fi	

	for thost in '%' localhost
	do
		usercount=`$MYSQL_BIN -B -u root --password="$db_root_password" -h $MYSQL_HOST --skip-column-names -e "select count(*) from mysql.user where user = '$AUDIT_USER' and host = '$thost';"`
		if  [ ${usercount} -eq 0 ]
		then
		  log "[I] Creating MySQL user '$AUDIT_USER'@'$thost' (using root priviledges)"
		  $MYSQL_BIN -B -u root --password="$db_root_password" -h $MYSQL_HOST -e "create user '$AUDIT_USER'@'$thost' identified by '$AUDIT_PASSWORD';"
		  check_ret_status $? "MySQL create user failed"
		fi
		
		mysqlquery="GRANT ALL ON $AUDIT_DB.* TO '$AUDIT_USER'@'$thost' ; 
		grant all privileges on $AUDIT_DB.* to '$AUDIT_USER'@'$thost' with grant option;
		FLUSH PRIVILEGES;"
		
		echo "${mysqlquery}" | $MYSQL_BIN -u root --password="$db_root_password" -h $MYSQL_HOST
		check_ret_status $? "MySQL query failed: $mysqlquery"
	done
	log "[I] Creating MySQL user '$AUDIT_USER' (using root priviledges) DONE"
	
	AUDIT_TABLE=xa_access_audit
	log "[I] Verifying table $AUDIT_TABLE in audit database $AUDIT_DB";
	existtbl=`${MYSQL_BIN} -u "$AUDIT_USER" --password="$AUDIT_PASSWORD" -D $AUDIT_DB -h $MYSQL_HOST -B --skip-column-names -e  "show tables like '$AUDIT_TABLE' ;"`

	if [ "${existtbl}" != "$AUDIT_TABLE" ]
	then
		log "[I] Importing Audit Database file: $db_audit_file..."
  	$MYSQL_BIN -u "$AUDIT_USER" --password="$AUDIT_PASSWORD" -h $MYSQL_HOST $AUDIT_DB < $db_audit_file
  	check_ret_status $? "Importing Audit Database Failed.."

		log "[I] Importing Audit Database file : $db_audit_file DONE";
	else
		log "[I] - table $AUDIT_TABLE already exists in audit database $AUDIT_DB"
	fi	
}

do_unixauth_setup() {

	XASECURE_JAAS_DIR="${INSTALL_DIR}/xasecure_jaas"

	if [ -d "${XASECURE_JAAS_DIR}" ]
	then
		mv "${XASECURE_JAAS_DIR}" "${XASECURE_JAAS_DIR}_archive_`date '+%s'`"
	fi

	mkdir -p ${XASECURE_JAAS_DIR}

	cp ./unixauth-config/*  ${XASECURE_JAAS_DIR}

	cat unixauth-config/unixauth.properties | \
			grep -v '^remoteLoginEnabled=' | \
			grep -v '^authServiceHostName=' | \
			grep -v '^authServicePort=' > ${INSTALL_DIR}/xasecure_jaas/unixauth.properties

	echo "remoteLoginEnabled=${remoteLoginEnabled}"   >> ${INSTALL_DIR}/xasecure_jaas/unixauth.properties
	echo "authServiceHostName=${authServiceHostName}" >> ${INSTALL_DIR}/xasecure_jaas/unixauth.properties
	echo "authServicePort=${authServicePort}"         >> ${INSTALL_DIR}/xasecure_jaas/unixauth.properties

	owner=xasecure
	group=xasecure
	chown -R ${owner}:${group} ${XASECURE_JAAS_DIR}
	chmod -R go-rwx ${XASECURE_JAAS_DIR}

	

}
do_authentication_setup(){
	log "[I] Starting setup based on user authentication method=$authentication_method";     
	./setup_authentication.sh $authentication_method $app_home

    if [ $authentication_method = "LDAP" ] ; then
    	log "[I] Loading LDAP attributes and properties";
		newPropertyValue=''	
		ldap_file=$app_home/WEB-INF/classes/xa_ldap.properties
		if test -f $ldap_file; then
			log "[I] $ldap_file file found" 
			propertyName=xa_ldap_url
			newPropertyValue="${xa_ldap_url}"
			
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
			
			propertyName=xa_ldap_userDNpattern
			newPropertyValue="${xa_ldap_userDNpattern}"
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
			
			propertyName=xa_ldap_groupSearchBase
			newPropertyValue="${xa_ldap_groupSearchBase}"
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
			
			propertyName=xa_ldap_groupSearchFilter
			newPropertyValue="${xa_ldap_groupSearchFilter}"
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
			
			propertyName=xa_ldap_groupRoleAttribute
			newPropertyValue="${xa_ldap_groupRoleAttribute}"
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
			
			propertyName=authentication_method
			newPropertyValue="${authentication_method}"
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
		else
			log "[E] $ldap_file does not exists" ; exit 1;
		
    	fi
    fi
    if [ $authentication_method = "ACTIVE_DIRECTORY" ] ; then
    	log "[I] Loading ACTIVE DIRECTORY attributes and properties";
		newPropertyValue=''
		ldap_file=$app_home/WEB-INF/classes/xa_ldap.properties
		if test -f $ldap_file; then
			log "[I] $ldap_file file found" 
			propertyName=xa_ldap_ad_url
			newPropertyValue="${xa_ldap_ad_url}"
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
		
			propertyName=xa_ldap_ad_domain
			newPropertyValue="${xa_ldap_ad_domain}"
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
			
			propertyName=authentication_method
			newPropertyValue="${authentication_method}"
			updatePropertyToFile $propertyName $newPropertyValue $ldap_file
		else
			log "[E] $ldap_file does not exists" ; exit 1;
		fi
    fi
    if [ $authentication_method = "UNIX" ] ; then
        do_unixauth_setup
    fi
    log "[I] Finished setup based on user authentication method=$authentication_method";  
}

#=====================================================================

setup_unix_user_group(){

	log "[I] Setting up UNIX user : ${unix_user} and group: ${unix_group}";

	id -g ${unix_group} > /dev/null 2>&1

	if [ $? -ne 0 ]
	then
		groupadd ${unix_group}
		check_ret_status $? "Creating group ${unix_group} failed"
	fi

	id -u ${unix_user} > /dev/null 2>&1

	if [ $? -ne 0 ]
	then
        useradd ${unix_user} -g ${unix_group} -m
		check_ret_status $? "useradd ${unix_user} failed"
	fi

	log "[I] Setting up UNIX user : ${unix_user} and group: ${unix_group} DONE";
}

setup_install_files(){

	log "[I] Setting up installation files and directory";
	if [ -d ${INSTALL_DIR} ]
	then
		mv ${INSTALL_DIR} ${INSTALL_DIR}_${curDt}
	fi

	mkdir -p ${INSTALL_DIR}
	mkdir -p ${INSTALL_DIR}/ews
	mkdir -p ${WEBAPP_ROOT}

	cp -r ews/* ${INSTALL_DIR}/
	mv ${INSTALL_DIR}/lib ${INSTALL_DIR}/ews/
	mv ${INSTALL_DIR}/xapolicymgr.properties ${INSTALL_DIR}/ews/
	mv ${INSTALL_DIR}/xapolicymgr /etc/init.d/xapolicymgr

	cat ews/startpolicymgr.sh | sed -e "s|[ \t]*JAVA_HOME=| JAVA_HOME=${JAVA_HOME}|" > ${INSTALL_DIR}/startpolicymgr.sh

	chmod ug+rx /etc/init.d/xapolicymgr

	if [ -d /etc/rc2.d ]
    then
		RC_DIR=/etc/rc2.d
        log "[I] Creating script S88xapolicymgr/K90xapolicymgr in $RC_DIR directory .... "
		rm -f $RC_DIR/S88xapolicymgr  $RC_DIR/K90xapolicymgr
		ln -s /etc/init.d/xapolicymgr $RC_DIR/S88xapolicymgr
		ln -s /etc/init.d/xapolicymgr $RC_DIR/K90xapolicymgr
    fi

    if [ -d /etc/rc3.d ]
    then
	    RC_DIR=/etc/rc3.d
        log "[I] Creating script S88xapolicymgr/K90xapolicymgr in $RC_DIR directory .... "
		rm -f $RC_DIR/S88xapolicymgr  $RC_DIR/K90xapolicymgr
		ln -s /etc/init.d/xapolicymgr $RC_DIR/S88xapolicymgr
		ln -s /etc/init.d/xapolicymgr $RC_DIR/K90xapolicymgr
    fi

	# SUSE has rc2.d and rc3.d under /etc/rc.d
    if [ -d /etc/rc.d/rc2.d ]
    then
		RC_DIR=/etc/rc.d/rc2.d
        log "[I] Creating script S88xapolicymgr/K90xapolicymgr in $RC_DIR directory .... "
		rm -f $RC_DIR/S88xapolicymgr  $RC_DIR/K90xapolicymgr
		ln -s /etc/init.d/xapolicymgr $RC_DIR/S88xapolicymgr
		ln -s /etc/init.d/xapolicymgr $RC_DIR/K90xapolicymgr
    fi
    if [ -d /etc/rc.d/rc3.d ]
    then
		RC_DIR=/etc/rc.d/rc3.d
        log "[I] Creating script S88xapolicymgr/K90xapolicymgr in $RC_DIR directory .... "
		rm -f $RC_DIR/S88xapolicymgr  $RC_DIR/K90xapolicymgr
		ln -s /etc/init.d/xapolicymgr $RC_DIR/S88xapolicymgr
		ln -s /etc/init.d/xapolicymgr $RC_DIR/K90xapolicymgr
    fi


	if [ -L ${XAPOLICYMGR_DIR} ]
	then 
		rm -f ${XAPOLICYMGR_DIR}
	fi

	ln -s ${INSTALL_DIR} ${XAPOLICYMGR_DIR}

	if [ ! -L /var/log/xapolicymgr ]
	then
		ln -s ${XAPOLICYMGR_DIR}/ews/logs  /var/log/xapolicymgr
	fi
	log "[I] Setting up installation files and directory DONE";

	if [ -d ${INSTALL_DIR}/ ]
	then
		chown -R ${unix_user}:${unix_group} ${INSTALL_DIR}
	fi
}

restart_policymgr(){

	log "[I] Restarting xapolicymgr";
	service xapolicymgr stop 
	service xapolicymgr start
	sleep 30  # To ensure that the root application is initialized fully
	log "[I] Restarting xapolicymgr DONE";

}

init_logfiles
log " --------- Running XASecure PolicyManager Web Application Install Script --------- "
log "[I] uname=`uname`"
log "[I] hostname=`hostname`"
init_variables
get_distro
check_java_version
check_mysql_version
check_mysql_connector
setup_unix_user_group
setup_install_files
sanity_check_files
create_mysql_user
extract_war
copy_mysql_connector
import_db
upgrade_db
create_audit_mysql_user
update_properties
do_authentication_setup
copy_to_webapps
restart_policymgr
echo "Installation of XASecure PolicyManager Web Application is completed."
