#!/bin/bash

#
# Ensure that the user is root
#

INSTALL_BASE=/usr/lib

MOD_NAME="uxugsync"

MY_ID=`id -u`

if [ "${MY_ID}" -ne 0 ]
then
	echo "ERROR: You must run the installation as root user."
	exit 1
fi

if [ "${JAVA_HOME}" == "" ]
then
	echo "ERROR: JAVA_HOME environment property not defined, aborting installation"
	exit 1
fi

INSTALL_DIR=${INSTALL_BASE}/${MOD_NAME}

#
# Embed the configuration from install.properties to conf/unixauthservice.properties
#

cdir=`dirname $0`

POLICY_MGR_URL=`grep '^[ \t]*POLICY_MGR_URL[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
MIN_UNIX_USER_ID_TO_SYNC=`grep '^[ \t]*MIN_UNIX_USER_ID_TO_SYNC[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_SOURCE=`grep '^[ \t]*SYNC_SOURCE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_INTERVAL=`grep '^[ \t]*SYNC_INTERVAL[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_URL=`grep '^[ \t]*SYNC_LDAP_URL[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_URL[ \t]*=[ \t]*::'`

SYNC_LDAP_BIND_DN=`grep '^[ \t]*SYNC_LDAP_BIND_DN[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_BIND_DN[ \t]*=[ \t]*::'`

SYNC_LDAP_BIND_PASSWORD=`grep '^[ \t]*SYNC_LDAP_BIND_PASSWORD[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_BIND_PASSWORD[ \t]*=[ \t]*::'`

SYNC_LDAP_USER_SEARCH_BASE=`grep '^[ \t]*SYNC_LDAP_USER_SEARCH_BASE[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_USER_SEARCH_BASE[ \t]*=[ \t]*::'`

SYNC_LDAP_USER_SEARCH_SCOPE=`grep '^[ \t]*SYNC_LDAP_USER_SEARCH_SCOPE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_USER_OBJECT_CLASS=`grep '^[ \t]*SYNC_LDAP_USER_OBJECT_CLASS[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_USER_SEARCH_FILTER=`grep '^[ \t]*SYNC_LDAP_USER_SEARCH_FILTER[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_SEARCH_FILTER[ \t]*=[ \t]*::'`

SYNC_LDAP_USER_NAME_ATTRIBUTE=`grep '^[ \t]*SYNC_LDAP_USER_NAME_ATTRIBUTE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE=`grep '^[ \t]*SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_USERNAME_CASE_CONVERSION=`grep '^[ \t]*SYNC_LDAP_USERNAME_CASE_CONVERSION[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_GROUPNAME_CASE_CONVERSION=`grep '^[ \t]*SYNC_LDAP_GROUPNAME_CASE_CONVERSION[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

if [ "${SYNC_LDAP_USERNAME_CASE_CONVERSION}" == "" ]
then
    SYNC_LDAP_USERNAME_CASE_CONVERSION="none"
fi

if [ "${SYNC_LDAP_GROUPNAME_CASE_CONVERSION}" == "" ]
then
    SYNC_LDAP_GROUPNAME_CASE_CONVERSION="none"
fi

SYNC_LDAP_BIND_KEYSTOREPATH=`grep '^[ \t]*CRED_KEYSTORE_FILENAME[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*CRED_KEYSTORE_FILENAME[ \t]*=[ \t]*::'`

SYNC_LDAP_BIND_ALIAS=ldap.bind.password

if [ "${SYNC_INTERVAL}" != "" ]
then 
    SYNC_INTERVAL=$((${SYNC_INTERVAL}*60*1000))
else
    SYNC_INTERVAL=$((5*60*1000))
fi

if [ "${SYNC_SOURCE}" == "" ]
then
  SYNC_SOURCE="com.xasecure.unixusersync.process.UnixUserGroupBuilder"
elif [ "${SYNC_SOURCE}" == "unix" ]
then
  SYNC_SOURCE="com.xasecure.unixusersync.process.UnixUserGroupBuilder"
elif [ "${SYNC_SOURCE}" == "ldap" ]
then
  SYNC_SOURCE="com.xasecure.ldapusersync.process.LdapUserGroupBuilder"
else
  echo "Unsupported value for SYNC_SOURCE: ${SYNC_SOURCE}, supported values: ldap, unix, default: unix"
  exit 2
fi


if [ "${SYNC_SOURCE}" == "com.xasecure.ldapusersync.process.LdapUserGroupBuilder" ]
then

  if [ "${SYNC_INTERVAL}" == "" ]
  then
    SYNC_INTERVAL=$((360*60*1000))
  fi

  if [ "${SYNC_LDAP_URL}" == "" ]
  then
    echo "SYNC_LDAP_URL must be specified when SYNC_SOURCE is ldap"
    exit 3
  fi

  if [ "${SYNC_LDAP_BIND_DN}" == "" ]
  then
    echo "SYNC_LDAP_BIND_DN must be specified when SYNC_SOURCE is ldap"
    exit 4
  fi

  if [ "${SYNC_LDAP_USER_SEARCH_BASE}" == "" ]
  then
    echo "SYNC_LDAP_USER_SEARCH_BASE must be specified when SYNC_SOURCE is ldap"
    exit 5
  fi

  if [ "${SYNC_LDAP_USER_SEARCH_SCOPE}" == "" ]
  then
    SYNC_LDAP_USER_SEARCH_SCOPE="sub"
  fi

  if [ "${SYNC_LDAP_USER_SEARCH_SCOPE}" != "base" ] && [ "${SYNC_LDAP_USER_SEARCH_SCOPE}" != "one" ] && [ "${SYNC_LDAP_USER_SEARCH_SCOPE}" != "sub" ]
  then
    echo "Unsupported value for SYNC_LDAP_USER_SEARCH_SCOPE: ${SYNC_LDAP_USER_SEARCH_SCOPE}, supported values: base, one, sub"
    exit 6
  fi

  if [ "${SYNC_LDAP_USER_OBJECT_CLASS}" == "" ]
  then
    SYNC_LDAP_USER_OBJECT_CLASS="person"
  fi

  if [ "${SYNC_LDAP_USER_NAME_ATTRIBUTE}" == "" ]
  then
    SYNC_LDAP_USER_NAME_ATTRIBUTE="cn"
  fi

  if [ "${SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE}" == "" ]
  then
    SYNC_LDAP_USER_NAME_ATTRIBUTE="memberof,ismemberof"
  fi

#ldap credential keystore creation  
	echo "Starting configuration for LDAP credentials:"		
   	if [[ "${SYNC_LDAP_BIND_ALIAS}" != ""  && "${SYNC_LDAP_BIND_KEYSTOREPATH}" != "" ]]
   	then
		mkdir -p `dirname "${SYNC_LDAP_BIND_KEYSTOREPATH}"`
		java -cp "lib/*" com.hortonworks.credentialapi.buildks create $SYNC_LDAP_BIND_ALIAS -value $SYNC_LDAP_BIND_PASSWORD -provider jceks://file$SYNC_LDAP_BIND_KEYSTOREPATH
    	#java -cp "lib/commons-cli-1.2.jar:lib/commons-collections-3.2.1.jar:lib/commons-configuration-1.6.jar:lib/commons-lang-2.6.jar:lib/commons-logging-1.1.1.jar:lib/guava-11.0.2.jar:lib/hadoop-auth-2.2.0.jar:lib/hadoop-common-plus-3.0.0-SNAPSHOT.jar:lib/slf4j-api-1.7.5.jar:lib/local-jks-builder.jar" com.hortonworks.credentialapi.buildks create $SYNC_LDAP_BIND_ALIAS -value $SYNC_LDAP_BIND_PASSWORD -provider jceks://file$SYNC_LDAP_BIND_KEYSTOREPATH
    	SYNC_LDAP_BIND_PASSWORD="_"
    fi
#
fi

CFG_FILE="${cdir}/conf/unixauthservice.properties"
NEW_CFG_FILE=${CFG_FILE}_`date '+%s'`

if [ -f  ${CFG_FILE}  ]
then
  sed \
    -e "s|^\( *usergroupSync.policymanager.baseURL *=\).*|\1 ${POLICY_MGR_URL}|" \
    -e "s|^\( *usergroupSync.unix.minUserId *=\).*|\1 ${MIN_UNIX_USER_ID_TO_SYNC}|" \
    -e "s|^\( *usergroupSync.sleepTimeInMillisBetweenSyncCycle *=\).*|\1 ${SYNC_INTERVAL}|" \
    -e "s|^\( *usergroupSync.source.impl.class *=\).*|\1 ${SYNC_SOURCE}|" \
    -e "s|^\( *ldapGroupSync.ldapUrl *=\).*|\1 ${SYNC_LDAP_URL}|" \
    -e "s|^\( *ldapGroupSync.ldapBindDn *=\).*|\1 ${SYNC_LDAP_BIND_DN}|" \
    -e "s|^\( *ldapGroupSync.ldapBindPassword *=\).*|\1 ${SYNC_LDAP_BIND_PASSWORD}|" \
    -e "s|^\( *ldapGroupSync.ldapBindKeystore *=\).*|\1 ${SYNC_LDAP_BIND_KEYSTOREPATH}|" \
    -e "s|^\( *ldapGroupSync.ldapBindAlias *=\).*|\1 ${SYNC_LDAP_BIND_ALIAS}|" \
    -e "s|^\( *ldapGroupSync.userSearchBase *=\).*|\1 ${SYNC_LDAP_USER_SEARCH_BASE}|" \
    -e "s|^\( *ldapGroupSync.userSearchScope *=\).*|\1 ${SYNC_LDAP_USER_SEARCH_SCOPE}|" \
    -e "s|^\( *ldapGroupSync.userObjectClass *=\).*|\1 ${SYNC_LDAP_USER_OBJECT_CLASS}|" \
    -e "s|^\( *ldapGroupSync.userSearchFilter *=\).*-|1 ${SYNC_LDAP_USER_SEARCH_FILTER}|" \
    -e "s|^\( *ldapGroupSync.userNameAttribute *=\).*|\1 ${SYNC_LDAP_USER_NAME_ATTRIBUTE}|" \
    -e "s|^\( *ldapGroupSync.userGroupNameAttribute *=\).*|\1 ${SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE}|" \
    -e "s|^\( *ldapGroupSync.username.caseConversion *=\).*|\1 ${SYNC_LDAP_USERNAME_CASE_CONVERSION}|" \
    -e "s|^\( *ldapGroupSync.groupname.caseConversion *=\).*|\1 ${SYNC_LDAP_GROUPNAME_CASE_CONVERSION}|" \
    ${CFG_FILE} > ${NEW_CFG_FILE}
	mv ${CFG_FILE}  ${CFG_FILE}_archive_`date '+%s'`
	mv ${NEW_CFG_FILE}  ${CFG_FILE}
fi

#
# get current directory name
#

if [ "${cdir}" = "." ]
then
	cdir=`pwd`
fi

cdirname=`basename ${cdir}`

if [ "${cdirname}" != "" ]
then

	dstdir=${INSTALL_BASE}/${cdirname}

	if [ -d ${dstdir} ]
	then
		ctime=`date '+%s'`
        archive_dir=${dstdir}-${ctime}
        mkdir -p ${archive_dir}
        mv ${dstdir} ${archive_dir}		
	fi

	mkdir ${dstdir}
	
	if [ -L ${INSTALL_DIR} ]
    then
        rm -f ${INSTALL_DIR}
    fi

	ln -s  ${dstdir} ${INSTALL_DIR}

	(cd ${cdir} ; find . -print | cpio -pdm ${dstdir}) 
	(cd ${cdir} ; cat start.sh | sed -e "s|[ \t]*JAVA_HOME=| JAVA_HOME=${JAVA_HOME}|" > ${dstdir}/start.sh)

fi

#
# Install the init.d process in /etc/init.d and create appropriate link to /etc/rc2.d folder
#

if [ -d /etc/init.d ]
then
	cp ${cdir}/initd  /etc/init.d/${MOD_NAME}
	chmod +x /etc/init.d/${MOD_NAME}
	
	if [ -d /etc/rc2.d ] 
	then
		echo "Creating boot script S99${MOD_NAME} in rc2.d directory .... "
		ln -sf /etc/init.d/${MOD_NAME}  /etc/rc2.d/S99${MOD_NAME}
		ln -sf /etc/init.d/${MOD_NAME}  /etc/rc2.d/K00${MOD_NAME}
	fi
	if [ -d /etc/rc3.d ]
	then
		echo "Creating boot script S99${MOD_NAME} in rc3.d directory .... "
		ln -sf /etc/init.d/${MOD_NAME}  /etc/rc3.d/S99${MOD_NAME}
		ln -sf /etc/init.d/${MOD_NAME}  /etc/rc3.d/K00${MOD_NAME}
	fi

  # SUSE has rc2.d and rc3.d under /etc/rc.d
	if [ -d /etc/rc.d/rc2.d ] 
	then
		echo "Creating boot script S99${MOD_NAME} in rc2.d directory .... "
		ln -sf /etc/init.d/${MOD_NAME}  /etc/rc.d/rc2.d/S99${MOD_NAME}
		ln -sf /etc/init.d/${MOD_NAME}  /etc/rc.d/rc2.d/K00${MOD_NAME}
	fi
	if [ -d /etc/rc.d/rc3.d ]
	then
		echo "Creating boot script S99${MOD_NAME} in rc3.d directory .... "
		ln -sf /etc/init.d/${MOD_NAME}  /etc/rc.d/rc3.d/S99${MOD_NAME}
		ln -sf /etc/init.d/${MOD_NAME}  /etc/rc.d/rc3.d/K00${MOD_NAME}
	fi

fi

#
# Start the service
#

service ${MOD_NAME} start
