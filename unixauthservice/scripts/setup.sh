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


INSTALL_BASE=$PWD

MOD_NAME="ranger-usersync"
unix_user=ranger
unix_group=ranger

INSTALL_DIR=${INSTALL_BASE}
pidf=/var/run/ranger
curDt=`date '+%Y%m%d%H%M%S'`
LOGFILE=setup.log.$curDt

log() {
   local prefix="[$(date +%Y/%m/%d\ %H:%M:%S)]: "
   echo "${prefix} $@" >> $LOGFILE
   echo "${prefix} $@"
}

mkdir -p ${pidf}
chown -R ${unix_user} ${pidf}

# Ensure that the user is root
MY_ID=`id -u`
if [ "${MY_ID}" -ne 0 ]
then
  echo "ERROR: You must run the installation as root user."
  exit 1
fi

# Ensure JAVA_HOME is set
if [ "${JAVA_HOME}" == "" ]
then
  echo "ERROR: JAVA_HOME environment property not defined, aborting installation"
  exit 2
fi


# Grep configuration properties from install.properties
cdir=`dirname $0`

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

setup_unix_user_group

POLICY_MGR_URL=`grep '^[ \t]*POLICY_MGR_URL[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
MIN_UNIX_USER_ID_TO_SYNC=`grep '^[ \t]*MIN_UNIX_USER_ID_TO_SYNC[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

logdir=`grep '^[ \t]*logdir[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_SOURCE=`grep '^[ \t]*SYNC_SOURCE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_INTERVAL=`grep '^[ \t]*SYNC_INTERVAL[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_URL=`grep '^[ \t]*SYNC_LDAP_URL[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_URL[ \t]*=[ \t]*::'`

SYNC_LDAP_BIND_DN=`grep '^[ \t]*SYNC_LDAP_BIND_DN[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_BIND_DN[ \t]*=[ \t]*::'`

SYNC_LDAP_BIND_PASSWORD=`grep '^[ \t]*SYNC_LDAP_BIND_PASSWORD[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_BIND_PASSWORD[ \t]*=[ \t]*::'`

SYNC_LDAP_SEARCH_BASE=`grep '^[ \t]*SYNC_LDAP_SEARCH_BASE[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_SEARCH_BASE[ \t]*=[ \t]*::'`
echo "$SYNC_LDAP_SEARCH_BASE"

SYNC_LDAP_USER_SEARCH_BASE=`grep '^[ \t]*SYNC_LDAP_USER_SEARCH_BASE[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_USER_SEARCH_BASE[ \t]*=[ \t]*::'`

SYNC_LDAP_USER_SEARCH_SCOPE=`grep '^[ \t]*SYNC_LDAP_USER_SEARCH_SCOPE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_USER_OBJECT_CLASS=`grep '^[ \t]*SYNC_LDAP_USER_OBJECT_CLASS[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_USER_SEARCH_FILTER=`grep '^[ \t]*SYNC_LDAP_USER_SEARCH_FILTER[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_USER_SEARCH_FILTER[ \t]*=[ \t]*::'`

SYNC_LDAP_USER_NAME_ATTRIBUTE=`grep '^[ \t]*SYNC_LDAP_USER_NAME_ATTRIBUTE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE=`grep '^[ \t]*SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_USERNAME_CASE_CONVERSION=`grep '^[ \t]*SYNC_LDAP_USERNAME_CASE_CONVERSION[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_LDAP_GROUPNAME_CASE_CONVERSION=`grep '^[ \t]*SYNC_LDAP_GROUPNAME_CASE_CONVERSION[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_PAGED_RESULTS_ENABLED=`grep '^[ \t]*SYNC_PAGED_RESULTS_ENABLED[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
SYNC_PAGED_RESULTS_SIZE=`grep '^[ \t]*SYNC_PAGED_RESULTS_SIZE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`


SYNC_GROUP_SEARCH_ENABLED=`grep '^[ \t]*SYNC_GROUP_SEARCH_ENABLED[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
SYNC_GROUP_USER_MAP_SYNC_ENABLED=`grep '^[ \t]*SYNC_GROUP_USER_MAP_SYNC_ENABLED[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`

SYNC_GROUP_SEARCH_BASE=`grep '^[ \t]*SYNC_GROUP_SEARCH_BASE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
SYNC_GROUP_SEARCH_SCOPE=`grep '^[ \t]*SYNC_GROUP_SEARCH_SCOPE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
SYNC_GROUP_OBJECT_CLASS=`grep '^[ \t]*SYNC_GROUP_OBJECT_CLASS[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
SYNC_LDAP_GROUP_SEARCH_FILTER=`grep '^[ \t]*SYNC_LDAP_GROUP_SEARCH_FILTER[ \t]*=' ${cdir}/install.properties | sed -e 's:^[ \t]*SYNC_LDAP_GROUP_SEARCH_FILTER[ \t]*=[ \t]*::'`
SYNC_GROUP_NAME_ATTRIBUTE=`grep '^[ \t]*SYNC_GROUP_NAME_ATTRIBUTE[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
SYNC_GROUP_MEMBER_ATTRIBUTE_NAME=`grep '^[ \t]*SYNC_GROUP_MEMBER_ATTRIBUTE_NAME[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`


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
  SYNC_SOURCE="org.apache.ranger.unixusersync.process.UnixUserGroupBuilder"
elif [ "${SYNC_SOURCE}" == "unix" ]
then
  SYNC_SOURCE="org.apache.ranger.unixusersync.process.UnixUserGroupBuilder"
elif [ "${SYNC_SOURCE}" == "ldap" ]
then
  SYNC_SOURCE="org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder"
else
  echo "Unsupported value for SYNC_SOURCE: ${SYNC_SOURCE}, supported values: ldap, unix, default: unix"
  exit 3
fi


if [ "${SYNC_SOURCE}" == "org.apache.ranger.ldapusersync.process.LdapUserGroupBuilder" ]
then

  if [ "${SYNC_INTERVAL}" == "" ]
  then
    SYNC_INTERVAL=$((360*60*1000))
  fi

  if [ "${SYNC_LDAP_URL}" == "" ]
  then
    echo "SYNC_LDAP_URL must be specified when SYNC_SOURCE is ldap"
    exit 4
  fi

  if [ "${SYNC_LDAP_BIND_DN}" == "" ]
  then
    echo "SYNC_LDAP_BIND_DN must be specified when SYNC_SOURCE is ldap"
    exit 5
  fi

  if [ "${SYNC_LDAP_USER_SEARCH_BASE}" == "" ] && [ "${SYNC_LDAP_SEARCH_BASE}" == "" ]
  then
    echo "SYNC_LDAP_USER_SEARCH_BASE or SYNC_LDAP_SEARCH_BASE must be specified when SYNC_SOURCE is ldap"
    exit 6
  fi

  if [ "${SYNC_LDAP_USER_SEARCH_SCOPE}" == "" ]
  then
    SYNC_LDAP_USER_SEARCH_SCOPE="sub"
  fi

  if [ "${SYNC_LDAP_USER_SEARCH_SCOPE}" != "base" ] && [ "${SYNC_LDAP_USER_SEARCH_SCOPE}" != "one" ] && [ "${SYNC_LDAP_USER_SEARCH_SCOPE}" != "sub" ]
  then
    echo "Unsupported value for SYNC_LDAP_USER_SEARCH_SCOPE: ${SYNC_LDAP_USER_SEARCH_SCOPE}, supported values: base, one, sub"
    exit 7
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

  # Store ldap bind password in credential store
  if [[ "${SYNC_LDAP_BIND_ALIAS}" != ""  && "${SYNC_LDAP_BIND_KEYSTOREPATH}" != "" ]]
  then
    echo "Storing ldap bind password in credential store"
	mkdir -p `dirname "${SYNC_LDAP_BIND_KEYSTOREPATH}"`
	chown ${unix_user}:${unix_group} `dirname "${SYNC_LDAP_BIND_KEYSTOREPATH}"`
	$JAVA_HOME/bin/java -cp "./lib/*" org.apache.ranger.credentialapi.buildks create $SYNC_LDAP_BIND_ALIAS -value $SYNC_LDAP_BIND_PASSWORD -provider jceks://file$SYNC_LDAP_BIND_KEYSTOREPATH
    SYNC_LDAP_BIND_PASSWORD="_"
  fi

fi
# END Grep configuration properties from install.properties

# changing ownership for ranger-usersync install directory
if [ -d ${INSTALL_DIR} ]; then
    chown -R ${unix_user}:${unix_group} ${INSTALL_DIR}
fi


# Create $INSTALL_DIR/conf/unixauthservice.properties

if [ ! -d conf ]; then
    #Manual install
    log "[I] Copying conf.dist conf"
    mkdir conf
    cp conf.dist/* conf
    chown ${unix_user}:${unix_group} conf
    chmod 750 conf
fi
if [ ! -f conf/cert/unixauthservice.jks ] 
then
    if [ ! -d conf/cert ]
    then
        mkdir -p conf/cert
    fi
    ${JAVA_HOME}/bin/keytool -genkeypair -keyalg RSA -alias selfsigned -keystore conf/cert/unixauthservice.jks \
                             -keypass UnIx529p -storepass UnIx529p -validity 360 -keysize 2048 \
                             -dname "cn=unixauthservice,ou=authenticator,o=mycompany,c=US" 

	chmod o-rwx conf/cert/unixauthservice.jks
	chgrp ${unix_group} conf/cert/unixauthservice.jks

fi

echo "export JAVA_HOME=${JAVA_HOME}" > conf/java_home.sh
chmod a+rx conf/java_home.sh

if [ ! -d logs ]; then
    #Manual install
    log "[I] Creating logs folder"
    mkdir logs
    chown ${unix_user}:${unix_group} logs
fi


CFG_FILE="${cdir}/conf/unixauthservice.properties"
NEW_CFG_FILE=${cdir}/conf/unixauthservice.properties.tmp

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
	-e "s|^\( *ldapGroupSync.searchBase *=\).*|\1 ${SYNC_LDAP_SEARCH_BASE}|" \
	-e "s|^\( *ldapGroupSync.userSearchScope *=\).*|\1 ${SYNC_LDAP_USER_SEARCH_SCOPE}|" \
	-e "s|^\( *ldapGroupSync.userObjectClass *=\).*|\1 ${SYNC_LDAP_USER_OBJECT_CLASS}|" \
	-e "s%^\( *ldapGroupSync.userSearchFilter *=\).*%\1 ${SYNC_LDAP_USER_SEARCH_FILTER}%" \
	-e "s|^\( *ldapGroupSync.userNameAttribute *=\).*|\1 ${SYNC_LDAP_USER_NAME_ATTRIBUTE}|" \
	-e "s|^\( *ldapGroupSync.userGroupNameAttribute *=\).*|\1 ${SYNC_LDAP_USER_GROUP_NAME_ATTRIBUTE}|" \
	-e "s|^\( *ldapGroupSync.username.caseConversion *=\).*|\1 ${SYNC_LDAP_USERNAME_CASE_CONVERSION}|" \
	-e "s|^\( *ldapGroupSync.groupname.caseConversion *=\).*|\1 ${SYNC_LDAP_GROUPNAME_CASE_CONVERSION}|" \
	-e "s|^\( *logdir *=\).*|\1 ${logdir}|" \
	-e "s|^\( *ldapGroupSync.pagedResultsEnabled *=\).*|\1 ${SYNC_PAGED_RESULTS_ENABLED}|" \
	-e "s|^\( *ldapGroupSync.pagedResultsSize *=\).*|\1 ${SYNC_PAGED_RESULTS_SIZE}|" \
	-e "s|^\( *ldapGroupSync.groupSearchEnabled *=\).*|\1 ${SYNC_GROUP_SEARCH_ENABLED}|" \
	-e "s|^\( *ldapGroupSync.groupUserMapSyncEnabled *=\).*|\1 ${SYNC_GROUP_USER_MAP_SYNC_ENABLED}|" \
	-e "s|^\( *ldapGroupSync.groupSearchBase *=\).*|\1 ${SYNC_GROUP_SEARCH_BASE}|" \
	-e "s|^\( *ldapGroupSync.groupSearchScope *=\).*|\1 ${SYNC_GROUP_SEARCH_SCOPE}|" \
	-e "s|^\( *ldapGroupSync.groupObjectClass *=\).*|\1 ${SYNC_GROUP_OBJECT_CLASS}|" \
	-e "s|^\( *ldapGroupSync.groupSearchFilter *=\).*|\1 ${SYNC_GROUP_SEARCH_FILTER}|" \
	-e "s|^\( *ldapGroupSync.groupNameAttribute *=\).*|\1 ${SYNC_GROUP_NAME_ATTRIBUTE}|" \
	-e "s|^\( *ldapGroupSync.groupMemberAttributeName *=\).*|\1 ${SYNC_GROUP_MEMBER_ATTRIBUTE_NAME}|" \
	${CFG_FILE} > ${NEW_CFG_FILE}

    echo "<${logdir}> ${CFG_FILE} > ${NEW_CFG_FILE}"
else
    echo "ERROR: Required file, not found: ${CFG_FILE}, Aborting installation"
    exit 8
fi

mv ${cdir}/conf/unixauthservice.properties ${cdir}/conf/unixauthservice.properties.${curDt}
mv ${cdir}/conf/unixauthservice.properties.tmp ${cdir}/conf/unixauthservice.properties

#END Create $INSTALL_DIR/conf/unixauthservice.properties

#Update native exe
#ranger-usersync/native/credValidator.uexe
if [ -f ${cdir}/native/credValidator.uexe ]; then
	chmod 750 ${cdir}/native/credValidator.uexe
	chown root ${cdir}/native/credValidator.uexe
	chgrp $unix_group ${cdir}/native/credValidator.uexe
	chmod u+s ${cdir}/native/credValidator.uexe
fi

# Install the init.d process in /etc/init.d and create appropriate link to /etc/rc2.d folder
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

# Create SoftLink of ranger-usersync-services to /usr/bin/
ln -sf ${INSTALL_DIR}/ranger-usersync-services.sh /usr/bin/${MOD_NAME}
chmod ug+rx /usr/bin/${MOD_NAME}

# Start the service
#service ${MOD_NAME} start
