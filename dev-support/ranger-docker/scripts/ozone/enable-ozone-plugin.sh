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

function getInstallProperty() {
    local propertyName=$1
    local propertyValue=""

    for file in "${COMPONENT_INSTALL_ARGS}" "${INSTALL_ARGS}"
    do
        if [ -f "${file}" ]
        then
            propertyValue=`grep "^${propertyName}[ \t]*=" ${file} | awk -F= '{  sub("^[ \t]*", "", $2); sub("[ \t]*$", "", $2); print $2 }'`
            if [ "${propertyValue}" != "" ]
            then
                break
            fi
        fi
    done

    echo ${propertyValue}
}

#
# Base env variable for Ranger related files/directories
#
PROJ_NAME=ranger

#
# The script should be run by "root" user
#

if [ ! -w /etc/passwd ]
then
    echo "ERROR: $0 script should be run as root."
    exit 1
fi

#Check for JAVA_HOME
if [ "${JAVA_HOME}" == "" ]
then
    echo "ERROR: JAVA_HOME environment property not defined, aborting installation."
    exit 1
fi

#
# Identify the component, action from the script file
#

basedir=`dirname $0`
if [ "${basedir}" = "." ]
then
    basedir=`pwd`
elif [ "${basedir}" = ".." ]
then
    basedir=`(cd .. ;pwd)`
fi

#
# As this script is common to all component, find the component name based on the script-name
#

COMPONENT_NAME=`basename $0 | cut -d. -f1 | sed -e 's:^disable-::' | sed -e 's:^enable-::'`

echo "${COMPONENT_NAME}" | grep 'plugin' > /dev/null 2>&1

if [ $? -ne 0 ]
then
	echo "$0 : is not applicable for component [${COMPONENT_NAME}]. It is applicable only for ranger plugin component; Exiting ..."
	exit 0
fi

HCOMPONENT_NAME=`echo ${COMPONENT_NAME} | sed -e 's:-plugin::'`

CFG_OWNER_INF="${HCOMPONENT_NAME}:${HCOMPONENT_NAME}"

if [ "${HCOMPONENT_NAME}" = "hdfs" ]
then
	HCOMPONENT_NAME="hadoop"
fi

#
# Based on script name, identify if the action is enabled or disabled
#

basename $0 | cut -d. -f1 | grep '^enable-' > /dev/null 2>&1

if [ $? -eq 0 ]
then
	action=enable
else
	action=disable
fi


#
# environment variables for enable|disable scripts
#

PROJ_INSTALL_DIR=`(cd ${basedir} ; pwd)`
SET_ENV_SCRIPT_NAME=set-${COMPONENT_NAME}-env.sh
SET_ENV_SCRIPT_TEMPLATE=${PROJ_INSTALL_DIR}/install/conf.templates/enable/${SET_ENV_SCRIPT_NAME}
DEFAULT_XML_CONFIG=${PROJ_INSTALL_DIR}/install/conf.templates/default/configuration.xml
PROJ_LIB_DIR=${PROJ_INSTALL_DIR}/lib
PROJ_INSTALL_LIB_DIR="${PROJ_INSTALL_DIR}/install/lib"
INSTALL_ARGS="${PROJ_INSTALL_DIR}/install.properties"
COMPONENT_INSTALL_ARGS="${PROJ_INSTALL_DIR}/${COMPONENT_NAME}-install.properties"
JAVA=$JAVA_HOME/bin/java

PLUGIN_DEPENDENT_LIB_DIR=lib/"${PROJ_NAME}-${COMPONENT_NAME}-impl"
PROJ_LIB_PLUGIN_DIR=${PROJ_INSTALL_DIR}/${PLUGIN_DEPENDENT_LIB_DIR}

HCOMPONENT_INSTALL_DIR_NAME=$(getInstallProperty 'COMPONENT_INSTALL_DIR_NAME')

CUSTOM_USER=$(getInstallProperty 'CUSTOM_USER')
CUSTOM_USER=${CUSTOM_USER// }

CUSTOM_GROUP=$(getInstallProperty 'CUSTOM_GROUP')
CUSTOM_GROUP=${CUSTOM_GROUP// }

CUSTOM_GROUP_STATUS=${CUSTOM_GROUP};
CUSTOM_USER_STATUS=${CUSTOM_USER};
egrep "^$CUSTOM_GROUP" /etc/group >& /dev/null
if [ $? -ne 0 ]
then
	CUSTOM_GROUP_STATUS=""
fi
id -u ${CUSTOM_USER} > /dev/null 2>&1
if [ $? -ne 0 ]
then
	CUSTOM_USER_STATUS=""
fi

if [ ! -z "${CUSTOM_USER_STATUS}" ] && [ ! -z "${CUSTOM_GROUP_STATUS}" ]
then
  echo "Custom user and group is available, using custom user and group."
  CFG_OWNER_INF="${CUSTOM_USER}:${CUSTOM_GROUP}"
elif [ ! -z "${CUSTOM_USER_STATUS}" ] && [ -z "${CUSTOM_GROUP_STATUS}" ]
then
  echo "Custom user is available, using custom user and default group."
  CFG_OWNER_INF="${CUSTOM_USER}:${HCOMPONENT_NAME}"
elif [ -z  "${CUSTOM_USER_STATUS}" ] && [ ! -z  "${CUSTOM_GROUP_STATUS}" ]
then
  echo "Custom group is available, using default user and custom group."
  CFG_OWNER_INF="${HCOMPONENT_NAME}:${CUSTOM_GROUP}"
else
  echo "Custom user and group are not available, using default user and group."
  CFG_OWNER_INF="${HCOMPONENT_NAME}:${HCOMPONENT_NAME}"
fi

if [ "${HCOMPONENT_INSTALL_DIR_NAME}" = "" ]
then
  HCOMPONENT_INSTALL_DIR_NAME=${HCOMPONENT_NAME}
fi

firstletter=${HCOMPONENT_INSTALL_DIR_NAME:0:1}
if [ "$firstletter" = "/" ]; then
    hdir=${HCOMPONENT_INSTALL_DIR_NAME}
else
    hdir=${PROJ_INSTALL_DIR}/../${HCOMPONENT_INSTALL_DIR_NAME}
fi

#
# TEST - START
#
if [ ! -d ${hdir} ]
then
	mkdir -p ${hdir}
fi
#
# TEST - END
#
HCOMPONENT_INSTALL_DIR=`(cd ${hdir} ; pwd)`
HCOMPONENT_LIB_DIR=${HCOMPONENT_INSTALL_DIR}/lib
if [ "${HCOMPONENT_NAME}" = "hadoop" ] ||
     [ "${HCOMPONENT_NAME}" = "yarn" ]; then
    HCOMPONENT_LIB_DIR=${HCOMPONENT_INSTALL_DIR}/share/hadoop/hdfs/lib
fi

HCOMPONENT_CONF_DIR=${HCOMPONENT_INSTALL_DIR}/conf
HCOMPONENT_ARCHIVE_CONF_DIR=${HCOMPONENT_CONF_DIR}/.archive
SET_ENV_SCRIPT=${HCOMPONENT_CONF_DIR}/${SET_ENV_SCRIPT_NAME}


if [ ! -d "${HCOMPONENT_INSTALL_DIR}" ]
then
	echo "ERROR: Unable to find the install directory of component [${HCOMPONENT_NAME}]; dir [${HCOMPONENT_INSTALL_DIR}] not found."
	echo "Exiting installation."
	exit 1
fi

if [ ! -d "${HCOMPONENT_CONF_DIR}" ]
then
	echo "ERROR: Unable to find the conf directory of component [${HCOMPONENT_NAME}]; dir [${HCOMPONENT_CONF_DIR}] not found."
	echo "Exiting installation."
	exit 1
fi

if [ ! -d "${HCOMPONENT_LIB_DIR}" ]
then
    mkdir -p "${HCOMPONENT_LIB_DIR}"
    if [ ! -d "${HCOMPONENT_LIB_DIR}" ]
    then
        echo "ERROR: Unable to find the lib directory of component [${HCOMPONENT_NAME}];  dir [${HCOMPONENT_LIB_DIR}] not found."
        echo "Exiting installation."
        exit 1
    fi
fi

#
# Common functions used by all enable/disable scripts
#

log() {
	echo "+ `date` : $*"
}


create_jceks() {

	alias=$1
	pass=$2
	jceksFile=$3

	if [ -f "${jceksFile}" ]
	then
		jcebdir=`dirname ${jceksFile}`
		jcebname=`basename ${jceksFile}`
		archive_jce=${jcebdir}/.${jcebname}.`date '+%Y%m%d%H%M%S'`
		log "Saving current JCE file: ${jceksFile} to ${archive_jce} ..."
		cp ${jceksFile} ${archive_jce}
	fi

	tempFile=/tmp/jce.$$.out

  $JAVA_HOME/bin/java -cp ":${PROJ_INSTALL_LIB_DIR}/*:" org.apache.ranger.credentialapi.buildks create "${alias}" -value "${pass}" -provider "jceks://file${jceksFile}" > ${tempFile} 2>&1

	if [ $? -ne 0 ]
	then
		echo "Unable to store password in non-plain text format. Error: [`cat ${tempFile}`]"
		echo "Exiting plugin installation"
		rm -f ${tempFile}
		exit 0
	fi

	rm -f ${tempFile}
}

log "${HCOMPONENT_NAME}: lib folder=$HCOMPONENT_LIB_DIR conf folder=$HCOMPONENT_CONF_DIR"

#
# If there is a set-ranger-${COMPONENT}-env.sh, install it
#
dt=`date '+%Y%m%d-%H%M%S'`

if [ -f "${SET_ENV_SCRIPT_TEMPLATE}" ]
then
	#
	# If the setenv script already exists, move it to the archive folder
	#
	if [ -f "${SET_ENV_SCRIPT}" ]
	then
		if [ ! -d "${HCOMPONENT_ARCHIVE_CONF_DIR}" ]
		then
			mkdir -p ${HCOMPONENT_ARCHIVE_CONF_DIR}
		fi
		log "Saving current ${SET_ENV_SCRIPT_NAME} to ${HCOMPONENT_ARCHIVE_CONF_DIR} ..."
		mv ${SET_ENV_SCRIPT} ${HCOMPONENT_ARCHIVE_CONF_DIR}/${SET_ENV_SCRIPT_NAME}.${dt}
	fi

	if [ "${action}" = "enable" ]
	then

		cp ${SET_ENV_SCRIPT_TEMPLATE} ${SET_ENV_SCRIPT}

		DEST_SCRIPT_FILE=${HCOMPONENT_INSTALL_DIR}/libexec/${HCOMPONENT_NAME}-config.sh

		DEST_SCRIPT_ARCHIVE_FILE=${HCOMPONENT_INSTALL_DIR}/libexec/.${HCOMPONENT_NAME}-config.sh.${dt}

		if [ -f "${DEST_SCRIPT_FILE}" ]
		then

			log "Saving current ${DEST_SCRIPT_FILE} to ${DEST_SCRIPT_ARCHIVE_FILE} ..."

			cp ${DEST_SCRIPT_FILE} ${DEST_SCRIPT_ARCHIVE_FILE}

			grep 'xasecure-.*-env.sh' ${DEST_SCRIPT_FILE} > /dev/null 2>&1
			if [ $? -eq 0 ]
			then
				ts=`date '+%Y%m%d%H%M%S'`
				grep -v 'xasecure-.*-env.sh' ${DEST_SCRIPT_FILE} > ${DEST_SCRIPT_FILE}.${ts}
				if [ $? -eq 0 ]
				then
					log "Removing old reference to xasecure setenv source ..."
					cat ${DEST_SCRIPT_FILE}.${ts} > ${DEST_SCRIPT_FILE}
					rm -f ${DEST_SCRIPT_FILE}.${ts}
				fi
			fi

			grep "[ \t]*.[ \t]*${SET_ENV_SCRIPT}" ${DEST_SCRIPT_FILE} > /dev/null
			if [ $? -ne 0 ]
			then
				log "Appending sourcing script, ${SET_ENV_SCRIPT_NAME} in the file: ${DEST_SCRIPT_FILE} "
				cat >> ${DEST_SCRIPT_FILE} <<!
if [ -f ${SET_ENV_SCRIPT} ]
then
	.  ${SET_ENV_SCRIPT}
fi
!
			else
				log "INFO: ${DEST_SCRIPT_FILE} is being sourced from file: ${HCOMPONENT_CONF_DIR}/${HCOMPONENT_NAME}-env.sh "
			fi
		fi
	fi
fi

#
# Run, the enable|disable ${COMPONENT} configurations
#

if [ -d "${PROJ_INSTALL_DIR}/install/conf.templates/${action}" ]
then
	INSTALL_CP="${PROJ_INSTALL_LIB_DIR}/*"
	if [ "${action}" = "enable" ]
	then
		echo "<ranger>\n<enabled>`date`</enabled>\n</ranger>" > ${HCOMPONENT_CONF_DIR}/ranger-security.xml
		chown ${CFG_OWNER_INF} ${HCOMPONENT_CONF_DIR}/ranger-security.xml
		chmod a+r ${HCOMPONENT_CONF_DIR}/ranger-security.xml
		for cf in ${PROJ_INSTALL_DIR}/install/conf.templates/${action}/*.xml
		do
			cfb=`basename ${cf}`
			if [ -f "${HCOMPONENT_CONF_DIR}/${cfb}" ]
			then
				log "Saving ${HCOMPONENT_CONF_DIR}/${cfb} to ${HCOMPONENT_CONF_DIR}/.${cfb}.${dt} ..."
				cp ${HCOMPONENT_CONF_DIR}/${cfb} ${HCOMPONENT_CONF_DIR}/.${cfb}.${dt}
			fi
			cp ${cf} ${HCOMPONENT_CONF_DIR}/
			chown ${CFG_OWNER_INF} ${HCOMPONENT_CONF_DIR}/${cfb}
			chmod a+r ${HCOMPONENT_CONF_DIR}/${cfb}
		done
    else
		if [ -f ${HCOMPONENT_CONF_DIR}/ranger-security.xml ]
		then
			mv ${HCOMPONENT_CONF_DIR}/ranger-security.xml ${HCOMPONENT_CONF_DIR}/.ranger-security.xml.`date '+%Y%m%d%H%M%S'`
		fi
	fi

	#
	# Ensure that POLICY_CACHE_FILE_PATH is accessible
	#
	REPO_NAME=$(getInstallProperty 'REPOSITORY_NAME')
	export POLICY_CACHE_FILE_PATH=/etc/${PROJ_NAME}/${REPO_NAME}/policycache
	export CREDENTIAL_PROVIDER_FILE=/etc/${PROJ_NAME}/${REPO_NAME}/cred.jceks
	if [ ! -d ${POLICY_CACHE_FILE_PATH} ]
	then
		mkdir -p ${POLICY_CACHE_FILE_PATH}
	fi
	chmod a+rx /etc/${PROJ_NAME}
	chmod a+rx /etc/${PROJ_NAME}/${REPO_NAME}
	chmod a+rx ${POLICY_CACHE_FILE_PATH}
	chown -R ${CFG_OWNER_INF} /etc/${PROJ_NAME}/${REPO_NAME}

	for f in ${PROJ_INSTALL_DIR}/install/conf.templates/${action}/*.cfg
	do
		if [ -f "${f}" ]
		then
			fn=`basename $f`
        	orgfn=`echo $fn | sed -e 's:-changes.cfg:.xml:'`
        	fullpathorgfn="${HCOMPONENT_CONF_DIR}/${orgfn}"
        	if [ ! -f ${fullpathorgfn} ]
        	then
				if [ -f ${DEFAULT_XML_CONFIG} ]
				then
					log "Creating default file from [${DEFAULT_XML_CONFIG}] for [${fullpathorgfn}] .."
					cp ${DEFAULT_XML_CONFIG} ${fullpathorgfn}
				 	chown ${CFG_OWNER_INF} ${fullpathorgfn}
					chmod a+r ${fullpathorgfn}
				else
        			echo "ERROR: Unable to find ${fullpathorgfn}"
        			exit 1
				fi
        	fi
			archivefn="${HCOMPONENT_CONF_DIR}/.${orgfn}.${dt}"
        	newfn="${HCOMPONENT_CONF_DIR}/.${orgfn}-new.${dt}"
			log "Saving current config file: ${fullpathorgfn} to ${archivefn} ..."
            cp ${fullpathorgfn} ${archivefn}
			if [ $? -eq 0 ]
			then
				${JAVA} -cp "${INSTALL_CP}" org.apache.ranger.utils.install.XmlConfigChanger -i ${archivefn} -o ${newfn} -c ${f} -p  ${INSTALL_ARGS}
				if [ $? -eq 0 ]
                then
                	diff -w ${newfn} ${fullpathorgfn} > /dev/null 2>&1
                    if [ $? -ne 0 ]
                    then
                    	cat ${newfn} > ${fullpathorgfn}
                    fi

               	else
				    echo "ERROR: Unable to make changes to config. file: ${fullpathorgfn}"
                    echo "exiting ...."
                    exit 1
				fi
			else
				echo "ERROR: Unable to save config. file: ${fullpathorgfn}  to ${archivefn}"
                echo "exiting ...."
                exit 1
			fi
		fi
	done
fi

#
# Create library link
#
if [ "${action}" = "enable" ]
then
	dt=`date '+%Y%m%d%H%M%S'`
	#
	# Encrypt the password and keep it secure in Credential Provider API
	#
	CredFile=${CREDENTIAL_PROVIDER_FILE}
	if ! [ `echo ${CredFile} | grep '^/.*'` ]
	then
		echo "ERROR:Please enter the Credential File Store with proper file path"
		exit 1
	fi

	pardir=`dirname ${CredFile}`

	if [ ! -d "${pardir}" ]
	then
		mkdir -p "${pardir}"
		if [ $? -ne 0 ]
		then
    		echo "ERROR: Unable to create credential store file path"
			exit 1
		fi
		chmod a+rx "${pardir}"
	fi
fi


#Check Properties whether in File, return code 1 if not exist
#$1 -> propertyName; $2 -> fileName
checkPropertyInFile(){
	validate=$(sed '/^\#/d' $2 | grep "^$1"  | tail -n 1 | cut -d "=" -f1-) # for validation
	if test -z "$validate" ; then return 1; fi
}

#Add Properties to File
#$1 -> propertyName; $2 -> newPropertyValue; $3 -> fileName
addPropertyToFile(){
	echo "$1=$2">>$3
	validate=$(sed '/^\#/d' $3 | grep "^$1"  | tail -n 1 | cut -d "=" -f2-) # for validation
	if test -z "$validate" ; then log "[E] Failed to add properties '$1' to $3 file!"; exit 1; fi
	echo "Property $1 added successfully with : '$2'"
}

#Update Properties to File
#$1 -> propertyName; $2 -> newPropertyValue; $3 -> fileName
updatePropertyToFile(){
	sed -i 's@^'$1'=[^ ]*$@'$1'='$2'@g' $3
	validate=$(sed '/^\#/d' $3 | grep "^$1"  | tail -n 1 | cut -d "=" -f2-) # for validation
	if test -z "$validate" ; then log "[E] '$1' not found in $3 file while Updating....!!"; exit 1; fi
	echo "Property $1 updated successfully with : '$2'"
}

#Add or Update Properties to File
#$1 -> propertyName; $2 -> newPropertyValue; $3 -> fileName
addOrUpdatePropertyToFile(){
	checkPropertyInFile $1 $3
	if [ $? -eq 1 ]
	then
		addPropertyToFile $1 $2 $3
	else
		updatePropertyToFile $1 $2 $3
	fi
}


# Set notice to restart the ${HCOMPONENT_NAME}
echo "Ranger Plugin for ${HCOMPONENT_NAME} has been ${action}d. Please restart ${HCOMPONENT_NAME} to ensure that changes are effective."

exit 0
