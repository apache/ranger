#!/bin/bash

create_jceks()
{
	alias=$1
	pass=$2
	jceksFile=$3
	
	java -cp "${install_dir}/cred/lib/*:${install_dir}/installer/lib/*" com.hortonworks.credentialapi.buildks create ${alias} -value ${pass} -provider jceks://file${jceksFile}
	if [ $? -ne 0 ]
	then
		echo "ERROR: Unable to create/update credential file [${jceksFile}] for alias [${alias}]"
		exit 1
	fi
}

storm_dir=/usr/lib/storm
storm_lib_dir=${storm_dir}/lib
storm_conf_dir=/etc/storm/conf
storm_bin_dir=${storm_dir}/bin

CONFIG_FILE_OWNER=storm:storm

storm_srv_conf_dir=${storm_conf_dir}
storm_cli_conf_dir="${storm_conf_dir}"

install_dir=`dirname $0`

[ "${install_dir}" = "." ] && install_dir=`pwd`

#echo "Current Install Directory: [${install_dir}]"


#
# --- Backup current configuration for backup - START
#

COMPONENT_NAME=storm

XASECURE_VERSION=`cat ${install_dir}/version`

CFG_DIR=${storm_conf_dir}
XASECURE_ROOT=/etc/xasecure/${COMPONENT_NAME}
BACKUP_TYPE=pre
CUR_VERSION_FILE=${XASECURE_ROOT}/.current_version
CUR_CFG_DIR_FILE=${XASECURE_ROOT}/.config_dir
PRE_INSTALL_CONFIG=${XASECURE_ROOT}/${BACKUP_TYPE}-${XASECURE_VERSION}

if [ -d ${XASECURE_ROOT} ]
then
	mkdir -p ${XASECURE_ROOT}
fi

backup_dt=`date '+%Y%m%d%H%M%S'`

if [ -d "${PRE_INSTALL_CONFIG}" ]
then
	PRE_INSTALL_CONFIG="${PRE_INSTALL_CONFIG}.${backup_dt}"
fi

if [ -d ${CFG_DIR} ]
then
	( cd ${CFG_DIR} ; find . -print | cpio -pdm ${PRE_INSTALL_CONFIG} )
	[ -f ${CUR_VERSION_FILE} ] && mv ${CUR_VERSION_FILE} ${CUR_VERSION_FILE}-${backup_dt}
	echo ${XASECURE_VERSION} > ${CUR_VERSION_FILE}
	echo ${CFG_DIR} > ${CUR_CFG_DIR_FILE}
else
	echo "+ mkdir -p ${CFG_DIR} ..."
	mkdir -p ${CFG_DIR}
fi

cp -f ${install_dir}/uninstall.sh ${XASECURE_ROOT}/

#
# --- Backup current configuration for backup  - END
#


dt=`date '+%Y%m%d%H%M%S'`
for f in ${install_dir}/conf/*
do
	if [ -f ${f} ]
	then
		fn=`basename $f`
		if [ ! -f ${storm_conf_dir}/${fn} ]
		then
			echo "+cp ${f} ${storm_conf_dir}/${fn}"
			cp ${f} ${storm_conf_dir}/${fn}
		else
			echo "WARN: ${fn} already exists in the ${storm_conf_dir} - Using existing configuration ${fn}"
		fi
	fi
done


if [ ! -d ${storm_lib_dir} ]
then
	echo "+mkdir -p ${storm_lib_dir}"
	mkdir -p ${storm_lib_dir}
fi

for f in ${install_dir}/dist/*.jar ${install_dir}/lib/*.jar
do
	if [ -f ${f} ]
	then
		fn=`basename $f`
		echo "+cp ${f} ${storm_lib_dir}/${fn}"
		cp ${f} ${storm_lib_dir}/${fn}
	fi
done

#
# Copy the SSL parameters
#

CredFile=`grep '^CREDENTIAL_PROVIDER_FILE' ${install_dir}/install.properties | awk -F= '{ print $2 }'`

if ! [ `echo ${CredFile} | grep '^/.*'` ]
then
  echo "ERROR:Please enter the Credential File Store with proper file path"
  exit 1
fi
pardir=`dirname ${CredFile}`

if [ ! -d ${pardir} ]
then
        mkdir -p ${pardir}
        chmod go+rx ${pardir}
fi

#
# Generate Credential Provider file and Credential for Audit DB access.
#


auditCredAlias="auditDBCred"

auditdbCred=`grep '^XAAUDIT.DB.PASSWORD' ${install_dir}/install.properties | awk -F= '{ print $2 }'`

create_jceks ${auditCredAlias} ${auditdbCred} ${CredFile}


#
# Generate Credential Provider file and Credential for SSL KEYSTORE AND TRUSTSTORE
#


sslkeystoreAlias="sslKeyStore"

sslkeystoreCred=`grep '^SSL_KEYSTORE_PASSWORD' ${install_dir}/install.properties | awk -F= '{ print $2 }'`

create_jceks ${sslkeystoreAlias} ${sslkeystoreCred} ${CredFile}


ssltruststoreAlias="sslTrustStore"

ssltruststoreCred=`grep '^SSL_TRUSTSTORE_PASSWORD' ${install_dir}/install.properties | awk -F= '{ print $2 }'`

create_jceks ${ssltruststoreAlias} ${ssltruststoreCred} ${CredFile}

chown ${CONFIG_FILE_OWNER} ${CredFile} 

PROP_ARGS="-p  ${install_dir}/install.properties"

for f in ${install_dir}/installer/conf/*-changes.cfg
do
        if [ -f ${f} ]
        then
                fn=`basename $f`
                orgfn=`echo $fn | sed -e 's:-changes.cfg:.xml:'`
                fullpathorgfn="${storm_conf_dir}/${orgfn}"
                if [ ! -f ${fullpathorgfn} ]
                then
                        echo "ERROR: Unable to find ${fullpathorgfn}"
                        exit 1
                fi
                archivefn="${storm_conf_dir}/.${orgfn}.${dt}"
                newfn="${storm_conf_dir}/.${orgfn}-new.${dt}"
                cp ${fullpathorgfn} ${archivefn}
                if [ $? -eq 0 ]
                then
                	cp="${install_dir}/installer/lib/*:${install_dir}/cred/lib/*:"
                        java -cp "${cp}" com.xasecure.utils.install.XmlConfigChanger -i ${archivefn} -o ${newfn} -c ${f} ${PROP_ARGS}
                        if [ $? -eq 0 ]
                        then
                                diff -w ${newfn} ${fullpathorgfn} > /dev/null 2>&1 
                                if [ $? -ne 0 ]
                                then
	                        		#echo "Changing config file:  ${fullpathorgfn} with following changes:"
	                                #echo "==============================================================="
	                                #diff -w ${newfn} ${fullpathorgfn}
	                                #echo "==============================================================="
	                                echo "NOTE: Current config file: ${fullpathorgfn} is being saved as ${archivefn}"
	                                #echo "==============================================================="
	                                cp ${newfn} ${fullpathorgfn}
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

chmod go-rwx ${storm_conf_dir}/xasecure-policymgr-ssl.xml
chown ${CONFIG_FILE_OWNER} ${storm_conf_dir}/xasecure-policymgr-ssl.xml

exit 0
