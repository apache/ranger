#!/bin/bash
COMPONENT_NAME=storm
CFG_DIR=/etc/${COMPONENT_NAME}/conf
XASECURE_ROOT=/etc/xasecure/${COMPONENT_NAME}
BACKUP_TYPE=pre
CUR_VERSION_FILE=${XASECURE_ROOT}/.current_version
CUR_CFG_DIR_FILE=${XASECURE_ROOT}/.config_dir
if [ -f ${CUR_VERSION_FILE} ]
then
	XASECURE_VERSION=`cat ${CUR_VERSION_FILE}`
	PRE_INSTALL_CONFIG=${XASECURE_ROOT}/${BACKUP_TYPE}-${XASECURE_VERSION}
	dt=`date '+%Y%m%d%H%M%S'`
	if [ -d "${PRE_INSTALL_CONFIG}" ]
	then
		if [ -f ${CUR_CFG_DIR_FILE} ] 
		then
			CFG_DIR=`cat ${CUR_CFG_DIR_FILE}`
		fi 
		[ -d ${CFG_DIR} ] && mv ${CFG_DIR} ${CFG_DIR}-${dt}
		( cd ${PRE_INSTALL_CONFIG} ; find . -print | cpio -pdm ${CFG_DIR} )
		[ -f ${CUR_VERSION_FILE} ] && mv ${CUR_VERSION_FILE} ${CUR_VERSION_FILE}-uninstalled-${dt}
		echo "XASecure version - ${XASECURE_VERSION} has been uninstalled successfully."
	else
		echo "ERROR: Unable to find pre-install configuration directory: [${PRE_INSTALL_CONFIG}]"
		exit 1
	fi
else
	cd ${CFG_DIR}
	saved_files=`find . -type f -name '.*' |  sort | grep -v -- '-new.' | grep '[0-9]*$' | grep -v -- '-[0-9]*$' | sed -e 's:\.[0-9]*$::' | sed -e 's:^./::' | sort -u`
	dt=`date '+%Y%m%d%H%M%S'`
	if [ "${saved_files}" != "" ]
	then
	        for f in ${saved_files}
	        do
	                oldf=`ls ${f}.[0-9]* | sort | head -1`
	                if [ -f "${oldf}" ]
	                then
	                        nf=`echo ${f} | sed -e 's:^\.::'`
	                        if [ -f "${nf}" ]
	                        then
	                                echo "+cp -p ${nf} .${nf}-${dt}"
	                                cp -p ${nf} .${nf}-${dt}
	                                echo "+cp ${oldf} ${nf}"
	                                cp ${oldf} ${nf}
	                        else
	                                echo "ERROR: ${nf} not found to save. However, old file is being recovered."
	                                echo "+cp -p ${oldf} ${nf}"
	                                cp -p ${oldf} ${nf}
	                        fi
	                fi
	        done
	        echo "XASecure configuration has been uninstalled successfully."
	fi
fi
