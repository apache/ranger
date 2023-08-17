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

    for file in "${INSTALL_ARGS}"
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

if [[ -z $1 ]]; then
        echo "Invalid argument [$1];"
        echo "Usage: Only start | stop | restart | version, are supported."
        exit;
fi
action=$1
action=`echo $action | tr '[:lower:]' '[:upper:]'`
realScriptPath=`readlink -f $0`
realScriptDir=`dirname $realScriptPath`
cd $realScriptDir
cdir=`pwd`
ranger_usersync_max_heap_size=1g

for custom_env_script in `find ${cdir}/conf/ -name "ranger-usersync-env*"`; do
        if [ -f $custom_env_script ]; then
                . $custom_env_script
        fi
done
if [ -z "${USERSYNC_PID_DIR_PATH}" ]; then
        USERSYNC_PID_DIR_PATH=/var/run/ranger
fi
if [ -z "${USERSYNC_PID_NAME}" ]
then
        USERSYNC_PID_NAME=usersync.pid
fi
if [ ! -d "${USERSYNC_PID_DIR_PATH}" ]
then  
	mkdir -p  $USERSYNC_PID_DIR_PATH
	chmod 660 $USERSYNC_PID_DIR_PATH
fi

# User can set their own pid path using USERSYNC_PID_DIR_PATH and
# USERSYNC_PID_NAME variable before calling the script. The user can modify
# the value of the USERSYNC_PID_DIR_PATH in ranger-usersync-env-piddir.sh to
# change pid path and set the value of USERSYNC_PID_NAME to change the
# pid file.
pidf=${USERSYNC_PID_DIR_PATH}/${USERSYNC_PID_NAME}

if [ -z "${UNIX_USERSYNC_USER}" ]; then
        UNIX_USERSYNC_USER=ranger
fi

INSTALL_ARGS="${cdir}/install.properties"
RANGER_BASE_DIR=$(getInstallProperty 'ranger_base_dir')

JAVA_OPTS=" ${JAVA_OPTS} -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx${ranger_usersync_max_heap_size} -Xms1g "

if [ "${action}" == "START" ]; then

	#Export JAVA_HOME
	if [ -f ${cdir}/conf/java_home.sh ]; then
		. ${cdir}/conf/java_home.sh
	fi

	if [ "$JAVA_HOME" != "" ]; then
        	export PATH=$JAVA_HOME/bin:$PATH
	fi

	cp="${cdir}/dist/*:${cdir}/lib/*:${cdir}/conf:${RANGER_USERSYNC_HADOOP_CONF_DIR}/*"

	cd ${cdir}
	
	if [ -z "${logdir}" ]; then 
	    logdir=${cdir}/logs
	fi
	if [ -z "${USERSYNC_CONF_DIR}" ]; then
	    USERSYNC_CONF_DIR=${cdir}/conf
	fi
	if [ -f "$pidf" ] ; then
		pid=`cat $pidf`
		if  ps -p $pid > /dev/null
		then
			echo "Apache Ranger Usersync Service is already running [pid={$pid}]"
			exit ;
		else
			rm -rf $pidf
        fi
    fi
	SLEEP_TIME_AFTER_START=5
	nohup java -Dproc_rangerusersync -Dlogback.configurationFile=file:${USERSYNC_CONF_DIR}/logback.xml ${JAVA_OPTS} -Duser=${USER} -Dhostname=${HOSTNAME} -Dlogdir="${logdir}" -cp "${cp}" org.apache.ranger.authentication.UnixAuthenticationService -enableUnixAuth > ${logdir}/auth.log 2>&1 &
	VALUE_OF_PID=$!
    echo "Starting Apache Ranger Usersync Service"
    sleep $SLEEP_TIME_AFTER_START
    if ps -p $VALUE_OF_PID > /dev/null
    then
		echo $VALUE_OF_PID > ${pidf}
                chown ${UNIX_USERSYNC_USER} ${pidf}
		chmod 660 ${pidf}
		pid=`cat $pidf`
		echo "Apache Ranger Usersync Service with pid ${pid} has started."
	else
		echo "Apache Ranger Usersync Service failed to start!"
	fi
	exit;

elif [ "${action}" == "STOP" ]; then
	WAIT_TIME_FOR_SHUTDOWN=2
	NR_ITER_FOR_SHUTDOWN_CHECK=15
	if [ -f "$pidf" ] ; then
		pid=`cat $pidf` > /dev/null 2>&1
		echo "Getting pid from $pidf .."
	else
		pid=`ps -ef | grep java | grep -- '-Dproc_rangerusersync' | grep -v grep | awk '{ print $2 }'`
		if [ "$pid" != "" ];then
			echo "pid file($pidf) not present, taking pid from \'ps\' command.."
		else
			echo "Apache Ranger Usersync Service is not running"
			return	
		fi
	fi
	echo "Found Apache Ranger Usersync Service with pid $pid, Stopping it..."
	kill -15 $pid
	for ((i=0; i<$NR_ITER_FOR_SHUTDOWN_CHECK; i++))
	do
		sleep $WAIT_TIME_FOR_SHUTDOWN
		if ps -p $pid > /dev/null ; then
			echo "Shutdown in progress. Will check after $WAIT_TIME_FOR_SHUTDOWN secs again.."
			continue;
		else
			break;
		fi
	done
	# if process is still around, use kill -9
	if ps -p $pid > /dev/null ; then
		echo "Initial kill failed, getting serious now..."
		kill -9 $pid
	fi
	sleep 1 #give kill -9  sometime to "kill"
	if ps -p $pid > /dev/null ; then
		echo "Wow, even kill -9 failed, giving up! Sorry.."
		exit 1

	else
		rm -rf $pidf
		echo "Apache Ranger Usersync Service with pid ${pid} has been stopped."
	fi
	exit;
	
elif [ "${action}" == "RESTART" ]; then
	echo "Restarting Apache Ranger Usersync"
	${cdir}/ranger-usersync-services.sh stop
	${cdir}/ranger-usersync-services.sh start
	exit;
elif [ "${action}" == "VERSION" ]; then
	cd ${cdir}/lib
	java -cp ranger-util-*.jar org.apache.ranger.common.RangerVersionInfo
	exit
else 
	echo "Invalid argument [$1];"
	echo "Usage: Only start | stop | restart | version, are supported."
	exit;
fi
