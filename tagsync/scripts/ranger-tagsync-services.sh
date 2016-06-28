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

if [[ -z $1 ]]; then
        echo "No argument provided.."
        echo "Usage: $0 {start | stop | restart | version}"
        exit;
fi
action=$1
action=`echo $action | tr '[:lower:]' '[:upper:]'`
realScriptPath=`readlink -f $0`
realScriptDir=`dirname $realScriptPath`
cd $realScriptDir
cdir=`pwd`

pidd=/var/run/ranger

if [ -d $pidd ]; then
	mkdir -p $pidd
fi

pidf=${pidd}/tagsync.pid

if [ "${action}" == "START" ]; then

	#Export JAVA_HOME
	if [ -f ${cdir}/conf/java_home.sh ]; then
		. ${cdir}/conf/java_home.sh
	fi

	for custom_env_script in `find ${cdir}/conf.dist/ -name "ranger-tagsync-env*"`; do
        	if [ -f $custom_env_script ]; then
                	. $custom_env_script
	        fi
	done

	if [ "$JAVA_HOME" != "" ]; then
        	export PATH=$JAVA_HOME/bin:$PATH
	fi

    logdir=/var/log/ranger/tagsync

	if [ ! -d $logdir ]; then
		mkdir -p $logdir
		chmod 777 $logdir
	fi

	cp="${cdir}/conf:${cdir}/dist/*:${cdir}/lib/*:${RANGER_TAGSYNC_HADOOP_CONF_DIR}/*"

	if [ -f "$pidf" ] ; then
		pid=`cat $pidf`
		if  ps -p $pid > /dev/null
		then
			echo "Apache Ranger Tagsync Service is already running [pid={$pid}]"
			exit 1
		else
			rm -rf $pidf
		fi
	fi

	cd ${cdir}
	umask 0077
	SLEEP_TIME_AFTER_START=5
	nohup java -Dproc_rangertagsync ${JAVA_OPTS} -Dlogdir="${logdir}" -Dlog4j.configuration=file:/etc/ranger/tagsync/conf/log4j.properties -cp "${cp}" org.apache.ranger.tagsync.process.TagSynchronizer  > ${logdir}/tagsync.out 2>&1 &
	VALUE_OF_PID=$!
	echo "Starting Apache Ranger Tagsync Service"
	sleep $SLEEP_TIME_AFTER_START
	if ps -p $VALUE_OF_PID > /dev/null
	then
		echo $VALUE_OF_PID > ${pidf}
		chown ranger ${pidf}
		chmod 660 ${pidf}
		pid=`cat $pidf`
		echo "Apache Ranger Tagsync Service with pid ${pid} has started."
	else
		echo "Apache Ranger Tagsync Service failed to start!"
	fi
	exit;

elif [ "${action}" == "STOP" ]; then
	WAIT_TIME_FOR_SHUTDOWN=2
	NR_ITER_FOR_SHUTDOWN_CHECK=15
	if [ -f "$pidf" ] ; then
		pid=`cat $pidf` > /dev/null 2>&1
		echo "Found Apache Ranger TagSync Service with pid $pid, Stopping..."
		kill -9 $pid > /dev/null 2>&1
		sleep 1 #Give kill -9 sometime to "kill"
		if ps -p $pid > /dev/null; then
			echo "Wow, even kill -9 failed, giving up! Sorry.."
                else
			rm -f $pidf
			echo "Apache Ranger Tagsync Service pid = ${pid} has been stopped."
                fi
	else
            echo "Ranger Tagsync Service not running"
	fi
	exit;
	
elif [ "${action}" == "RESTART" ]; then
	echo "Restarting Apache Ranger Tagsync"
	${cdir}/ranger-tagsync-services.sh stop
	${cdir}/ranger-tagsync-services.sh start
	exit;
elif [ "${action}" == "VERSION" ]; then
	cd ${cdir}/lib
	java -cp ranger-util-*.jar org.apache.ranger.common.RangerVersionInfo
	exit
else 
	echo "Invalid argument [$1];"
	echo "Usage: $0 {start | stop | restart | version}"
    exit;
fi

