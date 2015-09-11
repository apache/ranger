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

	for custom_env_script in `find ${cdir}/conf/ -name "ranger-tagsync-env*"`; do
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
	fi

	cp="${cdir}/conf.dist:${cdir}/dist/*:${cdir}/lib/*"

    if [ -f $pidf ]; then
            PID=`cat $pidf`
            if [ -z "`ps axf | grep ${PID} | grep -v grep`" ]; then
                    rm -f ${pidf}
            else
                    kill -9 ${PID} > /dev/null 2>&1
                    rm -f ${pidf}
                    echo "Ranger Tagsync Service [pid = ${PID}] has been stopped."
            fi
    fi

	cd ${cdir}
	umask 0077
	nohup java -Dproc_rangertagsync ${JAVA_OPTS} -Dlogdir="${logdir}" -cp "${cp}" org.apache.ranger.process.TagSynchronizer  > ${logdir}/tagsync.log 2>&1 &
	echo $! >  ${pidf}
	chown ranger ${pidf}
	sleep 5
	pid=`cat $pidf`

	if [ "${pid}" != "" ]
	then
        	echo "Ranger Tagsync Service has started successfully."
	else
        	echo "Ranger Tagsync Service failed to start. Please refer to log files under ${logdir} for further details."
	fi
	exit;

elif [ "${action}" == "STOP" ]; then

    if [ -f $pidf ]; then
	        PID=`cat $pidf` > /dev/null 2>&1
            kill -9 $PID > /dev/null 2>&1
            rm -f $pidf
            echo "Ranger Tagsync Service [pid = ${PID}] has been stopped."
    else
            echo "Ranger Tagsync Service not running"
    fi

	exit;
	
elif [ "${action}" == "RESTART" ]; then
	echo "Stopping Ranger Tagsync"
	${cdir}/ranger-tagsync-services.sh stop
	echo "Starting Apache Ranger Tagsync"
	${cdir}/ranger-tagsync-services.sh start
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

