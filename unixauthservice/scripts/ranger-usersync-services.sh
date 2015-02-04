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

realScriptPath=`readlink -f $0`
realScriptDir=`dirname $realScriptPath`
cd $realScriptDir
cdir=`pwd`

pidf=${cdir}/.mypid


if [ ${action^^} == "START" ]; then

	#Export JAVA_HOME
	. ${cdir}/conf/java_home.sh

	for custom_env_script in `find ${cdir}/conf/ -name "ranger-usersync-env*"`; do
        	if [ -f $custom_env_script ]; then
                	. $custom_env_script
	        fi
	done

	if [ "$JAVA_HOME" != "" ]; then
        	export PATH=$JAVA_HOME/bin:$PATH
	fi
	
	logdir=`grep -P '^[ \t]*logdir[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | tr '\t' ' ' | sed -e 's:[ ]::g'`
	if [ ! -d ${logdir} ]
	then
        	logdir=/var/log/ranger-usersync
	fi
	cp="${cdir}/dist/*:${cdir}/lib/*:${cdir}/conf"
	[ ! -d ${logdir} ] && mkdir -p ${logdir}
	${cdir}/ranger-usersync-services.sh stop
	cd ${cdir}
	umask 0077
	nohup java -Dproc_rangerusersync ${JAVA_OPTS} -Dlogdir="${logdir}" -cp "${cp}" org.apache.ranger.authentication.UnixAuthenticationService -enableUnixAuth > ${logdir}/auth.log 2>&1 &
	echo $! >  ${pidf}
	sleep 5
	port=`grep  '^[ ]*authServicePort' ${cdir}/conf/unixauthservice.properties | awk -F= '{ print $2 }' | awk '{ print $1 }'`
	pid=`netstat -antp | grep LISTEN | grep  ${port} | awk '{ print $NF }' | awk -F/ '{ if ($2 == "java") { print $1 } }'`
	if [ "${pid}" != "" ]
	then
        	echo "UnixAuthenticationService has started successfully."
	else
        	echo "UnixAuthenticationService failed to start. Please refer to log files under ${logdir} for further details."
	fi
	exit;

elif [ ${action^^} == "STOP" ]; then
	port=`grep  '^[ ]*authServicePort' ${cdir}/conf/unixauthservice.properties | awk -F= '{ print $2 }' | awk '{ print $1 }'`
	pid=`netstat -antp | grep LISTEN | grep  ${port} | awk '{ print $NF }' | awk -F/ '{ if ($2 == "java") { print $1 } }'`
	if [ "${pid}" != "" ]
	then
        	kill -9 ${pid}
	        echo "AuthenticationService [pid = ${pid}] has been stopped."
	fi
	if [ -f ${pidf} ]
	then
        	npid=`cat ${pidf}`
	        if [ "${npid}" != "" ]
        	then
                	if [ "${pid}" != "${npid}" ]
	                then
        	                if [ -a /proc/${npid} ]
                	        then
                        	        echo "AuthenticationService [pid = ${npid}] has been stopped."
                                	kill -9 ${npid} > /dev/null 2>&1
	                                echo > ${pidf}
        	                fi
                	fi
	        fi
	fi
	exit;
	
elif [ ${action^^} == "RESTART" ]; then
	echo "Stopping Apache Ranger Usersync"
	${cdir}/ranger-usersync-services.sh stop
	echo "Starting Apache Ranger Usersync"
	${cdir}/ranger-usersync-services.sh start
	exit;
elif [ ${action^^} == "VERSION" ]; then
	cd ${cdir}/lib
	java -cp ranger-util-*.jar org.apache.ranger.common.RangerVersionInfo
	exit
else 
	        echo "Invalid argument [$1];"
        echo "Usage: Only start | stop | restart | version, are supported."
        exit;
fi
