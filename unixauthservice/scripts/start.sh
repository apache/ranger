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


realScriptPath=`readlink -f $0`
realScriptDir=`dirname $realScriptPath`
cd $realScriptDir
cdir=`pwd`

echo "Starting UnixAuthenticationService"
#export JAVA_HOME=
. ${cdir}/conf/java_home.sh

for custom_env_script in `find ${cdir}/conf/ -name "ranger-usersync-env*"`; do
	if [ -f $custom_env_script ]; then
		. $custom_env_script
	fi
done

if [ "$JAVA_HOME" != "" ]; then
	export PATH=$JAVA_HOME/bin:$PATH
fi

pidf=${cdir}/.mypid

#logdir=`grep '^[ \t]*logdir[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | sed -e 's:[ \t]*::g'`
logdir=`grep -P '^[ \t]*logdir[ \t]*=' ${cdir}/install.properties | awk -F= '{ print $2 }' | tr '\t' ' ' | sed -e 's:[ ]::g'`
if [ ! -d ${logdir} ]
then
	logdir=/var/log/ranger-usersync
fi
cp="${cdir}/dist/*:${cdir}/lib/*:${cdir}/conf"
[ ! -d ${logdir} ] && mkdir -p ${logdir}
${cdir}/stop.sh
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
