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

pidf=${cdir}/.mypid
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