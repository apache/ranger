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

#
# Review and update following variables as needed
#
MEMORY=1g

#
# Usage:
#  ranger-mem-sizing.sh -p policies.json -t tags.json -u userstore.json -r roles.json
#

#
#
#
cdir=$(cd "$(dirname "$0")"; pwd)
cp="${cdir}/dist/*:${cdir}/lib/*"

if [ "${JAVA_HOME}" != "" ]
then
  export JAVA_HOME
  PATH="${JAVA_HOME}/bin:${PATH}"
  export PATH
fi

JAVA_CMD="java -Xms${MEMORY} -Xmx${MEMORY} -Xloggc:./gc.log -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=. -Dlogback.configurationFile=file:${cdir}/conf/logback-mem-sizing.xml -cp ${cp} org.apache.ranger.sizing.RangerMemSizing"

cd ${cdir}

echo "JAVA command = $JAVA_CMD " "$@"
$JAVA_CMD "$@"
