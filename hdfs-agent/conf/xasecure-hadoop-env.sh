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


if [ -f ${HADOOP_HOME}/conf/ranger-security.xml ]
then
	echo "$0" | grep -q beeswax_server.sh > /dev/null 2>&1
	if [ $? -ne 0 ]
	then
		XASECURE_AGENT_PATH="`ls -1 ${HADOOP_HOME}/lib/ranger-hdfs-plugin-*.jar 2> /dev/null | head -1`"
		if [ -f "${XASECURE_AGENT_PATH}" ]
		then
	    	if [ "${XASECURE_INIT}" != "0" ]
	    	then
	        	XASECURE_INIT="0"
	        	XASECURE_AGENT_OPTS=" -javaagent:${XASECURE_AGENT_PATH}=authagent "
	        	echo ${HADOOP_NAMENODE_OPTS} | grep -q -- "${XASECURE_AGENT_OPTS}" > /dev/null 2>&1
	        	if [ $? -ne 0 ]
	        	then
	                	export HADOOP_NAMENODE_OPTS=" ${XASECURE_AGENT_OPTS} ${HADOOP_NAMENODE_OPTS} "
	                	export HADOOP_SECONDARYNAMENODE_OPTS=" ${XASECURE_AGENT_OPTS} ${HADOOP_SECONDARYNAMENODE_OPTS}"
	        	fi
	    	fi
	    fi
	fi
fi
