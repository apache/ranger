#!/bin/bash

echo "$0" | grep -q beeswax_server.sh > /dev/null 2>&1
if [ $? -ne 0 ]
then
	XASECURE_AGENT_PATH="`ls -1 /usr/hdp/current/hadoop/lib/hdfs-agent-*.jar | head -1`"
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
	else
	    echo "ERROR: XASecure Agent is not located at [${XASECURE_AGENT_PATH}]. Exiting ..."
	    #exit 0
	fi
fi
