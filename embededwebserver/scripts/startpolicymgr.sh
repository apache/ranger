#!/bin/bash

export JAVA_HOME=
export PATH=$JAVA_HOME/bin:$PATH

XAPOLICYMGR_DIR=/usr/lib/xapolicymgr
XAPOLICYMGR_EWS_DIR=${XAPOLICYMGR_DIR}/ews
cd ${XAPOLICYMGR_EWS_DIR}
if [ ! -d logs ]
then
	mkdir logs
fi
java -Xmx1024m -Xms1024m -Dcatalina.base=${XAPOLICYMGR_EWS_DIR} -cp "${XAPOLICYMGR_EWS_DIR}/lib/*:${XAPOLICYMGR_DIR}/xasecure_jaas/*:${XAPOLICYMGR_DIR}/xasecure_jaas:${JAVA_HOME}/lib/*" com.xasecure.server.tomcat.EmbededServer > logs/catalina.out 2>&1 &
echo "XAPolicyManager has started successfully."
