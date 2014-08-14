#!/bin/bash
XAPOLICYMGR_DIR=/usr/lib/xapolicymgr
XAPOLICYMGR_EWS_DIR=${XAPOLICYMGR_DIR}/ews
cd ${XAPOLICYMGR_EWS_DIR}
if [ ! -d logs ]
then
	mkdir logs
fi
java -Dcatalina.base=${XAPOLICYMGR_EWS_DIR} -cp "${XAPOLICYMGR_EWS_DIR}/lib/*:${XAPOLICYMGR_DIR}/xasecure_jaas/*" com.xasecure.server.tomcat.StopEmbededServer > logs/catalina.out 2>&1
echo "XAPolicyManager has been stopped."

