#!/bin/bash
#
# Replacing authorizer to storm.yaml configuration file ...
#
STORM_DIR=/etc/storm
STORM_CONFIG_FILE=storm.yaml

dt=`date '+%Y%m%d%H%M%S'`
CONFIG_FILE=${STORM_DIR}/${STORM_CONFIG_FILE}
ARCHIVE_FILE=${STORM_DIR}/.${STORM_CONFIG_FILE}.${dt}

cp ${CONFIG_FILE} ${ARCHIVE_FILE}

awk -F: 'BEGIN {
	configured = 0 ;
}
{ 
	if ($1 == "nimbus.authorizer") {
		if ($2 ~ /^[ \t]*"backtype.storm.security.auth.authorizer.SimpleACLAuthorizer"[ \t]*$/) {
			configured = 1 ;
			printf("%s\n",$0) ;
		}
		else {
			printf("#%s\n",$0);
			printf("nimbus.authorizer: \"backtype.storm.security.auth.authorizer.SimpleACLAuthorizer\"\n") ;
			configured = 1 ;
		}
	}
	else {
		printf("%s\n",$0) ;
	}
}
END {
	if (configured == 0) {
		printf("nimbus.authorizer: \"backtype.storm.security.auth.authorizer.SimpleACLAuthorizer\"\n") ;
	}
}' ${ARCHIVE_FILE} > ${ARCHIVE_FILE}.new 

if [ ! -z ${ARCHIVE_FILE}.new ] 
then
	cat ${ARCHIVE_FILE}.new > ${CONFIG_FILE}
	rm -f ${ARCHIVE_FILE}.new
	echo "Apache Argus Plugin has been uninstalled from Storm Service. Please restart Storm nimbus and ui services ..."
else
	echo "ERROR: ${ARCHIVE_FILE}.new file has not created successfully."
	exit 1
fi

exit 0
