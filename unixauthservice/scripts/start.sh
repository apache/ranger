#!/bin/bash

echo "Starting UnixAuthenticationService"
export JAVA_HOME=
export PATH=$JAVA_HOME/bin:$PATH

cdir=`dirname $0`

if [ "${cdir}" = "." ]
then
	cdir=`pwd`
fi

pidf=${cdir}/.mypid

logdir=/var/log/uxugsync

cp="${cdir}/dist/*:${cdir}/lib/*:${cdir}/conf"
[ ! -d ${logdir} ] && mkdir -p ${logdir}
${cdir}/stop.sh
cd ${cdir}
umask 0077
nohup java -cp "${cp}" com.xasecure.authentication.UnixAuthenticationService > ${logdir}/auth.log 2>&1 &
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
