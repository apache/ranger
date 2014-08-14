#!/bin/bash
cdir=`dirname $0`
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