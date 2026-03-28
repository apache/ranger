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

if [[ -z $1 ]]; then
    echo "No argument provided."
    echo "Usage: $0 {start | run | stop | restart | version}"
    exit 1
fi

action=$1
action=$(echo "$action" | tr '[:lower:]' '[:upper:]')

realScriptPath=$(readlink -f "$0")
realScriptDir=$(dirname "$realScriptPath")
cd "$realScriptDir" || exit 1
cdir=$(pwd)

ranger_pdp_max_heap_size=${RANGER_PDP_MAX_HEAP_SIZE:-1g}

for custom_env_script in $(find "${cdir}/conf/" -name "ranger-pdp-env*" 2>/dev/null); do
    if [ -f "$custom_env_script" ]; then
        . "$custom_env_script"
    fi
done

if [ -z "${RANGER_PDP_PID_DIR_PATH}" ]; then
    RANGER_PDP_PID_DIR_PATH=/var/run/ranger
fi

if [ -z "${RANGER_PDP_PID_NAME}" ]; then
    RANGER_PDP_PID_NAME=pdp.pid
fi

pidf="${RANGER_PDP_PID_DIR_PATH}/${RANGER_PDP_PID_NAME}"

if [ -z "${UNIX_PDP_USER}" ]; then
    UNIX_PDP_USER=ranger
fi

JAVA_OPTS=" ${JAVA_OPTS} -XX:MetaspaceSize=100m -XX:MaxMetaspaceSize=200m -Xmx${ranger_pdp_max_heap_size} -Xms256m"

if [ "${action}" == "START" ]; then

    if [ -f "${cdir}/conf/java_home.sh" ]; then
        . "${cdir}/conf/java_home.sh"
    fi

    for custom_env_script in $(find "${cdir}/conf.dist/" -name "ranger-pdp-env*" 2>/dev/null); do
        if [ -f "$custom_env_script" ]; then
            . "$custom_env_script"
        fi
    done

    if [ "$JAVA_HOME" != "" ]; then
        export PATH=$JAVA_HOME/bin:$PATH
    fi

    if [ -z "${RANGER_PDP_LOG_DIR}" ]; then
        RANGER_PDP_LOG_DIR=/var/log/ranger/pdp
    fi

    if [ ! -d "$RANGER_PDP_LOG_DIR" ]; then
        mkdir -p "$RANGER_PDP_LOG_DIR"
        chmod 755 "$RANGER_PDP_LOG_DIR"
    fi

    cp="${cdir}/conf:${cdir}/dist/*:${cdir}/lib/*"

    if [ -f "$pidf" ]; then
        pid=$(cat "$pidf")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo "Ranger PDP Service is already running [pid=${pid}]"
            exit 0
        else
            rm -f "$pidf"
        fi
    fi

    if [ -z "${RANGER_PDP_CONF_DIR}" ]; then
        RANGER_PDP_CONF_DIR=${cdir}/conf
    fi

    mkdir -p "${RANGER_PDP_PID_DIR_PATH}"

    SLEEP_TIME_AFTER_START=5
    nohup java -Dproc_rangerpdp ${JAVA_OPTS} \
        -Dlogdir="${RANGER_PDP_LOG_DIR}" \
        -Dlogback.configurationFile="file:${RANGER_PDP_CONF_DIR}/logback.xml" \
        -Dranger.pdp.conf.dir="${RANGER_PDP_CONF_DIR}" \
        -Duser="${USER}" \
        -Dhostname="${HOSTNAME}" \
        -cp "${cp}" \
        org.apache.ranger.pdp.RangerPdpServer \
        > "${RANGER_PDP_LOG_DIR}/pdp.out" 2>&1 &

    VALUE_OF_PID=$!
    echo "Starting Ranger PDP Service"
    sleep $SLEEP_TIME_AFTER_START

    if ps -p $VALUE_OF_PID > /dev/null 2>&1; then
        echo $VALUE_OF_PID > "${pidf}"
        chown "${UNIX_PDP_USER}" "${pidf}" 2>/dev/null || true
        chmod 660 "${pidf}"
        pid=$(cat "$pidf")
        echo "Ranger PDP Service with pid ${pid} has started."
    else
        echo "Ranger PDP Service failed to start!"
        exit 1
    fi
    exit 0

elif [ "${action}" == "RUN" ]; then
    if [ -f "${cdir}/conf/java_home.sh" ]; then
        . "${cdir}/conf/java_home.sh"
    fi

    for custom_env_script in $(find "${cdir}/conf.dist/" -name "ranger-pdp-env*" 2>/dev/null); do
        if [ -f "$custom_env_script" ]; then
            . "$custom_env_script"
        fi
    done

    if [ "$JAVA_HOME" != "" ]; then
        export PATH=$JAVA_HOME/bin:$PATH
    fi

    if [ -z "${RANGER_PDP_LOG_DIR}" ]; then
        RANGER_PDP_LOG_DIR=/var/log/ranger/pdp
    fi

    if [ ! -d "$RANGER_PDP_LOG_DIR" ]; then
        mkdir -p "$RANGER_PDP_LOG_DIR"
        chmod 755 "$RANGER_PDP_LOG_DIR"
    fi

    if [ -z "${RANGER_PDP_CONF_DIR}" ]; then
        RANGER_PDP_CONF_DIR=${cdir}/conf
    fi

    cp="${cdir}/conf:${cdir}/dist/*:${cdir}/lib/*"
    exec java -Dproc_rangerpdp ${JAVA_OPTS} \
        -Dlogdir="${RANGER_PDP_LOG_DIR}" \
        -Dlogback.configurationFile="file:${RANGER_PDP_CONF_DIR}/logback.xml" \
        -Dranger.pdp.conf.dir="${RANGER_PDP_CONF_DIR}" \
        -Duser="${USER}" \
        -Dhostname="${HOSTNAME}" \
        -cp "${cp}" \
        org.apache.ranger.pdp.RangerPdpServer

elif [ "${action}" == "STOP" ]; then

    WAIT_TIME_FOR_SHUTDOWN=2
    NR_ITER_FOR_SHUTDOWN_CHECK=15

    if [ -f "$pidf" ]; then
        pid=$(cat "$pidf")
    else
        pid=$(ps -ef | grep java | grep -- '-Dproc_rangerpdp' | grep -v grep | awk '{ print $2 }')
        if [ "$pid" != "" ]; then
            echo "pid file (${pidf}) not found; taking pid from 'ps' output."
        else
            echo "Ranger PDP Service is not running."
            exit 0
        fi
    fi

    echo "Stopping Ranger PDP Service (pid=${pid})..."
    kill -15 "$pid"

    for ((i=0; i<NR_ITER_FOR_SHUTDOWN_CHECK; i++)); do
        sleep $WAIT_TIME_FOR_SHUTDOWN
        if ps -p "$pid" > /dev/null 2>&1; then
            echo "Shutdown in progress. Checking again in ${WAIT_TIME_FOR_SHUTDOWN}s..."
        else
            break
        fi
    done

    if ps -p "$pid" > /dev/null 2>&1; then
        echo "Graceful stop failed; sending SIGKILL..."
        kill -9 "$pid"
    fi

    sleep 1
    if ps -p "$pid" > /dev/null 2>&1; then
        echo "kill -9 failed. Process still running. Giving up."
        exit 1
    else
        rm -f "$pidf"
        echo "Ranger PDP Service with pid ${pid} has been stopped."
    fi
    exit 0

elif [ "${action}" == "RESTART" ]; then
    echo "Restarting Ranger PDP Service..."
    "${cdir}/ranger-pdp-services.sh" stop
    "${cdir}/ranger-pdp-services.sh" start
    exit 0

elif [ "${action}" == "VERSION" ]; then
    cd "${cdir}/lib" || exit 1
    java -cp "ranger-util-*.jar" org.apache.ranger.common.RangerVersionInfo
    exit 0

else
    echo "Invalid argument [${action}]"
    echo "Usage: $0 {start | run | stop | restart | version}"
    exit 1
fi
