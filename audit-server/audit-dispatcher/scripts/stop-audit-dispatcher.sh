#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

realScriptPath=$(readlink -f "$0" 2>/dev/null)
if [ $? -eq 0 ]; then
    scriptDir=$(dirname "$realScriptPath")
else
    scriptDir=$(cd "$(dirname "$0")"; pwd)
fi

AUDIT_DISPATCHER_HOME_DIR=${AUDIT_DISPATCHER_HOME_DIR:-$(cd "${scriptDir}/.."; pwd)}
AUDIT_DISPATCHER_LOG_DIR=${AUDIT_DISPATCHER_LOG_DIR:-"${AUDIT_DISPATCHER_HOME_DIR}/logs"}

DISPATCHER_TYPE=$1

if [ -n "${DISPATCHER_TYPE}" ]; then
    PID_FILE="${AUDIT_DISPATCHER_LOG_DIR}/ranger-audit-dispatcher-${DISPATCHER_TYPE}.pid"
else
    PID_FILE="${AUDIT_DISPATCHER_LOG_DIR}/ranger-audit-dispatcher.pid"
fi

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null; then
        echo "Stopping Ranger Audit Dispatcher (Type: ${DISPATCHER_TYPE:-unknown}) with PID: $PID"
        kill "$PID"

        # Wait for process to exit
        for i in {1..30}; do
            if ! ps -p $PID > /dev/null; then
                echo "Service stopped."
                rm -f "$PID_FILE"
                exit 0
            fi
            sleep 1
        done

        echo "Service did not stop gracefully, forcing kill..."
        kill -9 "$PID"
        rm -f "$PID_FILE"
    else
        echo "Process with PID $PID is not running. Removing stale PID file."
        rm -f "$PID_FILE"
    fi
else
    echo "PID file not found at $PID_FILE. Service may not be running."
fi
