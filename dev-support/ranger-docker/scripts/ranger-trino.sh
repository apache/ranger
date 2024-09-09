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


echo "export JAVA_HOME=${JAVA_HOME}" >> /tmp/trino-setup-env.sh

sudo /home/ranger/scripts/ranger-trino-setup.sh

/usr/lib/trino/bin/run-trino

TRINO_PID=$(ps -ef  | grep -v grep | grep -i "io.trino.server.TrinoServer" | awk '{print $2}')

# prevent the container from exiting
if [ -z "$TRINO_PID" ]
then
  echo "The Trino process probably exited, no process id found!"
else
  tail --pid="$TRINO_PID" -f /dev/null
fi
