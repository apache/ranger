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


if [ ! -e ${RANGER_HOME}/.setupDone ]
then
  SETUP_RANGER=true
else
  SETUP_RANGER=false
fi

if [ "${SETUP_RANGER}" == "true" ]
then
  cd "${RANGER_HOME}"/usersync || exit
  if ./setup.sh;
  then
    touch "${RANGER_HOME}"/.setupDone
  else
    echo "Ranger UserSync Setup Script didn't complete proper execution."
  fi
fi

if [ "${ENABLE_FILE_SYNC_SOURCE}" == "true" ]
then
  # shellcheck disable=SC2164
  cd "${RANGER_HOME}"/usersync/conf
  xmlstarlet ed -u "//property[name='ranger.usersync.source.impl.class']/value" -v "org.apache.ranger.unixusersync.process.FileSourceUserGroupBuilder" ranger-ugsync-site.xml > temp.xml
  mv temp.xml ranger-ugsync-site.xml
  xmlstarlet ed -L -s "//configuration" -t elem -n "property" -s "//property[last()]" -t elem -n "name" -v "ranger.usersync.filesource.file" -s "//property[last()]" -t elem -n "value" -v "${RANGER_SCRIPTS}/ugsync-file-source.csv" ranger-ugsync-site.xml
fi

cd ${RANGER_HOME}/usersync && ./start.sh

RANGER_USERSYNC_PID=`ps -ef  | grep -v grep | grep -i "org.apache.ranger.authentication.UnixAuthenticationService" | awk '{print $2}'`

# prevent the container from exiting
if [ -z "$RANGER_USERSYNC_PID" ]
then
  echo "The UserSync process probably exited, no process id found!"
else
  tail --pid=$RANGER_USERSYNC_PID -f /dev/null
fi
