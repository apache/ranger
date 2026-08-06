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

if [ "${JAVA_HOME}" == "" ]
then
	echo "JAVA_HOME environment property not defined, aborting installation."
	exit 1
elif [ ! -d "${JAVA_HOME}" ]
then
	echo "JAVA_HOME environment property was set incorrectly, aborting installation."
	exit 1
else
	export JAVA_HOME
	PATH="${JAVA_HOME}/bin:${PATH}"
	export PATH
fi

./setup.py

if [ "${ENABLE_FILE_SYNC_SOURCE}" == "true" ]
then
  # shellcheck disable=SC2164
  cd "${RANGER_HOME}"/usersync/conf
  xmlstarlet ed -L -u "//property[name='ranger.usersync.source.impl.class']/value" -v "org.apache.ranger.unixusersync.process.FileSourceUserGroupBuilder" ranger-ugsync-site.xml
  xmlstarlet ed -L -s "//configuration" -t elem -n "property" -s "//property[last()]" -t elem -n "name" -v "ranger.usersync.filesource.file" -s "//property[last()]" -t elem -n "value" -v "${RANGER_SCRIPTS}/ugsync-file-source.csv" ranger-ugsync-site.xml
fi

if [ "${DEBUG_USERSYNC}" == "true" ]
then
  # shellcheck disable=SC2164
  cd "${RANGER_HOME}"/usersync/conf
  xmlstarlet ed -L -u "//root/@level" -v "debug" logback.xml
fi