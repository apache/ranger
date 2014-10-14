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

XAPOLICYMGR_DIR=/usr/lib/xapolicymgr
XAPOLICYMGR_EWS_DIR=${XAPOLICYMGR_DIR}/ews
cd ${XAPOLICYMGR_EWS_DIR}
if [ ! -d logs ]
then
	mkdir logs
fi
java -Dcatalina.base=${XAPOLICYMGR_EWS_DIR} -cp "${XAPOLICYMGR_EWS_DIR}/lib/*:${XAPOLICYMGR_DIR}/xasecure_jaas/*" com.xasecure.server.tomcat.StopEmbededServer > logs/catalina.out 2>&1
echo "XAPolicyManager has been stopped."

