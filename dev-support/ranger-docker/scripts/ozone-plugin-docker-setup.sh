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

source .env

if [ ! -d dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin ]
then
  tar xvfz dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin.tar.gz --directory=dist/
fi

cp -f config/ozone/ranger-ozone-plugin-install.properties dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin/install.properties
cp -f config/ozone/ranger-ozone-setup.sh dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin/
cp -f config/ozone/enable-ozone-plugin.sh dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin/
chmod +x dist/ranger-${OZONE_PLUGIN_VERSION}-ozone-plugin/ranger-ozone-setup.sh
