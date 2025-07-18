#!/usr/bin/env bash

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

set -u -o pipefail

: ${BASE_URL:="https://www.apache.org/dyn/closer.lua?action=download&filename="}
: ${CHECKSUM_BASE_URL:="https://downloads.apache.org/"}
: ${RANGER_VERSION:=2.6.0}

source lib.sh

mkdir -p dist

# source
download_and_verify "ranger/${RANGER_VERSION}/apache-ranger-${RANGER_VERSION}.tar.gz"
# binary
download_and_verify "ranger/${RANGER_VERSION}/services/admin/ranger-${RANGER_VERSION}-admin.tar.gz"