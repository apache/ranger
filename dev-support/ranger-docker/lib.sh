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

: ${BASE_URL:="https://www.apache.org/dyn/closer.lua?action=download&filename="}
: ${CHECKSUM_BASE_URL:="https://downloads.apache.org/"}

download_if_not_exists() {
  local url="$1"
  local file="$2"

  if [[ -e "${file}" ]]; then
    echo "${file} already downloaded"
  else
    echo "Downloading ${file} from ${url}"
    curl --fail --location --output "${file}" --show-error --silent "${url}" || rm -fv "${file}"
  fi
}

download_and_verify() {
  local remote_path="$1"
  local file="$(basename "${remote_path}")"

  pushd dist
  download_if_not_exists "${BASE_URL}${remote_path}" "${file}"
  download_if_not_exists "${CHECKSUM_BASE_URL}${remote_path}.asc" "${file}.asc"
  gpg --verify "${file}.asc" "${file}" || exit 1
  popd
}
