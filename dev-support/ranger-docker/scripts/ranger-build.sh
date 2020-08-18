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

BRANCH=master
PROFILES=
SKIP_TESTS="-DskipTests=true"

while getopts ":b:p:s:" arg
do
  case $arg in
    b) BRANCH=$OPTARG;;
    p) PROFILES="-P \"$OPTARG\"";;
    s) SKIP_TESTS="-DskipTests=$OPTARG";;
  esac
done


export MAVEN_OPTS="-Xms2g -Xmx2g"
export M2=/home/ranger/.m2

cd /home/ranger/git/ranger

git checkout ${BRANCH}
git pull

mvn ${PROFILES} ${SKIP_TESTS} -DskipDocs clean package

mv -f target/version /home/ranger/dist/
mv -f target/ranger-* /home/ranger/dist/
