<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Overview

Dockerfile.build-ubuntu in this folder builds a Docker image to build Apache Ranger.

## Usage

1. Ensure that you have a recent version of Docker installed from
   [docker.io](http://www.docker.io).

2. Set this folder as your working directory.

3. Execute following command to build a Docker image called **ranger-base-ubuntu**.
       docker build -f Dockerfile.ranger-base-ubuntu -t ranger-base-ubuntu .
   This might take about 10 minutes to complete.

4. Execute following command to build a Docker image called **ranger-build-ubuntu**.
       docker build -f Dockerfile.ranger-build-ubuntu -t ranger-build-ubuntu .
   This might take about 10 minutes to complete.

5. Build Apache Ranger with the following commands:
       mkdir -p ./dist
       docker run -it --rm -v $(pwd)/scripts:/home/ranger/scripts -v ${HOME}/.m2:/home/ranger/.m2 -v $(pwd)/dist:/home/ranger/dist ranger-build-ubuntu -b master
   Time taken to complete the build might vary (upto an hour), depending on status of ${HOME}/.m2 directory cache.

6. After completion of build, dist files (like ranger-admin-<version>.tar.gz) will be available under ./dist directory.
