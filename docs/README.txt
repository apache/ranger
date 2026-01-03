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

# ---------------------------------------------------------------------
# Build Instruction for Apache Ranger Documentation
# apidocs are only added if mvn install is run on the root directory
# first
# ---------------------------------------------------------------------

$ mvn clean install
$ mvn enunciate:docs
$ export DOC_SRC_DIR=ranger/docs
$ cd ${DOC_SRC_DIR}

$ mvn site
$ sh fix-external-site-reference.sh


# ---------------------------------------------------------------------
# Deployment instruction
# ---------------------------------------------------------------------

DOC_DEPLOY_DIR=/tmp/doc_deploy_dir.$$
mkdir -p ${DOC_DEPLOY_DIR}
svn co https://svn.apache.org/repos/asf/ranger/site/trunk ranger

cd ${DOC_SRC_DIR}/target
rsync -avcn * ${DOC_DEPLOY_DIR}/ranger
#Review the files that are getting overwritten

#Replaced cp with rsync, so we copy only the changed files
#cp -r * ${DOC_DEPLOY_DIR}/ranger/
rsync -avc * ${DOC_DEPLOY_DIR}/ranger

cd ${DOC_DEPLOY_DIR}/ranger

#
# The following command should show list of changes to be committed in SVN repo
#
svn status

# For adding any new files, use the following command
# svn add PATH

# For deleteing any existing file, use the following command
# svn delete PATH

#
# The following command should commit the changes to SVN repo
#

svn commit
