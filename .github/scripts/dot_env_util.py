#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse

branch_to_snapshot_version_map = {
	'master': '3.0.0-SNAPSHOT',
	'ranger-2.4': '2.4.1-SNAPSHOT',
	'ranger-2.5': '2.5.1-SNAPSHOT',
	'ranger-2.6': '2.6.0',
}


def update_env_mvn_build(data, release_branch):
	data = data.replace('BUILD_HOST_SRC=true', 'BUILD_HOST_SRC=false')
	data = data.replace('BRANCH=master', f'BRANCH={release_branch}')
	return data


def update_env(current_branch, release_branch, is_mvn_build):
	with open(r'.env', 'r') as file:
		data = file.read()

	ranger_release_version = branch_to_snapshot_version_map[release_branch]
	ranger_current_version = branch_to_snapshot_version_map[current_branch]

	if is_mvn_build:
		data = update_env_mvn_build(data, release_branch)

	data = data.replace(ranger_current_version, ranger_release_version)

	with open(r'.env', 'w') as file:
		file.write(data)
	return

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Utility to update .env')
	parser.add_argument('--release_branch', help='release branch')
	parser.add_argument('--current_branch', default='master', help='current checked out ranger branch')
	parser.add_argument('--maven_build', default=False, help='Maven build in Docker required?')

	args = parser.parse_args()
	update_env(args.current_branch, args.release_branch, args.maven_build)

