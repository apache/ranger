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

This file describes how to build, setup, configure and run the performance testing tool.

1. 	Build Apache Ranger using the following command.
	% mvn clean compile package assembly:assembly

	The following artifact will be created under target directory.

	target/ranger-0.5.0-ranger-tools.tar.gz

2. 	Copy this artifact to the directory where you want to run the tool.

	% cp target/ranger-0.5.0-ranger-tools.tar.gz <perf-tool-run-dir>
	% cd <perf-tool-run-dir>

3.	Unzip the artifact.

	% tar xvfz ranger-0.5.0-ranger-tools.tar.gz

	This will create the following directory structure under <perf-tool-run-dir>

	ranger-0.5.0-ranger-tools
	ranger-0.5.0-ranger-tools/conf
	ranger-0.5.0-ranger-tools/dist
	ranger-0.5.0-ranger-tools/lib

4.	% cd ranger-0.5.0-ranger-tools

5.	Configure the policies and requests to use in the test run

	Following sample data files are packaged with the perf-tool:

	testdata/test_servicepolicies_hive.json	- Contains service-policies used to initialize the policy-engine;

	testdata/test_servicetags_hive.json 	- This is used only for tag-based policies. This is referenced 
						  from service-policies file. It contains specification of 
						  tag-definitions, and service-resources with their associated tags;

	testdata/test_requests_hive.json	- Contains access requests to be made to the policy-engine;
	
	Please review the contents of these files and modify to suit your profiling needs.

	Update conf/log4j.properties to specify the filename where perf run results will be written to. Property to update is 'log4j.appender.PERF.File'.

6.	Run the tool with the following command

	% ./ranger-perftester.sh -s <service-policies-file>  -r <requests-file> -c <number-of-concurrent-clients> -n <number-of-times-requests-file-to-be-run>

	Example:
	% ./ranger-perftester.sh -s testdata/test_servicepolicies_hive.json  -r testdata/test_requests_hive.json -c 2 -n 1

7. 	At the end of the run, the performance-statistics are printed on the console and in the log specified file in conf/log4j.properties file as shown below. This is for time spent in evaluating access by Ranger Policy Engine during the course of a test run.  The time values shown are in milliseconds.

[RangerPolicyEngine.isAccessAllowed] execCount:64, totalTimeTaken:1873, maxTimeTaken:276, minTimeTaken:4, avgTimeTaken:29

