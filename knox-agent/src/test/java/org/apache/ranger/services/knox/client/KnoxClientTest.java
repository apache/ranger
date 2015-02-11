/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.services.knox.client;

public class KnoxClientTest  {
	
	
	/*
   Sample curl calls to knox REST API to discover topologies
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/admin
	*/
	
	public static void main(String[] args) {
		System.out.println(System.getProperty("java.class.path"));
		System.setProperty("javax.net.ssl.trustStore", "/tmp/cacertswithknox)");
		String[] testArgs = {
				"https://localhost:8443/gateway/admin/api/v1/topologies",
				"admin",
				"admin-password"
				};
		KnoxClient.main(testArgs);
	}
	
	
}
