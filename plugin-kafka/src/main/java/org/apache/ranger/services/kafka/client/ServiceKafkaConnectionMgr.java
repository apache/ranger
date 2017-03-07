/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.ranger.services.kafka.client;

import java.util.Map;

public class ServiceKafkaConnectionMgr {

	static public ServiceKafkaClient getKafkaClient(String serviceName,
			Map<String, String> configs) throws Exception {
		String zookeeperConnect = configs.get("zookeeper.connect");
		if (zookeeperConnect != null) {
			ServiceKafkaClient serviceKafkaClient = new ServiceKafkaClient(
					serviceName, zookeeperConnect);
			return serviceKafkaClient;
		}
		throw new Exception("Required properties are not set for "
				+ serviceName + ". URL or Zookeeper information not provided.");
	}

	/**
	 * @param serviceName
	 * @param configs
	 * @return
	 */
	public static Map<String, Object> connectionTest(String serviceName,
			Map<String, String> configs) throws Exception {
		ServiceKafkaClient serviceKafkaClient = getKafkaClient(serviceName,
				configs);
		return serviceKafkaClient.connectionTest();
	}

}
