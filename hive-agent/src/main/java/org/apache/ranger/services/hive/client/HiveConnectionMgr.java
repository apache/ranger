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

package org.apache.ranger.services.hive.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.ranger.plugin.util.TimedEventUtil;
import org.apache.ranger.plugin.store.ServiceStoreFactory;
import org.apache.ranger.services.hive.client.HiveClient;


public class HiveConnectionMgr {

	private static Logger LOG = Logger.getLogger(HiveConnectionMgr.class);
	
	protected HashMap<String, HiveClient> 	hiveConnectionCache;
	protected HashMap<String, Boolean> 		repoConnectStatusMap;


	 public	HiveConnectionMgr() {
		 hiveConnectionCache = new HashMap<String, HiveClient>();
		 repoConnectStatusMap = new HashMap<String, Boolean>();
	 }
	 

	 public HiveClient getHiveConnection(final String serviceName, final String serviceType, final Map<String,String> configs) {
			HiveClient hiveClient  = null;
			
			if (serviceType != null) {
				// get it from the cache
				synchronized (hiveConnectionCache) {
					hiveClient = hiveConnectionCache.get(serviceType);
					if (hiveClient == null) {
						if (configs != null) {
						
							final Callable<HiveClient> connectHive = new Callable<HiveClient>() {
								@Override
								public HiveClient call() throws Exception {
									return new HiveClient(serviceName, configs);
								}
							};
							try {
								hiveClient = TimedEventUtil.timedTask(connectHive, 5, TimeUnit.SECONDS);
							} catch(Exception e){
								LOG.error("Error connecting hive repository : "+ 
										serviceName +" using config : "+ configs, e);
							}
							hiveConnectionCache.put(serviceName, hiveClient);
							repoConnectStatusMap.put(serviceName, true);
						} else {
							LOG.error("Connection Config not defined for asset :"
									+ serviceName, new Throwable());
						}
					} else {
						try {
							List<String> testConnect = hiveClient.getDatabaseList("*",null);
						} catch(Exception e) {
							hiveConnectionCache.remove(serviceType);
							hiveClient = getHiveConnection(serviceName,serviceType,configs);
						}
					}
				}
			} else {
				LOG.error("Asset not found with name "+serviceName, new Throwable());
			}
			return hiveClient;
		}
}
