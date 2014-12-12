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

 package org.apache.ranger.biz;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.StringUtil;
import org.apache.ranger.common.TimedEventUtil;
import org.apache.ranger.db.XADaoManager;
import org.apache.ranger.entity.XXAsset;
import org.apache.ranger.hadoop.client.HadoopFS;
import org.apache.ranger.hbase.client.HBaseClient;
import org.apache.ranger.hive.client.HiveClient;
import org.apache.ranger.knox.client.KnoxClient;
import org.apache.ranger.service.XAssetService;
import org.apache.ranger.storm.client.StormClient;
import org.apache.ranger.view.VXAsset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("singleton")
public class AssetConnectionMgr {
	
	private static Logger logger = Logger.getLogger(AssetConnectionMgr.class);
	
	protected HashMap<String, HadoopFS> hadoopConnectionCache;
	protected HashMap<String, HiveClient> hiveConnectionCache;
	protected HashMap<String, HBaseClient> hbaseConnectionCache;

	protected HashMap<String, Boolean> repoConnectStatusMap;

	@Autowired
	protected JSONUtil jsonUtil;

	@Autowired
	protected StringUtil stringUtil;
	
	@Autowired
	protected XADaoManager xADaoManager;
	
	@Autowired
	XAssetService xAssetService;
	
	public AssetConnectionMgr(){
		hadoopConnectionCache = new HashMap<String, HadoopFS>();
		hiveConnectionCache = new HashMap<String, HiveClient>();
		hbaseConnectionCache = new HashMap<String, HBaseClient>();
		repoConnectStatusMap = new HashMap<String, Boolean>();
	}
	
	public HadoopFS getHadoopConnection(final String dataSourceName) {
		HadoopFS hadoopFS = null;
		XXAsset asset = xADaoManager.getXXAsset().findByAssetName(dataSourceName);
		if (asset != null) {
			// get it from the cache
			synchronized (hadoopConnectionCache) {
				hadoopFS = hadoopConnectionCache.get(asset.getName());
				if (hadoopFS == null) {
				// if it doesn't exist in cache then create the connection
					String config = asset.getConfig();
					if(!stringUtil.isEmpty(config)){
						config=xAssetService.getConfigWithDecryptedPassword(config);
					}
					// FIXME remove this once we start using putting config for
					// default asset "hadoopdev" (should come from properties)
					if (stringUtil.isEmpty(config)
							&& asset.getName().equals("hadoopdev")) {
						
						final Callable<HadoopFS> connectHDFS = new Callable<HadoopFS>() {
							@Override
							public HadoopFS call() throws Exception {
								return new HadoopFS(dataSourceName);
							}
						};
						
						try {
							hadoopFS = TimedEventUtil.timedTask(connectHDFS, 10, TimeUnit.SECONDS);
						} catch(Exception e){
							logger.error("Error establishing connection for HDFS repository : "
									+ dataSourceName, e);
						}
						
					} else if (!stringUtil.isEmpty(config)) {
						final HashMap<String, String> configMap = (HashMap<String, String>) jsonUtil
								.jsonToMap(config);
						final String assetName = asset.getName();
						
						final Callable<HadoopFS> connectHDFS = new Callable<HadoopFS>() {
							@Override
							public HadoopFS call() throws Exception {
								return new HadoopFS(assetName, configMap);
							}
						};
						
						try {
							hadoopFS = TimedEventUtil.timedTask(connectHDFS, 5, TimeUnit.SECONDS);
						} catch(Exception e){
								logger.error("Error establishing connection for HDFS repository : "
										+ dataSourceName + " using configuration : " +config, e);
						}
						// put it into the cache
					} else {
						logger.error("Connection Config not defined for asset :"
								+ asset.getName(), new Throwable());
					}
					hadoopConnectionCache.put(asset.getName(), hadoopFS);
					repoConnectStatusMap.put(asset.getName(), true);
				} else {
					List<String> testConnect = hadoopFS.listFiles("/", "*");
					if(testConnect == null){
						hadoopConnectionCache.remove(dataSourceName);
						hadoopFS = getHadoopConnection(dataSourceName);
					}
				}
			}
		} else {
			logger.error("Asset not found with name "+dataSourceName, new Throwable());
		}

		return hadoopFS;
	}
	
	public HiveClient getHiveConnection(final String dataSourceName) {
		HiveClient hiveClient = null;
		XXAsset asset = xADaoManager.getXXAsset().findByAssetName(dataSourceName);
		if (asset != null) {
			// get it from the cache
			synchronized (hiveConnectionCache) {
				hiveClient = hiveConnectionCache.get(asset.getName());
				if (hiveClient == null) {
					String config = asset.getConfig();
					if (!stringUtil.isEmpty(config)) {
						config=xAssetService.getConfigWithDecryptedPassword(config);
						final HashMap<String, String> configMap = (HashMap<String, String>) jsonUtil
								.jsonToMap(config);
						
						final Callable<HiveClient> connectHive = new Callable<HiveClient>() {
							@Override
							public HiveClient call() throws Exception {
								return new HiveClient(dataSourceName, configMap);
							}
						};
						try {
							hiveClient = TimedEventUtil.timedTask(connectHive, 5, TimeUnit.SECONDS);
						} catch(Exception e){
							logger.error("Error connecting hive repository : "+ 
									dataSourceName +" using config : "+ config, e);
						}
						hiveConnectionCache.put(asset.getName(), hiveClient);
						repoConnectStatusMap.put(asset.getName(), true);
					} else {
						logger.error("Connection Config not defined for asset :"
								+ asset.getName(), new Throwable());
					}
				} else {
					try {
						List<String> testConnect = hiveClient.getDatabaseList("*");
					} catch(Exception e) {
						hiveConnectionCache.remove(dataSourceName);
						hiveClient = getHiveConnection(dataSourceName);
					}
				}
			}
		} else {
			logger.error("Asset not found with name "+dataSourceName, new Throwable());
		}
		return hiveClient;
	}
	
	public KnoxClient getKnoxClient(String dataSourceName) {
		KnoxClient knoxClient = null;
		logger.debug("Getting knoxClient for datasource: " + dataSourceName);
		XXAsset asset = xADaoManager.getXXAsset().findByAssetName(dataSourceName);
		if (asset == null) {
			logger.error("Asset not found with name " + dataSourceName, new Throwable());
		} else {
			knoxClient = getKnoxClient(asset);
		}
		return knoxClient;
	}
	
	public KnoxClient getKnoxClient(XXAsset asset) {
		KnoxClient knoxClient = null;
		if (asset == null) {
			logger.error("Asset is null", new Throwable());
		} else {
			String config = asset.getConfig();
			if(!stringUtil.isEmpty(config)){
				config=xAssetService.getConfigWithDecryptedPassword(config);
			}
			knoxClient = getKnoxClientByConfig(config);
		}
		return knoxClient;
	}
	
	public KnoxClient getKnoxClientByConfig(String config) {
		KnoxClient knoxClient = null;
		if (config == null || config.trim().isEmpty()) {
			logger.error("Connection Config is empty");
				
		} else {
			final HashMap<String, String> configMap = (HashMap<String, String>) jsonUtil
					.jsonToMap(config);
			String knoxUrl = configMap.get("knox.url");
			String knoxAdminUser = configMap.get("username");
			String knoxAdminPassword = configMap.get("password");
			knoxClient =  new KnoxClient(knoxUrl, knoxAdminUser, knoxAdminPassword);
		}
		return knoxClient;
	}

	public KnoxClient getKnoxClient(String dataSourceName, 
			Map<String, String> configMap) {
		KnoxClient knoxClient = null;
		logger.debug("Getting knoxClient for datasource: " + dataSourceName +
				"configMap: " + configMap);
		if (configMap == null || configMap.isEmpty()) {
			logger.error("Connection ConfigMap is empty");
		} else {
			String knoxUrl = configMap.get("knox.url");
			String knoxAdminUser = configMap.get("username");
			String knoxAdminPassword = configMap.get("password");
			knoxClient =  new KnoxClient(knoxUrl, knoxAdminUser, knoxAdminPassword);
		}
		return knoxClient;
	}
	
	
	public static KnoxClient getKnoxClient(final String knoxUrl, String knoxAdminUser, String knoxAdminPassword) {
		KnoxClient knoxClient = null;
		if (knoxUrl == null || knoxUrl.isEmpty()) {
			logger.error("Can not create KnoxClient: knoxUrl is empty");
		} else if (knoxAdminUser == null || knoxAdminUser.isEmpty()) {
			logger.error("Can not create KnoxClient: knoxAdminUser is empty");
		} else if (knoxAdminPassword == null || knoxAdminPassword.isEmpty()) {
			logger.error("Can not create KnoxClient: knoxAdminPassword is empty");
		} else {
			knoxClient =  new KnoxClient(knoxUrl, knoxAdminUser, knoxAdminPassword);
		}
		return knoxClient;
	}
	
	public HBaseClient getHBaseConnection(final String dataSourceName) {
		HBaseClient client = null;
		XXAsset asset = xADaoManager.getXXAsset().findByAssetName(
				dataSourceName);
		if (asset != null) {
			// get it from the cache
			synchronized (hbaseConnectionCache) {
				client = hbaseConnectionCache.get(asset.getName());
				if (client == null) {
					// if it doesn't exist in cache then create the connection
					String config = asset.getConfig();
					if(!stringUtil.isEmpty(config)){
						config=xAssetService.getConfigWithDecryptedPassword(config);
					}
					// FIXME remove this once we start using putting config for
					// default asset "dev-hive" (should come from properties)
					if (stringUtil.isEmpty(config)
							&& asset.getName().equals("hbase")) {
						
						final Callable<HBaseClient> connectHBase = new Callable<HBaseClient>() {
							@Override
							
							public HBaseClient call() throws Exception {
								HBaseClient hBaseClient=null;
								if(dataSourceName!=null){
									try{
										hBaseClient=new HBaseClient(dataSourceName);
									}catch(Exception ex){
										logger.error("Error connecting HBase repository : ", ex);
									}
								}
								return hBaseClient;
							}
							
						};
						
						try {
							if(connectHBase!=null){
								client = TimedEventUtil.timedTask(connectHBase, 5, TimeUnit.SECONDS);
							}
						} catch(Exception e){
							logger.error("Error connecting HBase repository : " + dataSourceName);
						}
					} else if (!stringUtil.isEmpty(config)) {
						final HashMap<String, String> configMap = (HashMap<String, String>) jsonUtil
								.jsonToMap(config);

						final Callable<HBaseClient> connectHBase = new Callable<HBaseClient>() {
							@Override
							public HBaseClient call() throws Exception {
								HBaseClient hBaseClient=null;
								if(dataSourceName!=null && configMap!=null){
									try{
										hBaseClient=new HBaseClient(dataSourceName,configMap);
									}catch(Exception ex){
										logger.error("Error connecting HBase repository : ", ex);
									}
								}
								return hBaseClient;
							}
						};
						
						try {
							if(connectHBase!=null){
								client = TimedEventUtil.timedTask(connectHBase, 5, TimeUnit.SECONDS);
							}
						} catch(Exception e){
							logger.error("Error connecting HBase repository : "+ 
									dataSourceName +" using config : "+ config);
						}
						
					} else {
						logger.error(
								"Connection Config not defined for asset :"
										+ asset.getName(), new Throwable());
					}
					if(client!=null){
						hbaseConnectionCache.put(asset.getName(), client);
					}
				} else {
					List<String> testConnect = client.getTableList(".\\*");
					if(testConnect == null){
						hbaseConnectionCache.remove(dataSourceName);
						client = getHBaseConnection(dataSourceName);
					}
				}
				repoConnectStatusMap.put(asset.getName(), true);
			}
		} else {
			logger.error("Asset not found with name " + dataSourceName,
					new Throwable());
		}

		return client;
	}

	public boolean destroyConnection(VXAsset asset) {
		boolean result = false;
		if (asset != null) {
			if(asset.getAssetType() == AppConstants.ASSET_HDFS) {
				synchronized (hadoopConnectionCache) {
					
					@SuppressWarnings("unused")
					HadoopFS hadoopFS = hadoopConnectionCache.get(asset.getName());
					// TODO need a way to close the connection
					hadoopConnectionCache.remove(asset.getName());
					repoConnectStatusMap.remove(asset.getName());
					
				}
			} else if(asset.getAssetType() == AppConstants.ASSET_HIVE) {
				synchronized (hadoopConnectionCache) {
					
					HiveClient hiveClient = hiveConnectionCache.get(asset.getName());
					if(hiveClient != null) {
						hiveClient.close();
					}
					hadoopConnectionCache.remove(asset.getName());
					repoConnectStatusMap.remove(asset.getName());
					
				}
			} else if (asset.getAssetType() == AppConstants.ASSET_HBASE) {
				synchronized (hbaseConnectionCache) {
					@SuppressWarnings("unused")
					HBaseClient hBaseClient = hbaseConnectionCache.get(asset
							.getName());					
					// TODO need a way to close the connection
					hbaseConnectionCache.remove(asset.getName());
					repoConnectStatusMap.remove(asset.getName());

				}
			}
			result = true;
		}
		return result;
	}
	
	public HadoopFS resetHadoopConnection(final String dataSourceName){
		hadoopConnectionCache.remove(dataSourceName);
		return getHadoopConnection(dataSourceName);
	}
	
    public static StormClient getStormClient(final String stormUIURL, String userName, String password) {
        StormClient stormClient = null;
        if (stormUIURL == null || stormUIURL.isEmpty()) {
            logger.error("Can not create KnoxClient: stormUIURL is empty");
        } else if (userName == null || userName.isEmpty()) {
            logger.error("Can not create KnoxClient: knoxAdminUser is empty");
        } else if (password == null || password.isEmpty()) {
            logger.error("Can not create KnoxClient: knoxAdminPassword is empty");
        } else {
            stormClient =  new StormClient(stormUIURL, userName, password);
        }
        return stormClient;
    }

}
