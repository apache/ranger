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

 package org.apache.ranger.pdp.constants;

public class RangerConstants {
	public static final String PUBLIC_ACCESS_ROLE = "public" ;
	
	public static final String RANGER_HBASE_POLICYMGR_URL_PROP 								= "xasecure.hbase.policymgr.url";
	public static final String RANGER_HBASE_POLICYMGR_URL_SAVE_FILE_PROP 				  		= "xasecure.hbase.policymgr.url.saveAsFile";
	public static final String RANGER_HBASE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP     	= "xasecure.hbase.policymgr.url.reloadIntervalInMillis";
	public static final String RANGER_HBASE_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.hbase.policymgr.ssl.config";
	public static final long   RANGER_HBASE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT 	= 60000L ;
	public static final String RANGER_HBASE_LAST_SAVED_POLICY_FILE_PROP 					    = "xasecure.hbase.policymgr.url.laststoredfile";
	
	public static final String RANGER_HDFS_POLICYMGR_URL_PROP 						  = "xasecure.hdfs.policymgr.url";
	public static final String RANGER_HDFS_POLICYMGR_URL_SAVE_FILE_PROP 				  = "xasecure.hdfs.policymgr.url.saveAsFile";
	public static final String RANGER_HDFS_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP = "xasecure.hdfs.policymgr.url.reloadIntervalInMillis";
	public static final String RANGER_HDFS_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.hdfs.policymgr.ssl.config";
	public static final long   RANGER_HDFS_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT = 60000L ;
	public static final String RANGER_HDFS_LAST_SAVED_POLICY_FILE_PROP 					 = "xasecure.hdfs.policymgr.url.laststoredfile";


	public static final String RANGER_KNOX_POLICYMGR_URL_PROP 						  = "xasecure.knox.policymgr.url";
	public static final String RANGER_KNOX_POLICYMGR_URL_SAVE_FILE_PROP 				  = "xasecure.knox.policymgr.url.saveAsFile";
	public static final String RANGER_KNOX_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP = "xasecure.knox.policymgr.url.reloadIntervalInMillis";
	public static final String RANGER_KNOX_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.knox.policymgr.ssl.config";
	public static final long   RANGER_KNOX_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT = 60000L ;
	public static final String RANGER_KNOX_LAST_SAVED_POLICY_FILE_PROP 					 = "xasecure.knox.policymgr.url.laststoredfile";

	
	public static final String RANGER_HIVE_POLICYMGR_URL_PROP 						  = "xasecure.hive.policymgr.url";
	public static final String RANGER_HIVE_POLICYMGR_URL_SAVE_FILE_PROP 				  = "xasecure.hive.policymgr.url.saveAsFile";
	public static final String RANGER_HIVE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP = "xasecure.hive.policymgr.url.reloadIntervalInMillis";
	public static final String RANGER_HIVE_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.hive.policymgr.ssl.config";
	public static final long   RANGER_HIVE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT = 60000L ;
	public static final String RANGER_HIVE_LAST_SAVED_POLICY_FILE_PROP 					 = "xasecure.hive.policymgr.url.laststoredfile";

	
	// xasecure 2-way ssl configuration 

	public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE 						  = "xasecure.policymgr.clientssl.keystore";	
	public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_PASSWORD				  = "xasecure.policymgr.clientssl.keystore.password";	
	public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE 					  = "xasecure.policymgr.clientssl.keystore.type";
	public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL     	      = "xasecure.policymgr.clientssl.keystore.credential.file";
	public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS	      = "sslKeyStore";

	public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT 			  = "jks";	

	public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE						  = "xasecure.policymgr.clientssl.truststore";	
	public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_PASSWORD				  = "xasecure.policymgr.clientssl.truststore.password";	
	public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE				      = "xasecure.policymgr.clientssl.truststore.type";	
	public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL     	      = "xasecure.policymgr.clientssl.truststore.credential.file";
	public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS		  = "sslTrustStore";

	public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT			  = "jks";	
	
	
	public static final String RANGER_SSL_KEYMANAGER_ALGO_TYPE						  = "SunX509" ;
	public static final String RANGER_SSL_TRUSTMANAGER_ALGO_TYPE						  = "SunX509" ;
	public static final String RANGER_SSL_CONTEXT_ALGO_TYPE						      = "SSL" ;
	
	
	
	public static final String RANGER_STORM_POLICYMGR_URL_PROP 						  = "xasecure.storm.policymgr.url";
	public static final String RANGER_STORM_POLICYMGR_URL_SAVE_FILE_PROP 				  = "xasecure.storm.policymgr.url.saveAsFile";
	public static final String RANGER_STORM_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP = "xasecure.storm.policymgr.url.reloadIntervalInMillis";
	public static final String RANGER_STORM_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.storm.policymgr.ssl.config";
	public static final long   RANGER_STORM_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT = 60000L ;
	public static final String RANGER_STORM_LAST_SAVED_POLICY_FILE_PROP 					 = "xasecure.storm.policymgr.url.laststoredfile";


}
