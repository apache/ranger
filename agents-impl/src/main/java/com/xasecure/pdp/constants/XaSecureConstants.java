package com.xasecure.pdp.constants;

public class XaSecureConstants {
	public static final String PUBLIC_ACCESS_ROLE = "public" ;
	
	public static final String XASECURE_HBASE_POLICYMGR_URL_PROP 								= "xasecure.hbase.policymgr.url";
	public static final String XASECURE_HBASE_POLICYMGR_URL_SAVE_FILE_PROP 				  		= "xasecure.hbase.policymgr.url.saveAsFile";
	public static final String XASECURE_HBASE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP     	= "xasecure.hbase.policymgr.url.reloadIntervalInMillis";
	public static final String XASECURE_HBASE_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.hbase.policymgr.ssl.config";
	public static final long   XASECURE_HBASE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT 	= 60000L ;
	public static final String XASECURE_HBASE_LAST_SAVED_POLICY_FILE_PROP 					    = "xasecure.hbase.policymgr.url.laststoredfile";
	
	public static final String XASECURE_HDFS_POLICYMGR_URL_PROP 						  = "xasecure.hdfs.policymgr.url";
	public static final String XASECURE_HDFS_POLICYMGR_URL_SAVE_FILE_PROP 				  = "xasecure.hdfs.policymgr.url.saveAsFile";
	public static final String XASECURE_HDFS_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP = "xasecure.hdfs.policymgr.url.reloadIntervalInMillis";
	public static final String XASECURE_HDFS_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.hdfs.policymgr.ssl.config";
	public static final long   XASECURE_HDFS_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT = 60000L ;
	public static final String XASECURE_HDFS_LAST_SAVED_POLICY_FILE_PROP 					 = "xasecure.hdfs.policymgr.url.laststoredfile";


	public static final String XASECURE_KNOX_POLICYMGR_URL_PROP 						  = "xasecure.knox.policymgr.url";
	public static final String XASECURE_KNOX_POLICYMGR_URL_SAVE_FILE_PROP 				  = "xasecure.knox.policymgr.url.saveAsFile";
	public static final String XASECURE_KNOX_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP = "xasecure.knox.policymgr.url.reloadIntervalInMillis";
	public static final String XASECURE_KNOX_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.knox.policymgr.ssl.config";
	public static final long   XASECURE_KNOX_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT = 60000L ;
	public static final String XASECURE_KNOX_LAST_SAVED_POLICY_FILE_PROP 					 = "xasecure.knox.policymgr.url.laststoredfile";

	
	public static final String XASECURE_HIVE_POLICYMGR_URL_PROP 						  = "xasecure.hive.policymgr.url";
	public static final String XASECURE_HIVE_POLICYMGR_URL_SAVE_FILE_PROP 				  = "xasecure.hive.policymgr.url.saveAsFile";
	public static final String XASECURE_HIVE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_PROP = "xasecure.hive.policymgr.url.reloadIntervalInMillis";
	public static final String XASECURE_HIVE_POLICYMGR_SSL_CONFIG_FILE_PROP     				= "xasecure.hive.policymgr.ssl.config";
	public static final long   XASECURE_HIVE_POLICYMGR_URL_RELOAD_INTERVAL_IN_MILLIS_DEFAULT = 60000L ;
	public static final String XASECURE_HIVE_LAST_SAVED_POLICY_FILE_PROP 					 = "xasecure.hive.policymgr.url.laststoredfile";

	
	// xasecure 2-way ssl configuration 

	public static final String XASECURE_POLICYMGR_CLIENT_KEY_FILE 						  = "xasecure.policymgr.clientssl.keystore";	
	public static final String XASECURE_POLICYMGR_CLIENT_KEY_FILE_PASSWORD				  = "xasecure.policymgr.clientssl.keystore.password";	
	public static final String XASECURE_POLICYMGR_CLIENT_KEY_FILE_TYPE 					  = "xasecure.policymgr.clientssl.keystore.type";
	public static final String XASECURE_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL     	      = "xasecure.policymgr.clientssl.keystore.credential.file";
	public static final String XASECURE_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS	      = "sslKeyStore";

	public static final String XASECURE_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT 			  = "jks";	

	public static final String XASECURE_POLICYMGR_TRUSTSTORE_FILE						  = "xasecure.policymgr.clientssl.truststore";	
	public static final String XASECURE_POLICYMGR_TRUSTSTORE_FILE_PASSWORD				  = "xasecure.policymgr.clientssl.truststore.password";	
	public static final String XASECURE_POLICYMGR_TRUSTSTORE_FILE_TYPE				      = "xasecure.policymgr.clientssl.truststore.type";	
	public static final String XASECURE_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL     	      = "xasecure.policymgr.clientssl.truststore.credential.file";
	public static final String XASECURE_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS		  = "sslTrustStore";

	public static final String XASECURE_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT			  = "jks";	
	
	
	public static final String XASECURE_SSL_KEYMANAGER_ALGO_TYPE						  = "SunX509" ;
	public static final String XASECURE_SSL_TRUSTMANAGER_ALGO_TYPE						  = "SunX509" ;
	public static final String XASECURE_SSL_CONTEXT_ALGO_TYPE						      = "SSL" ;

}
