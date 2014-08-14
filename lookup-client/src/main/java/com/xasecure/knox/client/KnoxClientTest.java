package com.xasecure.knox.client;


public class KnoxClientTest  {
	
	
	/*
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/admin
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/sandbox
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/hdp
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/hdp1
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/hdp2
	 curl -ivk -u admin:admin-password https://localhost:8443/gateway/admin/api/v1/topologies/xa
	*/
	
	public static void main(String[] args) {
		System.out.println(System.getProperty("java.class.path"));
		String[] testArgs = {
				"https://localhost:8443/gateway/admin/api/v1/topologies",
				"guest",
				"guest-password"
				};
		KnoxClient.main(testArgs);
	}
	
	
}

// http://hdp.example.com:6080/service/assets/hdfs/resources?dataSourceName=nn1&baseDirectory=%2F
// http://hdp.example.com:6080/service/assets/knox/resources?dataSourceName=knox1&topology=%2F

// com.xasecure.rest. AssetREST
// com.xasecure.biz.AssetMgr