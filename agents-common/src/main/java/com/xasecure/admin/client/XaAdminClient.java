package com.xasecure.admin.client;


import com.xasecure.admin.client.datatype.GrantRevokeData;


public interface XaAdminClient {
	String getPolicies(String repositoryName, long lastModifiedTime, int policyCount, String agentName);

	void grantPrivilege(GrantRevokeData grData) throws Exception;

	void revokePrivilege(GrantRevokeData grData) throws Exception;
}
