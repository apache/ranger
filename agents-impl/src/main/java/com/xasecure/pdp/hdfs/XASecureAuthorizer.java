package com.xasecure.pdp.hdfs;

import java.util.Set;

import com.xasecure.authorization.hadoop.HDFSAccessVerifier;

public class XASecureAuthorizer implements HDFSAccessVerifier {

	private static URLBasedAuthDB authDB = URLBasedAuthDB.getInstance() ;
	
	@Override
	public boolean isAccessGranted(String aPathName, String aPathOwnerName, String access, String username, Set<String> groups) {
		return authDB.isAccessGranted(aPathName, aPathOwnerName, access, username, groups);
	}

	@Override
	public boolean isAuditLogEnabled(String aPathName) {
		return authDB.isAuditLogEnabled(aPathName) ;
	}

}
