package com.xasecure.pdp.hive;

import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.security.UserGroupInformation;

import com.xasecure.authorization.hive.XaHiveAccessVerifier;
import com.xasecure.authorization.hive.XaHiveObjectAccessInfo;

public class XASecureAuthorizer implements XaHiveAccessVerifier {
	
	private XaHiveAccessVerifier authDB = URLBasedAuthDB.getInstance() ;
	

	@Override
	public boolean isAccessAllowed(UserGroupInformation ugi, XaHiveObjectAccessInfo objAccessInfo) {
		if (authDB == null) {
			throw new AuthorizationException("No Authorization Agent is available for AuthorizationCheck") ;
		}
		return authDB.isAccessAllowed(ugi, objAccessInfo);
	}

	@Override
	public boolean isAudited(XaHiveObjectAccessInfo objAccessInfo) {
		if (authDB == null) {
			throw new AuthorizationException("No Authorization Agent is available for AuthorizationCheck") ;
		}
		return authDB.isAudited(objAccessInfo) ;
	}
}
