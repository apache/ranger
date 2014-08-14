package com.xasecure.authorization.hive;

import org.apache.hadoop.security.UserGroupInformation;


public interface XaHiveAccessVerifier {
	public boolean isAccessAllowed(UserGroupInformation ugi, XaHiveObjectAccessInfo objAccessInfo) ;
	
	public boolean isAudited(XaHiveObjectAccessInfo objAccessInfo) ;
}
