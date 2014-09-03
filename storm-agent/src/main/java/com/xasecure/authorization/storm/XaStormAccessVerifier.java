package com.xasecure.authorization.storm;

public interface XaStormAccessVerifier {
	public boolean isAccessAllowed(String userName, String[] groups, String operation, String aTopologyName) ;
	public boolean isAudited(String aTopologyName) ;

}
