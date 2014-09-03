package com.xasecure.pdp.storm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.storm.XaStormAccessVerifier;

public class XASecureAuthorizer implements XaStormAccessVerifier {
	
	private static final Log LOG = LogFactory.getLog(XASecureAuthorizer.class) ;
	
	private static URLBasedAuthDB authDB = URLBasedAuthDB.getInstance() ;
	
	
	@Override
	public boolean isAccessAllowed(String aUserName, String[] aGroupName, String aOperationName, String aTopologyName) {
		boolean ret = false ;
		
		if (authDB != null) {
			ret = authDB.isAccessAllowed(aUserName, aGroupName, aOperationName, aTopologyName) ;
		}
		else {
			LOG.error("Unable to find a URLBasedAuthDB for authorization - Found null");
		}
		
		return ret ;
	}

	@Override
	public boolean isAudited(String aTopologyName) {
		boolean ret = false ;
		
		if (authDB != null) {
			ret = authDB.isAudited(aTopologyName) ;
		}
		else {
			LOG.error("Unable to find a URLBasedAuthDB for authorization - Found null");
		}
		
		return ret ;
	}

}
