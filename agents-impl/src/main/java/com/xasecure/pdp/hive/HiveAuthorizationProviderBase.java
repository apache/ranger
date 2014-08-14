/**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *
 *                                                                        *
 * Copyright (c) 2013 XASecure, Inc.  All rights reserved.                *
 *                                                                        *
 *************************************************************************/

 /**
  *
  *	@version: 1.0.004
  *
  */

package com.xasecure.pdp.hive;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.security.UserGroupInformation;

import com.xasecure.authorization.hive.XaHiveAccessVerifier;
import com.xasecure.authorization.hive.XaHiveObjectAccessInfo;

public class HiveAuthorizationProviderBase implements XaHiveAccessVerifier {

	private static final Log LOG = LogFactory.getLog(HiveAuthorizationProviderBase.class);

	protected HiveAuthDB authDB = new HiveAuthDB()  ;

	
	public HiveAuthDB getAuthDB() {
		return authDB ;
	}

	@Override
	public boolean isAccessAllowed(UserGroupInformation ugi, XaHiveObjectAccessInfo objAccessInfo) {
		HiveAuthDB ldb = authDB ;

		if (ldb == null) {
			throw new AuthorizationException("No Authorization Agent is available for AuthorizationCheck") ;
		}
		
		boolean ret = ldb.isAccessAllowed(ugi, objAccessInfo);
		
		return ret;
	}

	@Override
	public boolean isAudited(XaHiveObjectAccessInfo objAccessInfo) {
		HiveAuthDB ldb = authDB ;

		if (ldb == null) {
			throw new AuthorizationException("No Authorization Agent is available for AuthorizationCheck") ;
		}

		return ldb.isAudited(objAccessInfo) ;
	}
}
