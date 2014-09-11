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

 /**************************************************************************
 *                                                                        *
 * The information in this document is proprietary to XASecure Inc.,      *
 * It may not be used, reproduced or disclosed without the written        *
 * approval from the XASecure Inc.,                                       *
 *                                                                        *
 * PRIVILEGED AND CONFIDENTIAL XASECURE PROPRIETARY INFORMATION           *

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
