/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xasecure.pdp.knox;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xasecure.authorization.knox.KnoxAccessVerifier;

public class XASecureAuthorizer  implements KnoxAccessVerifier  {

	private static final Log LOG = LogFactory.getLog(XASecureAuthorizer.class) ;
	
	private static URLBasedAuthDB authDB = URLBasedAuthDB.getInstance() ;
	
	public XASecureAuthorizer() {
	}

	@Override
	public boolean isAccessAllowed(String topologyName, String serviceName, String accessType,
			String userName, Set<String> groups, String requestIp) {
		boolean accessAllowed = authDB.isAccessGranted(topologyName, serviceName, accessType, userName, groups, 
				requestIp);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Computed access permission for topology: " + topologyName +
					", service: " + serviceName +
					", access: " + accessType +
					", requestingIp: " +requestIp +
					", requestingUser: " + userName +
					", requestingUserGroups: " + groups +
					", permitted: " + accessAllowed);
		}
		return accessAllowed;
	}
	
	@Override
	public boolean isAuditEnabled(String topologyName, String serviceName) {
		boolean auditEnabled = authDB.isAuditEnabled(topologyName, serviceName);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Computed audit enabled for topology: " + topologyName +
					", service: " + serviceName +
					", auditLogEnabled: " + auditEnabled);
		}
		return auditEnabled;
	}

}
