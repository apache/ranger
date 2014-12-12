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

 package org.apache.ranger.pdp.hive;

import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hive.RangerHiveAccessVerifier;
import org.apache.ranger.authorization.hive.RangerHiveObjectAccessInfo;

public class RangerAuthorizer implements RangerHiveAccessVerifier {
	
	private RangerHiveAccessVerifier authDB = URLBasedAuthDB.getInstance() ;
	

	@Override
	public boolean isAccessAllowed(UserGroupInformation ugi, RangerHiveObjectAccessInfo objAccessInfo) {
		if (authDB == null) {
			throw new AuthorizationException("No Authorization Agent is available for AuthorizationCheck") ;
		}
		return authDB.isAccessAllowed(ugi, objAccessInfo);
	}

	@Override
	public boolean isAudited(RangerHiveObjectAccessInfo objAccessInfo) {
		if (authDB == null) {
			throw new AuthorizationException("No Authorization Agent is available for AuthorizationCheck") ;
		}
		return authDB.isAudited(objAccessInfo) ;
	}
}
