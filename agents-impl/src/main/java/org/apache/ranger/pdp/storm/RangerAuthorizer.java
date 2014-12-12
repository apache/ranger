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

 package org.apache.ranger.pdp.storm;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.authorization.storm.RangerStormAccessVerifier;

public class RangerAuthorizer implements RangerStormAccessVerifier {
	
	private static final Log LOG = LogFactory.getLog(RangerAuthorizer.class) ;
	
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
