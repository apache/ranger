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

 package org.apache.ranger.authorization.hive;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;


public class RangerHiveAccessContext {
	private String mClientIpAddress;
	private String mClientType;
	private String mCommandString;
	private String mSessionString;
	
	public RangerHiveAccessContext(HiveAuthzContext context, HiveAuthzSessionContext sessionContext) {
		if(context != null) {
			mClientIpAddress = context.getIpAddress();
			mCommandString   = context.getCommandString();
		}
		
		if(sessionContext != null) {
			mClientType      = sessionContext.getClientType().name();
			mSessionString   = sessionContext.getSessionString();
		}
	}

	public String getClientIpAddress() {
		return mClientIpAddress;
	}

	public void setClientIpAddress(String clientIpAddress) {
		this.mClientIpAddress = clientIpAddress;
	}

	public String getClientType() {
		return mClientType;
	}

	public void setClientType(String clientType) {
		this.mClientType = clientType;
	}

	public String getCommandString() {
		return mCommandString;
	}

	public void setCommandString(String commandString) {
		this.mCommandString = commandString;
	}

	public String getSessionString() {
		return mSessionString;
	}

	public void setSessionString(String sessionString) {
		this.mSessionString = sessionString;
	}
}
