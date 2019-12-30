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

package org.apache.ranger.plugin.policyengine;

import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.service.RangerAuthContext;
import org.apache.ranger.plugin.service.RangerAuthContextListener;

public class RangerPluginContext {
	private final RangerPluginConfig        config;
	private       RangerAuthContext         authContext;
	private       RangerAuthContextListener authContextListener;


	public RangerPluginContext(RangerPluginConfig config) {
		this.config = config;
	}

	public RangerPluginConfig getConfig() { return  config; }

	public String getClusterName() {
		return config.getClusterName();
	}

	public String getClusterType() {
		return config.getClusterType();
	}

	public RangerAuthContext getAuthContext() { return authContext; }

	public void setAuthContext(RangerAuthContext authContext) { this.authContext = authContext; }

	public void setAuthContextListener(RangerAuthContextListener authContextListener) { this.authContextListener = authContextListener; }

	public void notifyAuthContextChanged() {
		RangerAuthContextListener authContextListener = this.authContextListener;

		if (authContextListener != null) {
			authContextListener.contextChanged();
		}
	}

}
