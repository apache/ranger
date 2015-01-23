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

package org.apache.ranger.plugin.service;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.ServiceStoreFactory;
import org.apache.ranger.plugin.util.PolicyRefresher;


public class RangerBasePlugin {
	private boolean         initDone    = false;
	private String          serviceType = null;
	private PolicyRefresher refresher   = null;

	
	public RangerBasePlugin(String serviceType) {
		this.serviceType = serviceType;
	}

	public RangerPolicyEngine getPolicyEngine() {
		return refresher == null ? null : refresher.getPolicyEngine();
	}

	public String getServiceName() {
		return refresher == null ? null : refresher.getServiceName();
	}

	public boolean init(RangerPolicyEngine policyEngine) {
		if(!initDone) {
			synchronized(this) {
				if(! initDone) {
					String serviceName = null;

					// get the serviceName from download URL: http://ranger-admin-host:port/service/assets/policyList/serviceName
					String policyDownloadUrl = RangerConfiguration.getInstance().get("xasecure." + serviceType + ".policymgr.url");

					if(! StringUtils.isEmpty(policyDownloadUrl)) {
						int idx = policyDownloadUrl.lastIndexOf('/');

						if(idx != -1) {
							serviceName = policyDownloadUrl.substring(idx + 1);
						}
					}

					if(StringUtils.isEmpty(serviceName)) {
						serviceName = RangerConfiguration.getInstance().get("ranger.plugin." + serviceType + ".service.name");
					}

					ServiceStore serviceStore = ServiceStoreFactory.instance().getServiceStore();

					refresher = new PolicyRefresher(policyEngine, serviceName, serviceStore);

					refresher.startRefresher();

					initDone = true;
				}
			}
		}

		return initDone;
	}

	public void cleanup() {
		PolicyRefresher refresher = this.refresher;

		if(refresher != null) {
			refresher.stopRefresher();
		}
	}
}
