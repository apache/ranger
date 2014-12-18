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

package org.apache.ranger.plugin.manager;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.file.ServiceFileStore;


public class ServiceManager {
	private static final Log LOG = LogFactory.getLog(ServiceManager.class);

	private ServiceStore svcStore = null;

	public ServiceManager() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.ServiceManager()");
		}

		init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.ServiceManager()");
		}
	}

	public RangerService create(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.create(" + service + ")");
		}

		RangerService ret = svcStore.create(service);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.create(" + service + "): " + ret);
		}

		return ret;
	}

	public RangerService update(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.update(" + service + ")");
		}

		RangerService ret = svcStore.update(service);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.update(" + service + "): " + ret);
		}

		return ret;
	}

	public void delete(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.delete(" + id + ")");
		}

		svcStore.delete(id);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.delete(" + id + ")");
		}
	}

	public RangerService get(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.get(" + id + ")");
		}

		RangerService ret = svcStore.get(id);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.get(" + id + "): " + ret);
		}

		return ret;
	}

	public RangerService getByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.getByName(" + name + ")");
		}

		RangerService ret = svcStore.getByName(name);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.getByName(" + name + "): " + ret);
		}

		return ret;
	}

	public List<RangerService> getAll() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.getAll()");
		}

		List<RangerService> ret = svcStore.getAll();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.getAll(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	public void validateConfig(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.validateConfig(" + service + ")");
		}

		// TODO: call validateConfig() on the implClass

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.validateConfig(" + service + ")");
		}
	}

	public RangerPolicy createPolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.createPolicy(" + policy + ")");
		}

		RangerPolicy ret = svcStore.createPolicy(policy);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.createPolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	public RangerPolicy updatePolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.updatePolicy(" + policy + ")");
		}

		RangerPolicy ret = svcStore.updatePolicy(policy);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.updatePolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	public void deletePolicy(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.deletePolicy(" + id + ")");
		}

		svcStore.deletePolicy(id);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.deletePolicy(" + id + ")");
		}
	}

	public RangerPolicy getPolicy(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.getPolicy(" + id + ")");
		}

		RangerPolicy ret = svcStore.getPolicy(id);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.getPolicy(" + id + "): " + ret);
		}

		return ret;
	}

	public List<RangerPolicy> getPolicies(Long svcId) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.getPolicies(" + svcId + ")");
		}

		List<RangerPolicy> ret = svcStore.getServicePolicies(svcId);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.getPolicies(" + svcId + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	public RangerPolicy getPolicyByName(String svcName, String policyName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.getPolicyByName(" + svcName + "," + policyName + ")");
		}

		RangerPolicy ret = svcStore.getPolicyByName(svcName, policyName);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.getPolicyByName(" + svcName + "," + policyName + "): " + ret);
		}

		return ret;
	}

	public List<RangerPolicy> getAllPolicies() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.getAllPolicies()");
		}

		List<RangerPolicy> ret = svcStore.getAllPolicies();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== getAllPolicies.getAll(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	private void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceManager.init()");
		}

		svcStore = new ServiceFileStore(); // TODO: store type should be configurable

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceManager.init()");
		}
	}
}
