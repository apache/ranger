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

package org.apache.ranger.plugin.store.file;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.store.ServiceStore;


public class ServiceFileStore extends BaseFileStore implements ServiceStore {
	private static final Log LOG = LogFactory.getLog(ServiceFileStore.class);

	private long nextServiceId = 0;
	private long nextPolicyId  = 0;

	public ServiceFileStore() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.ServiceManagerFile()");
		}

		init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.ServiceManagerFile()");
		}
	}

	@Override
	public RangerService create(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.create(" + service + ")");
		}

		RangerService existing = getByName(service.getName());

		if(existing != null) {
			throw new Exception("service already exists - '" + service.getName() + "'. ID=" + existing.getId());
		}

		RangerService ret = null;

		try {
			preCreate(service);

			service.setId(nextServiceId++);

			Path filePath = new Path(getServiceFile(service.getId()));

			ret = saveToFile(service, filePath, false);

			postCreate(service);
		} catch(Exception excp) {
			throw new Exception("failed to save service '" + service.getName() + "'", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.create(" + service + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerService update(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.update(" + service + ")");
		}

		RangerService existing = get(service.getId());

		if(existing == null) {
			throw new Exception("no service exists with ID=" + service.getId());
		}

		String existingName = existing.getName();

		boolean renamed = !service.getName().equalsIgnoreCase(existingName);
		
		if(renamed) {
			RangerService newNameService = getByName(service.getName());

			if(newNameService != null) {
				throw new Exception("another service already exists with name '" + service.getName() + "'. ID=" + newNameService.getId());
			}
		}

		RangerService ret = null;

		try {
			existing.updateFrom(service);

			preUpdate(existing);

			Path filePath = new Path(getServiceFile(existing.getId()));

			ret = saveToFile(existing, filePath, true);

			postUpdate(ret);

			if(renamed) {
				handleServiceRename(ret, existingName);
			}
		} catch(Exception excp) {
			throw new Exception("failed to update service '" + existing.getName() + "'", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.update(" + service + "): " + ret);
		}

		return ret;
	}

	@Override
	public void delete(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.delete(" + id + ")");
		}

		RangerService existing = get(id);

		if(existing == null) {
			throw new Exception("no service exists with ID=" + id);
		}

		try {
			Path filePath = new Path(getServiceFile(id));

			preDelete(existing);

			handleServiceDelete(existing);

			deleteFile(filePath);

			postDelete(existing);
		} catch(Exception excp) {
			throw new Exception("failed to delete service with ID=" + id, excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.delete(" + id + ")");
		}
	}

	@Override
	public RangerService get(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.get(" + id + ")");
		}

		RangerService ret = null;

		try {
			Path filePath = new Path(getServiceFile(id));
	
			ret = loadFromFile(filePath,  RangerService.class);
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.get(" + id + "): failed to read service", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.get(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerService getByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getByName(" + name + ")");
		}

		RangerService ret = null;

		try {
			List<RangerService> services = getAll();

			if(services != null) {
				for(RangerService service : services) {
					if(service.getName().equalsIgnoreCase(name)) {
						ret = service;
	
						break;
					}
				}
			}
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getByName(" + name + "): failed to read service", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerService> getAll() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getAll()");
		}

		List<RangerService> ret = null;

		try {
			ret = loadFromDir(new Path(getDataDir()), FILE_PREFIX_SERVICE, RangerService.class);
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getAll(): failed to read services", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getAll(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public RangerPolicy createPolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.createPolicy(" + policy + ")");
		}

		RangerService service = getByName(policy.getService());
		
		if(service == null) {
			throw new Exception("service does not exist - name=" + policy.getService());
		}

		RangerPolicy existing = getPolicyByName(policy.getService(), policy.getName());

		if(existing != null) {
			throw new Exception("policy already exists: ServiceName=" + policy.getService() + "; PolicyName=" + policy.getName() + ". ID=" + existing.getId());
		}

		RangerPolicy ret = null;

		try {
			preCreate(policy);

			policy.setId(nextPolicyId++);

			Path filePath = new Path(getPolicyFile(service.getId(), policy.getId()));

			ret = saveToFile(policy, filePath, false);

			postCreate(ret);
		} catch(Exception excp) {
			throw new Exception("failed to save policy: ServiceName=" + policy.getService() + "; PolicyName=" + policy.getName(), excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.createPolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerPolicy updatePolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.updatePolicy(" + policy + ")");
		}

		RangerPolicy existing = getPolicy(policy.getId());

		if(existing == null) {
			throw new Exception("no policy exists with ID=" + policy.getId());
		}

		RangerService service = getByName(policy.getService());
		
		if(service == null) {
			throw new Exception("service does not exist - name=" + policy.getService());
		}

		if(! existing.getService().equalsIgnoreCase(policy.getService())) {
			throw new Exception("policy id=" + policy.getId() + " already exists in service " + existing.getService() + ". It can not be moved to service " + policy.getService());
		}

		boolean renamed = !policy.getName().equalsIgnoreCase(existing.getName());
		
		if(renamed) {
			RangerPolicy newNamePolicy = getPolicyByName(service.getName(), policy.getName());

			if(newNamePolicy != null) {
				throw new Exception("another policy already exists with name '" + policy.getName() + "'. ID=" + newNamePolicy.getId());
			}
		}

		RangerPolicy ret = null;

		try {
			existing.updateFrom(policy);

			preUpdate(existing);

			Path filePath = new Path(getPolicyFile(service.getId(), existing.getId()));

			ret = saveToFile(existing, filePath, true);

			postUpdate(ret);
		} catch(Exception excp) {
			throw new Exception("failed to update policy - ID=" + existing.getId(), excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.updatePolicy(" + policy + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deletePolicy(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.deletePolicy(" + id + ")");
		}

		RangerPolicy existing = getPolicy(id);

		if(existing == null) {
			throw new Exception("no policy exists with ID=" + id);
		}

		RangerService service = getByName(existing.getService());
		
		if(service == null) {
			throw new Exception("service does not exist - name='" + existing.getService());
		}

		try {
			preDelete(existing);

			Path filePath = new Path(getPolicyFile(service.getId(), existing.getId()));

			deleteFile(filePath);

			postDelete(existing);
		} catch(Exception excp) {
			throw new Exception(existing.getId() + ": failed to delete policy", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.deletePolicy(" + id + ")");
		}
	}

	@Override
	public RangerPolicy getPolicy(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getPolicy(" + id + ")");
		}

		RangerPolicy ret = null;

		try {
			List<RangerPolicy> policies = getAllPolicies();

			if(policies != null) {
				for(RangerPolicy policy : policies) {
					if(policy.getId().equals(id)) {
						ret = policy;
	
						break;
					}
				}
			}
		} catch(Exception excp) {
			throw new Exception(id + ": failed to read policy", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getPolicy(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerPolicy getPolicyByName(String serviceName, String policyName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getPolicyByName(" + serviceName + ", " + policyName + ")");
		}

		RangerService service = getByName(serviceName);

		if(service == null) {
			throw new Exception("service does not exist - name='" + serviceName);
		}

		RangerPolicy ret = null;

		try {
			List<RangerPolicy> policies = getServicePolicies(service.getId());

			if(policies != null) {
				for(RangerPolicy policy : policies) {
					if(policy.getName().equals(policyName)) {
						ret = policy;

						break;
					}
				}
			}
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getPolicyByName(" + serviceName + ", " + policyName + "): failed to read policies", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getPolicyByName(" + serviceName + ", " + policyName + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(String serviceName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getPolicies(" + serviceName + ")");
		}

		RangerService service = getByName(serviceName);

		if(service == null) {
			throw new Exception("service does not exist - name='" + serviceName);
		}

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		try {
			List<RangerPolicy> policies = getAllPolicies();

			if(policies != null) {
				for(RangerPolicy policy : policies) {
					if(policy.getService().equals(serviceName)) {
						ret.add(policy);
					}
				}
			}
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getPolicies(" + serviceName + "): failed to read policies", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getPolicies(" + serviceName + "): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(Long serviceId) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getPolicies(" + serviceId + ")");
		}

		RangerService service = get(serviceId);

		if(service == null) {
			throw new Exception("service does not exist - id='" + serviceId);
		}

		List<RangerPolicy> ret = getServicePolicies(service.getName());

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getPolicies(" + serviceId + "): " + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getAllPolicies() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getAllPolicies()");
		}

		List<RangerPolicy> ret = null;

		try {
			ret = loadFromDir(new Path(getDataDir()), FILE_PREFIX_POLICY, RangerPolicy.class);
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getAllPolicies(): failed to read policies", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getAllPolicies(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	protected void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.init()");
		}

		super.init();

		try {
			List<RangerService> services = loadFromDir(new Path(getDataDir()), FILE_PREFIX_SERVICE, RangerService.class);
			List<RangerPolicy>  policies = loadFromDir(new Path(getDataDir()), FILE_PREFIX_POLICY, RangerPolicy.class);

			nextServiceId = getMaxId(services) + 1;
			nextPolicyId  = getMaxId(policies) + 1;
		} catch(Exception excp) {
			LOG.error("ServiceDefFileStore.init() failed", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.init()");
		}
	}

	private void handleServiceRename(RangerService service, String oldName) throws Exception {
		List<RangerPolicy> policies = getAllPolicies();

		if(policies != null) {
			for(RangerPolicy policy : policies) {
				if(policy.getService().equalsIgnoreCase(oldName)) {
					policy.setService(service.getName());
	
					preUpdate(policy);
	
					Path filePath = new Path(getPolicyFile(service.getId(), policy.getId()));
	
					saveToFile(policy, filePath, true);
	
					postUpdate(policy);
				}
			}
		}
	}

	private void handleServiceDelete(RangerService service) throws Exception {
		List<RangerPolicy> policies = getServicePolicies(service.getName());

		if(policies != null) {
			for(RangerPolicy policy : policies) {
				preDelete(policy);

				Path filePath = new Path(getPolicyFile(service.getId(), policy.getId()));

				deleteFile(filePath);

				postDelete(policy);
			}
		}
	}
}
