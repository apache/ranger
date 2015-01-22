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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.ServicePolicies;


public class ServiceFileStore extends BaseFileStore implements ServiceStore {
	private static final Log LOG = LogFactory.getLog(ServiceFileStore.class);

	private long nextServiceDefId = 0;
	private long nextServiceId    = 0;
	private long nextPolicyId     = 0;

	static Map<String, Long> legacyServiceDefs = new HashMap<String, Long>();

	static {
		legacyServiceDefs.put("hdfs",  new Long(1));
		legacyServiceDefs.put("hbase", new Long(2));
		legacyServiceDefs.put("hive",  new Long(3));
		legacyServiceDefs.put("knox",  new Long(5));
		legacyServiceDefs.put("storm", new Long(6));
	}

	public ServiceFileStore() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.ServiceFileStore()");
		}

		init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.ServiceFileStore()");
		}
	}


	@Override
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.createServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef existing = findServiceDefByName(serviceDef.getName());
		
		if(existing != null) {
			throw new Exception(serviceDef.getName() + ": service-def already exists (id=" + existing.getId() + ")");
		}

		RangerServiceDef ret = null;

		try {
			preCreate(serviceDef);

			serviceDef.setId(nextServiceDefId++);

			ret = saveToFile(serviceDef, false);

			postCreate(ret);
		} catch(Exception excp) {
			LOG.warn("ServiceDefFileStore.createServiceDef(): failed to save service-def '" + serviceDef.getName() + "'", excp);

			throw new Exception("failed to save service-def '" + serviceDef.getName() + "'", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.createServiceDef(" + serviceDef + ")");
		}

		return ret;
	}

	@Override
	public RangerServiceDef updateServiceDef(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.updateServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef existing = findServiceDefById(serviceDef.getId());

		if(existing == null) {
			throw new Exception(serviceDef.getId() + ": service-def does not exist");
		}

		if(isLegacyServiceDef(existing)) {
			String msg = existing.getName() + ": is an in-built service-def. Update not allowed";

			LOG.warn(msg);

			throw new Exception(msg);
		}

		String existingName = existing.getName();

		boolean renamed = !StringUtils.equalsIgnoreCase(serviceDef.getName(), existingName);

		// renaming service-def would require updating services that refer to this service-def
		if(renamed) {
			LOG.warn("ServiceDefFileStore.updateServiceDef(): service-def renaming not supported. " + existingName + " ==> " + serviceDef.getName());

			throw new Exception("service-def renaming not supported. " + existingName + " ==> " + serviceDef.getName());
		}

		RangerServiceDef ret = null;

		try {
			existing.updateFrom(serviceDef);

			preUpdate(existing);

			ret = saveToFile(existing, true);

			postUpdate(ret);
		} catch(Exception excp) {
			LOG.warn("ServiceDefFileStore.updateServiceDef(): failed to save service-def '" + existing.getName() + "'", excp);

			throw new Exception("failed to save service-def '" + existing.getName() + "'", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.updateServiceDef(" + serviceDef + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteServiceDef(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.deleteServiceDef(" + id + ")");
		}

		RangerServiceDef existing = findServiceDefById(id);

		if(existing == null) {
			throw new Exception("service-def does not exist. id=" + id);
		}

		if(isLegacyServiceDef(existing)) {
			String msg = existing.getName() + ": is an in-built service-def. Update not allowed";

			LOG.warn(msg);

			throw new Exception(msg);
		}

		// TODO: deleting service-def would require deleting services that refer to this service-def

		try {
			preDelete(existing);

			Path filePath = new Path(getServiceDefFile(id));

			deleteFile(filePath);

			postDelete(existing);
		} catch(Exception excp) {
			throw new Exception("failed to delete service-def. id=" + id + "; name=" + existing.getName(), excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.deleteServiceDef(" + id + ")");
		}
	}

	@Override
	public RangerServiceDef getServiceDef(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.getServiceDef(" + id + ")");
		}

		RangerServiceDef ret = findServiceDefById(id);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.getServiceDef(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerServiceDef getServiceDefByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.getServiceDefByName(" + name + ")");
		}

		RangerServiceDef ret = findServiceDefByName(name);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.getServiceDefByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerServiceDef> getAllServiceDefs() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.getAllServiceDefs()");
		}

		List<RangerServiceDef> ret = new ArrayList<RangerServiceDef>();

		try {
			// load definitions for legacy services from embedded resources
			String[] legacyServiceDefResources = {
					"/service-defs/ranger-servicedef-hdfs.json",
					"/service-defs/ranger-servicedef-hive.json",
					"/service-defs/ranger-servicedef-hbase.json",
					"/service-defs/ranger-servicedef-knox.json",
					"/service-defs/ranger-servicedef-storm.json",
			};
			
			for(String resource : legacyServiceDefResources) {
				RangerServiceDef sd = loadFromResource(resource, RangerServiceDef.class);
				
				if(sd != null) {
					ret.add(sd);
				}
			}
			nextServiceDefId = getMaxId(ret) + 1;

			// load service definitions from file system
			List<RangerServiceDef> sds = loadFromDir(new Path(getDataDir()), FILE_PREFIX_SERVICE_DEF, RangerServiceDef.class);
			
			if(sds != null) {
				for(RangerServiceDef sd : sds) {
					if(sd != null) {
						if(isLegacyServiceDef(sd)) {
							LOG.warn("Found in-built service-def '" + sd.getName() + "'  under " + getDataDir() + ". Ignorning");

							continue;
						}
						
						// if the ServiceDef is already found, remove the earlier definition
						for(int i = 0; i < ret.size(); i++) {
							RangerServiceDef currSd = ret.get(i);
							
							if(StringUtils.equals(currSd.getName(), sd.getName()) ||
							   ObjectUtils.equals(currSd.getId(), sd.getId())) {
								ret.remove(i);
							}
						}

						ret.add(sd);
					}
				}
			}
			nextServiceDefId = getMaxId(ret) + 1;
		} catch(Exception excp) {
			LOG.error("ServiceDefFileStore.getAllServiceDefs(): failed to read service-defs", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.getAllServiceDefs(): count=" + (ret == null ? 0 : ret.size()));
		}

		if(ret != null) {
			Collections.sort(ret, RangerServiceDef.idComparator);
		}

		return ret;
	}


	@Override
	public RangerService createService(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.createService(" + service + ")");
		}

		RangerService existing = getServiceByName(service.getName());

		if(existing != null) {
			throw new Exception("service already exists - '" + service.getName() + "'. ID=" + existing.getId());
		}

		RangerService ret = null;

		try {
			preCreate(service);

			service.setId(nextServiceId++);

			ret = saveToFile(service, false);

			postCreate(service);
		} catch(Exception excp) {
			throw new Exception("failed to save service '" + service.getName() + "'", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.createService(" + service + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerService updateService(RangerService service) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.updateService(" + service + ")");
		}

		RangerService existing = getService(service.getId());

		if(existing == null) {
			throw new Exception("no service exists with ID=" + service.getId());
		}

		String existingName = existing.getName();

		boolean renamed = !StringUtils.equalsIgnoreCase(service.getName(), existingName);
		
		if(renamed) {
			RangerService newNameService = getServiceByName(service.getName());

			if(newNameService != null) {
				throw new Exception("another service already exists with name '" + service.getName() + "'. ID=" + newNameService.getId());
			}
		}

		RangerService ret = null;

		try {
			existing.updateFrom(service);

			preUpdate(existing);

			ret = saveToFile(existing, true);

			postUpdate(ret);

			if(renamed) {
				handleServiceRename(ret, existingName);
			}
		} catch(Exception excp) {
			throw new Exception("failed to update service '" + existing.getName() + "'", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.updateService(" + service + "): " + ret);
		}

		return ret;
	}

	@Override
	public void deleteService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.deleteService(" + id + ")");
		}

		RangerService existing = getService(id);

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
			LOG.debug("<== ServiceFileStore.deleteService(" + id + ")");
		}
	}

	@Override
	public RangerService getService(Long id) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getService(" + id + ")");
		}

		RangerService ret = null;

		try {
			Path filePath = new Path(getServiceFile(id));
	
			ret = loadFromFile(filePath,  RangerService.class);
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getService(" + id + "): failed to read service", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getService(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public RangerService getServiceByName(String name) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getServiceByName(" + name + ")");
		}

		RangerService ret = null;

		try {
			List<RangerService> services = getAllServices();

			if(services != null) {
				for(RangerService service : services) {
					if(StringUtils.equalsIgnoreCase(service.getName(), name)) {
						ret = service;

						break;
					}
				}
			}
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getServiceByName(" + name + "): failed to read service", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getServiceByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerService> getAllServices() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getAllServices()");
		}

		List<RangerService> ret = null;

		try {
			ret = loadFromDir(new Path(getDataDir()), FILE_PREFIX_SERVICE, RangerService.class);

			nextServiceId = getMaxId(ret) + 1;
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getAllServices(): failed to read services", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getAllServices(): count=" + (ret == null ? 0 : ret.size()));
		}

		if(ret != null) {
			Collections.sort(ret, RangerService.idComparator);
		}

		return ret;
	}

	@Override
	public RangerPolicy createPolicy(RangerPolicy policy) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.createPolicy(" + policy + ")");
		}

		RangerService service = getServiceByName(policy.getService());
		
		if(service == null) {
			throw new Exception("service does not exist - name=" + policy.getService());
		}

		RangerPolicy existing = findPolicyByName(policy.getService(), policy.getName());

		if(existing != null) {
			throw new Exception("policy already exists: ServiceName=" + policy.getService() + "; PolicyName=" + policy.getName() + ". ID=" + existing.getId());
		}

		RangerPolicy ret = null;

		try {
			preCreate(policy);

			policy.setId(nextPolicyId++);

			ret = saveToFile(policy, service.getId(), false);

			handlePolicyUpdate(service);

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

		RangerService service = getServiceByName(policy.getService());
		
		if(service == null) {
			throw new Exception("service does not exist - name=" + policy.getService());
		}

		if(! StringUtils.equalsIgnoreCase(existing.getService(), policy.getService())) {
			throw new Exception("policy id=" + policy.getId() + " already exists in service " + existing.getService() + ". It can not be moved to service " + policy.getService());
		}

		boolean renamed = !StringUtils.equalsIgnoreCase(policy.getName(), existing.getName());
		
		if(renamed) {
			RangerPolicy newNamePolicy = findPolicyByName(service.getName(), policy.getName());

			if(newNamePolicy != null) {
				throw new Exception("another policy already exists with name '" + policy.getName() + "'. ID=" + newNamePolicy.getId());
			}
		}

		RangerPolicy ret = null;

		try {
			existing.updateFrom(policy);

			preUpdate(existing);

			ret = saveToFile(existing, service.getId(), true);

			handlePolicyUpdate(service);

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

		RangerService service = getServiceByName(existing.getService());
		
		if(service == null) {
			throw new Exception("service does not exist - name='" + existing.getService());
		}

		try {
			preDelete(existing);

			Path filePath = new Path(getPolicyFile(service.getId(), existing.getId()));

			deleteFile(filePath);

			handlePolicyUpdate(service);

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
	public List<RangerPolicy> getAllPolicies() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getAllPolicies()");
		}

		List<RangerPolicy> ret = null;

		try {
			ret = loadFromDir(new Path(getDataDir()), FILE_PREFIX_POLICY, RangerPolicy.class);

			nextPolicyId  = getMaxId(ret) + 1;
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getAllPolicies(): failed to read policies", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getAllPolicies(): count=" + (ret == null ? 0 : ret.size()));
		}

		if(ret != null) {
			Collections.sort(ret, RangerPolicy.idComparator);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(Long serviceId) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getPolicies(" + serviceId + ")");
		}

		RangerService service = getService(serviceId);

		if(service == null) {
			throw new Exception("service does not exist - id='" + serviceId);
		}

		List<RangerPolicy> ret = getServicePolicies(service.getName());

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getPolicies(" + serviceId + "): " + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(String serviceName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getPolicies(" + serviceName + ")");
		}

		RangerService service = getServiceByName(serviceName);

		if(service == null) {
			throw new Exception("service does not exist - name='" + serviceName);
		}

		RangerServiceDef serviceDef = findServiceDefByName(service.getType());
		
		if(serviceDef == null) {
			throw new Exception(service.getType() + ": unknown service-def)");
		}

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		try {
			List<RangerPolicy> policies = getAllPolicies();

			if(policies != null) {
				for(RangerPolicy policy : policies) {
					if(StringUtils.equals(policy.getService(), serviceName)) {
						ret.add(policy);
					}
				}
			}
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getPolicies(" + serviceName + "): failed to read policies", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getPolicies(" + serviceName + "): count=" + ((ret == null) ? 0 : ret.size()));
		}

		if(ret != null) {
			Collections.sort(ret, RangerPolicy.idComparator);
		}

		return ret;
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		RangerService service = getServiceByName(serviceName);

		if(service == null) {
			throw new Exception("service does not exist - name='" + serviceName);
		}

		RangerServiceDef serviceDef = findServiceDefByName(service.getType());
		
		if(serviceDef == null) {
			throw new Exception(service.getType() + ": unknown service-def)");
		}

		ServicePolicies ret = new ServicePolicies();
		ret.setServiceId(service.getId());
		ret.setServiceName(service.getName());
		ret.setPolicyVersion(service.getPolicyVersion());
		ret.setPolicyUpdateTime(service.getPolicyUpdateTime());
		ret.setServiceDef(serviceDef);
		ret.setPolicies(new ArrayList<RangerPolicy>());

		if(lastKnownVersion == null || service.getPolicyVersion() == null || lastKnownVersion.longValue() != service.getPolicyVersion().longValue()) {

			try {
				List<RangerPolicy> policies = getAllPolicies();

				if(policies != null) {
					for(RangerPolicy policy : policies) {
						if(StringUtils.equals(policy.getService(), serviceName)) {
							ret.getPolicies().add(policy);
						}
					}
				}
			} catch(Exception excp) {
				LOG.error("ServiceFileStore.getServicePoliciesIfUpdated(" + serviceName + "): failed to read policies", excp);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		if(ret != null && ret.getPolicies() != null) {
			Collections.sort(ret.getPolicies(), RangerPolicy.idComparator);
		}

		return ret;
	}

	@Override
	protected void init() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.init()");
		}

		super.init();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.init()");
		}
	}


	private void handleServiceRename(RangerService service, String oldName) throws Exception {
		List<RangerPolicy> policies = getAllPolicies();

		if(policies != null) {
			for(RangerPolicy policy : policies) {
				if(StringUtils.equalsIgnoreCase(policy.getService(), oldName)) {
					policy.setService(service.getName());
	
					preUpdate(policy);
	
					saveToFile(policy, service.getId(), true);
	
					postUpdate(policy);
				}
			}
		}
	}

	private void handleServiceDelete(RangerService service) throws Exception {
		List<RangerPolicy> policies = getAllPolicies();

		if(policies != null) {
			for(RangerPolicy policy : policies) {
				if(! StringUtils.equals(policy.getService(), service.getName())) {
					continue;
				}

				preDelete(policy);

				Path filePath = new Path(getPolicyFile(service.getId(), policy.getId()));

				deleteFile(filePath);

				postDelete(policy);
			}
		}
	}

	private void handlePolicyUpdate(RangerService service) throws Exception {
		if(service == null) {
			return;
		}
		
		Long policyVersion = service.getPolicyVersion();

		if(policyVersion == null) {
			policyVersion = new Long(1);
		} else {
			policyVersion = new Long(policyVersion.longValue() + 1);
		}
		
		service.setPolicyVersion(policyVersion);
		service.setPolicyUpdateTime(new Date());

		saveToFile(service, true);
	}

	private RangerServiceDef findServiceDefById(long id) throws Exception {
		RangerServiceDef ret = null;

		List<RangerServiceDef> serviceDefs = getAllServiceDefs();

		for(RangerServiceDef sd : serviceDefs) {
			if(sd != null && sd.getId() != null && sd.getId().longValue() == id) {
				ret = sd;

				break;
			}
		}

		return ret;
	}

	private RangerServiceDef findServiceDefByName(String sdName) throws Exception {
		RangerServiceDef ret = null;

		List<RangerServiceDef> serviceDefs = getAllServiceDefs();

		for(RangerServiceDef sd : serviceDefs) {
			if(sd != null && StringUtils.equalsIgnoreCase(sd.getName(), sdName)) {
				ret = sd;

				break;
			}
		}

		return ret;
	}

	private RangerPolicy findPolicyByName(String serviceName, String policyName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.findPolicyByName(" + serviceName + ", " + policyName + ")");
		}

		RangerService service = getServiceByName(serviceName);

		if(service == null) {
			throw new Exception("service does not exist - name='" + serviceName);
		}

		RangerPolicy ret = null;

		try {
			List<RangerPolicy> policies = getAllPolicies();

			if(policies != null) {
				for(RangerPolicy policy : policies) {
					if(StringUtils.equals(policy.getService(),  service.getName()) &&
					   StringUtils.equals(policy.getName(), policyName)) {
						ret = policy;

						break;
					}
				}
			}
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.findPolicyByName(" + serviceName + ", " + policyName + "): failed to read policies", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.findPolicyByName(" + serviceName + ", " + policyName + "): " + ret);
		}

		return ret;
	}

	private boolean isLegacyServiceDef(RangerServiceDef sd) {
		return sd == null ? false : (isLegacyServiceDef(sd.getName()) || isLegacyServiceDef(sd.getId()));
	}

	private boolean isLegacyServiceDef(String name) {
		return name == null ? false : legacyServiceDefs.containsKey(name);
	}

	private boolean isLegacyServiceDef(Long id) {
		return id == null ? false : legacyServiceDefs.containsValue(id);
	}
}
