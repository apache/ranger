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
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.SearchFilter;
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

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.ServiceFileStore()");
		}
	}

	@Override
	public void init() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.init()");
		}

		super.initStore();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.init()");
		}
	}

	@Override
	public RangerServiceDef createServiceDef(RangerServiceDef serviceDef) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.createServiceDef(" + serviceDef + ")");
		}

		RangerServiceDef existing = getServiceDefByName(serviceDef.getName());
		
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

		RangerServiceDef existing = getServiceDef(serviceDef.getId());

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

		RangerServiceDef existing = getServiceDef(id);

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

		RangerServiceDef ret = null;

		if(id != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.SERVICE_TYPE_ID, id.toString());

			List<RangerServiceDef> serviceDefs = getServiceDefs(filter);

			ret = CollectionUtils.isEmpty(serviceDefs) ? null : serviceDefs.get(0);
		}

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

		RangerServiceDef ret = null;

		if(name != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.SERVICE_TYPE, name);

			List<RangerServiceDef> serviceDefs = getServiceDefs(filter);

			ret = CollectionUtils.isEmpty(serviceDefs) ? null : serviceDefs.get(0);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.getServiceDefByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerServiceDef> getServiceDefs(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.getServiceDefs()");
		}

		List<RangerServiceDef> ret = getAllServiceDefs();

		if(ret != null && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, getPredicate(filter));

			Comparator<RangerBaseModelObject> comparator = getSorter(filter);

			if(comparator != null) {
				Collections.sort(ret, comparator);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceDefFileStore.getServiceDefs(): count=" + (ret == null ? 0 : ret.size()));
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

		if(name != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.SERVICE_NAME, name);

			List<RangerService> services = getServices(filter);

			ret = CollectionUtils.isEmpty(services) ? null : services.get(0);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getServiceByName(" + name + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerService> getServices(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getServices()");
		}

		List<RangerService> ret = getAllServices();

		if(ret != null && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, getPredicate(filter));

			Comparator<RangerBaseModelObject> comparator = getSorter(filter);

			if(comparator != null) {
				Collections.sort(ret, comparator);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getServices(): count=" + (ret == null ? 0 : ret.size()));
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

		if(id != null) {
			SearchFilter filter = new SearchFilter(SearchFilter.POLICY_ID, id.toString());

			List<RangerPolicy> policies = getPolicies(filter);

			ret = CollectionUtils.isEmpty(policies) ? null : policies.get(0);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getPolicy(" + id + "): " + ret);
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getPolicies(SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getPolicies()");
		}

		List<RangerPolicy> ret = getAllPolicies();

		if(ret != null && filter != null && !filter.isEmpty()) {
			CollectionUtils.filter(ret, getPredicate(filter));

			Comparator<RangerBaseModelObject> comparator = getSorter(filter);

			if(comparator != null) {
				Collections.sort(ret, comparator);
			}
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getPolicies(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getServicePolicies(" + serviceId + ")");
		}

		RangerService service = getService(serviceId);

		if(service == null) {
			throw new Exception("service does not exist - id='" + serviceId);
		}

		List<RangerPolicy> ret = getServicePolicies(service.getName(), filter);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getServicePolicies(" + serviceId + "): " + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public List<RangerPolicy> getServicePolicies(String serviceName, SearchFilter filter) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getServicePolicies(" + serviceName + ")");
		}

		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		try {
			if(filter == null) {
				filter = new SearchFilter();
			}

			filter.setParam(SearchFilter.SERVICE_NAME, serviceName);

			ret = getPolicies(filter);
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getServicePolicies(" + serviceName + "): failed to read policies", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getServicePolicies(" + serviceName + "): count=" + ((ret == null) ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public ServicePolicies getServicePoliciesIfUpdated(String serviceName, Long lastKnownVersion) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + ")");
		}

		ServicePolicies ret = null;

		RangerService service = getServiceByName(serviceName);

		if(service == null) {
			throw new Exception("service does not exist - name=" + serviceName);
		}

		RangerServiceDef serviceDef = getServiceDefByName(service.getType());
		
		if(serviceDef == null) {
			throw new Exception(service.getType() + ": unknown service-def)");
		}

		if(lastKnownVersion == null || service.getPolicyVersion() == null || lastKnownVersion.longValue() != service.getPolicyVersion().longValue()) {
			SearchFilter filter = new SearchFilter(SearchFilter.SERVICE_NAME, serviceName);

			List<RangerPolicy> policies = getPolicies(filter);

			ret = new ServicePolicies();

			ret.setServiceId(service.getId());
			ret.setServiceName(service.getName());
			ret.setPolicyVersion(service.getPolicyVersion());
			ret.setPolicyUpdateTime(service.getPolicyUpdateTime());
			ret.setPolicies(policies);
			ret.setServiceDef(serviceDef);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getServicePoliciesIfUpdated(" + serviceName + ", " + lastKnownVersion + "): count=" + ((ret == null || ret.getPolicies() == null) ? 0 : ret.getPolicies().size()));
		}

		if(ret != null && ret.getPolicies() != null) {
			Collections.sort(ret.getPolicies(), idComparator);
		}

		return ret;
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

	private RangerPolicy findPolicyByName(String serviceName, String policyName) throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.findPolicyByName(" + serviceName + ", " + policyName + ")");
		}

		RangerService service = getServiceByName(serviceName);

		if(service == null) {
			throw new Exception("service does not exist - name='" + serviceName);
		}

		RangerPolicy ret = null;

		SearchFilter filter = new SearchFilter();

		filter.setParam(SearchFilter.SERVICE_NAME, serviceName);
		filter.setParam(SearchFilter.POLICY_NAME, policyName);

		List<RangerPolicy> policies = getPolicies(filter);

		ret = CollectionUtils.isEmpty(policies) ? null : policies.get(0);

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

	private List<RangerServiceDef> getAllServiceDefs() throws Exception {
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
			Collections.sort(ret, idComparator);

			for(RangerServiceDef sd : ret) {
				Collections.sort(sd.getResources(), resourceLevelComparator);
			}
		}

		return ret;
	}

	private List<RangerService> getAllServices() throws Exception {
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
			Collections.sort(ret, idComparator);
		}

		return ret;
	}

	private List<RangerPolicy> getAllPolicies() throws Exception {
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

		if(ret != null) {
			Collections.sort(ret, idComparator);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getAllPolicies(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	private String getServiceType(String serviceName) {
		RangerService service = null;

		try {
			service = getServiceByName(serviceName);
		} catch(Exception excp) {
			// ignore
		}

		return service != null ? service.getType() : null;
	}

	private Long getServiceId(String serviceName) {
		RangerService service = null;

		try {
			service = getServiceByName(serviceName);
		} catch(Exception excp) {
			// ignore
		}

		return service != null ? service.getId() : null;
	}

	private final static Comparator<RangerBaseModelObject> idComparator = new Comparator<RangerBaseModelObject>() {
		@Override
		public int compare(RangerBaseModelObject o1, RangerBaseModelObject o2) {
			Long val1 = (o1 != null) ? o1.getId() : null;
			Long val2 = (o2 != null) ? o2.getId() : null;

			return ObjectUtils.compare(val1, val2);
		}
	};

	private final static Comparator<RangerBaseModelObject> createTimeComparator = new Comparator<RangerBaseModelObject>() {
		@Override
		public int compare(RangerBaseModelObject o1, RangerBaseModelObject o2) {
			Date val1 = (o1 != null) ? o1.getCreateTime() : null;
			Date val2 = (o2 != null) ? o2.getCreateTime() : null;

			return ObjectUtils.compare(val1, val2);
		}
	};

	private final static Comparator<RangerBaseModelObject> updateTimeComparator = new Comparator<RangerBaseModelObject>() {
		@Override
		public int compare(RangerBaseModelObject o1, RangerBaseModelObject o2) {
			Date val1 = (o1 != null) ? o1.getUpdateTime() : null;
			Date val2 = (o2 != null) ? o2.getUpdateTime() : null;

			return ObjectUtils.compare(val1, val2);
		}
	};

	private final static Comparator<RangerBaseModelObject> serviceDefNameComparator = new Comparator<RangerBaseModelObject>() {
		@Override
		public int compare(RangerBaseModelObject o1, RangerBaseModelObject o2) {
			String val1 = null;
			String val2 = null;

			if(o1 != null) {
				if(o1 instanceof RangerServiceDef) {
					val1 = ((RangerServiceDef)o1).getName();
				} else if(o1 instanceof RangerService) {
					val1 = ((RangerService)o1).getType();
				}
			}

			if(o2 != null) {
				if(o2 instanceof RangerServiceDef) {
					val2 = ((RangerServiceDef)o2).getName();
				} else if(o2 instanceof RangerService) {
					val2 = ((RangerService)o2).getType();
				}
			}

			return ObjectUtils.compare(val1, val2);
		}
	};

	private final static Comparator<RangerBaseModelObject> serviceNameComparator = new Comparator<RangerBaseModelObject>() {
		@Override
		public int compare(RangerBaseModelObject o1, RangerBaseModelObject o2) {
			String val1 = null;
			String val2 = null;

			if(o1 != null) {
				if(o1 instanceof RangerPolicy) {
					val1 = ((RangerPolicy)o1).getService();
				} else if(o1 instanceof RangerService) {
					val1 = ((RangerService)o1).getType();
				}
			}

			if(o2 != null) {
				if(o2 instanceof RangerPolicy) {
					val2 = ((RangerPolicy)o2).getService();
				} else if(o2 instanceof RangerService) {
					val2 = ((RangerService)o2).getType();
				}
			}

			return ObjectUtils.compare(val1, val2);
		}
	};

	private final static Comparator<RangerBaseModelObject> policyNameComparator = new Comparator<RangerBaseModelObject>() {
		@Override
		public int compare(RangerBaseModelObject o1, RangerBaseModelObject o2) {
			String val1 = (o1 != null && o1 instanceof RangerPolicy) ? ((RangerPolicy)o1).getName() : null;
			String val2 = (o2 != null && o2 instanceof RangerPolicy) ? ((RangerPolicy)o2).getName() : null;

			return ObjectUtils.compare(val1, val2);
		}
	};

	private final static Comparator<RangerResourceDef> resourceLevelComparator = new Comparator<RangerResourceDef>() {
		@Override
		public int compare(RangerResourceDef o1, RangerResourceDef o2) {
			Integer val1 = (o1 != null) ? o1.getLevel() : null;
			Integer val2 = (o2 != null) ? o2.getLevel() : null;

			return ObjectUtils.compare(val1, val2);
		}
	};

	private Predicate getPredicate(SearchFilter filter) {
		if(filter == null || filter.isEmpty()) {
			return null;
		}

		List<Predicate> predicates = new ArrayList<Predicate>();

		addPredicateForLoginUser(filter.getParam(SearchFilter.LOGIN_USER), predicates);
		addPredicateForServiceType(filter.getParam(SearchFilter.SERVICE_TYPE), predicates);
		addPredicateForServiceTypeId(filter.getParam(SearchFilter.SERVICE_TYPE_ID), predicates);
		addPredicateForServiceName(filter.getParam(SearchFilter.SERVICE_NAME), predicates);
		addPredicateForServiceId(filter.getParam(SearchFilter.SERVICE_ID), predicates);
		addPredicateForPolicyName(filter.getParam(SearchFilter.POLICY_NAME), predicates);
		addPredicateForPolicyId(filter.getParam(SearchFilter.POLICY_ID), predicates);
		addPredicateForStatus(filter.getParam(SearchFilter.STATUS), predicates);
		addPredicateForUserName(filter.getParam(SearchFilter.USER), predicates);
		addPredicateForGroupName(filter.getParam(SearchFilter.GROUP), predicates);
		addPredicateForResources(filter.getParamsWithPrefix(SearchFilter.RESOURCE_PREFIX, true), predicates);

		Predicate ret = CollectionUtils.isEmpty(predicates) ? null : PredicateUtils.allPredicate(predicates);

		return ret;
	}

	private static Map<String, Comparator<RangerBaseModelObject>> sorterMap  = new HashMap<String, Comparator<RangerBaseModelObject>>();

	static {
		sorterMap.put(SearchFilter.SERVICE_TYPE, serviceDefNameComparator);
		sorterMap.put(SearchFilter.SERVICE_TYPE_ID, idComparator);
		sorterMap.put(SearchFilter.SERVICE_NAME, serviceNameComparator);
		sorterMap.put(SearchFilter.SERVICE_TYPE_ID, idComparator);
		sorterMap.put(SearchFilter.POLICY_NAME, policyNameComparator);
		sorterMap.put(SearchFilter.POLICY_ID, idComparator);
		sorterMap.put(SearchFilter.CREATE_TIME, createTimeComparator);
		sorterMap.put(SearchFilter.UPDATE_TIME, updateTimeComparator);
	}

	private Comparator<RangerBaseModelObject> getSorter(SearchFilter filter) {
		String sortBy = filter == null ? null : filter.getParam(SearchFilter.SORT_BY);

		if(StringUtils.isEmpty(sortBy)) {
			return null;
		}

		Comparator<RangerBaseModelObject> ret = sorterMap.get(sortBy);

		return ret;
	}

	private Predicate addPredicateForLoginUser(final String loginUser, List<Predicate> predicates) {
		if(StringUtils.isEmpty(loginUser)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;

					for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
						if(!policyItem.getDelegateAdmin()) {
							continue;
						}

						if(policyItem.getUsers().contains(loginUser)) { // TODO: group membership check
							ret = true;

							break;
						}
					}
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(ret != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForServiceType(final String serviceType, List<Predicate> predicates) {
		if(StringUtils.isEmpty(serviceType)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;

					ret = StringUtils.equals(serviceType, getServiceType(policy.getService()));
				} else if(object instanceof RangerService) {
					RangerService service = (RangerService)object;

					ret = StringUtils.equals(serviceType, service.getType());
				} else if(object instanceof RangerServiceDef) {
					RangerServiceDef serviceDef = (RangerServiceDef)object;

					ret = StringUtils.equals(serviceType, serviceDef.getName());
				}

				return ret;
			}
		};

		if(predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForServiceTypeId(final String serviceTypeId, List<Predicate> predicates) {
		if(StringUtils.isEmpty(serviceTypeId)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerServiceDef) {
					RangerServiceDef serviceDef = (RangerServiceDef)object;
					Long             svcDefId   = serviceDef.getId();

					if(svcDefId != null) {
						ret = StringUtils.equals(serviceTypeId, svcDefId.toString());
					}
				} else {
					ret = true;
				}

				return ret;
			}
		};
		
		if(predicates != null) {
			predicates.add(ret);
		}
		
		return ret;
	}

	private Predicate addPredicateForServiceName(final String serviceName, List<Predicate> predicates) {
		if(StringUtils.isEmpty(serviceName)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;

					ret = StringUtils.equals(serviceName, policy.getService());
				} else if(object instanceof RangerService) {
					RangerService service = (RangerService)object;

					ret = StringUtils.equals(serviceName, service.getName());
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(ret != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForServiceId(final String serviceId, List<Predicate> predicates) {
		if(StringUtils.isEmpty(serviceId)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;
					Long         svcId  = getServiceId(policy.getService());

					if(svcId != null) {
						ret = StringUtils.equals(serviceId, svcId.toString());
					}
				} else if(object instanceof RangerService) {
					RangerService service = (RangerService)object;

					if(service.getId() != null) {
						ret = StringUtils.equals(serviceId, service.getId().toString());
					}
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForPolicyName(final String policyName, List<Predicate> predicates) {
		if(StringUtils.isEmpty(policyName)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;

					ret = StringUtils.equals(policyName, policy.getName());
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(predicates != null) {
			predicates.add(ret);
		}
			
		return ret;
	}

	private Predicate addPredicateForPolicyId(final String policyId, List<Predicate> predicates) {
		if(StringUtils.isEmpty(policyId)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;

					if(policy.getId() != null) {
						ret = StringUtils.equals(policyId, policy.getId().toString());
					}
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForUserName(final String userName, List<Predicate> predicates) {
		if(StringUtils.isEmpty(userName)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;

					for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
						if(policyItem.getUsers().contains(userName)) { // TODO: group membership check
							ret = true;

							break;
						}
					}
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForGroupName(final String groupName, List<Predicate> predicates) {
		if(StringUtils.isEmpty(groupName)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;

					for(RangerPolicyItem policyItem : policy.getPolicyItems()) {
						if(policyItem.getGroups().contains(groupName)) {
							ret = true;

							break;
						}
					}
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForStatus(final String status, List<Predicate> predicates) {
		if(StringUtils.isEmpty(status)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerBaseModelObject) {
					RangerBaseModelObject obj = (RangerBaseModelObject)object;

					if(StringUtils.equals(status, "enabled")) {
						ret = obj.getIsEnabled();
					} else if(StringUtils.equals(status, "disabled")) {
						ret = !obj.getIsEnabled();
					}
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}

	private Predicate addPredicateForResources(final Map<String, String> resources, List<Predicate> predicates) {
		if(MapUtils.isEmpty(resources)) {
			return null;
		}

		Predicate ret = new Predicate() {
			@Override
			public boolean evaluate(Object object) {
				if(object == null) {
					return false;
				}

				boolean ret = false;

				if(object instanceof RangerPolicy) {
					RangerPolicy policy = (RangerPolicy)object;

					if(! MapUtils.isEmpty(policy.getResources())) {
						int numFound = 0;
						for(String name : resources.keySet()) {
							boolean isMatch = false;

							RangerPolicyResource policyResource = policy.getResources().get(name);

							if(policyResource != null && !CollectionUtils.isEmpty(policyResource.getValues())) {
								String val = resources.get(name);

								if(policyResource.getValues().contains(val)) {
									isMatch = true;
								} else {
									for(String policyResourceValue : policyResource.getValues()) {
										if(val.matches(RangerAbstractResourceMatcher.getWildCardPattern(policyResourceValue))) {
											isMatch = true;
											break;
										}
									}
								}
							}

							if(isMatch) {
								numFound++;
							} else {
								break;
							}
						}

						ret = numFound == resources.size();
					}
				} else {
					ret = true;
				}

				return ret;
			}
		};

		if(predicates != null) {
			predicates.add(ret);
		}

		return ret;
	}
}
