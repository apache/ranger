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
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.store.AbstractServiceStore;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.ServicePredicateUtil;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;

public class ServiceFileStore extends AbstractServiceStore {
	private static final Log LOG = LogFactory.getLog(ServiceFileStore.class);

	public static final String PROPERTY_SERVICE_FILE_STORE_DIR = "ranger.service.store.file.dir";

	protected static final String FILE_PREFIX_SERVICE_DEF = "ranger-servicedef-";
	protected static final String FILE_PREFIX_SERVICE     = "ranger-service-";
	protected static final String FILE_PREFIX_POLICY      = "ranger-policy-";

	private String dataDir          = null;
	private long   nextServiceDefId = 0;
	private long   nextServiceId    = 0;
	private long   nextPolicyId     = 0;

	private ServicePredicateUtil predicateUtil = null;
	private FileStoreUtil fileStoreUtil = null;
	private Boolean populateExistingBaseFields = false;

	public ServiceFileStore() {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.ServiceFileStore()");
		}

		this.dataDir = RangerConfiguration.getInstance().get(PROPERTY_SERVICE_FILE_STORE_DIR, "file:///etc/ranger/data");
		predicateUtil = new ServicePredicateUtil(this);
		fileStoreUtil = new FileStoreUtil();

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.ServiceFileStore()");
		}
	}

	public ServiceFileStore(String dataDir) {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.ServiceFileStore()");
		}

		this.dataDir = dataDir;
		predicateUtil = new ServicePredicateUtil(this);
		fileStoreUtil = new FileStoreUtil();
		fileStoreUtil.initStore(dataDir);

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.ServiceFileStore()");
		}
	}

	@Override
	public void init() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.init()");
		}

		fileStoreUtil.initStore(dataDir);

		EmbeddedServiceDefsUtil.instance().init(this);

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

			ret = fileStoreUtil.saveToFile(serviceDef, FILE_PREFIX_SERVICE_DEF, false);

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

			ret = fileStoreUtil.saveToFile(existing, FILE_PREFIX_SERVICE_DEF, true);

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

		// TODO: deleting service-def would require deleting services that refer to this service-def

		try {
			preDelete(existing);

			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_SERVICE_DEF, id));

			fileStoreUtil.deleteFile(filePath);

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
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));

			Comparator<RangerBaseModelObject> comparator = predicateUtil.getSorter(filter);

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

			ret = fileStoreUtil.saveToFile(service, FILE_PREFIX_SERVICE, false);

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

		boolean hasIsEnabledChanged = !existing.getIsEnabled().equals(service.getIsEnabled());

		if (hasIsEnabledChanged) {
			handlePolicyUpdate(service);
		}

		RangerService ret = null;

		try {
			existing.updateFrom(service);

			preUpdate(existing);

			ret = fileStoreUtil.saveToFile(existing, FILE_PREFIX_SERVICE, true);

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
			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_SERVICE, id));

			preDelete(existing);

			handleServiceDelete(existing);

			fileStoreUtil.deleteFile(filePath);

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
			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_SERVICE, id));
	
			ret = fileStoreUtil.loadFromFile(filePath,  RangerService.class);
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
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));

			Comparator<RangerBaseModelObject> comparator = predicateUtil.getSorter(filter);

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

			ret = fileStoreUtil.saveToFile(policy, FILE_PREFIX_POLICY, service.getId(), false);

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

			ret = fileStoreUtil.saveToFile(existing, FILE_PREFIX_POLICY, service.getId(), true);

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

			Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_POLICY, service.getId(), existing.getId()));

			fileStoreUtil.deleteFile(filePath);

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
	public List<RangerPolicy> getPoliciesByResourceSignature(String serviceName, String policySignature, Boolean isPolicyEnabled) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("==> ServiceFileStore.getPoliciesByResourceSignature(%s, %s, %s)", serviceName, policySignature, isPolicyEnabled));
		}

		List<RangerPolicy> ret = getAllPolicies();

		CollectionUtils.filter(ret, predicateUtil.createPredicateForResourceSignature(policySignature));

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("<== ServiceFileStore.getPoliciesByResourceSignature(%s, %s, %s): count[%d]: %s", 
					serviceName, policySignature, isPolicyEnabled, (ret == null ? 0 : ret.size()), ret));
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
			CollectionUtils.filter(ret, predicateUtil.getPredicate(filter));

			Comparator<RangerBaseModelObject> comparator = predicateUtil.getSorter(filter);

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

			List<RangerPolicy> policies = null;

			if (service.getIsEnabled()) {
				SearchFilter filter = new SearchFilter(SearchFilter.SERVICE_NAME, serviceName);

				policies = getPolicies(filter);
			} else {
				policies = new ArrayList<RangerPolicy>();
			}

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
			Collections.sort(ret.getPolicies(), ServicePredicateUtil.idComparator);
		}

		return ret;
	}

	@Override
	public ServicePolicies getServicePolicies(String serviceName) throws Exception {
		return getServicePoliciesIfUpdated(serviceName, -1L);
	}

	private void handleServiceRename(RangerService service, String oldName) throws Exception {
		List<RangerPolicy> policies = getAllPolicies();

		if(policies != null) {
			for(RangerPolicy policy : policies) {
				if(StringUtils.equalsIgnoreCase(policy.getService(), oldName)) {
					policy.setService(service.getName());
					preUpdate(policy);
	
					fileStoreUtil.saveToFile(policy, FILE_PREFIX_POLICY, service.getId(), true);
	
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

				Path filePath = new Path(fileStoreUtil.getDataFile(FILE_PREFIX_POLICY, service.getId(), policy.getId()));

				fileStoreUtil.deleteFile(filePath);

				postDelete(policy);
			}
		}
	}

	private void handlePolicyUpdate(RangerService service) throws Exception {
		if(service == null) {
			return;
		}

		service.setPolicyVersion(getNextVersion(service.getPolicyVersion()));
		service.setPolicyUpdateTime(new Date());

		fileStoreUtil.saveToFile(service, FILE_PREFIX_SERVICE, true);

		boolean isTagServiceDef = StringUtils.equals(service.getType(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME);

		if(isTagServiceDef) {
			SearchFilter filter = new SearchFilter();
			filter.setParam(SearchFilter.TAG_SERVICE_NAME, service.getName());

			List<RangerService> referringServices = getServices(filter);

			if(CollectionUtils.isNotEmpty(referringServices)) {
				for(RangerService referringService : referringServices) {
					referringService.setPolicyVersion(getNextVersion(referringService.getPolicyVersion()));
					referringService.setPolicyUpdateTime(service.getPolicyUpdateTime());

					fileStoreUtil.saveToFile(referringService, FILE_PREFIX_SERVICE, true);
				}
			}
		}
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

	private List<RangerServiceDef> getAllServiceDefs() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceDefFileStore.getAllServiceDefs()");
		}

		List<RangerServiceDef> ret = new ArrayList<RangerServiceDef>();

		try {
			// load service definitions from file system
			List<RangerServiceDef> sds = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_SERVICE_DEF, RangerServiceDef.class);
			
			if(sds != null) {
				for(RangerServiceDef sd : sds) {
					if(sd != null) {
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
			Collections.sort(ret, ServicePredicateUtil.idComparator);

			for(RangerServiceDef sd : ret) {
				Collections.sort(sd.getResources(), ServicePredicateUtil.resourceLevelComparator);
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
			ret = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_SERVICE, RangerService.class);

			nextServiceId = getMaxId(ret) + 1;
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getAllServices(): failed to read services", excp);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getAllServices(): count=" + (ret == null ? 0 : ret.size()));
		}

		if(ret != null) {
			Collections.sort(ret, ServicePredicateUtil.idComparator);
		}

		return ret;
	}

	private List<RangerPolicy> getAllPolicies() throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.debug("==> ServiceFileStore.getAllPolicies()");
		}

		List<RangerPolicy> ret = null;

		try {
			ret = fileStoreUtil.loadFromDir(new Path(fileStoreUtil.getDataDir()), FILE_PREFIX_POLICY, RangerPolicy.class);

			nextPolicyId  = getMaxId(ret) + 1;
		} catch(Exception excp) {
			LOG.error("ServiceFileStore.getAllPolicies(): failed to read policies", excp);
		}

		if(ret != null) {
			Collections.sort(ret, ServicePredicateUtil.idComparator);
		}

		if(LOG.isDebugEnabled()) {
			LOG.debug("<== ServiceFileStore.getAllPolicies(): count=" + (ret == null ? 0 : ret.size()));
		}

		return ret;
	}

	@Override
	public void setPopulateExistingBaseFields(Boolean populateExistingBaseFields) {
		this.populateExistingBaseFields = populateExistingBaseFields;
	}

	@Override
	public Boolean getPopulateExistingBaseFields() {
		return populateExistingBaseFields;
	}
}
