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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.util.ServiceDefUtil;


public abstract class RangerBaseService {
	private static final Log LOG = LogFactory.getLog(RangerBaseService.class);

	protected static final String ADMIN_USER_PRINCIPAL = "ranger.admin.kerberos.principal";
	protected static final String ADMIN_USER_KEYTAB    = "ranger.admin.kerberos.keytab";
	protected static final String LOOKUP_PRINCIPAL     = "ranger.lookup.kerberos.principal";
	protected static final String LOOKUP_KEYTAB        = "ranger.lookup.kerberos.keytab";
	protected static final String RANGER_AUTH_TYPE     = "hadoop.security.authentication";

	protected static final String KERBEROS_TYPE        = "kerberos";

	protected RangerServiceDef serviceDef;
	protected RangerService    service;

	protected Map<String, String>   configs;
	protected String 			    serviceName;
	protected String 				serviceType;

	public void init(RangerServiceDef serviceDef, RangerService service) {
		this.serviceDef    = serviceDef;
		this.service       = service;
		this.configs	   = service.getConfigs();
		this.serviceName   = service.getName();
		this.serviceType   = service.getType();
	}

	/**
	 * @return the serviceDef
	 */
	public RangerServiceDef getServiceDef() {
		return serviceDef;
	}

	/**
	 * @return the service
	 */
	public RangerService getService() {
		return service;
	}

	public Map<String, String> getConfigs() {
		return configs;
	}

	public void setConfigs(Map<String, String> configs) {
		this.configs = configs;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getServiceType() {
		return serviceType;
	}

	public void setServiceType(String serviceType) {
		this.serviceType = serviceType;
	}

	public abstract Map<String, Object> validateConfig() throws Exception;
	
	public abstract List<String> lookupResource(ResourceLookupContext context) throws Exception;

	public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBaseService.getDefaultRangerPolicies() ");
		}
		final Boolean additionalDefaultPolicySetup = Boolean.valueOf(configs.get("setup.additional.default.policies"));
		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		try {
			// we need to create one policy for each resource hierarchy
			RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);
			for (List<RangerServiceDef.RangerResourceDef> aHierarchy : serviceDefHelper.filterHierarchies_containsOnlyMandatoryResources(RangerPolicy.POLICY_TYPE_ACCESS)) {
				RangerPolicy policy = getDefaultPolicy(aHierarchy);
				if (policy != null) {
					ret.add(policy);
				}
			}
		} catch (Exception e) {
			LOG.error("Error getting default polcies for Service: " + service.getName(), e);
		}
		if (additionalDefaultPolicySetup) {
			final String PROP_POLICY_NAME_PREFIX = "default-policy.";
			final String PROP_POLICY_NAME_SUFFIX = ".name";

			TreeSet<String> policyIndexes = new TreeSet<>();

			for (String configName : configs.keySet()) {
			    if (configName.startsWith(PROP_POLICY_NAME_PREFIX) && configName.endsWith(PROP_POLICY_NAME_SUFFIX)) {
			      policyIndexes.add(configName.substring(PROP_POLICY_NAME_PREFIX.length(), configName.length() - PROP_POLICY_NAME_SUFFIX.length()));
			    }
			}

			for (String policyIndex : policyIndexes) {
			    String                            resourcePropertyPrefix  = "default-policy." + policyIndex + ".resource.";
			    Map<String, RangerPolicyResource> policyResources = getResourcesForPrefix(resourcePropertyPrefix);

			    if(MapUtils.isNotEmpty(policyResources)){
			    	addCustomRangerDefaultPolicies(ret, policyResources,policyIndex);
			    }

			}

		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBaseService.getDefaultRangerPolicies(): " + ret);
		}
		return ret;
	}

	private Map<String, RangerPolicyResource> getResourcesForPrefix(String resourcePropertyPrefix) {
		Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap = new HashMap<String, RangerPolicy.RangerPolicyResource>();
		if (configs != null) {
			for (Map.Entry<String, String> entry : configs.entrySet()) {
				if(entry.getKey().indexOf(resourcePropertyPrefix) > -1 && StringUtils.isNotBlank(entry.getValue())){
					RangerPolicyResource rPolRes=new RangerPolicyResource();
					String resourceKey = entry.getKey().substring(resourcePropertyPrefix.length());
					List<String> resourceList = new ArrayList<String>(Arrays.asList(entry.getValue().split(",")));
					rPolRes.setIsExcludes(false);
					rPolRes.setIsRecursive(false);
					rPolRes.setValues(resourceList);
					policyResourceMap.put(resourceKey, rPolRes);
				}
			}
		}
		return policyResourceMap;
	}


	private void addCustomRangerDefaultPolicies(List<RangerPolicy> ret, Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap, String policyIndex) throws Exception {

		LOG.info("Setting additional default policies");
		String startConfigName = "default-policy.";
		RangerPolicy addDefaultPolicy1 = null;
		String policyName = configs.get(startConfigName+policyIndex+".name");
		if(policyResourceMap!= null && StringUtils.isNotBlank(policyName)){
			String polItem1Users = configs.get(startConfigName+policyIndex+".policyItem.1.users");
			String polItem2Users = configs.get(startConfigName+policyIndex+".policyItem.2.users");
			String polItem1group = configs.get(startConfigName+policyIndex+".policyItem.1.groups");
			String polItem2group = configs.get(startConfigName+policyIndex+".policyItem.2.groups");
			String polItem1AccessTypes = configs.get(startConfigName+policyIndex+".policyItem.1.accessTypes");
			String polItem2AccessTypes = configs.get(startConfigName+policyIndex+".policyItem.2.accessTypes");
			if((StringUtils.isNotBlank(polItem1Users) && StringUtils.isNotBlank(polItem1AccessTypes)) || (StringUtils.isNotBlank(polItem2Users) && StringUtils.isNotBlank(polItem2AccessTypes)) ){
				addDefaultPolicy1 = getRangerCustomPolicy(policyName,policyResourceMap,polItem1Users,polItem2Users,polItem1group,polItem2group,polItem1AccessTypes,polItem2AccessTypes);
				if(addDefaultPolicy1 != null){
				ret.add(addDefaultPolicy1);
				}
			}
		}
	}

	public RangerPolicy getRangerCustomPolicy(String policyName, Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap, String polItem1Users, String polItem2Users, String polItem1Group, String polItem2Group, String polItem1AccessTypes, String polItem2AccessTypes) throws Exception {

		if(LOG.isDebugEnabled()){
			LOG.debug("==> RangerBaseService.getAtlasTopicPolicy(). resourcenames" + policyResourceMap + "policy users = " + polItem1Users + polItem2Users + "policy groups = " + polItem1Group + polItem2Group + "accessLists ");
		}
		RangerPolicy rPolicy = new RangerPolicy();
		List<RangerPolicyItem> policyItemList =new ArrayList<RangerPolicyItem>();

		List<String> policyItem1UserList = new ArrayList<String>();
		List<String> policyItem2UserList = new ArrayList<String>();
		List<String> pol1Item1AccessTypeList = new ArrayList<String>();
		List<String> pol1Item2AccessTypeList = new ArrayList<String>();
		List<String> policyItem1GroupList = new ArrayList<String>();
		List<String> policyItem2GroupList = new ArrayList<String>();

		if(StringUtils.isNotBlank(polItem1Group)){
			policyItem1GroupList.addAll(Arrays.asList(polItem1Group.split(",")));
		}
		if(StringUtils.isNotBlank(polItem2Group)){
			policyItem2GroupList.addAll(Arrays.asList(polItem2Group.split(",")));
		}
		if(StringUtils.isNotBlank(polItem1Users)){
			policyItem1UserList.addAll(Arrays.asList(polItem1Users.split(",")));
		}
		if(StringUtils.isNotBlank(polItem2Users)){
			policyItem2UserList.addAll(Arrays.asList(polItem2Users.split(",")));
		}
		if(StringUtils.isNotBlank(polItem1AccessTypes)){
			pol1Item1AccessTypeList.addAll(Arrays.asList(polItem1AccessTypes.split(",")));
		}
		if(StringUtils.isNotBlank(polItem2AccessTypes)){
			pol1Item2AccessTypeList.addAll(Arrays.asList(polItem2AccessTypes.split(",")));
		}

		if((CollectionUtils.isNotEmpty(policyItem1UserList)||CollectionUtils.isNotEmpty(policyItem1GroupList)) && CollectionUtils.isNotEmpty(pol1Item1AccessTypeList)){
		RangerPolicyItem policyItem1 = setCustomPolItem(policyItem1UserList,pol1Item1AccessTypeList,policyItem1GroupList);
		policyItemList.add(policyItem1);
		}
		if((CollectionUtils.isNotEmpty(policyItem2UserList)||CollectionUtils.isNotEmpty(policyItem2GroupList)) && CollectionUtils.isNotEmpty(pol1Item2AccessTypeList)){
		RangerPolicy.RangerPolicyItem policyItem2 = setCustomPolItem(policyItem2UserList,pol1Item2AccessTypeList,policyItem2GroupList);
		policyItemList.add(policyItem2);
		}
		rPolicy.setPolicyItems(policyItemList);
		rPolicy.setIsEnabled(true);
		rPolicy.setVersion(1L);
		rPolicy.setIsAuditEnabled(true);
		rPolicy.setService(serviceName);
		rPolicy.setDescription("Policy for " + policyName);
		rPolicy.setName(policyName);
		rPolicy.setResources(policyResourceMap);

		if(LOG.isDebugEnabled()){
			LOG.debug("<== RangerBaseService.getAtlasTopicPolicy() ");
		}
		return rPolicy;
	}

	private RangerPolicyItem setCustomPolItem(List<String> userList, List<String> pol1Item1AccessTypeList, List<String> groupList) {
		RangerPolicyItem polItem = new RangerPolicyItem();
		if(LOG.isDebugEnabled()){
			LOG.debug("==> RangerServiceKafka.setCustomPolItem(). userlist = " + userList + " accessType" + pol1Item1AccessTypeList);
		}

		List<RangerPolicyItemAccess> polAccesslist =new ArrayList<RangerPolicyItemAccess>();
		polItem.setDelegateAdmin(false);
		for(String pol1Item1AccessType : pol1Item1AccessTypeList){
			RangerPolicyItemAccess polAccess = new RangerPolicyItemAccess();
			polAccess.setIsAllowed(true);
			polAccess.setType(pol1Item1AccessType);
			polAccesslist.add(polAccess);
		}
		polItem.setAccesses(polAccesslist );
		polItem.setUsers(userList);
		polItem.setGroups(groupList);
		if(LOG.isDebugEnabled()){
			LOG.debug("<== RangerServiceKafka.setCustomPolItem()");
		}

		return  polItem;
	}

	private RangerPolicy getDefaultPolicy(List<RangerServiceDef.RangerResourceDef> resourceHierarchy) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBaseService.getDefaultPolicy()");
		}

		RangerPolicy policy = new RangerPolicy();

		String policyName=buildPolicyName(resourceHierarchy);

		policy.setIsEnabled(true);
		policy.setVersion(1L);
		policy.setName(policyName);
		policy.setService(service.getName());
		policy.setDescription("Policy for " + policyName);
		policy.setIsAuditEnabled(true);
		policy.setResources(createDefaultPolicyResource(resourceHierarchy));

		List<RangerPolicy.RangerPolicyItem> policyItems = new ArrayList<RangerPolicy.RangerPolicyItem>();
		//Create Default policy item for the service user
		RangerPolicy.RangerPolicyItem policyItem = createDefaultPolicyItem(policy.getResources());
		policyItems.add(policyItem);
		policy.setPolicyItems(policyItems);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBaseService.getDefaultPolicy()" + policy);
		}

		return policy;
	}

	private RangerPolicy.RangerPolicyItem createDefaultPolicyItem(Map<String, RangerPolicy.RangerPolicyResource> policyResources) throws Exception {

		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBaseService.createDefaultPolicyItem()");
		}

		RangerPolicy.RangerPolicyItem policyItem = new RangerPolicy.RangerPolicyItem();

		policyItem.setUsers(getUserList());

		List<RangerPolicy.RangerPolicyItemAccess> accesses = getAllowedAccesses(policyResources);
		policyItem.setAccesses(accesses);

		policyItem.setDelegateAdmin(true);

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBaseService.createDefaultPolicyItem(): " + policyItem );
		}
		return policyItem;
	}

	protected List<RangerPolicy.RangerPolicyItemAccess> getAllowedAccesses(Map<String, RangerPolicy.RangerPolicyResource> policyResources) {
		List<RangerPolicy.RangerPolicyItemAccess> ret = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();

		RangerServiceDef.RangerResourceDef leafResourceDef = ServiceDefUtil.getLeafResourceDef(serviceDef, policyResources);

		if (leafResourceDef != null) {
			Set<String> accessTypeRestrictions = leafResourceDef.getAccessTypeRestrictions();

			for (RangerServiceDef.RangerAccessTypeDef accessTypeDef : serviceDef.getAccessTypes()) {
				boolean isAccessTypeAllowed = CollectionUtils.isEmpty(accessTypeRestrictions) || accessTypeRestrictions.contains(accessTypeDef.getName());

				if (isAccessTypeAllowed) {
					RangerPolicy.RangerPolicyItemAccess access = new RangerPolicy.RangerPolicyItemAccess();
					access.setType(accessTypeDef.getName());
					access.setIsAllowed(true);
					ret.add(access);
				}
			}
		}
		return ret;
	}

	protected Map<String, RangerPolicy.RangerPolicyResource> createDefaultPolicyResource(List<RangerServiceDef.RangerResourceDef> resourceHierarchy) throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug("==> RangerBaseService.createDefaultPolicyResource()");
		}
		Map<String, RangerPolicy.RangerPolicyResource> resourceMap = new HashMap<>();

		for (RangerServiceDef.RangerResourceDef resourceDef : resourceHierarchy) {
			RangerPolicy.RangerPolicyResource polRes = new RangerPolicy.RangerPolicyResource();

			polRes.setIsExcludes(false);
			polRes.setIsRecursive(resourceDef.getRecursiveSupported());
			polRes.setValue(RangerAbstractResourceMatcher.WILDCARD_ASTERISK);

			resourceMap.put(resourceDef.getName(), polRes);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBaseService.createDefaultPolicyResource():" + resourceMap);
		}
		return resourceMap;
	}

	private String buildPolicyName(List<RangerServiceDef.RangerResourceDef> resourceHierarchy) {
		StringBuilder sb = new StringBuilder("all");
		if (CollectionUtils.isNotEmpty(resourceHierarchy)) {
			int resourceDefCount = 0;
			for (RangerServiceDef.RangerResourceDef resourceDef : resourceHierarchy) {
				if (resourceDefCount > 0) {
					sb.append(", ");
				} else {
					sb.append(" - ");
				}
				sb.append(resourceDef.getName());
				resourceDefCount++;
			}
		}
		return sb.toString().trim();
	}

	private List<String> getUserList() {
		List<String> ret = new ArrayList<>();
		Map<String, String> serviceConfig =  service.getConfigs();
		if (serviceConfig != null ) {
                        String serviceConfigUser = serviceConfig.get("username");
                        if (StringUtils.isNotBlank(serviceConfigUser)){
                                ret.add(serviceConfig.get("username"));
                        }
			String defaultUsers = serviceConfig.get("default.policy.users");
			if (!StringUtils.isEmpty(defaultUsers)) {
				List<String> defaultUserList = new ArrayList<>(Arrays.asList(StringUtils.split(defaultUsers,",")));
				if (!defaultUserList.isEmpty()) {
					ret.addAll(defaultUserList);
				}
			}
		}
		String authType = RangerConfiguration.getInstance().get(RANGER_AUTH_TYPE,"simple");
		String lookupPrincipal = RangerConfiguration.getInstance().get(LOOKUP_PRINCIPAL);
		String lookupKeytab = RangerConfiguration.getInstance().get(LOOKUP_KEYTAB);

		String lookUpUser = getLookupUser(authType, lookupPrincipal, lookupKeytab);

		if (StringUtils.isNotBlank(lookUpUser)) {
			ret.add(lookUpUser);
		}

		return ret;
	}

	protected String getLookupUser(String authType, String lookupPrincipal, String lookupKeytab) {
		String lookupUser = null;
		if(!StringUtils.isEmpty(authType) && authType.equalsIgnoreCase(KERBEROS_TYPE)){
			if(SecureClientLogin.isKerberosCredentialExists(lookupPrincipal, lookupKeytab)){
				KerberosName krbName = new KerberosName(lookupPrincipal);
				try {
					lookupUser = krbName.getShortName();
				} catch (IOException e) {
					LOG.error("Unknown lookup user", e);
				}
			}
		}
		return lookupUser;
	}


}
