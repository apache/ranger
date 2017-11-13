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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
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
		List<RangerPolicy> ret = new ArrayList<RangerPolicy>();

		try {
			// we need to create one policy for each resource hierarchy
			RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);
			for (List<RangerServiceDef.RangerResourceDef> aHierarchy : serviceDefHelper.getResourceHierarchies(RangerPolicy.POLICY_TYPE_ACCESS)) {
				RangerPolicy policy = getDefaultPolicy(aHierarchy);
				if (policy != null) {
					ret.add(policy);
				}
			}
		} catch (Exception e) {
			LOG.error("Error getting default polcies for Service: " + service.getName(), e);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("<== RangerBaseService.getDefaultRangerPolicies(): " + ret);
		}
		return ret;
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
				boolean isAccessTypeAllowed = CollectionUtils.isEmpty(accessTypeRestrictions) || accessTypeRestrictions.contains(accessTypeDef.getName());;

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
