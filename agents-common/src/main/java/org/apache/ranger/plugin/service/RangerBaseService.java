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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.SecureClientLogin;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefHelper;
import org.apache.ranger.plugin.resourcematcher.RangerAbstractResourceMatcher;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public abstract class RangerBaseService {
    private static final Logger LOG = LoggerFactory.getLogger(RangerBaseService.class);

    protected static final String ADMIN_USER_PRINCIPAL                       = "ranger.admin.kerberos.principal";
    protected static final String ADMIN_USER_KEYTAB                          = "ranger.admin.kerberos.keytab";
    protected static final String LOOKUP_PRINCIPAL                           = "ranger.lookup.kerberos.principal";
    protected static final String LOOKUP_KEYTAB                              = "ranger.lookup.kerberos.keytab";
    protected static final String RANGER_AUTH_TYPE                           = "hadoop.security.authentication";
    protected static final String KERBEROS_TYPE                              = "kerberos";
    protected static final String CONFIG_CREATE_DEFAULT_POLICY_PER_HIERARCHY = "create.default.policy.per.hierarchy";
    private   static final String PROP_DEFAULT_POLICY_PREFIX                 = "default-policy.";
    private   static final String PROP_DEFAULT_POLICY_NAME_SUFFIX            = "name";
    private   static final String PROP_RESOURCE_FLAG_SUFFIX_IS_EXCLUDES      = ".is-excludes";
    private   static final String PROP_RESOURCE_FLAG_SUFFIX_IS_RECURSIVE     = ".is-recursive";

    protected final RangerAdminConfig   config;
    protected       RangerServiceDef    serviceDef;
    protected       RangerService       service;
    protected       Map<String, String> configs;
    protected       String              serviceName;
    protected       String              serviceType;
    protected       String              lookUpUser;

    public RangerBaseService() {
        this.config = RangerAdminConfig.getInstance();

        String authType        = config.get(RANGER_AUTH_TYPE, "simple");
        String lookupPrincipal = config.get(LOOKUP_PRINCIPAL);
        String lookupKeytab    = config.get(LOOKUP_KEYTAB);

        lookUpUser = getLookupUser(authType, lookupPrincipal, lookupKeytab);
    }

    public void init(RangerServiceDef serviceDef, RangerService service) {
        this.serviceDef  = serviceDef;
        this.service     = service;
        this.configs     = service.getConfigs();
        this.serviceName = service.getName();
        this.serviceType = service.getType();
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

    public RangerAdminConfig getConfig() {
        return config;
    }

    public abstract Map<String, Object> validateConfig() throws Exception;

    public abstract List<String> lookupResource(ResourceLookupContext context) throws Exception;

    public List<RangerPolicy> getDefaultRangerPolicies() throws Exception {
        LOG.debug("==> RangerBaseService.getDefaultRangerPolicies() ");

        List<RangerPolicy> ret = new ArrayList<>();

        if (createDefaultPolicyPerHierarchy()) {
            try {
                // we need to create one policy for each resource hierarchy
                RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);

                for (List<RangerResourceDef> aHierarchy : serviceDefHelper.filterHierarchies_containsOnlyMandatoryResources(RangerPolicy.POLICY_TYPE_ACCESS)) {
                    RangerPolicy policy = getDefaultPolicy(aHierarchy);

                    if (policy != null) {
                        ret.add(policy);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error getting default policies for Service: {}", service.getName(), e);
            }
        }

        final boolean additionalDefaultPolicySetup = Boolean.parseBoolean(configs.get("setup.additional.default.policies"));

        if (additionalDefaultPolicySetup) {
            LOG.info("{}: looking for additional default policies in service-config", getServiceName());

            Set<String> policyIndexes = new TreeSet<>();

            for (String configName : configs.keySet()) {
                if (configName.startsWith(PROP_DEFAULT_POLICY_PREFIX) && configName.endsWith(PROP_DEFAULT_POLICY_NAME_SUFFIX)) {
                    policyIndexes.add(configName.substring(PROP_DEFAULT_POLICY_PREFIX.length(), configName.length() - PROP_DEFAULT_POLICY_NAME_SUFFIX.length() - 1));
                }
            }

            LOG.info("{}: found {} additional default policies in service-config", getServiceName(), policyIndexes.size());

            for (String policyIndex : policyIndexes) {
                String                            policyPropertyPrefix   = PROP_DEFAULT_POLICY_PREFIX + policyIndex + ".";
                String                            resourcePropertyPrefix = policyPropertyPrefix + "resource.";
                Map<String, RangerPolicyResource> policyResources        = getResourcesForPrefix(resourcePropertyPrefix);

                if (MapUtils.isNotEmpty(policyResources)) {
                    addCustomRangerDefaultPolicies(ret, policyResources, policyPropertyPrefix);
                } else {
                    LOG.warn("{}: no resources specified for default policy with prefix '{}'. Ignored", getServiceName(), policyPropertyPrefix);
                }
            }
        }

        LOG.debug("<== RangerBaseService.getDefaultRangerPolicies(): {}", ret);

        return ret;
    }

    protected List<RangerPolicy.RangerPolicyItemAccess> getAllowedAccesses(Map<String, RangerPolicy.RangerPolicyResource> policyResources) {
        List<RangerPolicy.RangerPolicyItemAccess> ret = new ArrayList<>();

        RangerResourceDef leafResourceDef = ServiceDefUtil.getLeafResourceDef(serviceDef, policyResources);

        if (leafResourceDef != null) {
            Set<String> accessTypeRestrictions = leafResourceDef.getAccessTypeRestrictions();

            for (RangerAccessTypeDef accessTypeDef : serviceDef.getAccessTypes()) {
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

    protected Map<String, RangerPolicy.RangerPolicyResource> createDefaultPolicyResource(List<RangerResourceDef> resourceHierarchy) {
        LOG.debug("==> RangerBaseService.createDefaultPolicyResource()");

        Map<String, RangerPolicy.RangerPolicyResource> resourceMap = new HashMap<>();

        for (RangerResourceDef resourceDef : resourceHierarchy) {
            RangerPolicy.RangerPolicyResource polRes = new RangerPolicy.RangerPolicyResource();

            polRes.setIsExcludes(false);
            polRes.setIsRecursive(resourceDef.getRecursiveSupported());
            polRes.setValue(RangerAbstractResourceMatcher.WILDCARD_ASTERISK);

            resourceMap.put(resourceDef.getName(), polRes);
        }

        LOG.debug("<== RangerBaseService.createDefaultPolicyResource():{}", resourceMap);

        return resourceMap;
    }

    protected boolean createDefaultPolicyPerHierarchy() {
        String ret = configs.get(CONFIG_CREATE_DEFAULT_POLICY_PER_HIERARCHY);

        if (ret == null) {
            LOG.debug("{} is not defined in service(name={}) config", CONFIG_CREATE_DEFAULT_POLICY_PER_HIERARCHY, getServiceName());

            ret = serviceDef.getOptions().get(CONFIG_CREATE_DEFAULT_POLICY_PER_HIERARCHY);

            if (ret == null) {
                ret = Boolean.TRUE.toString();

                LOG.debug("{} is not defined in service-def(name={}) options as well. Using default value: {}", CONFIG_CREATE_DEFAULT_POLICY_PER_HIERARCHY, getServiceType(), ret);
            }
        }

        LOG.debug("createDefaultPolicyPerHierarchy(serviceName={}, serviceType={}): ret={}", getServiceName(), getServiceType(), ret);

        return Boolean.parseBoolean(ret);
    }

    protected String getLookupUser(String authType, String lookupPrincipal, String lookupKeytab) {
        String lookupUser = null;

        if (!StringUtils.isEmpty(authType) && authType.equalsIgnoreCase(KERBEROS_TYPE)) {
            if (SecureClientLogin.isKerberosCredentialExists(lookupPrincipal, lookupKeytab)) {
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

    private Map<String, RangerPolicyResource> getResourcesForPrefix(String resourcePropertyPrefix) {
        Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap = new HashMap<>();

        if (configs != null) {
            for (Map.Entry<String, String> entry : configs.entrySet()) {
                String configName  = entry.getKey();
                String configValue = entry.getValue();

                if (configName.endsWith(PROP_RESOURCE_FLAG_SUFFIX_IS_EXCLUDES) || configName.endsWith(PROP_RESOURCE_FLAG_SUFFIX_IS_RECURSIVE)) {
                    continue;
                }

                if (configName.startsWith(resourcePropertyPrefix) && StringUtils.isNotBlank(configValue)) {
                    RangerPolicyResource rPolRes      = new RangerPolicyResource();
                    String               resourceKey  = configName.substring(resourcePropertyPrefix.length());
                    List<String>         resourceList = new ArrayList<>(Arrays.asList(configValue.split(",")));
                    boolean              isExcludes   = Boolean.parseBoolean(configs.getOrDefault(configName + PROP_RESOURCE_FLAG_SUFFIX_IS_EXCLUDES, "false"));
                    boolean              isRecursive  = Boolean.parseBoolean(configs.getOrDefault(configName + PROP_RESOURCE_FLAG_SUFFIX_IS_RECURSIVE, "false"));

                    rPolRes.setIsExcludes(isExcludes);
                    rPolRes.setIsRecursive(isRecursive);
                    rPolRes.setValues(resourceList);

                    policyResourceMap.put(resourceKey, rPolRes);
                }
            }
        }

        return policyResourceMap;
    }

    private void addCustomRangerDefaultPolicies(List<RangerPolicy> ret, Map<String, RangerPolicy.RangerPolicyResource> policyResourceMap, String policyPropertyPrefix) {
        String  policyName    = configs.get(policyPropertyPrefix + PROP_DEFAULT_POLICY_NAME_SUFFIX);
        String  description   = configs.get(policyPropertyPrefix + "description");
        Boolean isDenyAllElse = Boolean.valueOf(configs.get(policyPropertyPrefix + "isDenyAllElse"));

        if (StringUtils.isEmpty(description)) {
            description = "Policy for " + policyName;
        }

        RangerPolicy policy = new RangerPolicy();

        policy.setName(policyName);
        policy.setIsEnabled(true);
        policy.setVersion(1L);
        policy.setIsAuditEnabled(true);
        policy.setService(serviceName);
        policy.setDescription(description);
        policy.setName(policyName);
        policy.setResources(policyResourceMap);
        policy.setIsDenyAllElse(isDenyAllElse);

        for (int i = 1; ; i++) {
            String policyItemPropertyPrefix = policyPropertyPrefix + "policyItem." + i + ".";
            String policyItemUsers          = configs.get(policyItemPropertyPrefix + "users");
            String policyItemGroups         = configs.get(policyItemPropertyPrefix + "groups");
            String policyItemRoles          = configs.get(policyItemPropertyPrefix + "roles");
            String policyItemAccessTypes    = configs.get(policyItemPropertyPrefix + "accessTypes");
            String isDelegateAdmin          = configs.get(policyItemPropertyPrefix + "isDelegateAdmin");

            if (StringUtils.isEmpty(policyItemAccessTypes) || (StringUtils.isEmpty(policyItemUsers) && StringUtils.isEmpty(policyItemGroups) && StringUtils.isEmpty(policyItemRoles))) {
                break;
            }

            RangerPolicyItem policyItem = new RangerPolicyItem();

            policyItem.setDelegateAdmin(Boolean.parseBoolean(isDelegateAdmin));

            if (StringUtils.isNotBlank(policyItemUsers)) {
                policyItem.setUsers(Arrays.asList(policyItemUsers.split(",")));
            }

            if (StringUtils.isNotBlank(policyItemGroups)) {
                policyItem.setGroups(Arrays.asList(policyItemGroups.split(",")));
            }

            if (StringUtils.isNotBlank(policyItemRoles)) {
                policyItem.setRoles(Arrays.asList(policyItemRoles.split(",")));
            }

            if (StringUtils.isNotBlank(policyItemAccessTypes)) {
                for (String accessType : policyItemAccessTypes.split(",")) {
                    RangerPolicyItemAccess polAccess = new RangerPolicyItemAccess(accessType, true);

                    policyItem.addAccess(polAccess);
                }
            }

            policy.addPolicyItem(policyItem);
        }

        LOG.info("{}: adding default policy: name={}", getServiceName(), policy.getName());

        ret.add(policy);
    }

    private RangerPolicy getDefaultPolicy(List<RangerResourceDef> resourceHierarchy) {
        LOG.debug("==> RangerBaseService.getDefaultPolicy()");

        RangerPolicy policy = new RangerPolicy();

        String policyName = buildPolicyName(resourceHierarchy);

        policy.setIsEnabled(true);
        policy.setVersion(1L);
        policy.setName(policyName);
        policy.setService(service.getName());
        policy.setDescription("Policy for " + policyName);
        policy.setIsAuditEnabled(true);
        policy.setResources(createDefaultPolicyResource(resourceHierarchy));

        List<RangerPolicyItem> policyItems = new ArrayList<>();
        RangerPolicyItem       policyItem  = createDefaultPolicyItem(policy.getResources()); // Create Default policy item for the service user

        policyItems.add(policyItem);
        policy.setPolicyItems(policyItems);

        LOG.debug("<== RangerBaseService.getDefaultPolicy(){}", policy);

        return policy;
    }

    private RangerPolicyItem createDefaultPolicyItem(Map<String, RangerPolicy.RangerPolicyResource> policyResources) {
        LOG.debug("==> RangerBaseService.createDefaultPolicyItem()");

        RangerPolicyItem policyItem = new RangerPolicyItem();

        policyItem.setUsers(getUserList());
        policyItem.setGroups(getGroupList());
        policyItem.setAccesses(getAllowedAccesses(policyResources));

        policyItem.setDelegateAdmin(true);

        LOG.debug("<== RangerBaseService.createDefaultPolicyItem(): {}", policyItem);

        return policyItem;
    }

    private String buildPolicyName(List<RangerResourceDef> resourceHierarchy) {
        StringBuilder sb = new StringBuilder("all");

        if (CollectionUtils.isNotEmpty(resourceHierarchy)) {
            int resourceDefCount = 0;

            for (RangerResourceDef resourceDef : resourceHierarchy) {
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
        HashSet<String> uniqueUsers = new HashSet<>();
        String[]        users       = config.getStrings("ranger.default.policy.users");

        if (users != null) {
            Collections.addAll(uniqueUsers, users);
        }

        Map<String, String> serviceConfig = service.getConfigs();

        if (serviceConfig != null) {
            String serviceConfigUser = serviceConfig.get("username");

            if (StringUtils.isNotBlank(serviceConfigUser)) {
                uniqueUsers.add(serviceConfig.get("username"));
            }

            String defaultUsers = serviceConfig.get("default.policy.users");

            if (!StringUtils.isEmpty(defaultUsers)) {
                String[] defaultUserList = StringUtils.split(defaultUsers, ",");

                if (defaultUserList != null) {
                    Collections.addAll(uniqueUsers, defaultUserList);
                }
            }
        }

        return new ArrayList<>(uniqueUsers);
    }

    private List<String> getGroupList() {
        HashSet<String> uniqueGroups = new HashSet<>();
        String[]        groups       = config.getStrings("ranger.default.policy.groups");

        if (groups != null) {
            Collections.addAll(uniqueGroups, groups);
        }

        Map<String, String> serviceConfig = service.getConfigs();

        if (serviceConfig != null) {
            String defaultGroups = serviceConfig.get("default.policy.groups");

            if (!StringUtils.isEmpty(defaultGroups)) {
                List<String> defaultGroupList = new ArrayList<>(Arrays.asList(StringUtils.split(defaultGroups, ",")));

                if (!defaultGroupList.isEmpty()) {
                    uniqueGroups.addAll(defaultGroupList);
                }
            }
        }

        return new ArrayList<>(uniqueGroups);
    }
}
