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

package org.apache.ranger.authorization.hadoop.config;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.authorization.utils.StringUtil;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RangerPluginConfig extends RangerConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPluginConfig.class);

    private static final char RANGER_TRUSTED_PROXY_IPADDRESSES_SEPARATOR_CHAR = ',';

    private final String                    serviceType;
    private final String                    serviceName;
    private final String                    appId;
    private final String                    clusterName;
    private final String                    clusterType;
    private final RangerPolicyEngineOptions policyEngineOptions;
    private final boolean                   useForwardedIPAddress;
    private final String[]                  trustedProxyAddresses;
    private final String                    propertyPrefix;
    private final boolean                   useRangerGroups;
    private final boolean                   useOnlyRangerGroups;
    private final boolean                   convertEmailToUsername;
    private final boolean                   enableImplicitUserStoreEnricher;
    private final boolean                   enableImplicitGdsInfoEnricher;
    private       boolean                   isFallbackSupported;
    private       Set<String>               auditExcludedUsers  = Collections.emptySet();
    private       Set<String>               auditExcludedGroups = Collections.emptySet();
    private       Set<String>               auditExcludedRoles  = Collections.emptySet();
    private       Set<String>               superUsers          = new HashSet<>();
    private       Set<String>               superGroups         = Collections.emptySet();
    private       Set<String>               serviceAdmins       = Collections.emptySet();

    public RangerPluginConfig(String serviceType, String serviceName, String appId, String clusterName, String clusterType, RangerPolicyEngineOptions policyEngineOptions) {
        this(serviceType, serviceName, appId, clusterName, clusterType, null, policyEngineOptions);
    }

    public RangerPluginConfig(String serviceType, String serviceName, String appId, String clusterName, String clusterType, List<File> additionalConfigFiles, RangerPolicyEngineOptions policyEngineOptions) {
        super();

        addResourcesForServiceType(serviceType);

        this.serviceType    = serviceType;
        this.appId          = StringUtils.isEmpty(appId) ? serviceType : appId;
        this.propertyPrefix = "ranger.plugin." + serviceType;
        this.serviceName    = StringUtils.isEmpty(serviceName) ? this.get(propertyPrefix + ".service.name") : serviceName;

        addResourcesForServiceName(this.serviceType, this.serviceName);

        if (additionalConfigFiles != null) {
            for (File configFile : additionalConfigFiles) {
                try {
                    addResource(configFile.toURI().toURL());
                } catch (Throwable t) {
                    LOG.warn("failed to load configurations from {}", configFile, t);
                }
            }
        }

        String trustedProxyAddressString = this.get(propertyPrefix + ".trusted.proxy.ipaddresses");

        if (StringUtil.isEmpty(clusterName)) {
            clusterName = this.get(propertyPrefix + ".access.cluster.name", "");

            if (StringUtil.isEmpty(clusterName)) {
                clusterName = this.get(propertyPrefix + ".ambari.cluster.name", "");
            }
        }

        if (StringUtil.isEmpty(clusterType)) {
            clusterType = this.get(propertyPrefix + ".access.cluster.type", "");

            if (StringUtil.isEmpty(clusterType)) {
                clusterType = this.get(propertyPrefix + ".ambari.cluster.type", "");
            }
        }

        this.clusterName           = clusterName;
        this.clusterType           = clusterType;
        this.useForwardedIPAddress = this.getBoolean(propertyPrefix + ".use.x-forwarded-for.ipaddress", false);
        this.trustedProxyAddresses = StringUtils.split(trustedProxyAddressString, RANGER_TRUSTED_PROXY_IPADDRESSES_SEPARATOR_CHAR);

        if (trustedProxyAddresses != null) {
            for (int i = 0; i < trustedProxyAddresses.length; i++) {
                trustedProxyAddresses[i] = trustedProxyAddresses[i].trim();
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("{}.use.x-forwarded-for.ipaddress:{}", propertyPrefix, useForwardedIPAddress);
            LOG.debug("{}.trusted.proxy.ipaddresses:[{}]", propertyPrefix, StringUtils.join(trustedProxyAddresses, ", "));
        }

        if (useForwardedIPAddress && StringUtils.isBlank(trustedProxyAddressString)) {
            LOG.warn("Property {}.use.x-forwarded-for.ipaddress is set to true, and Property {}.trusted.proxy.ipaddresses is not set", propertyPrefix, propertyPrefix);
            LOG.warn("Ranger plugin will trust RemoteIPAddress and treat first X-Forwarded-Address in the access-request as the clientIPAddress");
        }

        if (policyEngineOptions == null) {
            policyEngineOptions = new RangerPolicyEngineOptions();

            policyEngineOptions.configureForPlugin(this, propertyPrefix);
        }

        this.policyEngineOptions = policyEngineOptions;

        useRangerGroups                 = this.getBoolean(propertyPrefix + ".use.rangerGroups", false);
        useOnlyRangerGroups             = this.getBoolean(propertyPrefix + ".use.only.rangerGroups", false);
        convertEmailToUsername          = this.getBoolean(propertyPrefix + ".convert.emailToUser", false);
        enableImplicitUserStoreEnricher = useRangerGroups || convertEmailToUsername || this.getBoolean(propertyPrefix + ".enable.implicit.userstore.enricher", false);
        enableImplicitGdsInfoEnricher   = this.getBoolean(propertyPrefix + ".enable.implicit.gdsinfo.enricher", true);

        LOG.info("{}", policyEngineOptions);
    }

    protected RangerPluginConfig(String serviceType, String serviceName, String appId, RangerPluginConfig sourcePluginConfig) {
        super();

        this.serviceType    = serviceType;
        this.appId          = StringUtils.isEmpty(appId) ? serviceType : appId;
        this.propertyPrefix = "ranger.plugin." + serviceType;
        this.serviceName    = serviceName;

        this.clusterName           = sourcePluginConfig.getClusterName();
        this.clusterType           = sourcePluginConfig.getClusterType();
        this.useForwardedIPAddress = sourcePluginConfig.isUseForwardedIPAddress();
        this.trustedProxyAddresses = sourcePluginConfig.getTrustedProxyAddresses();
        this.isFallbackSupported   = sourcePluginConfig.getIsFallbackSupported();

        this.policyEngineOptions = sourcePluginConfig.getPolicyEngineOptions();

        this.useRangerGroups                 = sourcePluginConfig.useRangerGroups;
        this.useOnlyRangerGroups             = sourcePluginConfig.useOnlyRangerGroups;
        this.convertEmailToUsername          = sourcePluginConfig.convertEmailToUsername;
        this.enableImplicitUserStoreEnricher = sourcePluginConfig.enableImplicitUserStoreEnricher;
        this.enableImplicitGdsInfoEnricher   = sourcePluginConfig.enableImplicitGdsInfoEnricher;
    }

    public String getServiceType() {
        return serviceType;
    }

    public String getAppId() {
        return appId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getClusterType() {
        return clusterType;
    }

    public boolean isUseForwardedIPAddress() {
        return useForwardedIPAddress;
    }

    public String[] getTrustedProxyAddresses() {
        return trustedProxyAddresses;
    }

    public String getPropertyPrefix() {
        return propertyPrefix;
    }

    public boolean isUseRangerGroups() {
        return useRangerGroups;
    }

    public boolean isUseOnlyRangerGroups() {
        return useOnlyRangerGroups;
    }

    public boolean isConvertEmailToUsername() {
        return convertEmailToUsername;
    }

    public boolean isEnableImplicitUserStoreEnricher() {
        return enableImplicitUserStoreEnricher;
    }

    public boolean isEnableImplicitGdsInfoEnricher() {
        return enableImplicitGdsInfoEnricher;
    }

    public boolean getIsFallbackSupported() {
        return isFallbackSupported;
    }

    public void setIsFallbackSupported(boolean isFallbackSupported) {
        this.isFallbackSupported = isFallbackSupported;
    }

    public RangerPolicyEngineOptions getPolicyEngineOptions() {
        return policyEngineOptions;
    }

    public void setAuditExcludedUsersGroupsRoles(Set<String> users, Set<String> groups, Set<String> roles) {
        auditExcludedUsers  = CollectionUtils.isEmpty(users) ? Collections.emptySet() : new HashSet<>(users);
        auditExcludedGroups = CollectionUtils.isEmpty(groups) ? Collections.emptySet() : new HashSet<>(groups);
        auditExcludedRoles  = CollectionUtils.isEmpty(roles) ? Collections.emptySet() : new HashSet<>(roles);

        LOG.debug("auditExcludedUsers={}, auditExcludedGroups={}, auditExcludedRoles={}", auditExcludedUsers, auditExcludedGroups, auditExcludedRoles);
    }

    public void setSuperUsersGroups(Set<String> users, Set<String> groups) {
        superUsers  = CollectionUtils.isEmpty(users) ? new HashSet<>() : new HashSet<>(users);
        superGroups = CollectionUtils.isEmpty(groups) ? Collections.emptySet() : new HashSet<>(groups);

        LOG.debug("superUsers={}, superGroups={}", superUsers, superGroups);
    }

    public void setServiceAdmins(Set<String> users) {
        serviceAdmins = CollectionUtils.isEmpty(users) ? Collections.emptySet() : new HashSet<>(users);
    }

    public boolean isAuditExcludedUser(String userName) {
        return auditExcludedUsers.contains(userName);
    }

    public boolean hasAuditExcludedGroup(Set<String> userGroups) {
        return userGroups != null && !userGroups.isEmpty() && !auditExcludedGroups.isEmpty() && CollectionUtils.containsAny(userGroups, auditExcludedGroups);
    }

    public boolean hasAuditExcludedRole(Set<String> userRoles) {
        return userRoles != null && !userRoles.isEmpty() && !auditExcludedRoles.isEmpty() && CollectionUtils.containsAny(userRoles, auditExcludedRoles);
    }

    public boolean isSuperUser(String userName) {
        return superUsers.contains(userName);
    }

    public boolean hasSuperGroup(Set<String> userGroups) {
        return userGroups != null && !userGroups.isEmpty() && !superGroups.isEmpty() && CollectionUtils.containsAny(userGroups, superGroups);
    }

    public boolean isServiceAdmin(String userName) {
        return serviceAdmins.contains(userName);
    }

    public void addSuperUsers(Collection<String> users) {
        if (users != null) {
            superUsers.addAll(users);
        }
    }

    private void addResourcesForServiceType(String serviceType) {
        String auditCfg    = "ranger-" + serviceType + "-audit.xml";
        String securityCfg = "ranger-" + serviceType + "-security.xml";
        String sslCfg      = "ranger-" + serviceType + "-policymgr-ssl.xml";

        if (!addResourceIfReadable(auditCfg)) {
            addAuditResource(serviceType);
        }

        if (!addResourceIfReadable(securityCfg)) {
            addSecurityResource(serviceType);
        }

        if (!addResourceIfReadable(sslCfg)) {
            addSslConfigResource(serviceType);
        }
    }

    // load service specific config overrides, if config files are available
    private void addResourcesForServiceName(String serviceType, String serviceName) {
        if (StringUtils.isNotBlank(serviceType) && StringUtils.isNotBlank(serviceName)) {
            String serviceAuditCfg    = "ranger-" + serviceType + "-" + serviceName + "-audit.xml";
            String serviceSecurityCfg = "ranger-" + serviceType + "-" + serviceName + "-security.xml";
            String serviceSslCfg      = "ranger-" + serviceType + "-" + serviceName + "-policymgr-ssl.xml";

            addResourceIfReadable(serviceAuditCfg);
            addResourceIfReadable(serviceSecurityCfg);
            addResourceIfReadable(serviceSslCfg);
        }
    }

    private void addSecurityResource(String serviceType) {
        LOG.debug("==> addSecurityResource(Service Type: {}", serviceType);

        Configuration rangerConf = RangerLegacyConfigBuilder.getSecurityConfig(serviceType);

        if (rangerConf != null) {
            addResource(rangerConf);
        } else {
            LOG.debug("Unable to add the Security Config for {}. Plugin won't be enabled!", serviceType);
        }

        LOG.debug("<= addSecurityResource(Service Type: {}", serviceType);
    }

    private void addAuditResource(String serviceType) {
        LOG.debug("==> addAuditResource(Service Type: {}", serviceType);

        try {
            URL url = RangerLegacyConfigBuilder.getAuditConfig(serviceType);

            if (url != null) {
                addResource(url);

                LOG.debug("==> addAuditResource() URL {}", url.getPath());
            }
        } catch (Throwable t) {
            LOG.warn("Unable to find Audit Config for {} Auditing not enabled !", serviceType);
            LOG.debug("Unable to find Audit Config for {} Auditing not enabled !", serviceType, t);
        }

        LOG.debug("<== addAuditResource(Service Type: {})", serviceType);
    }

    private void addSslConfigResource(String serviceType) {
        LOG.debug("==> addSslConfigResource(Service Type: {}", serviceType);

        try {
            String sslConfigFile = this.get(RangerLegacyConfigBuilder.getPropertyName(RangerConfigConstants.RANGER_PLUGIN_REST_SSL_CONFIG_FILE, serviceType));

            URL url = getSSLConfigResource(sslConfigFile);
            if (url != null) {
                addResource(url);

                LOG.debug("SSL config file URL: {}", url.getPath());
            }
        } catch (Throwable t) {
            LOG.warn(" Unable to find SSL Configs");
            LOG.debug(" Unable to find SSL Configs");
        }

        LOG.debug("<== addSslConfigResource(Service Type: {})", serviceType);
    }

    private URL getSSLConfigResource(String fileName) throws Throwable {
        URL ret = null;

        try {
            if (fileName != null) {
                File f = new File(fileName);
                if (f.exists() && f.canRead()) {
                    ret = f.toURI().toURL();
                }
            }
        } catch (Throwable t) {
            LOG.error("Unable to read SSL configuration file: {}", fileName);

            throw t;
        }

        return ret;
    }
}
