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
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerSecurityZoneMatcher;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.apache.ranger.plugin.util.RangerRoles;
import org.apache.ranger.plugin.util.RangerRolesUtil;
import org.apache.ranger.plugin.util.RangerUserStore;
import org.apache.ranger.plugin.util.RangerUserStoreUtil;
import org.apache.ranger.ugsyncutil.transform.Mapper;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants.CaseConversion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants.toCaseConversion;

public class RangerAuthContext {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAuthContext.class);

    private final Map<RangerContextEnricher, Object> requestContextEnrichers;
    private final RangerSecurityZoneMatcher          zoneMatcher;
    private       RangerRolesUtil                    rolesUtil;
    private       RangerUserStoreUtil                userStoreUtil;
    private       Mapper                             userNameTransformer;
    private       Mapper                             groupNameTransformer;
    private       CaseConversion                     userNameCaseConversion;
    private       CaseConversion                     groupNameCaseConversion;

    public RangerAuthContext(RangerAuthContext prevContext, RangerSecurityZoneMatcher zoneMatcher, RangerRoles roles) {
        this(null, zoneMatcher, roles, prevContext != null ? prevContext.getUserStoreUtil().getUserStore() : null);

        if (prevContext != null) {
            this.userNameTransformer     = prevContext.userNameTransformer;
            this.groupNameTransformer    = prevContext.groupNameTransformer;
            this.userNameCaseConversion  = prevContext.userNameCaseConversion;
            this.groupNameCaseConversion = prevContext.groupNameCaseConversion;
        }
    }

    public RangerAuthContext(Map<RangerContextEnricher, Object> requestContextEnrichers, RangerSecurityZoneMatcher zoneMatcher, RangerRoles roles, RangerUserStore userStore) {
        this.requestContextEnrichers = requestContextEnrichers != null ? requestContextEnrichers : new ConcurrentHashMap<>();
        this.zoneMatcher             = zoneMatcher;

        setRoles(roles);
        setUserStore(userStore);
    }

    public Map<RangerContextEnricher, Object> getRequestContextEnrichers() {
        return requestContextEnrichers;
    }

    public RangerSecurityZoneMatcher getZoneMatcher() {
        return zoneMatcher;
    }

    public void addOrReplaceRequestContextEnricher(RangerContextEnricher enricher, Object database) {
        // concurrentHashMap does not allow null to be inserted into it, so insert a dummy which is checked
        // when enrich() is called
        requestContextEnrichers.put(enricher, database != null ? database : enricher);

        if (database instanceof RangerUserStore) {
            setUserStore((RangerUserStore) database);
        }
    }

    public void cleanupRequestContextEnricher(RangerContextEnricher enricher) {
        requestContextEnrichers.remove(enricher);
    }

    public void setRoles(RangerRoles roles) {
        this.rolesUtil = new RangerRolesUtil(roles);
    }

    public Set<String> getRolesForUserAndGroups(String user, Set<String> groups) {
        RangerRolesUtil          rolesUtil        = this.rolesUtil;
        Map<String, Set<String>> userRoleMapping  = rolesUtil.getUserRoleMapping();
        Map<String, Set<String>> groupRoleMapping = rolesUtil.getGroupRoleMapping();
        Set<String>              allRoles         = new HashSet<>();

        if (MapUtils.isNotEmpty(userRoleMapping) && StringUtils.isNotEmpty(user)) {
            Set<String> userRoles = userRoleMapping.get(user);

            if (CollectionUtils.isNotEmpty(userRoles)) {
                allRoles.addAll(userRoles);
            }
        }

        if (MapUtils.isNotEmpty(groupRoleMapping)) {
            if (CollectionUtils.isNotEmpty(groups)) {
                for (String group : groups) {
                    Set<String> groupRoles = groupRoleMapping.get(group);

                    if (CollectionUtils.isNotEmpty(groupRoles)) {
                        allRoles.addAll(groupRoles);
                    }
                }
            }

            Set<String> publicGroupRoles = groupRoleMapping.get(RangerPolicyEngine.GROUP_PUBLIC);

            if (CollectionUtils.isNotEmpty(publicGroupRoles)) {
                allRoles.addAll(publicGroupRoles);
            }
        }

        return allRoles;
    }

    public long getRoleVersion() {
        return this.rolesUtil.getRoleVersion();
    }

    public RangerRolesUtil getRangerRolesUtil() {
        return this.rolesUtil;
    }

    public long getUserStoreVersion() {
        return this.userStoreUtil.getUserStoreVersion();
    }

    public RangerUserStoreUtil getUserStoreUtil() {
        return this.userStoreUtil;
    }

    public void setUserStore(RangerUserStore userStore) {
        this.userStoreUtil = new RangerUserStoreUtil(userStore);
    }

    public Mapper getUserNameTransformer() {
        return userNameTransformer;
    }

    public Mapper getGroupNameTransformer() {
        return groupNameTransformer;
    }

    public CaseConversion getUserNameCaseConversion() {
        return userNameCaseConversion;
    }

    public CaseConversion getGroupNameCaseConversion() {
        return groupNameCaseConversion;
    }

    public void onServiceConfigsUpdate(Map<String, String> serviceConfigs) {
        String userNameCaseConversion  = null;
        String groupNameCaseConversion = null;
        Mapper userNameTransformer     = null;
        Mapper groupNameTransformer    = null;

        if (MapUtils.isNotEmpty(serviceConfigs)) {
            LOG.debug("==> onServiceConfigsUpdate({})", serviceConfigs.keySet());

            userNameCaseConversion  = serviceConfigs.get(RangerCommonConstants.PLUGINS_CONF_USERNAME_CASE_CONVERSION_PARAM);
            groupNameCaseConversion = serviceConfigs.get(RangerCommonConstants.PLUGINS_CONF_GROUPNAME_CASE_CONVERSION_PARAM);

            String mappingUserNameHandler = serviceConfigs.get(RangerCommonConstants.PLUGINS_CONF_MAPPING_USERNAME_HANDLER);

            if (mappingUserNameHandler != null) {
                try {
                    Class<Mapper> regExClass = (Class<Mapper>) Class.forName(mappingUserNameHandler);

                    userNameTransformer = regExClass.newInstance();

                    String baseProperty = RangerCommonConstants.PLUGINS_CONF_MAPPING_USERNAME;

                    userNameTransformer.init(baseProperty, getAllRegexPatterns(baseProperty, serviceConfigs), serviceConfigs.get(RangerCommonConstants.PLUGINS_CONF_MAPPING_SEPARATOR));
                } catch (ClassNotFoundException cne) {
                    LOG.error("Failed to load {}", mappingUserNameHandler, cne);
                } catch (Throwable te) {
                    LOG.error("Failed to instantiate {}", mappingUserNameHandler, te);
                }
            }

            String mappingGroupNameHandler = serviceConfigs.get(RangerCommonConstants.PLUGINS_CONF_MAPPING_GROUPNAME_HANDLER);

            if (mappingGroupNameHandler != null) {
                try {
                    Class<Mapper> regExClass = (Class<Mapper>) Class.forName(mappingGroupNameHandler);

                    groupNameTransformer = regExClass.newInstance();

                    String baseProperty = RangerCommonConstants.PLUGINS_CONF_MAPPING_GROUPNAME;

                    groupNameTransformer.init(baseProperty, getAllRegexPatterns(baseProperty, serviceConfigs), serviceConfigs.get(RangerCommonConstants.PLUGINS_CONF_MAPPING_SEPARATOR));
                } catch (ClassNotFoundException cne) {
                    LOG.error("Failed to load {}", mappingGroupNameHandler, cne);
                } catch (Throwable te) {
                    LOG.error("Failed to instantiate {}", mappingGroupNameHandler, te);
                }
            }
        }

        setUserNameCaseConversion(userNameCaseConversion);
        setGroupNameCaseConversion(groupNameCaseConversion);
        setUserNameTransformer(userNameTransformer);
        setGroupNameTransformer(groupNameTransformer);
    }

    private void setUserNameTransformer(Mapper userNameTransformer) {
        this.userNameTransformer = userNameTransformer;
    }

    private void setGroupNameTransformer(Mapper groupNameTransformer) {
        this.groupNameTransformer = groupNameTransformer;
    }

    private void setUserNameCaseConversion(String userNameCaseConversion) {
        this.userNameCaseConversion = toCaseConversion(userNameCaseConversion);
    }

    private void setGroupNameCaseConversion(String groupNameCaseConversion) {
        this.groupNameCaseConversion = toCaseConversion(groupNameCaseConversion);
    }

    private List<String> getAllRegexPatterns(String baseProperty, Map<String, String> serviceConfig) {
        LOG.debug("==> getAllRegexPatterns({})", baseProperty);

        List<String> regexPatterns = new ArrayList<>();
        String       baseRegex     = serviceConfig != null ? serviceConfig.get(baseProperty) : null;

        LOG.debug("baseRegex = {}, pluginConfig = {}", baseRegex, serviceConfig == null ? null : serviceConfig.keySet());

        if (baseRegex != null) {
            regexPatterns.add(baseRegex);

            for (int i = 1; true; i++) {
                String nextRegex = serviceConfig.get(baseProperty + "." + i);

                if (nextRegex == null) {
                    break;
                }

                regexPatterns.add(nextRegex);
            }
        }

        LOG.debug("<== getAllRegexPatterns({}): ret={}", baseProperty, regexPatterns);

        return regexPatterns;
    }
}
