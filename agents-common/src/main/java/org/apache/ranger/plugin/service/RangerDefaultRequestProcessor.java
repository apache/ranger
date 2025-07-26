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
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.contextenricher.RangerContextEnricher;
import org.apache.ranger.plugin.policyengine.PolicyEngine;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestProcessor;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerMutableResource;
import org.apache.ranger.plugin.policyengine.RangerPluginContext;
import org.apache.ranger.plugin.util.RangerAccessRequestUtil;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerUserStoreUtil;
import org.apache.ranger.ugsyncutil.transform.Mapper;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class RangerDefaultRequestProcessor implements RangerAccessRequestProcessor {
    private static final Logger LOG                              = LoggerFactory.getLogger(RangerDefaultRequestProcessor.class);
    private static final Logger PERF_CONTEXTENRICHER_REQUEST_LOG = RangerPerfTracer.getPerfLogger("contextenricher.request");

    protected final PolicyEngine policyEngine;
    private   final boolean      useRangerGroups;
    private   final boolean      useOnlyRangerGroups;
    private   final boolean      convertEmailToUser;

    public RangerDefaultRequestProcessor(PolicyEngine policyEngine) {
        this.policyEngine = policyEngine;

        RangerPluginContext pluginContext = policyEngine.getPluginContext();
        RangerPluginConfig  pluginConfig  = pluginContext != null ? pluginContext.getConfig() : null;

        if (pluginConfig != null) {
            useRangerGroups     = pluginConfig.isUseRangerGroups();
            useOnlyRangerGroups = pluginConfig.isUseOnlyRangerGroups();
            convertEmailToUser  = pluginConfig.isConvertEmailToUsername();
        } else {
            useRangerGroups     = false;
            useOnlyRangerGroups = false;
            convertEmailToUser  = false;
        }
    }

    @Override
    public void preProcess(RangerAccessRequest request) {
        LOG.debug("==> preProcess({})", request);

        if (RangerAccessRequestUtil.getIsRequestPreprocessed(request.getContext())) {
            LOG.debug("<== preProcess({})", request);

            return;
        }

        setResourceServiceDef(request);

        RangerPluginContext     pluginContext = policyEngine.getPluginContext();
        RangerAccessRequestImpl reqImpl       = null;

        if (request instanceof RangerAccessRequestImpl) {
            reqImpl = (RangerAccessRequestImpl) request;

            if (reqImpl.getClientIPAddress() == null) {
                reqImpl.extractAndSetClientIPAddress(policyEngine.getUseForwardedIPAddress(), policyEngine.getTrustedProxyAddresses());
            }

            if (pluginContext != null) {
                if (reqImpl.getClusterName() == null) {
                    reqImpl.setClusterName(pluginContext.getClusterName());
                }

                if (reqImpl.getClusterType() == null) {
                    reqImpl.setClusterType(pluginContext.getClusterType());
                }

                RangerPluginConfig config = policyEngine.getPluginContext().getConfig();

                boolean isNameTransformationSupported = config.getBoolean(config.getPropertyPrefix() + RangerCommonConstants.PLUGIN_CONFIG_SUFFIX_NAME_TRANSFORMATION, false);

                LOG.debug("isNameTransformationSupported = {}", isNameTransformationSupported);

                if (isNameTransformationSupported) {
                    reqImpl.setUser(getTransformedUser(policyEngine, request));
                    reqImpl.setUserGroups(getTransformedGroups(policyEngine, request));
                }

                convertEmailToUsername(reqImpl);

                updateUserGroups(reqImpl);
            }
        }

        RangerAccessRequestUtil.setCurrentUserInContext(request.getContext(), request.getUser());

        String owner = request.getResource() != null ? request.getResource().getOwnerUser() : null;

        if (StringUtils.isNotEmpty(owner)) {
            RangerAccessRequestUtil.setOwnerInContext(request.getContext(), owner);
        }

        Set<String> roles = request.getUserRoles();
        if (pluginContext != null && CollectionUtils.isEmpty(roles)) {
            roles = pluginContext.getAuthContext().getRolesForUserAndGroups(request.getUser(), request.getUserGroups());

            if (reqImpl != null && roles != null && !roles.isEmpty()) {
                reqImpl.setUserRoles(roles);
            }
        }

        if (CollectionUtils.isNotEmpty(roles)) {
            RangerAccessRequestUtil.setCurrentUserRolesInContext(request.getContext(), roles);
        }

        Set<String> zoneNames = policyEngine.getMatchedZonesForResourceAndChildren(request.getResource());

        RangerAccessRequestUtil.setResourceZoneNamesInContext(request, zoneNames);

        enrich(request);

        RangerAccessRequestUtil.setIsRequestPreprocessed(request.getContext(), Boolean.TRUE);

        LOG.debug("<== preProcess({})", request);
    }

    @Override
    public void enrich(RangerAccessRequest request) {
        List<RangerContextEnricher> enrichers = policyEngine.getAllContextEnrichers();

        if (!CollectionUtils.isEmpty(enrichers)) {
            for (RangerContextEnricher enricher : enrichers) {
                RangerPerfTracer perf = null;

                if (RangerPerfTracer.isPerfTraceEnabled(PERF_CONTEXTENRICHER_REQUEST_LOG)) {
                    perf = RangerPerfTracer.getPerfTracer(PERF_CONTEXTENRICHER_REQUEST_LOG, "RangerContextEnricher.enrich(requestHashCode=" + Integer.toHexString(System.identityHashCode(request)) + ", enricherName=" + enricher.getName() + ")");
                }

                enricher.enrich(request);

                RangerPerfTracer.log(perf);
            }
        } else {
            LOG.debug("No context-enrichers!!!");
        }
    }

    private String getTransformedUser(PolicyEngine policyEngine, RangerAccessRequest request) {
        RangerAuthContext authContext     = policyEngine.getPluginContext().getAuthContext();
        boolean           toLowerCase     = authContext.getUserNameCaseConversion() == UgsyncCommonConstants.CaseConversion.TO_LOWER;
        boolean           toUpperCase     = authContext.getUserNameCaseConversion() == UgsyncCommonConstants.CaseConversion.TO_UPPER;
        Mapper            nameTransformer = authContext.getUserNameTransformer();

        if (toLowerCase || toUpperCase || nameTransformer != null) {
            String user = request.getUser();

            if (toLowerCase) {
                user = user.toLowerCase();
            } else if (toUpperCase) {
                user = user.toUpperCase();
            }

            if (nameTransformer != null) {
                user = nameTransformer.transform(user);
            }

            LOG.debug("Original username = {}, Transformed username = {}", request.getUser(), user);

            return user;
        }

        return request.getUser();
    }

    private Set<String> getTransformedGroups(PolicyEngine policyEngine, RangerAccessRequest request) {
        if (CollectionUtils.isNotEmpty(request.getUserGroups())) {
            RangerAuthContext authContext     = policyEngine.getPluginContext().getAuthContext();
            boolean           toLowerCase     = authContext.getGroupNameCaseConversion() == UgsyncCommonConstants.CaseConversion.TO_LOWER;
            boolean           toUpperCase     = authContext.getGroupNameCaseConversion() == UgsyncCommonConstants.CaseConversion.TO_UPPER;
            Mapper            nameTransformer = authContext.getGroupNameTransformer();

            if (toLowerCase || toUpperCase || nameTransformer != null) {
                return request.getUserGroups().stream()
                        .filter(Objects::nonNull)
                        .map(group -> {
                            String originalGroup = group;

                            if (toLowerCase) {
                                group = group.toLowerCase();
                            } else if (toUpperCase) {
                                group = group.toUpperCase();
                            }

                            String transformedGroup = nameTransformer.transform(group);

                            LOG.debug("Original group name = {}, Transformed group name = {}", originalGroup, transformedGroup);

                            return transformedGroup;
                        })
                        .collect(Collectors.toSet());
            }
        }

        return request.getUserGroups();
    }

    private void setResourceServiceDef(RangerAccessRequest request) {
        RangerAccessResource resource = request.getResource();

        if (resource.getServiceDef() == null) {
            if (resource instanceof RangerMutableResource) {
                RangerMutableResource mutable = (RangerMutableResource) resource;

                mutable.setServiceDef(policyEngine.getServiceDef());
            }
        }
    }

    private void convertEmailToUsername(RangerAccessRequestImpl reqImpl) {
        if (convertEmailToUser) {
            RangerPluginContext pluginContext = policyEngine.getPluginContext();
            RangerUserStoreUtil userStoreUtil = pluginContext != null ? pluginContext.getAuthContext().getUserStoreUtil() : null;

            if (userStoreUtil != null) {
                String userName = reqImpl.getUser();
                int    idxSep   = StringUtils.indexOf(userName, '@');

                if (idxSep > 0) {
                    String userNameFromEmail = userStoreUtil.getUserNameFromEmail(userName);

                    if (StringUtils.isBlank(userNameFromEmail)) {
                        userNameFromEmail = userName.substring(0, idxSep);
                    }

                    LOG.debug("replacing req.user '{}' with '{}'", userName, userNameFromEmail);

                    reqImpl.setUser(userNameFromEmail);
                }
            }
        }
    }

    private void updateUserGroups(RangerAccessRequestImpl reqImpl) {
        if (useRangerGroups) {
            RangerPluginContext pluginContext = policyEngine.getPluginContext();
            RangerUserStoreUtil userStoreUtil = pluginContext != null ? pluginContext.getAuthContext().getUserStoreUtil() : null;
            String              userName      = reqImpl.getUser();

            if (userStoreUtil != null && userName != null) {
                Set<String> userGroups       = reqImpl.getUserGroups();
                Set<String> rangerUserGroups = userStoreUtil.getUserGroups(userName);

                if (rangerUserGroups == null) {
                    rangerUserGroups = Collections.emptySet();
                }

                if (useOnlyRangerGroups) {
                    userGroups = new HashSet<>(rangerUserGroups);

                    LOG.debug("replacing req.userGroups '{}' with '{}'", reqImpl.getUserGroups(), userGroups);

                    reqImpl.setUserGroups(userGroups);
                } else {
                    if (!rangerUserGroups.isEmpty()) {
                        userGroups = userGroups != null ? new HashSet<>(userGroups) : new HashSet<>();

                        userGroups.addAll(rangerUserGroups);

                        LOG.debug("replacing req.userGroups '{}' with '{}'", reqImpl.getUserGroups(), userGroups);

                        reqImpl.setUserGroups(userGroups);
                    }
                }
            }
        }
    }
}
