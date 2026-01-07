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

package org.apache.ranger.authz.embedded;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAccessInfo;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerAuthzResult.AccessDecision;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourceInfo;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY;
import static org.apache.ranger.authz.embedded.RangerEmbeddedAuthzErrorCode.NO_DEFAULT_SERVICE_FOR_SERVICE_TYPE;
import static org.apache.ranger.authz.embedded.RangerEmbeddedAuthzErrorCode.NO_SERVICE_TYPE_FOR_SERVICE;

public class RangerEmbeddedAuthorizer extends RangerAuthorizer {
    private static final Logger LOG = LoggerFactory.getLogger(RangerEmbeddedAuthorizer.class);

    private final RangerAuthzConfig              config;
    private final Map<String, RangerAuthzPlugin> plugins = new HashMap<>();

    public RangerEmbeddedAuthorizer(Properties properties) {
        super(properties);

        this.config = new RangerAuthzConfig(properties);
    }

    public void init() throws RangerAuthzException {
        AuditProviderFactory.getInstance().init(config.getAuditProperties(), "ranger-authz");

        String[] initServices = config.getInitServices();

        for (String serviceName : initServices) {
            String serviceType = config.getServiceTypeForService(serviceName);

            getOrCreatePlugin(serviceName, serviceType);
        }
    }

    @Override
    public void close() {
        for (RangerAuthzPlugin plugin : plugins.values()) {
            plugin.cleanup();
        }

        plugins.clear();
    }

    @Override
    public RangerAuthzResult authorize(RangerAuthzRequest request) throws RangerAuthzException {
        validateRequest(request);

        RangerAuthzPlugin plugin = getOrCreatePlugin(request.getContext().getServiceName(), request.getContext().getServiceType());

        try (RangerAuthzAuditHandler auditHandler = new RangerAuthzAuditHandler(plugin.getPlugin())) {
            return authorize(request, plugin, auditHandler);
        }
    }

    @Override
    public RangerMultiAuthzResult authorize(RangerMultiAuthzRequest request) throws RangerAuthzException {
        validateRequest(request);

        RangerAuthzPlugin      plugin = getOrCreatePlugin(request.getContext().getServiceName(), request.getContext().getServiceType());
        RangerMultiAuthzResult result = new RangerMultiAuthzResult(request.getRequestId());

        if (request.getAccesses() != null) {
            int allowedCount       = 0;
            int deniedCount        = 0;
            int notDeterminedCount = 0;

            result.setAccesses(new ArrayList<>(request.getAccesses().size()));

            try (RangerAuthzAuditHandler auditHandler = new RangerAuthzAuditHandler(plugin.getPlugin())) {
                for (RangerAccessInfo accessInfo : request.getAccesses()) {
                    RangerAuthzRequest authzRequest = new RangerAuthzRequest(null, request.getUser(), accessInfo, request.getContext());
                    RangerAuthzResult  authzResult  = authorize(authzRequest, plugin, auditHandler);

                    if (authzResult.getDecision() == AccessDecision.ALLOW) {
                        allowedCount++;
                    } else if (authzResult.getDecision() == AccessDecision.DENY) {
                        deniedCount++;
                    } else if (authzResult.getDecision() == AccessDecision.NOT_DETERMINED) {
                        notDeterminedCount++;
                    }

                    result.getAccesses().add(authzResult);
                }
            }

            if (allowedCount == request.getAccesses().size()) {
                result.setDecision(AccessDecision.ALLOW);
            } else if (deniedCount == request.getAccesses().size()) {
                result.setDecision(AccessDecision.DENY);
            } else if (notDeterminedCount == request.getAccesses().size()) {
                result.setDecision(AccessDecision.NOT_DETERMINED);
            } else {
                result.setDecision(AccessDecision.PARTIAL);
            }
        }

        return result;
    }

    @Override
    public RangerResourcePermissions getResourcePermissions(RangerResourceInfo resource, RangerAccessContext context) throws RangerAuthzException {
        validateAccessContext(context);

        RangerAuthzPlugin plugin = getOrCreatePlugin(context.getServiceName(), context.getServiceType());

        return plugin.getResourcePermissions(resource, context);
    }

    @Override
    protected void validateAccessContext(RangerAccessContext context) throws RangerAuthzException {
        super.validateAccessContext(context);

        String serviceName = context.getServiceName();
        String serviceType = context.getServiceType();

        if (StringUtils.isBlank(serviceName)) {
            if (StringUtils.isBlank(serviceType)) {
                throw new RangerAuthzException(INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY);
            }

            serviceName = config.getDefaultServiceNameForServiceType(serviceType);

            if (StringUtils.isBlank(serviceName)) {
                throw new RangerAuthzException(NO_DEFAULT_SERVICE_FOR_SERVICE_TYPE, serviceName);
            }

            context.setServiceName(serviceName);
        }

        if (StringUtils.isBlank(serviceType)) {
            serviceType = config.getServiceTypeForService(serviceName);

            if (StringUtils.isBlank(serviceType)) {
                throw new RangerAuthzException(NO_SERVICE_TYPE_FOR_SERVICE, serviceName);
            }

            context.setServiceType(serviceType);
        }
    }

    private RangerAuthzResult authorize(RangerAuthzRequest request, RangerAuthzPlugin plugin, RangerAuthzAuditHandler auditHandler) throws RangerAuthzException {
        return plugin.authorize(request, auditHandler);
    }

    private RangerAuthzPlugin getOrCreatePlugin(String serviceName, String serviceType) throws RangerAuthzException {
        RangerAuthzPlugin ret = plugins.get(serviceName);

        if (ret == null) {
            synchronized (plugins) {
                ret = plugins.get(serviceName);

                if (ret == null) {
                    Properties pluginProperties = this.config.getServiceProperties(serviceName, serviceType);

                    LOG.debug("properties for service {}: {}", serviceName, pluginProperties);

                    ret = new RangerAuthzPlugin(serviceType, serviceName, pluginProperties);

                    plugins.put(serviceName, ret);
                }
            }
        }

        return ret;
    }
}
