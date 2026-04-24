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

package org.apache.ranger.authz.remote;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerResourcePermissionsRequest;

import java.util.Properties;

import static org.apache.ranger.authz.api.RangerAuthzApiErrorCode.INVALID_REQUEST_SERVICE_NAME_OR_TYPE_MANDATORY;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.INVALID_PROPERTY_VALUE;
import static org.apache.ranger.authz.remote.RangerRemoteAuthzErrorCode.REMOTE_REQUEST_FAILED;

public class RangerRemoteAuthorizer extends RangerAuthorizer {
    private final RangerRemoteAuthzConfig config;

    private volatile RangerPdpClient client;

    public RangerRemoteAuthorizer(Properties properties) {
        super(properties);

        this.config = new RangerRemoteAuthzConfig(properties);
    }

    @Override
    public void init() throws RangerAuthzException {
        config.getPdpUrl();
        config.getConnectTimeoutMs();
        config.getReadTimeoutMs();

        this.client = new RangerPdpClient(config);
    }

    @Override
    public void close() throws RangerAuthzException {
        RangerPdpClient client = this.client;

        this.client = null;

        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new RangerAuthzException(REMOTE_REQUEST_FAILED, e, RangerRemoteAuthzConfig.PROP_REMOTE_URL);
            }
        }
    }

    @Override
    public RangerAuthzResult authorize(RangerAuthzRequest request) throws RangerAuthzException {
        validateRequest(request);

        return getClient().authorize(request);
    }

    @Override
    public RangerMultiAuthzResult authorize(RangerMultiAuthzRequest request) throws RangerAuthzException {
        validateRequest(request);

        return getClient().authorize(request);
    }

    @Override
    public RangerResourcePermissions getResourcePermissions(RangerResourcePermissionsRequest request) throws RangerAuthzException {
        validateRequest(request);

        return getClient().getResourcePermissions(request);
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
                throw new RangerAuthzException(INVALID_PROPERTY_VALUE, RangerRemoteAuthzConfig.PROP_PREFIX_SERVICE_TYPE + serviceType + ".default.service", serviceName);
            }

            context.setServiceName(serviceName);
        }

        if (StringUtils.isBlank(serviceType)) {
            serviceType = config.getServiceTypeForService(serviceName);

            if (StringUtils.isBlank(serviceType)) {
                throw new RangerAuthzException(INVALID_PROPERTY_VALUE, RangerRemoteAuthzConfig.PROP_PREFIX_SERVICE + serviceName + ".servicetype", serviceType);
            }

            context.setServiceType(serviceType);
        }
    }

    private RangerPdpClient getClient() throws RangerAuthzException {
        RangerPdpClient ret = client;

        if (ret == null) {
            throw new RangerAuthzException(INVALID_PROPERTY_VALUE, RangerRemoteAuthzConfig.PROP_REMOTE_URL, "authorizer is not initialized");
        }

        return ret;
    }
}
