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

package org.apache.ranger.rest;

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.plugin.model.RangerServerHealth;
import org.apache.ranger.util.RangerServerHealthUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import java.util.List;

@Path("actuator")
@Component
@Scope("request")
public class RangerHealthREST {
    @Autowired
    RangerBizUtil bizUtil;

    @Autowired
    RangerServerHealthUtil rangerServerHealthUtil;

    @Autowired
    RESTErrorUtil restErrorUtil;

    /*
    This API is used to get the Health check of the Ranger Admin
    */
    @GET
    @Path("/health")
    @Produces("application/json")
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public RangerServerHealth getRangerServerHealth() {
        String dbVersion = bizUtil.getDBVersion();

        return rangerServerHealthUtil.getRangerServerHealth(dbVersion);
    }

    @GET
    @Path("/health/readiness")
    @Produces("application/json")
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public RangerServerHealth getRangerServerReadiness() {
        if (!bizUtil.isHealthCheckUser(rangerServerHealthUtil.resolveAuthenticatedLoginId())) {
            throw restErrorUtil.createRESTException(HttpServletResponse.SC_FORBIDDEN,
                    "Only the healthcheck user may query service-def names via this path.", true);
        }

        List<String> serviceDefNames = rangerServerHealthUtil.getServiceDefNames();
        RangerServerHealth serverHealth;

        if (serviceDefNames != null && !serviceDefNames.isEmpty()) {
            serverHealth = rangerServerHealthUtil.serviceUpWithAvailableServiceDefs(serviceDefNames);
        } else {
            serverHealth = rangerServerHealthUtil.serviceInitFailure();
        }

        return serverHealth;
    }

    @GET
    @Path("/health/liveness")
    @Produces("application/json")
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public RangerServerHealth getRangerServerLiveness() {
        return rangerServerHealthUtil.serviceUp();
    }
}
