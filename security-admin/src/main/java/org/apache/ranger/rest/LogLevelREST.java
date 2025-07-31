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

import org.apache.ranger.biz.RangerLogLevelService;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * REST API for logging-related operations.
 */
@Path("loggers")
@Component
@Scope("singleton")
public class LogLevelREST {
    private static final Logger LOG = LoggerFactory.getLogger(LogLevelREST.class);

    @Inject
    RangerLogLevelService logLevelService;

    @Inject
    RESTErrorUtil restErrorUtil;

    /**
     * An endpoint to dynamically reload the logging configuration from the
     * log4j2 properties file on the classpath.
     *
     * @return An HTTP response indicating success or failure.
     */
    @POST
    @Path("/reload")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response reloadLogConfiguration() {
        try {
            logLevelService.reloadLogConfiguration();
            return Response.ok("Log configuration reloaded successfully.").build();
        } catch (Exception e) {
            LOG.error("Error reloading Log configuration", e);
            throw restErrorUtil.createRESTException(e.getMessage(), MessageEnums.ERROR_SYSTEM);
        }
    }
}
