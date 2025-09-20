/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.rest;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.RangerLogLevelService;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("admin")
@Component
@Scope("singleton")
public class AdminREST {
    private static final Logger LOG = LoggerFactory.getLogger(AdminREST.class);

    @Inject
    RangerLogLevelService logLevelService;

    @Inject
    RESTErrorUtil restErrorUtil;

    /**
     * An endpoint to set the log level for a specific class or package.
     * This operation requires ROLE_SYS_ADMIN role as it affects system logging behavior
     * and can impact performance and security monitoring.
     *
     * @param request The request containing loggerName and logLevel
     * @return An HTTP response indicating success or failure.
     */
    @POST
    @Path("/set-logger-level")
    @Consumes("application/json")
    @Produces("application/json")
    @PreAuthorize("hasRole('ROLE_SYS_ADMIN')")
    public Response setLogLevel(LogLevelRequest request) {
        try {
            // Validate input parameters
            if (request == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Request body is required")
                        .build();
            }

            if (StringUtils.isBlank(request.getLoggerName())) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("loggerName is required")
                        .build();
            }

            if (StringUtils.isBlank(request.getLogLevel())) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("logLevel is required")
                        .build();
            }

            LOG.info("Setting log level for logger '{}' to '{}'", request.getLoggerName(), request.getLogLevel());

            String result = logLevelService.setLogLevel(request.getLoggerName().trim(), request.getLogLevel().trim());

            return Response.ok(result).build();
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid parameters for setting log level:", e);

            return Response.status(Response.Status.BAD_REQUEST)
                    .entity("Invalid parameters: " + e.getMessage())
                    .build();
        } catch (UnsupportedOperationException e) {
            LOG.error("Unsupported operation for setting log level:", e);

            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("Service not available: " + e.getMessage())
                    .build();
        } catch (Exception e) {
            LOG.error("Error setting log level for request: {}", request, e);

            throw restErrorUtil.createRESTException(e.getMessage(), MessageEnums.ERROR_SYSTEM);
        }
    }

    /**
     * Request class for JSON payload.
     */
    public static class LogLevelRequest {
        private String loggerName;
        private String logLevel;

        public LogLevelRequest() {
            // Default constructor for JSON deserialization
        }

        public LogLevelRequest(String loggerName, String logLevel) {
            this.loggerName = loggerName;
            this.logLevel   = logLevel;
        }

        public String getLoggerName() {
            return loggerName;
        }

        public void setLoggerName(String loggerName) {
            this.loggerName = loggerName;
        }

        public String getLogLevel() {
            return logLevel;
        }

        public void setLogLevel(String logLevel) {
            this.logLevel = logLevel;
        }

        @Override
        public String toString() {
            return "LogLevelRequest{" +
                    "loggerName='" + loggerName + '\'' +
                    ", logLevel='" + logLevel + '\'' +
                    '}';
        }
    }
}
