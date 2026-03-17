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

package org.apache.ranger.audit.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ranger.audit.consumer.SolrConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.Map;

/**
 * Health check REST endpoint for Solr Consumer Service
 */
@Path("/health")
@Component
@Scope("request")
public class HealthCheckREST {
    private static final Logger LOG = LoggerFactory.getLogger(HealthCheckREST.class);

    @Autowired(required = false)
    SolrConsumerManager solrConsumerManager;

    /**
     * Health check endpoint
     */
    @GET
    @Produces("application/json")
    public Response healthCheck() {
        LOG.debug("==> HealthCheckREST.healthCheck()");
        Response ret;
        String   jsonString;

        try {
            // Check if consumer manager is available and healthy
            if (solrConsumerManager != null) {
                Map<String, Object> resp = new HashMap<>();
                resp.put("status", "UP");
                resp.put("service", "audit-consumer-solr");
                resp.put("timestamp", System.currentTimeMillis());
                jsonString = buildResponse(resp);
                ret = Response.ok()
                        .entity(jsonString)
                        .build();
            } else {
                Map<String, Object> resp = new HashMap<>();
                resp.put("status", "DOWN");
                resp.put("service", "audit-consumer-solr");
                resp.put("reason", "SolrConsumerManager not available");
                resp.put("timestamp", System.currentTimeMillis());
                jsonString = buildResponse(resp);
                ret = Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity(jsonString)
                        .build();
            }
        } catch (Exception e) {
            LOG.error("Health check failed", e);
            Map<String, Object> resp = new HashMap<>();
            resp.put("status", "DOWN");
            resp.put("service", "audit-consumer-solr");
            resp.put("reason",  e.getMessage());
            resp.put("timestamp", System.currentTimeMillis());
            jsonString = buildResponse(resp);
            ret = Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(jsonString)
                    .build();
        }

        LOG.debug("<== HealthCheckREST.healthCheck(): {}", ret);

        return ret;
    }

    private String buildResponse(Map<String, Object> respMap) {
        String ret;

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            ret = objectMapper.writeValueAsString(respMap);
        } catch (Exception e) {
            ret = "Error: " + e.getMessage();
        }

        return ret;
    }
}
