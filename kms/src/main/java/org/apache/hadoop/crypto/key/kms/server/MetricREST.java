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

package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.ranger.kms.metrics.KMSMetricWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Map;

@Path("metrics")
@InterfaceAudience.Private
public class MetricREST {

    private static final Logger LOG = LoggerFactory.getLogger(MetricREST.class);

    private KMSMetricWrapper kmsMetricWrapper = KMSMetricWrapper.getInstance(KMSWebApp.isMetricCollectionThreadSafe());

    @GET
    @Path("/prometheus")
    @Produces(MediaType.TEXT_PLAIN)
    public String getMetricsPrometheus() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("MetricsREST.getMetricsPrometheus() ===>>");
        }
        String ret = "";
        try {
            ret = kmsMetricWrapper.getRangerMetricsInPrometheusFormat();
        } catch (Exception e) {
            LOG.error("MetricsREST.getMetricsPrometheus(): Exception occured while getting metric.", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("MetricsREST.getMetricsPrometheus() <<=== {}" + ret);
        }
        return ret;
    }

    @GET
    @Path("/json")
    @Produces({ "application/json", "application/xml" })
    public Map<String, Map<String, Object>> getMetricsJson() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("MetricsREST.getMetricsJson() ===>>");
        }

        Map<String, Map<String, Object>> ret = null;
        try {
            ret = kmsMetricWrapper.getRangerMetricsInJsonFormat();
        } catch (Exception e) {
            LOG.error("MetricsREST.getMetricsJson(): Exception occurred while getting metric.", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("MetricsREST.getMetricsJson() <<=== {}" + ret);
        }
        return ret;
    }
}
