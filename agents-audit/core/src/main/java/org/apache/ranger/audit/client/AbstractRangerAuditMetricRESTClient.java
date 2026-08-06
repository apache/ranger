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

package org.apache.ranger.audit.client;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.audit.model.RangerAuditMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRangerAuditMetricRESTClient {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractRangerAuditMetricRESTClient.class);
    public               Gson   gson;

    public void init(String url, String sslConfigFileName, Configuration config) {
        Gson gson = null;

        try {
            gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
        } catch (Throwable excp) {
            LOG.error("AbstractRangerAdminClient: failed to create GsonBuilder object", excp);
        }

        this.gson = gson;
    }

    public RangerAuditMetrics createAuditMetrics(RangerAuditMetrics request) throws Exception {
        return null;
    }
}
