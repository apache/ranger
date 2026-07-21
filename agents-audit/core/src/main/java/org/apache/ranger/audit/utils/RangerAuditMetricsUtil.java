/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.ranger.audit.client.AbstractRangerAuditMetricRESTClient;
import org.apache.ranger.audit.client.RangerAuditMetricRESTClient;
import org.apache.ranger.audit.model.RangerAuditMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class RangerAuditMetricsUtil {
    public static final String RANGER_ADMIN_URL                         = "ranger.plugin.%s.policy.rest.url";
    public static final String RANGER_ADMIN_SSL_CONFIG_FILENAME         = "ranger.plugin.%s.policy.rest.ssl.config.file";
    public static final String RANGER_ADMIN_IMPL_CLASS_CONF             = "ranger.plugin.%s.policy.source.impl";
    public static final String RANGER_SERVICE_TYPE_CONFIG               = "ranger.plugin.serviceType";
    public static final String RANGER_ADMIM_IMPL_CLASS                  = "org.apache.ranger.admin.client.RangerAdminRESTClient";
    public static final String RANGER_ADMIM_IMPL_JERSEY2_CLASS          = "org.apache.ranger.admin.client.RangerAdminJersey2RESTClient";
    public static final String RANGER_ADMIN_AUDIT_METRICS_JERSEY2_CLASS = "org.apache.ranger.admin.client.RangerAuditMetricsJersey2RESTClient";
    private static final Logger        LOG = LoggerFactory.getLogger(RangerAuditMetricsUtil.class);
    private final        Properties    props;
    private              Configuration auditConfig;
    private AbstractRangerAuditMetricRESTClient auditMetricRESTClient;

    public RangerAuditMetricsUtil(Properties props) {
        this.props = props;
        init(this.props);
    }

    public RangerAuditMetrics createAuditMetrics(RangerAuditMetrics request) throws Exception {
        LOG.debug("==> RangerAuditMetricsUtil.createAuditMetrics({})", request);

        if (auditMetricRESTClient == null) {
            throw new Exception("RangerAuditMetricRESTClient is null...Audit Metrics cannot be logged..");
        }

        RangerAuditMetrics ret = null;

        try {
            auditMetricRESTClient.createAuditMetrics(request);
        } catch (Exception e) {
            LOG.error("Error creating auditMetric: {}", request, e);
        }
        return ret;
    }

    private void init(Properties props) {
        auditConfig           = RangerAuditConfig.getInstance().getConfig(props);
        auditMetricRESTClient = getRangerClient();
        if (auditMetricRESTClient == null) {
            LOG.error("Error create RangerAuditMetricRESTClient...Audit Metrics collection won't happen");
        }
    }

    private AbstractRangerAuditMetricRESTClient getRangerClient() {
        AbstractRangerAuditMetricRESTClient ret               = null;
        String                              serviceType       = auditConfig.get(RANGER_SERVICE_TYPE_CONFIG);
        String                              url               = auditConfig.get(String.format(RANGER_ADMIN_URL, serviceType));
        String                              sslConfigFileName = auditConfig.get(String.format(RANGER_ADMIN_SSL_CONFIG_FILENAME, serviceType));
        String                              adminCls          = auditConfig.get(String.format(RANGER_ADMIN_IMPL_CLASS_CONF, serviceType));

        if (adminCls.equals(RANGER_ADMIM_IMPL_CLASS)) {
            ret = new RangerAuditMetricRESTClient();
        } else if (adminCls.equals(RANGER_ADMIM_IMPL_JERSEY2_CLASS)) {
            try {
                @SuppressWarnings("unchecked")
                Class<AbstractRangerAuditMetricRESTClient> auditMetricRESTClientClass = (Class<AbstractRangerAuditMetricRESTClient>) Class.forName(RANGER_ADMIN_AUDIT_METRICS_JERSEY2_CLASS);
                ret = auditMetricRESTClientClass.newInstance();
            } catch (Exception excp) {
                LOG.error("failed to instantiate policy source of type {}", RANGER_ADMIN_AUDIT_METRICS_JERSEY2_CLASS, excp);
            }
        }

        if (ret != null) {
            ret.init(url, sslConfigFileName, auditConfig);
        }
        return ret;
    }
}
