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

package org.apache.ranger.audit.destination;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.model.RangerAuditMetrics;
import org.apache.ranger.audit.model.RangerAuditMetricsText;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.ranger.audit.utils.JsonUtils;
import org.apache.ranger.audit.utils.RangerAuditMetricsUtil;
import org.apache.ranger.audit.utils.RollingTimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;

public class MetricsAuditDestination extends AuditDestination {
    private static final Logger logger                                   = LoggerFactory.getLogger(MetricsAuditDestination.class);
    private static final String PROP_SERVICE_TYPE                        = "ranger.plugin.serviceType";
    private static final String PROP_RAZ_SERVICE_TYPE                    = "ranger.plugin.raz.serviceType";
    private static final String PROP_APP_ID                              = "ranger.plugin.appId";
    private static final String PROP_SERVICE_NAME                        = "ranger.plugin.%s.service.name";
    private static final String PROP_AUDIT_METRICS_LOGGING_PERIOD        = "metrics.logging.period";
    private static final String AUDIT_METRICS_LOGGING_PERIOD_DEFAULT     = "1m";
    private static final String AUDIT_METRICS_LOGGING_PERIOD_DEFAULT_STR = "PER MINUTE";
    private static final String PROP_RAZ_SERVICE_TYPE_ID                 = "raz";

    private RollingTimeUtil        rollingTimeUtil;
    private RangerAuditMetricsUtil rangerAuditMetricsUtil;
    private String                 serviceName;
    private String                 serviceType;
    private String                 razServiceType;
    private String                 appId;
    private String                 metricLoggingPeriod;
    private String                 metricLoggingPeriodInStr;
    private Instant                startTime;
    private long                   metricLoggingPeriodinMs;
    private int                    auditCount;

    public MetricsAuditDestination() {
        startTime = Instant.now();
        logger.info("MetricsAuditDestination() called..");
    }

    @Override
    public void init(Properties props, String propPrefix) {
        super.init(props, propPrefix);
        rollingTimeUtil     = RollingTimeUtil.getInstance();
        metricLoggingPeriod = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_AUDIT_METRICS_LOGGING_PERIOD);
        if (StringUtils.isBlank(metricLoggingPeriod)) {
            metricLoggingPeriod = AUDIT_METRICS_LOGGING_PERIOD_DEFAULT;
        }
        try {
            String computePeriod = rollingTimeUtil.getTimeLiteral(metricLoggingPeriod);
            int    timeNumeral   = rollingTimeUtil.getTimeNumeral(metricLoggingPeriod, computePeriod);
            metricLoggingPeriodinMs  = rollingTimeUtil.getTimeInMilliSeconds(timeNumeral, computePeriod);
            metricLoggingPeriodInStr = rollingTimeUtil.getTimeString(timeNumeral, computePeriod);
        } catch (Exception e) {
            logger.error("Invalid logging period in Audit metrics...Using default period of 1 minute...");
            metricLoggingPeriodinMs  = RollingTimeUtil.MILLISECOND_CONVERSION_FACTOR;
            metricLoggingPeriodInStr = AUDIT_METRICS_LOGGING_PERIOD_DEFAULT_STR;
        }

        rangerAuditMetricsUtil = new RangerAuditMetricsUtil(props);
        this.serviceType       = props.getProperty(PROP_SERVICE_TYPE);
        this.serviceName       = props.getProperty(String.format(PROP_SERVICE_NAME, serviceType));
        this.appId             = props.getProperty(PROP_APP_ID);
        this.razServiceType    = props.getProperty(PROP_RAZ_SERVICE_TYPE);
    }

    public void setRollingTimeUtil(RollingTimeUtil rollingTimeUtil) {
        this.rollingTimeUtil = rollingTimeUtil;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getServiceType() {
        return serviceType;
    }

    public void setServiceType(String serviceType) {
        this.serviceType = serviceType;
    }

    public String getRazServiceType() {
        return razServiceType;
    }

    public void setRazServiceType(String razServiceType) {
        this.razServiceType = razServiceType;
    }

    public String getAppId() {
        return appId;
    }

    public int getAuditCount() {
        return auditCount;
    }

    public void setAuditCount(int auditCount) {
        this.auditCount = auditCount;
    }

    @Override
    public boolean log(AuditEventBase event) {
        try {
            if (event != null) {
                addAuditCount(1);
                if (checkIfThreshHoldReached()) {
                    //Call Ranger API to send metrics
                    logger.debug("Total number of Audits : {}, {}", auditCount, metricLoggingPeriodInStr);
                    persistAuditMetrics(metricLoggingPeriodInStr, auditCount, event);
                    resetAuditMetricsCount();
                }
            }
        } catch (Exception e) {
            resetAuditMetricsCount();
            logger.error("Failed to log audit metrics....", e);
        }
        return true;
    }

    @Override
    public boolean logJSON(String event) {
        try {
            if (event != null) {
                addAuditCount(1);
                if (checkIfThreshHoldReached()) {
                    //Call Ranger API to send metrics
                    logger.debug("Total number of Audits : {}, {}", auditCount, metricLoggingPeriodInStr);
                    persistAuditMetrics(metricLoggingPeriodInStr, auditCount, getSingleEvent(event));
                    resetAuditMetricsCount();
                }
            }
        } catch (Exception e) {
            resetAuditMetricsCount();
            logger.error("Failed to log audit metrics....", e);
        }
        return true;
    }

    @Override
    public boolean logJSON(Collection<String> events) {
        try {
            if (CollectionUtils.isNotEmpty(events)) {
                addAuditCount(events.size());
                if (checkIfThreshHoldReached()) {
                    //Call Ranger API to send metrics
                    logger.debug("Total number of Audits : {}, {}", auditCount, metricLoggingPeriodInStr);
                    persistAuditMetrics(metricLoggingPeriodInStr, auditCount, getSingleEventFromEvents(events));
                    resetAuditMetricsCount();
                }
            }
        } catch (Exception e) {
            resetAuditMetricsCount();
            logger.error("Failed to log audit metrics....", e);
        }
        return true;
    }

    @Override
    public boolean log(Collection<AuditEventBase> events) {
        try {
            if (CollectionUtils.isNotEmpty(events)) {
                addAuditCount(events.size());
                if (checkIfThreshHoldReached()) {
                    //Call Ranger API to send metrics
                    logger.debug("Total number of Audits :{}, {}", auditCount, metricLoggingPeriodInStr);
                    persistAuditMetrics(metricLoggingPeriodInStr, auditCount, getSingleEvent(events));
                    resetAuditMetricsCount();
                }
            }
        } catch (Exception e) {
            resetAuditMetricsCount();
            logger.error("Failed to log audit metrics....", e);
        }
        return true;
    }

    private boolean checkIfThreshHoldReached() {
        boolean  ret         = false;
        Duration timeElapsed = Duration.between(startTime, Instant.now());
        logger.debug("TimeElapsed in MilliSec: {}", timeElapsed.toMillis());
        if (timeElapsed.toMillis() >= metricLoggingPeriodinMs) {
            ret = true;
        }
        return ret;
    }

    private void addAuditCount(int count) {
        auditCount = auditCount + count;
    }

    private void persistAuditMetrics(String metricLogginPeriod, int auditCount, AuditEventBase event) throws Exception {
        RangerAuditMetrics rangerAuditMetrics = buildRangerAuditMetrics(metricLogginPeriod, auditCount, event);
        rangerAuditMetricsUtil.createAuditMetrics(rangerAuditMetrics);
    }

    private RangerAuditMetrics buildRangerAuditMetrics(String interval, int auditCount, AuditEventBase event) {
        RangerAuditMetrics ret             = new RangerAuditMetrics();
        AuthzAuditEvent    authzAuditEvent = (AuthzAuditEvent) event;

        ret.setServiceName(getServiceName());
        String serviceType = getServiceType();
        if (PROP_RAZ_SERVICE_TYPE_ID.equals(serviceType)) {
            serviceType = getRazServiceType();
        }
        ret.setServiceType(serviceType);
        ret.setAppId(getAppId());
        ret.setClusterName(authzAuditEvent.getClusterName());
        ret.setclientIP(getIPAddress(authzAuditEvent));
        ret.setThroughPutUnit(interval);
        ret.setNumberOfAudits(auditCount);
        RangerAuditMetricsText  metricText  = new RangerAuditMetricsText();
        HashMap<String, String> metricsData = new HashMap<>();
        metricsData.put(interval, Integer.toString(auditCount));
        metricText.setMetrics(metricsData);
        ret.setMetricsText(metricText);

        return ret;
    }

    private AuditEventBase getSingleEvent(Collection<AuditEventBase> events) {
        Optional<AuditEventBase> auditEventBase = events.stream().findFirst();
        return auditEventBase.get();
    }

    private AuditEventBase getSingleEvent(String event) {
        return JsonUtils.jsonToObject(event, AuthzAuditEvent.class);
    }

    private AuditEventBase getSingleEventFromEvents(Collection<String> events) {
        Optional<String> auditEventBase = events.stream().findFirst();
        return JsonUtils.jsonToObject(auditEventBase.get(), AuthzAuditEvent.class);
    }

    private String getIPAddress(AuthzAuditEvent event) {
        String ret = event.getClientIP();
        if (StringUtils.isBlank(ret)) {
            try {
                InetAddress ip = InetAddress.getLocalHost();
                if (ip != null) {
                    ret = ip.getHostAddress();
                }
            } catch (Exception e) {
                ret = StringUtils.EMPTY;
            }
        }
        return ret;
    }

    private void resetAuditMetricsCount() {
        startTime  = Instant.now();
        auditCount = 0;
    }
}
