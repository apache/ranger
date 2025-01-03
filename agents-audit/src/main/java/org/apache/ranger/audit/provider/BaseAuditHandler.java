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
package org.apache.ranger.audit.provider;

import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public abstract class BaseAuditHandler implements AuditHandler {
    private static final Logger LOG = LoggerFactory.getLogger(BaseAuditHandler.class);

    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE                  = "xasecure.policymgr.clientssl.keystore";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE             = "xasecure.policymgr.clientssl.keystore.type";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL       = "xasecure.policymgr.clientssl.keystore.credential.file";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_CREDENTIAL_ALIAS = "sslKeyStore";
    public static final String RANGER_POLICYMGR_CLIENT_KEY_FILE_TYPE_DEFAULT     = "jks";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE                  = "xasecure.policymgr.clientssl.truststore";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE             = "xasecure.policymgr.clientssl.truststore.type";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL       = "xasecure.policymgr.clientssl.truststore.credential.file";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_CREDENTIAL_ALIAS = "sslTrustStore";
    public static final String RANGER_POLICYMGR_TRUSTSTORE_FILE_TYPE_DEFAULT     = "jks";
    public static final String RANGER_SSL_KEYMANAGER_ALGO_TYPE                   = KeyManagerFactory.getDefaultAlgorithm();
    public static final String RANGER_SSL_TRUSTMANAGER_ALGO_TYPE                 = TrustManagerFactory.getDefaultAlgorithm();
    public static final String RANGER_SSL_CONTEXT_ALGO_TYPE                      = "TLSv1.2";
    public static final String PROP_CONFIG                                       = "config";
    public static final String FAILED_TO_LOG_AUDIT_EVENT                         = "failed to log audit event: {}";
    public static final String PROP_NAME                                         = "name";
    public static final String PROP_CLASS_NAME                                   = "classname";
    public static final String PROP_DEFAULT_PREFIX                               = "xasecure.audit.provider";

    static final String AUDIT_LOG_FAILURE_REPORT_MIN_INTERVAL_PROP = "xasecure.audit.log.failure.report.min.interval.ms";
    static final String  AUDIT_LOG_STATUS_LOG_ENABLED              = "xasecure.audit.log.status.log.enabled";
    static final String  AUDIT_LOG_STATUS_LOG_INTERVAL_SEC         = "xasecure.audit.log.status.log.interval.sec";
    static final boolean DEFAULT_AUDIT_LOG_STATUS_LOG_ENABLED      = false;
    static final long    DEFAULT_AUDIT_LOG_STATUS_LOG_INTERVAL_SEC = 5L * 60; // 5 minutes

    protected String              propPrefix = PROP_DEFAULT_PREFIX;
    protected String              providerName;
    protected String              parentPath;
    protected int                 failedRetryTimes = 3;
    protected int                 failedRetrySleep = 3 * 1000;
    protected Map<String, String> configProps      = new HashMap<>();
    protected Properties          props;

    int     errorLogIntervalMS = 30 * 1000; // Every 30 seconds
    long    lastErrorLogMS;
    long    totalCount;
    long    totalSuccessCount;
    long    totalFailedCount;
    long    totalStashedCount;
    long    totalDeferredCount;
    long    lastIntervalCount;
    long    lastIntervalSuccessCount;
    long    lastIntervalFailedCount;
    long    lastStashedCount;
    long    lastDeferredCount;
    boolean statusLogEnabled    = DEFAULT_AUDIT_LOG_STATUS_LOG_ENABLED;
    long    statusLogIntervalMS = DEFAULT_AUDIT_LOG_STATUS_LOG_INTERVAL_SEC * 1000;
    long    lastStatusLogTime   = System.currentTimeMillis();
    long    nextStatusLogTime   = lastStatusLogTime + statusLogIntervalMS;

    private       int        mLogFailureReportMinIntervalInMs = 60 * 1000;
    private final AtomicLong mFailedLogLastReportTime         = new AtomicLong(0);
    private final AtomicLong mFailedLogCountSinceLastReport   = new AtomicLong(0);
    private final AtomicLong mFailedLogCountLifeTime          = new AtomicLong(0);

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.ranger.audit.provider.AuditProvider#log(org.apache.ranger.
     * audit.model.AuditEventBase)
     */
    @Override
    public boolean log(AuditEventBase event) {
        return log(Collections.singletonList(event));
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.ranger.audit.provider.AuditProvider#logJSON(java.lang.String)
     */
    @Override
    public boolean logJSON(String event) {
        AuditEventBase eventObj = MiscUtil.fromJson(event, AuthzAuditEvent.class);

        return log(eventObj);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.ranger.audit.provider.AuditProvider#logJSON(java.util.Collection)
     */
    @Override
    public boolean logJSON(Collection<String> events) {
        List<AuditEventBase> eventList = new ArrayList<>(events.size());

        for (String event : events) {
            eventList.add(MiscUtil.fromJson(event, AuthzAuditEvent.class));
        }

        return log(eventList);
    }

    @Override
    public boolean logFile(File file) {
        return false;
    }

    @Override
    public void init(Properties props) {
        init(props, null);
    }

    @Override
    public void init(Properties props, String basePropertyName) {
        LOG.info("BaseAuditProvider.init()");

        this.props = props;

        if (basePropertyName != null) {
            propPrefix = basePropertyName;
        }

        LOG.info("propPrefix={}", propPrefix);

        String name = MiscUtil.getStringProperty(props, basePropertyName + "." + PROP_NAME);

        if (name != null && !name.isEmpty()) {
            setName(name);
        }

        // Get final token
        if (providerName == null) {
            List<String> tokens = MiscUtil.toArray(propPrefix, ".");

            if (!tokens.isEmpty()) {
                String finalToken = tokens.get(tokens.size() - 1);

                setName(finalToken);

                LOG.info("Using providerName from property prefix. providerName={}", getName());
            }
        }

        LOG.info("providerName={}", getName());

        mLogFailureReportMinIntervalInMs = MiscUtil.getIntProperty(props, AUDIT_LOG_FAILURE_REPORT_MIN_INTERVAL_PROP, 60 * 1000);

        boolean globalStatusLogEnabled     = MiscUtil.getBooleanProperty(props, AUDIT_LOG_STATUS_LOG_ENABLED, DEFAULT_AUDIT_LOG_STATUS_LOG_ENABLED);
        long    globalStatusLogIntervalSec = MiscUtil.getLongProperty(props, AUDIT_LOG_STATUS_LOG_INTERVAL_SEC, DEFAULT_AUDIT_LOG_STATUS_LOG_INTERVAL_SEC);

        statusLogEnabled    = MiscUtil.getBooleanProperty(props, basePropertyName + ".status.log.enabled", globalStatusLogEnabled);
        statusLogIntervalMS = MiscUtil.getLongProperty(props, basePropertyName + ".status.log.interval.sec", globalStatusLogIntervalSec) * 1000;
        nextStatusLogTime   = lastStatusLogTime + statusLogIntervalMS;

        LOG.info("{}={}", AUDIT_LOG_STATUS_LOG_ENABLED, globalStatusLogEnabled);
        LOG.info("{}={}", AUDIT_LOG_STATUS_LOG_INTERVAL_SEC, globalStatusLogIntervalSec);
        LOG.info("{}.status.log.enabled={}", basePropertyName, statusLogEnabled);
        LOG.info("{}.status.log.interval.sec={}", basePropertyName, (statusLogIntervalMS / 1000));

        String configPropsNamePrefix = propPrefix + "." + PROP_CONFIG + ".";

        for (Object propNameObj : props.keySet()) {
            String propName = propNameObj.toString();

            if (!propName.startsWith(configPropsNamePrefix)) {
                continue;
            }

            String configName  = propName.substring(configPropsNamePrefix.length());
            String configValue = props.getProperty(propName);

            configProps.put(configName, configValue);

            LOG.info("Found Config property: {} => {}", configName, configValue);
        }
    }

    @Override
    public String getName() {
        if (parentPath != null) {
            return parentPath + "." + providerName;
        }

        return providerName;
    }

    public void setName(String name) {
        providerName = name;
    }

    public String getParentPath() {
        return parentPath;
    }

    public void setParentPath(String parentPath) {
        this.parentPath = parentPath;
    }

    public String getFinalPath() {
        return getName();
    }

    public long addTotalCount(int count) {
        totalCount += count;

        return totalCount;
    }

    public long addSuccessCount(int count) {
        totalSuccessCount += count;

        return totalSuccessCount;
    }

    public long addFailedCount(int count) {
        totalFailedCount += count;

        return totalFailedCount;
    }

    public long addStashedCount(int count) {
        totalStashedCount += count;

        return totalStashedCount;
    }

    public long addDeferredCount(int count) {
        totalDeferredCount += count;

        return totalDeferredCount;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public long getTotalSuccessCount() {
        return totalSuccessCount;
    }

    public long getTotalFailedCount() {
        return totalFailedCount;
    }

    public long getTotalStashedCount() {
        return totalStashedCount;
    }

    public long getLastStashedCount() {
        return lastStashedCount;
    }

    public long getTotalDeferredCount() {
        return totalDeferredCount;
    }

    public long getLastDeferredCount() {
        return lastDeferredCount;
    }

    public boolean isStatusLogEnabled() {
        return statusLogEnabled;
    }

    public void logStatusIfRequired() {
        if (System.currentTimeMillis() > nextStatusLogTime) {
            logStatus();
        }
    }

    public void logStatus() {
        try {
            long currTime = System.currentTimeMillis();
            long diffTime = currTime - lastStatusLogTime;

            lastStatusLogTime = currTime;
            nextStatusLogTime = currTime + statusLogIntervalMS;

            long diffCount    = totalCount - lastIntervalCount;
            long diffSuccess  = totalSuccessCount - lastIntervalSuccessCount;
            long diffFailed   = totalFailedCount - lastIntervalFailedCount;
            long diffStashed  = totalStashedCount - lastStashedCount;
            long diffDeferred = totalDeferredCount - lastDeferredCount;

            if (diffCount == 0 && diffSuccess == 0 && diffFailed == 0 && diffStashed == 0 && diffDeferred == 0) {
                return;
            }

            lastIntervalCount        = totalCount;
            lastIntervalSuccessCount = totalSuccessCount;
            lastIntervalFailedCount  = totalFailedCount;
            lastStashedCount         = totalStashedCount;
            lastDeferredCount        = totalDeferredCount;

            if (statusLogEnabled) {
                String finalPath  = "";
                String tFinalPath = getFinalPath();

                if (!getName().equals(tFinalPath)) {
                    finalPath = ", finalDestination=" + tFinalPath;
                }

                logAuditStatus(diffTime, diffCount, diffSuccess, diffFailed, diffStashed, diffDeferred, finalPath);
            }
        } catch (Exception t) {
            LOG.error("Error while printing stats. auditProvider={}", getName());
        }
    }

    public void logError(String msg, Object...args) {
        long currTimeMS = System.currentTimeMillis();

        if (currTimeMS - lastErrorLogMS > errorLogIntervalMS) {
            LOG.error(msg, args);

            lastErrorLogMS = currTimeMS;
        }
    }

    public void logError(String msg, Throwable ex) {
        long currTimeMS = System.currentTimeMillis();

        if (currTimeMS - lastErrorLogMS > errorLogIntervalMS) {
            LOG.error(msg, ex);

            lastErrorLogMS = currTimeMS;
        }
    }

    public String getTimeDiffStr(long time1, long time2) {
        long timeInMs = Math.abs(time1 - time2);

        return formatIntervalForLog(timeInMs);
    }

    public String formatIntervalForLog(long timeInMs) {
        long hours    = timeInMs / (60 * 60 * 1000);
        long minutes  = (timeInMs / (60 * 1000)) % 60;
        long seconds  = (timeInMs % (60 * 1000)) / 1000;
        long mSeconds = (timeInMs % (1000));

        if (hours > 0) {
            return String.format("%02d:%02d:%02d.%03d hours", hours, minutes, seconds, mSeconds);
        } else if (minutes > 0) {
            return String.format("%02d:%02d.%03d minutes", minutes, seconds, mSeconds);
        } else if (seconds > 0) {
            return String.format("%02d.%03d seconds", seconds, mSeconds);
        } else {
            return String.format("%03d milli-seconds", mSeconds);
        }
    }

    public void logFailedEvent(AuditEventBase event) {
        logFailedEvent(event, "");
    }

    public void logFailedEvent(AuditEventBase event, Throwable excp) {
        long now                  = System.currentTimeMillis();
        long timeSinceLastReport  = now - mFailedLogLastReportTime.get();
        long countSinceLastReport = mFailedLogCountSinceLastReport.incrementAndGet();
        long countLifeTime        = mFailedLogCountLifeTime.incrementAndGet();

        if (timeSinceLastReport >= mLogFailureReportMinIntervalInMs) {
            mFailedLogLastReportTime.set(now);
            mFailedLogCountSinceLastReport.set(0);

            if (excp != null) {
                LOG.warn(FAILED_TO_LOG_AUDIT_EVENT, MiscUtil.stringify(event), excp);
            } else {
                LOG.warn(FAILED_TO_LOG_AUDIT_EVENT, MiscUtil.stringify(event));
            }

            if (countLifeTime > 1) { // no stats to print for the 1st failure
                LOG.warn("Log failure count: {} in past {}; {} during process lifetime", countSinceLastReport, formatIntervalForLog(timeSinceLastReport), countLifeTime);
            }
        }
    }

    public void logFailedEvent(Collection<AuditEventBase> events) {
        logFailedEvent(events, "");
    }

    public void logFailedEvent(Collection<AuditEventBase> events, Throwable excp) {
        for (AuditEventBase event : events) {
            logFailedEvent(event, excp);
        }
    }

    public void logFailedEvent(AuditEventBase event, String message) {
        long now                  = System.currentTimeMillis();
        long timeSinceLastReport  = now - mFailedLogLastReportTime.get();
        long countSinceLastReport = mFailedLogCountSinceLastReport.incrementAndGet();
        long countLifeTime        = mFailedLogCountLifeTime.incrementAndGet();

        if (timeSinceLastReport >= mLogFailureReportMinIntervalInMs) {
            mFailedLogLastReportTime.set(now);
            mFailedLogCountSinceLastReport.set(0);

            LOG.warn("failed to log audit event: {} , errorMessage={}", MiscUtil.stringify(event), message);

            if (countLifeTime > 1) { // no stats to print for the 1st failure
                LOG.warn("Log failure count: {} in past {}; {} during process lifetime", countSinceLastReport, formatIntervalForLog(timeSinceLastReport), countLifeTime);
            }
        }
    }

    public void logFailedEvent(Collection<AuditEventBase> events, String errorMessage) {
        for (AuditEventBase event : events) {
            logFailedEvent(event, errorMessage);
        }
    }

    public void logFailedEventJSON(String event, Throwable excp) {
        long now                  = System.currentTimeMillis();
        long timeSinceLastReport  = now - mFailedLogLastReportTime.get();
        long countSinceLastReport = mFailedLogCountSinceLastReport.incrementAndGet();
        long countLifeTime        = mFailedLogCountLifeTime.incrementAndGet();

        if (timeSinceLastReport >= mLogFailureReportMinIntervalInMs) {
            mFailedLogLastReportTime.set(now);
            mFailedLogCountSinceLastReport.set(0);

            if (excp != null) {
                LOG.warn(FAILED_TO_LOG_AUDIT_EVENT, event, excp);
            } else {
                LOG.warn(FAILED_TO_LOG_AUDIT_EVENT, event);
            }

            if (countLifeTime > 1) { // no stats to print for the 1st failure
                LOG.warn("Log failure count: {} in past {}; {} during process lifetime", countSinceLastReport, formatIntervalForLog(timeSinceLastReport), countLifeTime);
            }
        }
    }

    public void logFailedEventJSON(Collection<String> events, Throwable excp) {
        for (String event : events) {
            logFailedEventJSON(event, excp);
        }
    }

    private void logAuditStatus(long diffTime, long diffCount, long diffSuccess, long diffFailed, long diffStashed, long diffDeferred, String finalPath) {
        String msg = "Audit Status Log: name="
                + getName()
                + finalPath
                + ", interval="
                + formatIntervalForLog(diffTime)
                + ", events="
                + diffCount
                + (diffSuccess > 0 ? (", succcessCount=" + diffSuccess)
                : "")
                + (diffFailed > 0 ? (", failedCount=" + diffFailed) : "")
                + (diffStashed > 0 ? (", stashedCount=" + diffStashed) : "")
                + (diffDeferred > 0 ? (", deferredCount=" + diffDeferred)
                : "")
                + ", totalEvents="
                + totalCount
                + (totalSuccessCount > 0 ? (", totalSuccessCount=" + totalSuccessCount)
                : "")
                + (totalFailedCount > 0 ? (", totalFailedCount=" + totalFailedCount)
                : "")
                + (totalStashedCount > 0 ? (", totalStashedCount=" + totalStashedCount)
                : "")
                + (totalDeferredCount > 0 ? (", totalDeferredCount=" + totalDeferredCount)
                : "");
        LOG.info(msg);
    }
}
