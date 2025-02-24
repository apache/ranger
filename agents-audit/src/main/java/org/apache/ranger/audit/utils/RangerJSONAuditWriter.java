package org.apache.ranger.audit.utils;

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

import org.apache.commons.collections.CollectionUtils;
import org.apache.ranger.audit.provider.MiscUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Writes the Ranger audit to HDFS as JSON text
 */
public class RangerJSONAuditWriter extends AbstractRangerAuditWriter {
    private static final Logger logger = LoggerFactory.getLogger(RangerJSONAuditWriter.class);

    public static final String PROP_HDFS_ROLLOVER_ENABLE_PERIODIC_ROLLOVER = "file.rollover.enable.periodic.rollover";
    public static final String PROP_HDFS_ROLLOVER_PERIODIC_ROLLOVER_CHECK_TIME = "file.rollover.periodic.rollover.check.sec";

    protected String JSON_FILE_EXTENSION = ".log";

    /*
    Time frequency of next occurrence of periodic rollover check. By Default every 60 seconds the check is done if enabled
    */
    private long periodicRollOverCheckTimeinSec;

    public void init(Properties props, String propPrefix, String auditProviderName, Map<String, String> auditConfigs) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerJSONAuditWriter.init()");
        }
        init();
        super.init(props, propPrefix, auditProviderName, auditConfigs);

        // start AuditFilePeriodicRollOverTask if enabled.
        boolean enableAuditFilePeriodicRollOver = MiscUtil.getBooleanProperty(props, propPrefix + "." + PROP_HDFS_ROLLOVER_ENABLE_PERIODIC_ROLLOVER, false);
        if (enableAuditFilePeriodicRollOver) {
            periodicRollOverCheckTimeinSec = MiscUtil.getLongProperty(props, propPrefix + "." + PROP_HDFS_ROLLOVER_PERIODIC_ROLLOVER_CHECK_TIME, 60L);
            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("rolloverPeriod: " + rolloverPeriod + " nextRollOverTime: " + nextRollOverTime + " periodicRollOverTimeinSec: " + periodicRollOverCheckTimeinSec);
                }
                startAuditFilePeriodicRollOverTask();
            } catch (Exception e) {
                logger.warn("Error enabling audit file perodic rollover..! Default behavior will be");
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerJSONAuditWriter.init()");
        }
    }

    public void init() {
        setFileExtension(JSON_FILE_EXTENSION);
    }

    synchronized public boolean logJSON(final Collection<String> events) throws Exception {
        PrintWriter out = null;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("UGI = {}, will write to HDFS file = {}", MiscUtil.getUGILoginUser(), currentFileName);
            }
            out = MiscUtil.executePrivilegedAction(new PrivilegedExceptionAction<PrintWriter>() {
                @Override
                public PrintWriter run()  throws Exception {
                    PrintWriter out = null;

                    if (CollectionUtils.isEmpty(events)) {
                        closeFileIfNeeded();
                    } else {
                        out = getLogFileStream();

                        for (String event : events) {
                            out.println(event);
                        }
                    }

                    return out;
                };
            });
            // flush and check the stream for errors
            if (out != null && out.checkError()) {
                // In theory, this count may NOT be accurate as part of the messages may have been successfully written.
                // However, in practice, since client does buffering, either all or none would succeed.
                logger.error("Stream encountered errors while writing audits to HDFS!");
                closeWriter();
                resetWriter();
                return false;
            }
        } catch (Exception e) {
            logger.error("Exception encountered while writing audits to HDFS!", e);
            closeWriter();
            resetWriter();
            return false;
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Flushing HDFS audit. Event Size:" + events.size());
            }
            if (out != null) {
                out.flush();
            }
        }

        return true;
    }

    @Override
    public boolean log(Collection<String> events) throws  Exception {
        return logJSON(events);
    }

    synchronized public boolean logAsFile(final File file) throws Exception {
        boolean ret = false;
        if (logger.isDebugEnabled()) {
            logger.debug("UGI=" + MiscUtil.getUGILoginUser()
                    + ". Will write to HDFS file=" + currentFileName);
        }
        Boolean retVal = MiscUtil.executePrivilegedAction(new PrivilegedExceptionAction<Boolean>() {
            @Override
            public Boolean run()  throws Exception {
                boolean ret = logFileToHDFS(file);
                return  Boolean.valueOf(ret);
            };
        });
        ret = retVal.booleanValue();
        logger.info("Flushing HDFS audit File :" + file.getAbsolutePath() + file.getName());

        return ret;
    }

    @Override
    public boolean logFile(File file) throws Exception {
        return logAsFile(file);
    }

    synchronized public PrintWriter getLogFileStream() throws Exception {
        closeFileIfNeeded();

        // Either there are no open log file or the previous one has been rolled over
        PrintWriter logWriter = createWriter();
        return logWriter;
    }

    public void flush() {
        if (logger.isDebugEnabled()) {
            logger.debug("==> JSONWriter.flush() called. name=" + auditProviderName);
        }
        super.flush();
        if (logger.isDebugEnabled()) {
            logger.debug("<== JSONWriter.flush()");
        }
    }

    @Override
    public void start() {
        // nothing to start
    }

    @Override
    synchronized public void stop() {
        if (logger.isDebugEnabled()) {
            logger.debug("==> JSONWriter.stop()");
        }
        if (logWriter != null) {
            try {
                logWriter.flush();
                logWriter.close();
            } catch (Throwable t) {
                logger.error("Error on closing log writter. Exception will be ignored. name="
                        + auditProviderName + ", fileName=" + currentFileName);
            }
            logWriter = null;
            ostream = null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("<== JSONWriter.stop()");
        }
    }

    private void startAuditFilePeriodicRollOverTask() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new AuditFilePeriodicRollOverTaskThreadFactory());

        if (logger.isDebugEnabled()) {
            logger.debug("HDFSAuditDestination.startAuditFilePeriodicRollOverTask() strated.." + "Audit File rollover happens every " + rolloverPeriod );
        }

        executorService.scheduleAtFixedRate(new AuditFilePeriodicRollOverTask(), 0, periodicRollOverCheckTimeinSec, TimeUnit.SECONDS);
    }

    class AuditFilePeriodicRollOverTaskThreadFactory implements ThreadFactory {
        //Threadfactory to create a daemon Thread.
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "AuditFilePeriodicRollOverTask");
            t.setDaemon(true);
            return t;
        }
    }

    private class AuditFilePeriodicRollOverTask implements Runnable {
        public void run() {
            if (logger.isDebugEnabled()) {
                logger.debug("==> AuditFilePeriodicRollOverTask.run()");
            }
            try {
                logJSON(Collections.emptyList());
            } catch (Exception excp) {
                logger.error("AuditFilePeriodicRollOverTask Failed. Aborting..", excp);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("<== AuditFilePeriodicRollOverTask.run()");
            }
        }
    }

}
