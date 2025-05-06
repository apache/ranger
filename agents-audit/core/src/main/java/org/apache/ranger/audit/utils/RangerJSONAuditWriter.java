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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Writes the Ranger audit to HDFS as JSON text
 */
public class RangerJSONAuditWriter extends AbstractRangerAuditWriter {
    private static final Logger logger = LoggerFactory.getLogger(RangerJSONAuditWriter.class);

    public static final  String PROP_HDFS_ROLLOVER_ENABLE_PERIODIC_ROLLOVER     = "file.rollover.enable.periodic.rollover";
    public static final  String PROP_HDFS_ROLLOVER_PERIODIC_ROLLOVER_CHECK_TIME = "file.rollover.periodic.rollover.check.sec";

    protected static final String JSON_FILE_EXTENSION = ".log";

    /*
    Time frequency of next occurrence of periodic rollover check. By Default every 60 seconds the check is done if enabled
    */
    private long periodicRollOverCheckTimeinSec;

    public void init(Properties props, String propPrefix, String auditProviderName, Map<String, String> auditConfigs) {
        logger.debug("==> RangerJSONAuditWriter.init()");

        init();

        super.init(props, propPrefix, auditProviderName, auditConfigs);

        // start AuditFilePeriodicRollOverTask if enabled.
        boolean enableAuditFilePeriodicRollOver = MiscUtil.getBooleanProperty(props, propPrefix + "." + PROP_HDFS_ROLLOVER_ENABLE_PERIODIC_ROLLOVER, false);
        if (enableAuditFilePeriodicRollOver) {
            periodicRollOverCheckTimeinSec = MiscUtil.getLongProperty(props, propPrefix + "." + PROP_HDFS_ROLLOVER_PERIODIC_ROLLOVER_CHECK_TIME, 60L);

            try {
                logger.debug("rolloverPeriod: {} nextRollOverTime: {} periodicRollOverTimeinSec: {}", rolloverPeriod, nextRollOverTime, periodicRollOverCheckTimeinSec);

                startAuditFilePeriodicRollOverTask();
            } catch (Exception e) {
                logger.warn("Error enabling audit file perodic rollover..! Default behavior will be");
            }
        }

        logger.debug("<== RangerJSONAuditWriter.init()");
    }

    public void flush() {
        logger.debug("==> JSONWriter.flush() called. name={}", auditProviderName);

        super.flush();

        logger.debug("<== JSONWriter.flush()");
    }

    public void init() {
        setFileExtension(JSON_FILE_EXTENSION);
    }

    public synchronized boolean logJSON(final Collection<String> events) throws Exception {
        PrintWriter out = null;

        try {
            logger.debug("UGI = {}, will write to HDFS file = {}", MiscUtil.getUGILoginUser(), currentFileName);

            out = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<PrintWriter>) () -> {
                PrintWriter out1 = null;

                if (CollectionUtils.isEmpty(events)) {
                    closeFileIfNeeded();
                } else {
                    out1 = getLogFileStream();

                    for (String event : events) {
                        out1.println(event);
                    }
                }

                return out1;
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
            logger.debug("Flushing HDFS audit. Event Size:{}", events.size());

            if (out != null) {
                out.flush();
            }
        }

        return true;
    }

    @Override
    public boolean log(Collection<String> events) throws Exception {
        return logJSON(events);
    }

    @Override
    public boolean logFile(File file) throws Exception {
        return logAsFile(file);
    }

    @Override
    public void start() {
        // nothing to start
    }

    @Override
    public synchronized void stop() {
        logger.debug("==> JSONWriter.stop()");

        if (logWriter != null) {
            try {
                logWriter.flush();
                logWriter.close();
            } catch (Throwable t) {
                logger.error("Error on closing log writter. Exception will be ignored. name={}, fileName={}", auditProviderName, currentFileName);
            }

            logWriter = null;
            ostream   = null;
        }

        logger.debug("<== JSONWriter.stop()");
    }

    public synchronized boolean logAsFile(final File file) throws Exception {
        logger.debug("UGI={}. Will write to HDFS file={}", MiscUtil.getUGILoginUser(), currentFileName);

        boolean ret = MiscUtil.executePrivilegedAction((PrivilegedExceptionAction<Boolean>) () -> logFileToHDFS(file));

        logger.info("Flushing HDFS audit File :{}{}", file.getAbsolutePath(), file.getName());

        return ret;
    }

    public synchronized PrintWriter getLogFileStream() throws Exception {
        closeFileIfNeeded();

        // Either there are no open log file or the previous one has been rolled over
        return createWriter();
    }

    private void startAuditFilePeriodicRollOverTask() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new AuditFilePeriodicRollOverTaskThreadFactory());

        logger.debug("HDFSAuditDestination.startAuditFilePeriodicRollOverTask() strated..Audit File rollover happens every {}", rolloverPeriod);

        executorService.scheduleAtFixedRate(new AuditFilePeriodicRollOverTask(), 0, periodicRollOverCheckTimeinSec, TimeUnit.SECONDS);
    }

    static class AuditFilePeriodicRollOverTaskThreadFactory implements ThreadFactory {
        //Threadfactory to create a daemon Thread.
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "AuditFilePeriodicRollOverTask");

            t.setDaemon(true);

            return t;
        }
    }

    private class AuditFilePeriodicRollOverTask implements Runnable {
        @Override
        public void run() {
            logger.debug("==> AuditFilePeriodicRollOverTask.run()");

            try {
                logJSON(Collections.emptyList());
            } catch (Exception excp) {
                logger.error("AuditFilePeriodicRollOverTask Failed. Aborting..", excp);
            }

            logger.debug("<== AuditFilePeriodicRollOverTask.run()");
        }
    }
}
