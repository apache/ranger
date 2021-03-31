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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.audit.provider.MiscUtil;

import java.io.File;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

/**
 * Writes the Ranger audit to HDFS as JSON text
 */
public class RangerJSONAuditWriter extends AbstractRangerAuditWriter {

    private static final Log logger = LogFactory.getLog(RangerJSONAuditWriter.class);

    protected String JSON_FILE_EXTENSION = ".log";

    public void init(Properties props, String propPrefix, String auditProviderName, Map<String,String> auditConfigs) {
        if (logger.isDebugEnabled()) {
            logger.debug("==> RangerJSONAuditWriter.init()");
        }
        init();
        super.init(props,propPrefix,auditProviderName,auditConfigs);
        if (logger.isDebugEnabled()) {
            logger.debug("<== RangerJSONAuditWriter.init()");
        }
    }

    public void init() {
        setFileExtension(JSON_FILE_EXTENSION);
    }

    synchronized public boolean logJSON(final Collection<String> events) throws Exception {
        boolean     ret = false;
        PrintWriter out = null;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("UGI=" + MiscUtil.getUGILoginUser()
                        + ". Will write to HDFS file=" + currentFileName);
            }
            out = MiscUtil.executePrivilegedAction(new PrivilegedExceptionAction<PrintWriter>() {
                @Override
                public PrintWriter run()  throws Exception {
                    PrintWriter out = getLogFileStream();
                    for (String event : events) {
                        out.println(event);
                    }
                    return out;
                };
            });
            // flush and check the stream for errors
            if (out.checkError()) {
                // In theory, this count may NOT be accurate as part of the messages may have been successfully written.
                // However, in practice, since client does buffering, either all of none would succeed.
                out.close();
                closeWriter();
                return ret;
            }
        } catch (Exception e) {
            if (out != null) {
                out.close();
            }
            closeWriter();
            return ret;
        } finally {
            ret = true;
            if (logger.isDebugEnabled()) {
                logger.debug("Flushing HDFS audit. Event Size:" + events.size());
            }
            if (out != null) {
                out.flush();
            }
        }

        return ret;
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
        // Either there are no open log file or the previous one has been rolled
        // over
        PrintWriter logWriter = createWriter();
        return logWriter;
    }


    public void flush() {
        if (logger.isDebugEnabled()) {
            logger.debug("==> JSONWriter.flush()");
        }
        logger.info("Flush called. name=" + auditProviderName);
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
}