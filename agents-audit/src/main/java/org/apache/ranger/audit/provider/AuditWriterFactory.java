package org.apache.ranger.audit.provider;

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

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.audit.utils.RangerAuditWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class AuditWriterFactory {
    private static final Logger logger = LoggerFactory.getLogger(AuditWriterFactory.class);

    public static final  String AUDIT_FILETYPE_DEFAULT     = "json";
    public static final  String AUDIT_JSON_FILEWRITER_IMPL = "org.apache.ranger.audit.utils.RangerJSONAuditWriter";
    public static final  String AUDIT_ORC_FILEWRITER_IMPL  = "org.apache.ranger.audit.utils.RangerORCAuditWriter";

    private static volatile AuditWriterFactory me;

    public Map<String, String> auditConfigs;
    public Properties          props;
    public String              propPrefix;
    public String              auditProviderName;
    public RangerAuditWriter   auditWriter;

    public static AuditWriterFactory getInstance() {
        AuditWriterFactory auditWriter = me;

        if (auditWriter == null) {
            synchronized (AuditWriterFactory.class) {
                auditWriter = me;

                if (auditWriter == null) {
                    auditWriter = new AuditWriterFactory();
                    me          = auditWriter;
                }
            }
        }

        return auditWriter;
    }

    public void init(Properties props, String propPrefix, String auditProviderName, Map<String, String> auditConfigs) throws Exception {
        logger.debug("==> AuditWriterFactory.init()");

        this.props             = props;
        this.propPrefix        = propPrefix;
        this.auditProviderName = auditProviderName;
        this.auditConfigs      = auditConfigs;

        String auditFileType = MiscUtil.getStringProperty(props, propPrefix + ".batch.filequeue.filetype", AUDIT_FILETYPE_DEFAULT);
        String writerClass   = MiscUtil.getStringProperty(props, propPrefix + ".filewriter.impl");

        auditWriter = StringUtils.isEmpty(writerClass) ? createWriter(getDefaultWriter(auditFileType)) : createWriter(writerClass);

        if (auditWriter != null) {
            auditWriter.init(props, propPrefix, auditProviderName, auditConfigs);

            logger.debug("<== AuditWriterFactory.init() :{}", auditWriter.getClass().getName());
        }
    }

    public RangerAuditWriter createWriter(String writerClass) throws Exception {
        logger.debug("==> AuditWriterFactory.createWriter()");

        Class<RangerAuditWriter> cls = (Class<RangerAuditWriter>) Class.forName(writerClass);
        RangerAuditWriter        ret = cls.newInstance();

        logger.debug("<== AuditWriterFactory.createWriter()");

        return ret;
    }

    public String getDefaultWriter(String auditFileType) {
        logger.debug("==> AuditWriterFactory.getDefaultWriter()");

        final String ret;

        switch (auditFileType) {
            case "orc":
                ret = AUDIT_ORC_FILEWRITER_IMPL;
                break;
            case "json":
                ret = AUDIT_JSON_FILEWRITER_IMPL;
                break;
            default:
                ret = null;
                break;
        }

        logger.debug("<== AuditWriterFactory.getDefaultWriter() :{}", ret);

        return ret;
    }

    public RangerAuditWriter getAuditWriter() {
        return this.auditWriter;
    }
}
