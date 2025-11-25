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

package org.apache.ranger.authz.embedded;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerAuthzConfig {
    @Test
    public void testEmptyConfig() {
        RangerAuthzConfig config = new RangerAuthzConfig(new Properties());

        assertTrue(config.getServiceProperties("dev_hive", "hive").isEmpty());
        assertTrue(config.getServiceProperties("prod_hive", "hive").isEmpty());
        assertTrue(config.getServiceProperties("dev_hdfs", "hdfs").isEmpty());
        assertTrue(config.getAuditProperties().isEmpty());
    }

    @Test
    public void testDefaultConfigs() {
        RangerAuthzConfig config = new RangerAuthzConfig(createDefaultProperties());

        validateDevHiveProperties(config.getServiceProperties("dev_hive", "hive"));
        validateProdHiveProperties(config.getServiceProperties("prod_hive", "hive"));
        validateDevHdfsProperties(config.getServiceProperties("dev_hdfs", "hdfs"));
    }

    @Test
    public void testAuditConfigsV2() {
        RangerAuthzConfig config          = new RangerAuthzConfig(createAuditV2Properties());
        Properties        auditProperties = config.getAuditProperties();

        assertEquals(23, auditProperties.size());

        validateAuditConfigV2(auditProperties);
    }

    @Test
    public void testAuditConfigsV3() {
        RangerAuthzConfig config          = new RangerAuthzConfig(createAuditV3Properties());
        Properties        auditProperties = config.getAuditProperties();

        assertEquals(10, auditProperties.size());

        validateAuditConfigV3(auditProperties);
    }

    @Test
    public void testAllAuthzConfigs() {
        RangerAuthzConfig config = new RangerAuthzConfig(createAllAuthzProperties());

        validateDevHiveProperties(config.getServiceProperties("dev_hive", "hive"));
        validateProdHiveProperties(config.getServiceProperties("prod_hive", "hive"));
        validateDevHdfsProperties(config.getServiceProperties("dev_hdfs", "hdfs"));
        validateAuditConfigV2(config.getAuditProperties());
        validateAuditConfigV3(config.getAuditProperties());
    }

    private void validateDevHiveProperties(Properties prop) {
        assertEquals(7, prop.size());
        assertEquals("org.apache.ranger.admin.client.RangerAdminRESTClient", prop.getProperty("ranger.plugin.hive.policy.source.impl"));
        assertEquals("http://localhost:6080", prop.getProperty("ranger.plugin.hive.policy.rest.url"));
        assertEquals("/etc/hive/conf/ranger-policymgr-ssl.xml", prop.getProperty("ranger.plugin.hive.policy.rest.ssl.config.file"));
        assertEquals("120000", prop.getProperty("ranger.plugin.hive.policy.rest.client.connection.timeoutMs"));
        assertEquals("30000", prop.getProperty("ranger.plugin.hive.policy.rest.client.read.timeoutMs"));
        assertEquals("30000", prop.getProperty("ranger.plugin.hive.policy.pollIntervalMs"));
        assertEquals("/etc/ranger/policycache", prop.getProperty("ranger.plugin.hive.policy.cache.dir"));
    }

    private void validateProdHiveProperties(Properties prop) {
        assertEquals(7, prop.size());
        assertEquals("org.apache.ranger.admin.client.RangerAdminRESTClient", prop.getProperty("ranger.plugin.hive.policy.source.impl"));
        assertEquals("http://localhost:6080", prop.getProperty("ranger.plugin.hive.policy.rest.url"));
        assertEquals("/etc/hive/conf/ranger-policymgr-ssl.xml", prop.getProperty("ranger.plugin.hive.policy.rest.ssl.config.file"));
        assertEquals("120000", prop.getProperty("ranger.plugin.hive.policy.rest.client.connection.timeoutMs"));
        assertEquals("30000", prop.getProperty("ranger.plugin.hive.policy.rest.client.read.timeoutMs"));
        assertEquals("30000", prop.getProperty("ranger.plugin.hive.policy.pollIntervalMs"));
        assertEquals("/etc/ranger/policycache", prop.getProperty("ranger.plugin.hive.policy.cache.dir"));
    }

    private void validateDevHdfsProperties(Properties prop) {
        assertEquals(7, prop.size());
        assertEquals("org.apache.ranger.admin.client.RangerAdminRESTClient", prop.getProperty("ranger.plugin.hdfs.policy.source.impl"));
        assertEquals("http://localhost:6080", prop.getProperty("ranger.plugin.hdfs.policy.rest.url"));
        assertEquals("/etc/hive/conf/ranger-policymgr-ssl.xml", prop.getProperty("ranger.plugin.hdfs.policy.rest.ssl.config.file"));
        assertEquals("120000", prop.getProperty("ranger.plugin.hdfs.policy.rest.client.connection.timeoutMs"));
        assertEquals("30000", prop.getProperty("ranger.plugin.hdfs.policy.rest.client.read.timeoutMs"));
        assertEquals("30000", prop.getProperty("ranger.plugin.hdfs.policy.pollIntervalMs"));
        assertEquals("/etc/ranger/policycache", prop.getProperty("ranger.plugin.hdfs.policy.cache.dir"));
    }

    private void validateAuditConfigV2(Properties prop) {
        assertEquals("true", prop.getProperty("xasecure.audit.is.enabled"));
        assertEquals("false", prop.getProperty("xasecure.audit.hdfs.is.enabled"));
        assertEquals("true", prop.getProperty("xasecure.audit.solr.is.enabled"));
        assertEquals("false", prop.getProperty("xasecure.audit.log4j.is.enabled"));

        assertEquals("true", prop.getProperty("xasecure.audit.hdfs.is.async"));
        assertEquals("1048576", prop.getProperty("xasecure.audit.hdfs.async.max.queue.size"));
        assertEquals("30000", prop.getProperty("xasecure.audit.hdfs.async.max.flush.interval.ms"));
        assertEquals("hdfs://namenode:8020/ranger/audit/%app-type%/%time:yyyyMMdd%", prop.getProperty("xasecure.audit.hdfs.config.destination.directory"));
        assertEquals("%hostname%-audit.log", prop.getProperty("xasecure.audit.hdfs.config.destination.file"));
        assertEquals("900", prop.getProperty("xasecure.audit.hdfs.config.destination.flush.interval.seconds"));
        assertEquals("86400", prop.getProperty("xasecure.audit.hdfs.config.destination.rollover.interval.seconds"));
        assertEquals("60", prop.getProperty("xasecure.audit.hdfs.config.destination.open.retry.interval.seconds"));
        assertEquals("/var/log/hbase/audit/%app-type%", prop.getProperty("xasecure.audit.hdfs.config.local.buffer.directory"));
        assertEquals("%time:yyyyMMdd-HHmm.ss%.log", prop.getProperty("xasecure.audit.hdfs.config.local.buffer.file"));
        assertEquals("8192", prop.getProperty("xasecure.audit.hdfs.config.local.buffer.file.buffer.size.bytes"));
        assertEquals("60", prop.getProperty("xasecure.audit.hdfs.config.local.buffer.flush.interval.seconds"));
        assertEquals("600", prop.getProperty("xasecure.audit.hdfs.config.local.buffer.rollover.interval.seconds"));
        assertEquals("/var/log/hbase/audit/archive/%app-type%", prop.getProperty("xasecure.audit.hdfs.config.local.archive.directory"));
        assertEquals("10", prop.getProperty("xasecure.audit.hdfs.config.local.archive.max.file.count"));

        assertEquals("true", prop.getProperty("xasecure.audit.solr.is.async"));
        assertEquals("1", prop.getProperty("xasecure.audit.solr.async.max.queue.size"));
        assertEquals("1000", prop.getProperty("xasecure.audit.solr.async.max.flush.interval.ms"));
        assertEquals("http://localhost:6083/solr/ranger_audits", prop.getProperty("xasecure.audit.solr.solr_url"));
    }

    private void validateAuditConfigV3(Properties props) {
        assertEquals("true", props.getProperty("xasecure.audit.is.enabled"));
        assertEquals("false", props.getProperty("xasecure.audit.destination.hdfs"));
        assertEquals("true", props.getProperty("xasecure.audit.destination.solr"));
        assertEquals("false", props.getProperty("xasecure.audit.destination.log4j"));

        assertEquals("hdfs://namenode:8020/ranger/audit", props.getProperty("xasecure.audit.destination.hdfs.dir"));
        assertEquals("%app-type%/%time:yyyyMMdd%", props.getProperty("xasecure.audit.destination.hdfs.subdir"));
        assertEquals("%app-type%_ranger_audit_%hostname%.log", props.getProperty("xasecure.audit.destination.hdfs.filename.format"));
        assertEquals("org.apache.ranger.audit.utils.RangerJSONAuditWriter", props.getProperty("xasecure.audit.destination.hdfs.filewriter.impl"));

        assertEquals("http://localhost:6083/solr/ranger_audits", props.getProperty("xasecure.audit.destination.solr.urls"));
        assertEquals("ranger_audits", props.getProperty("xasecure.audit.destination.solr.collection"));
    }

    private static Properties createDefaultProperties() {
        Properties props = new Properties();

        props.setProperty("ranger.authz.default.policy.source.impl", "org.apache.ranger.admin.client.RangerAdminRESTClient");
        props.setProperty("ranger.authz.default.policy.rest.url", "http://localhost:6080");
        props.setProperty("ranger.authz.default.policy.rest.ssl.config.file", "/etc/hive/conf/ranger-policymgr-ssl.xml");
        props.setProperty("ranger.authz.default.policy.rest.client.connection.timeoutMs", "120000");
        props.setProperty("ranger.authz.default.policy.rest.client.read.timeoutMs", "30000");
        props.setProperty("ranger.authz.default.policy.pollIntervalMs", "30000");
        props.setProperty("ranger.authz.default.policy.cache.dir", "/etc/ranger/policycache");

        return props;
    }

    private static Properties createAuditV2Properties() {
        Properties props = new Properties();

        props.setProperty("ranger.authz.audit.is.enabled", "true");
        props.setProperty("ranger.authz.audit.hdfs.is.enabled", "false");
        props.setProperty("ranger.authz.audit.solr.is.enabled", "true");
        props.setProperty("ranger.authz.audit.log4j.is.enabled", "false");

        props.setProperty("ranger.authz.audit.hdfs.is.async", "true");
        props.setProperty("ranger.authz.audit.hdfs.async.max.queue.size", "1048576");
        props.setProperty("ranger.authz.audit.hdfs.async.max.flush.interval.ms", "30000");
        props.setProperty("ranger.authz.audit.hdfs.config.destination.directory", "hdfs://namenode:8020/ranger/audit/%app-type%/%time:yyyyMMdd%");
        props.setProperty("ranger.authz.audit.hdfs.config.destination.file", "%hostname%-audit.log");
        props.setProperty("ranger.authz.audit.hdfs.config.destination.flush.interval.seconds", "900");
        props.setProperty("ranger.authz.audit.hdfs.config.destination.rollover.interval.seconds", "86400");
        props.setProperty("ranger.authz.audit.hdfs.config.destination.open.retry.interval.seconds", "60");
        props.setProperty("ranger.authz.audit.hdfs.config.local.buffer.directory", "/var/log/hbase/audit/%app-type%");
        props.setProperty("ranger.authz.audit.hdfs.config.local.buffer.file", "%time:yyyyMMdd-HHmm.ss%.log");
        props.setProperty("ranger.authz.audit.hdfs.config.local.buffer.file.buffer.size.bytes", "8192");
        props.setProperty("ranger.authz.audit.hdfs.config.local.buffer.flush.interval.seconds", "60");
        props.setProperty("ranger.authz.audit.hdfs.config.local.buffer.rollover.interval.seconds", "600");
        props.setProperty("ranger.authz.audit.hdfs.config.local.archive.directory", "/var/log/hbase/audit/archive/%app-type%");
        props.setProperty("ranger.authz.audit.hdfs.config.local.archive.max.file.count", "10");

        props.setProperty("ranger.authz.audit.solr.is.async", "true");
        props.setProperty("ranger.authz.audit.solr.async.max.queue.size", "1");
        props.setProperty("ranger.authz.audit.solr.async.max.flush.interval.ms", "1000");
        props.setProperty("ranger.authz.audit.solr.solr_url", "http://localhost:6083/solr/ranger_audits");

        return props;
    }

    private static Properties createAuditV3Properties() {
        Properties props = new Properties();

        props.setProperty("ranger.authz.audit.is.enabled", "true");
        props.setProperty("ranger.authz.audit.destination.hdfs", "false");
        props.setProperty("ranger.authz.audit.destination.solr", "true");
        props.setProperty("ranger.authz.audit.destination.log4j", "false");

        props.setProperty("ranger.authz.audit.destination.hdfs.dir", "hdfs://namenode:8020/ranger/audit");
        props.setProperty("ranger.authz.audit.destination.hdfs.subdir", "%app-type%/%time:yyyyMMdd%");
        props.setProperty("ranger.authz.audit.destination.hdfs.filename.format", "%app-type%_ranger_audit_%hostname%.log");
        props.setProperty("ranger.authz.audit.destination.hdfs.filewriter.impl", "org.apache.ranger.audit.utils.RangerJSONAuditWriter");

        props.setProperty("ranger.authz.audit.destination.solr.urls", "http://localhost:6083/solr/ranger_audits");
        props.setProperty("ranger.authz.audit.destination.solr.collection", "ranger_audits");

        return props;
    }

    private static Properties createAllAuthzProperties() {
        Properties props = createDefaultProperties();

        props.putAll(createAuditV2Properties());
        props.putAll(createAuditV3Properties());

        props.setProperty("ranger.authz.service.hive.service.name", "dev_hive");

        return props;
    }
}
