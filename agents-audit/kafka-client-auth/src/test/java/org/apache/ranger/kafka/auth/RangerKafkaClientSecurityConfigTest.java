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

package org.apache.ranger.kafka.auth;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangerKafkaClientSecurityConfigTest {
    private static final String PREFIX = "ranger.audit.ingestor";

    @TempDir
    Path tempDir;

    @Test
    public void testPlaintextByDefaultSetsNoSaslKeys() {
        Map<String, Object> config = RangerKafkaClientSecurityConfig.build(new Properties(), PREFIX);

        assertEquals("PLAINTEXT", config.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertFalse(config.containsKey(SaslConfigs.SASL_MECHANISM));
        assertFalse(config.containsKey(SaslConfigs.SASL_JAAS_CONFIG));
        assertFalse(config.containsKey(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
    }

    @Test
    public void testSaslDefaultsToOAuthBearer() throws IOException {
        Path       token = Files.createFile(tempDir.resolve("token"));
        Properties props = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_PLAINTEXT");
        props.setProperty(PREFIX + ".kafka.sasl.oauthbearer.token.endpoint.url", token.toUri().toString());

        Map<String, Object> config = RangerKafkaClientSecurityConfig.build(props, PREFIX);

        assertEquals("OAUTHBEARER", config.get(SaslConfigs.SASL_MECHANISM));
        assertEquals(RangerKafkaOAuthBearerLoginCallbackHandler.class.getName(), config.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
    }

    @Test
    public void testSaslDefaultRequiresTokenUrlAndNamesGssapiOptOut() {
        Properties props = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_PLAINTEXT");
        props.setProperty(PREFIX + ".service.kerberos.principal", "rangeraudit@EXAMPLE.COM");
        props.setProperty(PREFIX + ".service.kerberos.keytab", "/etc/keytabs/ranger.keytab");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> RangerKafkaClientSecurityConfig.build(props, PREFIX));

        assertTrue(e.getMessage().contains(PREFIX + ".kafka.sasl.oauthbearer.token.endpoint.url"), e.getMessage());
        assertTrue(e.getMessage().contains(PREFIX + ".kafka.sasl.mechanism=GSSAPI"), e.getMessage());
    }

    @Test
    public void testExplicitGssapiBuildsKeytabJaas() throws IOException {
        Path       keytab = Files.createFile(tempDir.resolve("ranger.keytab"));
        Properties props  = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_PLAINTEXT");
        props.setProperty(PREFIX + ".kafka.sasl.mechanism", "GSSAPI");
        props.setProperty(PREFIX + ".service.kerberos.principal", "rangeraudit/_HOST@EXAMPLE.COM");
        props.setProperty(PREFIX + ".service.kerberos.keytab", keytab.toString());
        props.setProperty(PREFIX + ".host", "Audit-Host.example.com");

        Map<String, Object> config = RangerKafkaClientSecurityConfig.build(props, PREFIX);

        assertEquals("SASL_PLAINTEXT", config.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("GSSAPI", config.get(SaslConfigs.SASL_MECHANISM));
        assertEquals("kafka", config.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
        assertEquals(RangerKafkaClientSecurityConfig.KRB5_LOGIN_MODULE + " required useKeyTab=true keyTab=\"" + keytab + "\" storeKey=true useTicketCache=false serviceName=kafka principal=\"rangeraudit/audit-host.example.com@EXAMPLE.COM\";",
                config.get(SaslConfigs.SASL_JAAS_CONFIG));
        assertFalse(config.containsKey(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
    }

    @Test
    public void testGssapiHonoursConfiguredServiceName() throws IOException {
        Path       keytab = Files.createFile(tempDir.resolve("ranger.keytab"));
        Properties props  = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_SSL");
        props.setProperty(PREFIX + ".kafka.sasl.mechanism", "GSSAPI");
        props.setProperty(PREFIX + ".kafka.sasl.kerberos.service.name", "kafka-broker");
        props.setProperty(PREFIX + ".service.kerberos.principal", "rangeraudit@EXAMPLE.COM");
        props.setProperty(PREFIX + ".service.kerberos.keytab", keytab.toString());

        Map<String, Object> config = RangerKafkaClientSecurityConfig.build(props, PREFIX);

        assertEquals("kafka-broker", config.get(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
        assertTrue(config.get(SaslConfigs.SASL_JAAS_CONFIG).toString().contains("serviceName=kafka-broker principal=\"rangeraudit@EXAMPLE.COM\""));
    }

    @Test
    public void testGssapiRequiresPrincipalAndKeytab() {
        Properties props = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_PLAINTEXT");
        props.setProperty(PREFIX + ".kafka.sasl.mechanism", "GSSAPI");
        props.setProperty(PREFIX + ".service.kerberos.principal", "rangeraudit@EXAMPLE.COM");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> RangerKafkaClientSecurityConfig.build(props, PREFIX));

        assertTrue(e.getMessage().contains(PREFIX + ".service.kerberos.keytab"), e.getMessage());
    }

    @Test
    public void testGssapiRequiresReadableKeytab() {
        Properties props = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_PLAINTEXT");
        props.setProperty(PREFIX + ".kafka.sasl.mechanism", "GSSAPI");
        props.setProperty(PREFIX + ".service.kerberos.principal", "rangeraudit@EXAMPLE.COM");
        props.setProperty(PREFIX + ".service.kerberos.keytab", tempDir.resolve("missing.keytab").toString());

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> RangerKafkaClientSecurityConfig.build(props, PREFIX));

        assertTrue(e.getMessage().contains("not found"), e.getMessage());
    }

    @Test
    public void testOAuthBearerFileTokenUsesRangerHandler() throws IOException {
        Path       token = Files.createFile(tempDir.resolve("token"));
        Properties props = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_SSL");
        props.setProperty(PREFIX + ".kafka.sasl.mechanism", "oauthbearer");
        props.setProperty(PREFIX + ".kafka.sasl.oauthbearer.token.endpoint.url", token.toUri().toString());

        Map<String, Object> config = RangerKafkaClientSecurityConfig.build(props, PREFIX);

        assertEquals("oauthbearer", config.get(SaslConfigs.SASL_MECHANISM));
        assertEquals(RangerKafkaClientSecurityConfig.OAUTHBEARER_LOGIN_MODULE + " required;", config.get(SaslConfigs.SASL_JAAS_CONFIG));
        assertEquals(token.toUri().toString(), config.get(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL));
        assertEquals(RangerKafkaOAuthBearerLoginCallbackHandler.class.getName(), config.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
        assertFalse(config.containsKey(SaslConfigs.SASL_KERBEROS_SERVICE_NAME));
    }

    @Test
    public void testOAuthBearerHttpEndpointUsesKafkaHandler() {
        Properties props = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_SSL");
        props.setProperty(PREFIX + ".kafka.sasl.oauthbearer.token.endpoint.url", "https://idp.example.com/oauth/token");

        Map<String, Object> config = RangerKafkaClientSecurityConfig.build(props, PREFIX);

        assertEquals(OAuthBearerLoginCallbackHandler.class.getName(), config.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
    }

    @Test
    public void testPassThroughOverridesDerivedValues() throws IOException {
        Path       token = Files.createFile(tempDir.resolve("token"));
        Properties props = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_SSL");
        props.setProperty(PREFIX + ".kafka.sasl.oauthbearer.token.endpoint.url", token.toUri().toString());
        props.setProperty(PREFIX + ".kafka.sasl.login.callback.handler.class", "com.example.CustomHandler");
        props.setProperty(PREFIX + ".kafka.sasl.login.refresh.window.factor", "0.7");
        props.setProperty(PREFIX + ".kafka.ssl.truststore.location", "/etc/ssl/truststore.jks");
        props.setProperty(PREFIX + ".kafka.ssl.truststore.password", " ");
        props.setProperty(PREFIX + ".kafka.bootstrap.servers", "kafka:9093");

        Map<String, Object> config = RangerKafkaClientSecurityConfig.build(props, PREFIX);

        assertEquals("com.example.CustomHandler", config.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS));
        assertEquals("0.7", config.get(SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR));
        assertEquals("/etc/ssl/truststore.jks", config.get("ssl.truststore.location"));
        assertFalse(config.containsKey("ssl.truststore.password"));
        assertFalse(config.containsKey("bootstrap.servers"));
    }

    @Test
    public void testOtherMechanismsNeedExplicitJaasConfig() {
        Properties props = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_SSL");
        props.setProperty(PREFIX + ".kafka.sasl.mechanism", "SCRAM-SHA-512");

        assertThrows(IllegalStateException.class, () -> RangerKafkaClientSecurityConfig.build(props, PREFIX));

        props.setProperty(PREFIX + ".kafka.sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ranger\" password=\"secret\";");

        Map<String, Object> config = RangerKafkaClientSecurityConfig.build(props, PREFIX);

        assertEquals("SCRAM-SHA-512", config.get(SaslConfigs.SASL_MECHANISM));
        assertTrue(config.get(SaslConfigs.SASL_JAAS_CONFIG).toString().startsWith("org.apache.kafka.common.security.scram.ScramLoginModule"));
    }

    @Test
    public void testApplyAddsToExistingProperties() throws IOException {
        Path       token  = Files.createFile(tempDir.resolve("token"));
        Properties props  = new Properties();
        Properties target = new Properties();

        props.setProperty(PREFIX + ".kafka.security.protocol", "SASL_PLAINTEXT");
        props.setProperty(PREFIX + ".kafka.sasl.oauthbearer.token.endpoint.url", token.toUri().toString());
        target.put("bootstrap.servers", "kafka:9092");

        RangerKafkaClientSecurityConfig.apply(props, PREFIX, target);

        assertEquals("kafka:9092", target.get("bootstrap.servers"));
        assertEquals("SASL_PLAINTEXT", target.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        assertEquals("OAUTHBEARER", target.get(SaslConfigs.SASL_MECHANISM));
    }

    @Test
    public void testResolvePrincipal() {
        assertEquals("svc/host.example.com@REALM", RangerKafkaClientSecurityConfig.resolvePrincipal("svc/_HOST@REALM", "Host.Example.COM"));
        assertEquals("svc/other@REALM", RangerKafkaClientSecurityConfig.resolvePrincipal("svc/other@REALM", "host.example.com"));
        assertEquals("svc@REALM", RangerKafkaClientSecurityConfig.resolvePrincipal("svc@REALM", null));
        assertFalse(RangerKafkaClientSecurityConfig.resolvePrincipal("svc/_HOST@REALM", null).contains("_HOST"));
    }
}
