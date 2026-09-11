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

package org.apache.ranger.entraid.graph;

import org.apache.ranger.entraid.graph.EntraIdGraphConfig.AuthMode;
import org.apache.ranger.ugsyncutil.model.graph.MembershipMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TestEntraIdGraphConfig {
    private static final String TENANT = "11111111-1111-1111-1111-111111111111";
    private static final String CLIENT = "22222222-2222-2222-2222-222222222222";

    /** A builder pre-filled with the minimum valid CERTIFICATE configuration. */
    private EntraIdGraphConfig.Builder validCertBuilder() {
        return new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CERTIFICATE)
                .keystorePath("/etc/ranger/usersync/entraid.p12")
                .certAlias("entraid")
                .keystorePassword("secret".toCharArray());
    }

    /** A builder pre-filled with the minimum valid CLIENT_SECRET configuration. */
    private EntraIdGraphConfig.Builder validSecretBuilder() {
        return new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CLIENT_SECRET)
                .clientSecret("app-secret".toCharArray());
    }

    @Test
    public void test01_build_minimalCertificate_succeeds() {
        EntraIdGraphConfig config = validCertBuilder().build();
        Assertions.assertEquals(TENANT, config.getTenantId());
        Assertions.assertEquals(CLIENT, config.getClientId());
        Assertions.assertEquals(AuthMode.CERTIFICATE, config.getAuthMode());
    }

    @Test
    public void test02_build_minimalClientSecret_succeeds() {
        EntraIdGraphConfig config = validSecretBuilder().build();
        Assertions.assertEquals(AuthMode.CLIENT_SECRET, config.getAuthMode());
    }

    @Test
    public void test03_build_missingTenantId_throws() {
        EntraIdGraphConfig.Builder b = validCertBuilder().tenantId(null);
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test04_build_blankTenantId_throws() {
        EntraIdGraphConfig.Builder b = validCertBuilder().tenantId("   ");
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test05_build_missingClientId_throws() {
        EntraIdGraphConfig.Builder b = validCertBuilder().clientId(null);
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test06_build_missingAuthMode_throws() {
        EntraIdGraphConfig.Builder b = new EntraIdGraphConfig.Builder().tenantId(TENANT).clientId(CLIENT);
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test07_certificate_missingKeystorePath_throws() {
        EntraIdGraphConfig.Builder b = validCertBuilder().keystorePath(null);
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test08_certificate_missingCertAlias_throws() {
        EntraIdGraphConfig.Builder b = validCertBuilder().certAlias(null);
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test09_certificate_withClientSecret_throws() {
        // Mixing a client secret into certificate auth is a misconfiguration.
        EntraIdGraphConfig.Builder b = validCertBuilder().clientSecret("oops".toCharArray());
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test10_clientSecret_missingSecret_throws() {
        EntraIdGraphConfig.Builder b = new EntraIdGraphConfig.Builder().tenantId(TENANT).clientId(CLIENT).authMode(AuthMode.CLIENT_SECRET);
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test11_clientSecret_withKeystorePath_throws() {
        EntraIdGraphConfig.Builder b = validSecretBuilder().keystorePath("/etc/ranger/x.p12");
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test12_build_pageSizeBelowOne_throws() {
        EntraIdGraphConfig.Builder b = validCertBuilder().pageSize(0);
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test13_build_negativeMaxRetries_throws() {
        EntraIdGraphConfig.Builder b = validCertBuilder().maxRetries(-1);
        Assertions.assertThrows(IllegalStateException.class, b::build);
    }

    @Test
    public void test14_tokenEndpoint_derivedFromAuthorityAndTenant() {
        EntraIdGraphConfig config = validCertBuilder().authorityHost("https://login.microsoftonline.com").build();
        Assertions.assertEquals("https://login.microsoftonline.com/" + TENANT + "/oauth2/v2.0/token", config.getTokenEndpoint());
    }

    @Test
    public void test15_tokenEndpoint_honorsSovereignAuthorityHost() {
        EntraIdGraphConfig config = validCertBuilder().authorityHost("https://login.partner.microsoftonline.cn").build();
        Assertions.assertEquals("https://login.partner.microsoftonline.cn/" + TENANT + "/oauth2/v2.0/token", config.getTokenEndpoint());
    }

    @Test
    public void test16_defaultScope_derivedFromGraphBaseUrl() {
        EntraIdGraphConfig config = validCertBuilder().graphBaseUrl("https://graph.microsoft.com").build();
        Assertions.assertEquals("https://graph.microsoft.com/.default", config.getDefaultScope());
    }

    @Test
    public void test17_defaultScope_matchesSovereignGraphHost() {
        EntraIdGraphConfig config = validCertBuilder().graphBaseUrl("https://microsoftgraph.indiacloudapi.in").build();
        Assertions.assertEquals("https://microsoftgraph.indiacloudapi.in/.default", config.getDefaultScope());
    }

    @Test
    public void test18_defaults_areApplied() {
        EntraIdGraphConfig config = validCertBuilder().build();
        Assertions.assertEquals("https://login.microsoftonline.com", config.getAuthorityHost());
        Assertions.assertEquals(MembershipMode.DIRECT, config.getMembershipMode());
    }

    @Test
    public void test19_keystorePassword_isDefensivelyCopiedOnBuild() {
        char[] pwd = "secret".toCharArray();
        EntraIdGraphConfig config = new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CERTIFICATE)
                .keystorePath("/etc/ranger/usersync/entraid.p12")
                .certAlias("entraid")
                .keystorePassword(pwd)
                .build();
        // Mutating the caller's array must not affect the stored credential.
        Arrays.fill(pwd, '\0');
        Assertions.assertArrayEquals("secret".toCharArray(), config.getKeystorePassword());
    }

    @Test
    public void test20_clientSecret_isDefensivelyCopiedOnWriteAndRead() throws Exception {
        char[] pwd = "app-secret".toCharArray();
        EntraIdGraphConfig config = new EntraIdGraphConfig.Builder()
                .tenantId(TENANT)
                .clientId(CLIENT)
                .authMode(AuthMode.CLIENT_SECRET)
                .clientSecret(pwd)
                .build();
        // 1. Test defensive copy on WRITE (input)
        Arrays.fill(pwd, '\0');
        Assertions.assertArrayEquals("app-secret".toCharArray(), config.getClientSecret());
        // 2. Test defensive copy on READ (output)
        char[] first = config.getClientSecret();
        char[] second = config.getClientSecret();
        Assertions.assertNotSame(first, second);
        Arrays.fill(first, '\0');
        Assertions.assertArrayEquals("app-secret".toCharArray(), second);
    }
}
