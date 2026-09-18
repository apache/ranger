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

import org.apache.ranger.credentialapi.CredentialReader;
import org.apache.ranger.entraid.graph.EntraIdGraphConfig.AuthMode;
import org.apache.ranger.ugsyncutil.model.graph.MembershipMode;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestEntraIdGraphConfigLoader {
    private static final String TENANT = "11111111-1111-1111-1111";
    private static final String CLIENT = "22222222-2222-2222-2222";

    private UserGroupSyncConfig config;

    @BeforeEach
    public void setUp() {
        config = Mockito.mock(UserGroupSyncConfig.class, Mockito.withSettings().lenient());
    }

    private void stub(String key, String value) {
        Mockito.when(config.getProperty(key)).thenReturn(value);
    }

    /** Stub the minimum set for a valid CERTIFICATE configuration. */
    private void stubValidCertificate() {
        stub(EntraIdGraphConfigLoader.ENTRAID_TENANT_ID, TENANT);
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_ID, CLIENT);
        stub(EntraIdGraphConfigLoader.ENTRAID_AUTH_TYPE, "CERTIFICATE");
        stub(EntraIdGraphConfigLoader.ENTRAID_KEYSTORE_FILE, "/etc/ranger/usersync/entraid.p12");
        stub(EntraIdGraphConfigLoader.ENTRAID_CERT_ALIAS, "entraid");
        stub(EntraIdGraphConfigLoader.ENTRAID_KEYSTORE_PASSWORD, "secret");
    }

    @Test
    public void test01_load_mapsCoreIdentity() throws Exception {
        stubValidCertificate();
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals(TENANT, result.getTenantId());
        Assertions.assertEquals(CLIENT, result.getClientId());
    }

    @Test
    public void test02_load_certificateAuthType_isParsed() throws Exception {
        stubValidCertificate();
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals(AuthMode.CERTIFICATE, result.getAuthMode());
    }

    @Test
    public void test03_load_clientSecretAuthType_isParsed() throws Exception {
        stub(EntraIdGraphConfigLoader.ENTRAID_TENANT_ID, TENANT);
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_ID, CLIENT);
        stub(EntraIdGraphConfigLoader.ENTRAID_AUTH_TYPE, "CLIENT_SECRET");
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_SECRET, "app-secret");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals(AuthMode.CLIENT_SECRET, result.getAuthMode());
    }

    @Test
    public void test04_load_absentAuthType_defaultsToCertificate() throws Exception {
        // No auth.type property set; loader defaults to CERTIFICATE.
        stub(EntraIdGraphConfigLoader.ENTRAID_TENANT_ID, TENANT);
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_ID, CLIENT);
        stub(EntraIdGraphConfigLoader.ENTRAID_KEYSTORE_FILE, "/etc/ranger/usersync/entraid.p12");
        stub(EntraIdGraphConfigLoader.ENTRAID_CERT_ALIAS, "entraid");
        stub(EntraIdGraphConfigLoader.ENTRAID_KEYSTORE_PASSWORD, "secret");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals(AuthMode.CERTIFICATE, result.getAuthMode());
    }

    @Test
    public void test05_load_invalidAuthType_throws() {
        stub(EntraIdGraphConfigLoader.ENTRAID_TENANT_ID, TENANT);
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_ID, CLIENT);
        stub(EntraIdGraphConfigLoader.ENTRAID_AUTH_TYPE, "PASSWORD");
        EntraIdGraphConfigLoader loader = new EntraIdGraphConfigLoader(config);
        Assertions.assertThrows(GraphClientException.class, loader::load);
    }

    @Test
    public void test06_load_membershipMode_isParsed() throws Exception {
        stubValidCertificate();
        stub(EntraIdGraphConfigLoader.ENTRAID_MEMBERSHIP_MODE, "TRANSITIVE");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals(MembershipMode.TRANSITIVE, result.getMembershipMode());
    }

    @Test
    public void test07_load_absentMembershipMode_defaultsToDirect() throws Exception {
        stubValidCertificate();
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals(MembershipMode.DIRECT, result.getMembershipMode());
    }

    @Test
    public void test08_load_invalidMembershipMode_throws() {
        stubValidCertificate();
        stub(EntraIdGraphConfigLoader.ENTRAID_MEMBERSHIP_MODE, "SIDEWAYS");
        EntraIdGraphConfigLoader loader = new EntraIdGraphConfigLoader(config);
        Assertions.assertThrows(GraphClientException.class, loader::load);
    }

    @Test
    public void test09_load_userSelectAttrs_parsedAsCsv() throws Exception {
        stubValidCertificate();
        stub(EntraIdGraphConfigLoader.ENTRAID_USER_SELECT_ATTRS, "userPrincipalName, displayName ,mail");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertTrue(result.getUserSelectAttrs().contains("userPrincipalName"));
        Assertions.assertTrue(result.getUserSelectAttrs().contains("displayName"));
        Assertions.assertTrue(result.getUserSelectAttrs().contains("mail"));
    }

    @Test
    public void test10_load_csvParsing_trimsAndSkipsBlanks() throws Exception {
        stubValidCertificate();
        stub(EntraIdGraphConfigLoader.ENTRAID_USER_SELECT_ATTRS, " mail , , displayName ,");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        // Empty tokens between commas must not become set entries.
        Assertions.assertEquals(2, result.getUserSelectAttrs().size());
        Assertions.assertFalse(result.getUserSelectAttrs().contains(""));
    }

    @Test
    public void test11_load_groupFilter_isMapped() throws Exception {
        stubValidCertificate();
        stub(EntraIdGraphConfigLoader.ENTRAID_GROUP_FILTER, "startswith(displayName,'ENG-')");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals("startswith(displayName,'ENG-')", result.getGroupFilter());
    }

    @Test
    public void test12_load_numericPageSize_isMapped() throws Exception {
        stubValidCertificate();
        stub(EntraIdGraphConfigLoader.ENTRAID_PAGE_SIZE, "250");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals(250, result.getPageSize());
    }

    @Test
    public void test13_load_nonNumericPageSize_throws() {
        stubValidCertificate();
        stub(EntraIdGraphConfigLoader.ENTRAID_PAGE_SIZE, "lots");
        EntraIdGraphConfigLoader loader = new EntraIdGraphConfigLoader(config);
        Assertions.assertThrows(GraphClientException.class, loader::load);
    }

    @Test
    public void test14_load_overridableEndpoints_areMapped() throws Exception {
        stubValidCertificate();
        stub(EntraIdGraphConfigLoader.ENTRAID_AUTHORITY_HOST, "https://login.partner.microsoftonline.in");
        stub(EntraIdGraphConfigLoader.ENTRAID_GRAPH_BASE_URL, "https://microsoftgraph.indiacloudapi.in");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertEquals("https://login.partner.microsoftonline.in", result.getAuthorityHost());
        Assertions.assertEquals("https://microsoftgraph.indiacloudapi.in", result.getGraphBaseUrl());
    }

    @Test
    public void test15_clientSecret_directProperty_isUsed() throws Exception {
        stub(EntraIdGraphConfigLoader.ENTRAID_TENANT_ID, TENANT);
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_ID, CLIENT);
        stub(EntraIdGraphConfigLoader.ENTRAID_AUTH_TYPE, "CLIENT_SECRET");
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_SECRET, "direct-secret");
        EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
        Assertions.assertArrayEquals("direct-secret".toCharArray(), result.getClientSecret());
    }

    @Test
    public void test16_clientSecret_fallsBackToCredStoreAlias() throws Exception {
        // No direct secret; loader must resolve via CredentialReader using the alias.
        stub(EntraIdGraphConfigLoader.ENTRAID_TENANT_ID, TENANT);
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_ID, CLIENT);
        stub(EntraIdGraphConfigLoader.ENTRAID_AUTH_TYPE, "CLIENT_SECRET");
        stub(EntraIdGraphConfigLoader.ENTRAID_CREDSTORE_FILE, "/etc/ranger/usersync/.credstore.jceks");
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_SECRET_ALIAS, "entraid.client.secret");
        try (MockedStatic<CredentialReader> cr = Mockito.mockStatic(CredentialReader.class)) {
            cr.when(() -> CredentialReader.getDecryptedString(Mockito.anyString(), Mockito.eq("entraid.client.secret"), Mockito.any())).thenReturn("vaulted-secret");
            EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
            Assertions.assertArrayEquals("vaulted-secret".toCharArray(), result.getClientSecret());
        }
    }

    @Test
    public void test17_clientSecret_directValueWinsOverCredStore() throws Exception {
        stub(EntraIdGraphConfigLoader.ENTRAID_TENANT_ID, TENANT);
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_ID, CLIENT);
        stub(EntraIdGraphConfigLoader.ENTRAID_AUTH_TYPE, "CLIENT_SECRET");
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_SECRET, "direct-secret");
        stub(EntraIdGraphConfigLoader.ENTRAID_CREDSTORE_FILE, "/etc/ranger/usersync/.credstore.jceks");
        stub(EntraIdGraphConfigLoader.ENTRAID_CLIENT_SECRET_ALIAS, "entraid.client.secret");
        try (MockedStatic<CredentialReader> cr = Mockito.mockStatic(CredentialReader.class)) {
            EntraIdGraphConfig result = new EntraIdGraphConfigLoader(config).load();
            Assertions.assertArrayEquals("direct-secret".toCharArray(), result.getClientSecret());
            // The credstore must not be consulted when a direct value is present.
            cr.verifyNoInteractions();
        }
    }

    @Test
    public void test18_nullConfig_isRejected() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new EntraIdGraphConfigLoader(null));
    }
}
