/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.services.kms.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class KMSConnectionMgrTest {
    private static final String VALID_URL = "http://kms";
    private static final String VALID_USER = "user";
    private static final String VALID_PASS = "pass";
    private static final String VALID_PRINCIPAL = "principal";
    private static final String VALID_KEYTAB = "keytab";
    private static final String VALID_NAMERULES = "rules";
    private static final String VALID_AUTHTYPE = "authType";

    @Test
    void testGetKMSClient_NullKmsUrl() {
        KMSClient client = KMSConnectionMgr.getKMSClient(null, VALID_USER, VALID_PASS, VALID_PRINCIPAL, VALID_KEYTAB, VALID_NAMERULES, VALID_AUTHTYPE);
        assertNull(client);
    }

    @Test
    void testGetKMSClient_EmptyKmsUrl() {
        KMSClient client = KMSConnectionMgr.getKMSClient("", VALID_USER, VALID_PASS, VALID_PRINCIPAL, VALID_KEYTAB, VALID_NAMERULES, VALID_AUTHTYPE);
        assertNull(client);
    }

    @Test
    void testGetKMSClient_EmptyRangerPrincipal_EmptyUser() {
        KMSClient client = KMSConnectionMgr.getKMSClient(VALID_URL, "", VALID_PASS, "", VALID_KEYTAB, VALID_NAMERULES, VALID_AUTHTYPE);
        assertNotNull(client);
    }

    @Test
    void testGetKMSClient_EmptyRangerPrincipal_NullUser() {
        KMSClient client = KMSConnectionMgr.getKMSClient(VALID_URL, null, VALID_PASS, "", VALID_KEYTAB, VALID_NAMERULES, VALID_AUTHTYPE);
        assertNotNull(client);
    }

    @Test
    void testGetKMSClient_EmptyRangerPrincipal_EmptyPassword() {
        KMSClient client = KMSConnectionMgr.getKMSClient(VALID_URL, VALID_USER, "", "", VALID_KEYTAB, VALID_NAMERULES, VALID_AUTHTYPE);
        assertNotNull(client);
    }

    @Test
    void testGetKMSClient_EmptyRangerPrincipal_NullPassword() {
        KMSClient client = KMSConnectionMgr.getKMSClient(VALID_URL, VALID_USER, null, "", VALID_KEYTAB, VALID_NAMERULES, VALID_AUTHTYPE);
        assertNotNull(client);
    }

    @Test
    void testGetKMSClient_EmptyRangerPrincipal_ValidUserAndPassword() {
        KMSClient client = KMSConnectionMgr.getKMSClient(VALID_URL, VALID_USER, VALID_PASS, "", VALID_KEYTAB, VALID_NAMERULES, VALID_AUTHTYPE);
        assertNotNull(client);
    }

    @Test
    void testGetKMSClient_ValidRangerPrincipal() {
        KMSClient client = KMSConnectionMgr.getKMSClient(VALID_URL, VALID_USER, VALID_PASS, VALID_PRINCIPAL, VALID_KEYTAB, VALID_NAMERULES, VALID_AUTHTYPE);
        assertNotNull(client);
    }
}
