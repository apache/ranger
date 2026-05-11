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
package org.apache.ranger.services.nifi.registry.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSessionContext;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Integration tests for NiFiRegistryClient SSL hostname verification functionality.
 * These tests verify that the hostname verifier correctly validates SSL certificates
 * and prevents man-in-the-middle attacks.
 *
 * Originally removed during Jersey 1.x to Jersey 2.x migration due to API changes,
 * now restored using Jersey 2.x / Jakarta RS APIs.
 */
public class TestNiFiRegistryClientSSL {
    private static final String HOSTNAME = "example.com";
    private static final int HTTPS_PORT = 443;

    private NiFiRegistryClient sslClient;

    @BeforeEach
    public void setup() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sslContext = createInitializedSSLContext();
        sslClient = new NiFiRegistryClient("https://" + HOSTNAME + ":" + HTTPS_PORT, sslContext);
    }

    /**
     * Test that hostname verifier correctly validates a matching hostname.
     * This ensures legitimate connections are accepted.
     */
    @Test
    public void testHostnameVerifierMatch() throws NoSuchAlgorithmException, KeyManagementException,
            CertificateParsingException, SSLPeerUnverifiedException {
        MockSSLSession mockSession = createMockSSLSessionWithCertificate(HOSTNAME);

        HostnameVerifier verifier = sslClient.getHostnameVerifier();
        Assertions.assertNotNull(verifier, "Hostname verifier should not be null");

        boolean result = verifier.verify(HOSTNAME, mockSession);
        Assertions.assertTrue(result, "Hostname verification should pass for matching hostname");
    }

    /**
     * Test that hostname verifier correctly rejects a non-matching hostname.
     * This prevents MITM attacks where a certificate for a different hostname is presented.
     */
    @Test
    public void testHostnameVerifierNoMatch() throws NoSuchAlgorithmException, KeyManagementException,
            CertificateParsingException, SSLPeerUnverifiedException {
        MockSSLSession mockSession = createMockSSLSessionWithCertificate("attacker.com");

        HostnameVerifier verifier = sslClient.getHostnameVerifier();
        Assertions.assertNotNull(verifier, "Hostname verifier should not be null");

        boolean result = verifier.verify(HOSTNAME, mockSession);
        Assertions.assertFalse(result, "Hostname verification should fail for non-matching hostname");
    }

    /**
     * Test that hostname verifier handles missing certificates gracefully.
     * An empty certificate chain should result in verification failure.
     */
    @Test
    public void testHostnameVerifierNoCerts() throws SSLPeerUnverifiedException {
        MockSSLSession mockSession = createMockSSLSessionWithNoCertificates();

        HostnameVerifier verifier = sslClient.getHostnameVerifier();
        Assertions.assertNotNull(verifier, "Hostname verifier should not be null");

        boolean result = verifier.verify(HOSTNAME, mockSession);
        Assertions.assertFalse(result, "Hostname verification should fail when certificates are missing");
    }

    /**
     * Test that hostname verifier handles an empty certificate list.
     * Even if peer certificates exist but are empty, verification should fail.
     */
    @Test
    public void testHostnameVerifierEmptyCerts() throws SSLPeerUnverifiedException {
        MockSSLSession mockSession = createMockSSLSessionWithEmptyCertificates();

        HostnameVerifier verifier = sslClient.getHostnameVerifier();
        Assertions.assertNotNull(verifier, "Hostname verifier should not be null");

        boolean result = verifier.verify(HOSTNAME, mockSession);
        Assertions.assertFalse(result, "Hostname verification should fail when certificate list is empty");
    }

    /**
     * Test that hostname verifier only checks the leaf certificate for SAN.
     * A SAN in an intermediate certificate should not be used for hostname matching.
     * This is per RFC 6125 and prevents incorrect validation chains.
     */
    @Test
    public void testHostnameVerifierSanInIntermediateCertFails() throws NoSuchAlgorithmException,
            KeyManagementException, CertificateParsingException, SSLPeerUnverifiedException {
        MockSSLSession mockSession = createMockSSLSessionWithSanInIntermediate();

        HostnameVerifier verifier = sslClient.getHostnameVerifier();
        Assertions.assertNotNull(verifier, "Hostname verifier should not be null");

        boolean result = verifier.verify(HOSTNAME, mockSession);
        Assertions.assertFalse(result, "Hostname verification should fail when SAN is only in intermediate cert");
    }

    // Helper methods for creating mock SSL sessions

    private static SSLContext createInitializedSSLContext() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);
        return sslContext;
    }

    /**
     * Create a mock SSL session with a certificate containing the specified SAN hostname.
     */
    private MockSSLSession createMockSSLSessionWithCertificate(String sanHostname)
            throws CertificateParsingException {
        X509Certificate leafCert = new MockX509Certificate(sanHostname);
        return new MockSSLSession(new Certificate[] {leafCert});
    }

    /**
     * Create a mock SSL session with no peer certificates.
     */
    private MockSSLSession createMockSSLSessionWithNoCertificates() {
        return new MockSSLSession(new Certificate[0]);
    }

    /**
     * Create a mock SSL session with empty peer certificates array.
     */
    private MockSSLSession createMockSSLSessionWithEmptyCertificates() {
        return new MockSSLSession(new Certificate[0]);
    }

    /**
     * Create a mock SSL session where SAN is in intermediate cert, not leaf cert.
     */
    private MockSSLSession createMockSSLSessionWithSanInIntermediate()
            throws CertificateParsingException {
        X509Certificate leafCert = new MockX509Certificate(null);  // No SAN in leaf
        X509Certificate intermediateCert = new MockX509Certificate("other.com");
        return new MockSSLSession(new Certificate[] {leafCert, intermediateCert});
    }

    /**
     * Mock SSLSession for testing hostname verification without a real SSL connection.
     */
    private static class MockSSLSession implements SSLSession {
        private final Certificate[] peerCertificates;

        MockSSLSession(Certificate[] peerCertificates) {
            this.peerCertificates = peerCertificates;
        }

        @Override
        public byte[] getId() {
            return new byte[0];
        }

        @Override
        public SSLSessionContext getSessionContext() {
            return null;
        }

        @Override
        public long getCreationTime() {
            return 0;
        }

        @Override
        public long getLastAccessedTime() {
            return 0;
        }

        @Override
        public void invalidate() {
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public void putValue(String name, Object value) {
        }

        @Override
        public Object getValue(String name) {
            return null;
        }

        @Override
        public void removeValue(String name) {
        }

        @Override
        public String[] getValueNames() {
            return new String[0];
        }

        @Override
        public Certificate[] getPeerCertificates() throws SSLPeerUnverifiedException {
            if (peerCertificates == null || peerCertificates.length == 0) {
                throw new SSLPeerUnverifiedException("No peer certificates");
            }
            return peerCertificates;
        }

        @Override
        public Certificate[] getLocalCertificates() {
            return new Certificate[0];
        }

        @Override
        public javax.security.cert.X509Certificate[] getPeerCertificateChain() throws SSLPeerUnverifiedException {
            return new javax.security.cert.X509Certificate[0];
        }

        @Override
        public Principal getPeerPrincipal() throws SSLPeerUnverifiedException {
            return null;
        }

        @Override
        public Principal getLocalPrincipal() {
            return null;
        }

        @Override
        public String getCipherSuite() {
            return null;
        }

        @Override
        public String getProtocol() {
            return "TLS";
        }

        @Override
        public String getPeerHost() {
            return HOSTNAME;
        }

        @Override
        public int getPeerPort() {
            return HTTPS_PORT;
        }

        @Override
        public int getPacketBufferSize() {
            return 0;
        }

        @Override
        public int getApplicationBufferSize() {
            return 0;
        }
    }

    /**
     * Mock X509Certificate for testing SAN extraction and validation.
     * Provides getSubjectAlternativeNames() for hostname verification.
     */
    private static class MockX509Certificate extends X509Certificate {
        private final String sanHostname;

        MockX509Certificate(String sanHostname) {
            this.sanHostname = sanHostname;
        }

        @Override
        public Collection<List<?>> getSubjectAlternativeNames() throws CertificateParsingException {
            if (sanHostname == null) {
                return null;
            }
            // Return SAN in DNSName format (type 2)
            // Format: [type, value] where type 2 = DNSName
            return Collections.singletonList(
                    Arrays.asList(2, sanHostname.toLowerCase()));
        }

        // Stub implementations for abstract methods
        @Override
        public void checkValidity() {
        }

        @Override
        public void checkValidity(java.util.Date date) {
        }

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public java.math.BigInteger getSerialNumber() {
            return null;
        }

        @Override
        public java.security.Principal getIssuerDN() {
            return null;
        }

        @Override
        public java.security.Principal getSubjectDN() {
            return null;
        }

        @Override
        public java.util.Date getNotBefore() {
            return null;
        }

        @Override
        public java.util.Date getNotAfter() {
            return null;
        }

        @Override
        public byte[] getTBSCertificate() throws java.security.cert.CertificateEncodingException {
            return new byte[0];
        }

        @Override
        public byte[] getSignature() {
            return new byte[0];
        }

        @Override
        public String getSigAlgName() {
            return null;
        }

        @Override
        public String getSigAlgOID() {
            return null;
        }

        @Override
        public byte[] getSigAlgParams() {
            return new byte[0];
        }

        @Override
        public boolean[] getIssuerUniqueID() {
            return new boolean[0];
        }

        @Override
        public boolean[] getSubjectUniqueID() {
            return new boolean[0];
        }

        @Override
        public boolean[] getKeyUsage() {
            return new boolean[0];
        }

        @Override
        public int getBasicConstraints() {
            return 0;
        }

        @Override
        public byte[] getEncoded() throws java.security.cert.CertificateEncodingException {
            return new byte[0];
        }

        @Override
        public java.security.PublicKey getPublicKey() {
            return null;
        }

        @Override
        public void verify(java.security.PublicKey key) {
        }

        @Override
        public void verify(java.security.PublicKey key, String sigProvider) {
        }

        @Override
        public byte[] getExtensionValue(String oid) {
            return new byte[0];
        }

        @Override
        public java.util.Set<String> getCriticalExtensionOIDs() {
            return Collections.emptySet();
        }

        @Override
        public java.util.Set<String> getNonCriticalExtensionOIDs() {
            return Collections.emptySet();
        }

        @Override
        public boolean hasUnsupportedCriticalExtension() {
            return false;
        }

        @Override
        public String toString() {
            return "MockX509Certificate(san=" + sanHostname + ")";
        }
    }
}
