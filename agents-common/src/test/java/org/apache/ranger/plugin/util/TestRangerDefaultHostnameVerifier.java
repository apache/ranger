/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.plugin.util;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRangerDefaultHostnameVerifier {
    private static final RangerDefaultHostnameVerifier VERIFIER = new RangerDefaultHostnameVerifier();
    private static Path keystoreDir;
    private static HttpsServer mismatchedHostServer;
    private static HttpsServer matchedHostServer;
    private static HttpsServer wildcardHostServer;

    @BeforeAll
    static void generateKeystoresAndStartServers() throws Exception {
        keystoreDir = Files.createTempDirectory("ranger-hostname-verifier-test");

        java.net.URL scriptUrl = TestRangerDefaultHostnameVerifier.class
                .getClassLoader()
                .getResource("generate-test-keystores.sh");

        if (scriptUrl == null) {
            throw new IllegalStateException("generate-test-keystores.sh not found on classpath");
        }

        String scriptPath = new java.io.File(scriptUrl.toURI()).getAbsolutePath();

        ProcessBuilder pb = new ProcessBuilder("bash", scriptPath, keystoreDir.toString());
        pb.redirectErrorStream(true);
        Process process = pb.start();

        String output = readStream(process.getInputStream());
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IllegalStateException("generate-test-keystores.sh failed (exit=" + exitCode + "):\n" + output);
        }

        mismatchedHostServer = startServer("attacker-cert.jks");
        matchedHostServer = startServer("localhost-cert.jks");
        wildcardHostServer = startServer("wildcard-cert.jks");
    }

    @AfterAll
    static void stopServersAndCleanup() throws Exception {
        if (mismatchedHostServer != null) {
            mismatchedHostServer.stop(0);
        }
        if (matchedHostServer != null) {
            matchedHostServer.stop(0);
        }
        if (wildcardHostServer != null) {
            wildcardHostServer.stop(0);
        }
        try (Stream<Path> files = Files.walk(keystoreDir)) {
            files.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    @Test
    void rejectsCertificateIssuedForDifferentHostname() throws Exception {
        assertFalse(VERIFIER.verify("localhost", handshake(mismatchedHostServer)));
    }

    @Test
    void acceptsCertificateMatchingHostname() throws Exception {
        assertTrue(VERIFIER.verify("localhost", handshake(matchedHostServer)));
    }

    @Test
    void acceptsWildcardSanForSingleLabel() throws Exception {
        assertTrue(VERIFIER.verify("foo.example.test", handshake(wildcardHostServer)));
    }

    @Test
    void rejectsWildcardSanForMultiLabel() throws Exception {
        assertFalse(VERIFIER.verify("a.b.example.test", handshake(wildcardHostServer)));
    }

    @Test
    void rejectsNullHostnameOrSession() {
        assertFalse(VERIFIER.verify(null, null));
    }

    private static String readStream(InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        int read;

        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }

        return out.toString("UTF-8");
    }

    private static HttpsServer startServer(String keystoreFileName) throws Exception {
        char[] pw = "changeit".toCharArray();
        KeyStore ks = KeyStore.getInstance("JKS");
        try (FileInputStream in = new FileInputStream(keystoreDir.resolve(keystoreFileName).toFile())) {
            ks.load(in, pw);
        }

        KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
        kmf.init(ks, pw);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);

        HttpsServer server = HttpsServer.create(new InetSocketAddress(0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(ctx));
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });
        server.start();
        return server;
    }

    private static SSLSession handshake(HttpsServer server) throws Exception {
        TrustManager trustAllCerts = new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
        };
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[] {trustAllCerts}, null);
        try (SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket("localhost", server.getAddress().getPort())) {
            socket.startHandshake();
            return socket.getSession();
        }
    }
}
