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
package org.apache.ranger.services.kms.client;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test Connection to a KMS provider must not follow a redirect issued by that provider,
 * which could otherwise send Ranger Admin to its own loopback.
 */
public class TestKMSClientRedirect {
    @Test
    public void getKeyList_doesNotFollowRedirectToLoopback() throws Exception {
        InetAddress localAddress = localNonLoopbackAddress();
        Assumptions.assumeTrue(localAddress != null, "needs a non-loopback local address to show redirect following");

        AtomicInteger redirectHits = new AtomicInteger();
        AtomicInteger loopbackHits = new AtomicInteger();
        HttpServer    loopback     = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        HttpServer    redirector   = null;

        loopback.createContext("/", exchange -> {
            loopbackHits.incrementAndGet();
            byte[] body = "[\"leaked\"]".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        loopback.start();

        try {
            int loopbackPort = loopback.getAddress().getPort();

            redirector = HttpServer.create(new InetSocketAddress(localAddress, 0), 0);
            redirector.createContext("/", exchange -> {
                redirectHits.incrementAndGet();
                exchange.getResponseHeaders().set("Location", "http://127.0.0.1:" + loopbackPort + "/kms/v1/keys/names");
                exchange.sendResponseHeaders(302, -1);
                exchange.close();
            });
            redirector.start();

            String    provider = "kms://http@" + localAddress.getHostAddress() + ":" + redirector.getAddress().getPort() + "/kms";
            KMSClient client   = new KMSClient(provider, "keyadmin", null, null, null, null, "simple");

            try {
                client.getKeyList("", null);
            } catch (Exception ignored) {
                // A 302 is not a key list. The assertion is which server was contacted.
            }

            Assertions.assertTrue(redirectHits.get() > 0, "getKeyList did not contact the configured provider " + provider);
            Assertions.assertEquals(0, loopbackHits.get(), "getKeyList followed a redirect to loopback");
        } finally {
            loopback.stop(0);
            if (redirector != null) {
                redirector.stop(0);
            }
        }
    }

    private static InetAddress localNonLoopbackAddress() throws Exception {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while (ifaces.hasMoreElements()) {
            NetworkInterface iface = ifaces.nextElement();
            if (!iface.isUp() || iface.isLoopback()) {
                continue;
            }

            Enumeration<InetAddress> addresses = iface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address && !address.isLoopbackAddress() && !address.isLinkLocalAddress()) {
                    return address;
                }
            }
        }

        return null;
    }
}
