/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.pdp;

import org.apache.ranger.pdp.config.RangerPdpConfig;
import org.apache.ranger.pdp.config.RangerPdpConstants;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RangerPdpStatusServletTest {
    @AfterEach
    public void clearOverrides() {
        System.clearProperty(RangerPdpConstants.PROP_AUTHZ_POLICY_CACHE_DIR);
    }

    @Test
    public void testMetricsEndpointRendersCounters() throws Exception {
        RangerPdpStats stats = new RangerPdpStats();

        stats.recordRequestSuccess(5_000_000L);
        stats.recordRequestError(5_000_000L);
        stats.recordAuthFailure(5_000_000L);

        RangerPdpStatusServlet servlet = new RangerPdpStatusServlet(stats, new RangerPdpConfig(), RangerPdpStatusServlet.Mode.METRICS);
        HttpServletRequest     req     = proxy(HttpServletRequest.class, (proxy, method, args) -> null);
        ResponseCapture        capture = new ResponseCapture();
        HttpServletResponse    resp    = capture.responseProxy();

        servlet.doGet(req, resp);

        assertEquals(HttpServletResponse.SC_OK, capture.status);
        assertTrue(capture.body.toString().contains("ranger_pdp_requests_total 3"));
        assertTrue(capture.body.toString().contains("ranger_pdp_auth_failures_total 1"));
    }

    @Test
    public void testLoadedServicesCount() throws Exception {
        RangerPdpStatusServlet servlet = new RangerPdpStatusServlet(new RangerPdpStats(), new RangerPdpConfig(), RangerPdpStatusServlet.Mode.READY);
        HttpServletRequest     req     = proxy(HttpServletRequest.class, (proxy, method, args) -> null);
        Method                 method  = RangerPdpStatusServlet.class.getDeclaredMethod("getLoadedServicesCount", HttpServletRequest.class);

        method.setAccessible(true);

        Integer ageMs = (Integer) method.invoke(servlet, req);

        assertTrue(ageMs >= 0L);
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> iface, InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] {iface}, handler);
    }

    private static final class ResponseCapture {
        private       int                 status;
        private final StringWriter        body    = new StringWriter();
        private final Map<String, String> headers = new HashMap<>();

        private HttpServletResponse responseProxy() {
            return proxy(HttpServletResponse.class, (proxy, method, args) -> {
                switch (method.getName()) {
                    case "setStatus":
                        status = (Integer) args[0];
                        return null;
                    case "setContentType":
                        headers.put("Content-Type", (String) args[0]);
                        return null;
                    case "getWriter":
                        return new PrintWriter(body, true);
                    case "setHeader":
                        headers.put((String) args[0], (String) args[1]);
                        return null;
                    default:
                        return null;
                }
            });
        }
    }
}
