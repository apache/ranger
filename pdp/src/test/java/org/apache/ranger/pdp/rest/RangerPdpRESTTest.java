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

package org.apache.ranger.pdp.rest;

import org.apache.ranger.authz.api.RangerAuthorizer;
import org.apache.ranger.authz.api.RangerAuthzApiErrorCode;
import org.apache.ranger.authz.api.RangerAuthzException;
import org.apache.ranger.authz.model.RangerAccessContext;
import org.apache.ranger.authz.model.RangerAuthzRequest;
import org.apache.ranger.authz.model.RangerAuthzResult;
import org.apache.ranger.authz.model.RangerMultiAuthzRequest;
import org.apache.ranger.authz.model.RangerMultiAuthzResult;
import org.apache.ranger.authz.model.RangerResourcePermissions;
import org.apache.ranger.authz.model.RangerResourcePermissionsRequest;
import org.apache.ranger.pdp.RangerPdpStats;
import org.apache.ranger.pdp.config.RangerPdpConfig;
import org.apache.ranger.pdp.config.RangerPdpConstants;
import org.junit.jupiter.api.Test;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.ranger.pdp.config.RangerPdpConstants.PROP_PDP_SERVICE_PREFIX;
import static org.apache.ranger.pdp.config.RangerPdpConstants.PROP_SUFFIX_DELEGATION_USERS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RangerPdpRESTTest {
    @Test
    public void testAuthorizeReturnsUnauthorizedWhenCallerMissing() throws Exception {
        RangerPdpStats     stats      = new RangerPdpStats();
        TestAuthorizer     authorizer = new TestAuthorizer();
        RangerPdpREST      rest       = createRest(authorizer, stats);
        RangerAuthzRequest request    = new RangerAuthzRequest(null, null, new RangerAccessContext("hive", "svc1"));
        Response           response   = rest.authorize(request, httpRequest(null, stats));

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthFailures());
    }

    @Test
    public void testAuthorizeReturnsForbiddenWhenCallerNotAllowed() throws Exception {
        RangerPdpStats     stats      = new RangerPdpStats();
        TestAuthorizer     authorizer = new TestAuthorizer();
        RangerPdpREST      rest       = createRest(authorizer, stats);
        RangerAuthzRequest request    = new RangerAuthzRequest(null, null, new RangerAccessContext("hive", "svc1"));
        Response           response   = rest.authorize(request, httpRequest("bob", stats));

        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthFailures());
    }

    @Test
    public void testAuthorizeReturnsOkAndRecordsSuccess() throws Exception {
        RangerPdpStats     stats      = new RangerPdpStats();
        TestAuthorizer     authorizer = new TestAuthorizer(new RangerAuthzResult("req-1", RangerAuthzResult.AccessDecision.ALLOW));
        RangerPdpREST      rest       = createRest(authorizer, stats);
        RangerAuthzRequest request    = new RangerAuthzRequest("req-1", null, null, new RangerAccessContext("hive", "svc1"));
        Response           response   = rest.authorize(request, httpRequest("alice", stats));

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalRequests());
        assertEquals(1L, stats.getTotalAuthzSuccess());
    }

    @Test
    public void testAuthorizeReturnsBadRequestOnAuthzException() throws Exception {
        RangerPdpStats     stats      = new RangerPdpStats();
        TestAuthorizer     authorizer = new TestAuthorizer(new RangerAuthzException(RangerAuthzApiErrorCode.INVALID_REQUEST_ACCESS_CONTEXT_MISSING));
        RangerPdpREST      rest       = createRest(authorizer, stats);
        RangerAuthzRequest request    = new RangerAuthzRequest(null, null, new RangerAccessContext("hive", "svc1"));
        Response           response   = rest.authorize(request, httpRequest("alice", stats));

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthzBadRequest());
    }

    @Test
    public void testAuthorizeMultiReturnsUnauthorizedWhenCallerMissing() throws Exception {
        RangerPdpStats          stats      = new RangerPdpStats();
        TestAuthorizer          authorizer = new TestAuthorizer();
        RangerPdpREST           rest       = createRest(authorizer, stats);
        RangerMultiAuthzRequest request    = new RangerMultiAuthzRequest(null, null, new RangerAccessContext("hive", "svc1"));
        Response                response   = rest.authorizeMulti(request, httpRequest(null, stats));

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthFailures());
    }

    @Test
    public void testAuthorizeMultiReturnsForbiddenWhenCallerNotAllowed() throws Exception {
        RangerPdpStats          stats      = new RangerPdpStats();
        TestAuthorizer          authorizer = new TestAuthorizer();
        RangerPdpREST           rest       = createRest(authorizer, stats);
        RangerMultiAuthzRequest request    = new RangerMultiAuthzRequest(null, null, new RangerAccessContext("hive", "svc1"));
        Response                response   = rest.authorizeMulti(request, httpRequest("bob", stats));

        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthFailures());
    }

    @Test
    public void testAuthorizeMultiReturnsOkAndRecordsSuccess() throws Exception {
        RangerPdpStats          stats      = new RangerPdpStats();
        TestAuthorizer          authorizer = new TestAuthorizer(new RangerMultiAuthzResult("req-1", RangerAuthzResult.AccessDecision.ALLOW));
        RangerPdpREST           rest       = createRest(authorizer, stats);
        RangerMultiAuthzRequest request    = new RangerMultiAuthzRequest("req-1", null, null, new RangerAccessContext("hive", "svc1"));
        Response                response   = rest.authorizeMulti(request, httpRequest("alice", stats));

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalRequests());
        assertEquals(1L, stats.getTotalAuthzSuccess());
    }

    @Test
    public void testAuthorizeMultiReturnsBadRequestOnAuthzException() throws Exception {
        RangerPdpStats          stats      = new RangerPdpStats();
        TestAuthorizer          authorizer = new TestAuthorizer(new RangerAuthzException(RangerAuthzApiErrorCode.INVALID_REQUEST_ACCESS_CONTEXT_MISSING));
        RangerPdpREST           rest       = createRest(authorizer, stats);
        RangerMultiAuthzRequest request    = new RangerMultiAuthzRequest(null, null, new RangerAccessContext("hive", "svc1"));
        Response                response   = rest.authorizeMulti(request, httpRequest("alice", stats));

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthzBadRequest());
    }

    @Test
    public void testGetResourcePermissionsReturnsUnauthorizedWhenCallerMissing() throws Exception {
        RangerPdpStats                   stats      = new RangerPdpStats();
        TestAuthorizer                   authorizer = new TestAuthorizer();
        RangerPdpREST                    rest       = createRest(authorizer, stats);
        RangerResourcePermissionsRequest request    = new RangerResourcePermissionsRequest(null, new RangerAccessContext("hive", "svc1"));
        Response                         response   = rest.getResourcePermissions(request, httpRequest(null, stats));

        assertEquals(Response.Status.UNAUTHORIZED.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthFailures());
    }

    @Test
    public void testGetResourcePermissionsReturnsForbiddenWhenCallerNotAllowed() throws Exception {
        RangerPdpStats                   stats      = new RangerPdpStats();
        TestAuthorizer                   authorizer = new TestAuthorizer();
        RangerPdpREST                    rest       = createRest(authorizer, stats);
        RangerResourcePermissionsRequest request    = new RangerResourcePermissionsRequest(null, new RangerAccessContext("hive", "svc1"));
        Response                         response   = rest.getResourcePermissions(request, httpRequest("bob", stats));

        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthFailures());
    }

    @Test
    public void testGetResourcePermissionsReturnsOkAndRecordsSuccess() throws Exception {
        RangerPdpStats                   stats      = new RangerPdpStats();
        TestAuthorizer                   authorizer = new TestAuthorizer(new RangerResourcePermissions());
        RangerPdpREST                    rest       = createRest(authorizer, stats);
        RangerResourcePermissionsRequest request    = new RangerResourcePermissionsRequest("req-1", null, new RangerAccessContext("hive", "svc1"));
        Response                         response   = rest.getResourcePermissions(request, httpRequest("alice", stats));

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalRequests());
        assertEquals(1L, stats.getTotalAuthzSuccess());
    }

    @Test
    public void testGetResourcePermissionsReturnsBadRequestOnAuthzException() throws Exception {
        RangerPdpStats                   stats      = new RangerPdpStats();
        TestAuthorizer                   authorizer = new TestAuthorizer(new RangerAuthzException(RangerAuthzApiErrorCode.INVALID_REQUEST_ACCESS_CONTEXT_MISSING));
        RangerPdpREST                    rest       = createRest(authorizer, stats);
        RangerResourcePermissionsRequest request    = new RangerResourcePermissionsRequest("req-1", null, new RangerAccessContext("hive", "svc1"));
        Response                         response   = rest.getResourcePermissions(request, httpRequest("alice", stats));

        assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
        assertEquals(1L, stats.getTotalAuthzBadRequest());
    }

    private static RangerPdpREST createRest(TestAuthorizer authorizer, RangerPdpStats stats) throws Exception {
        RangerPdpREST rest = new RangerPdpREST();

        setField(rest, "authorizer", authorizer);
        setField(rest, "config", new TestConfig(defaultConfig()));
        setField(rest, "servletContext", servletContextWithStats(stats));
        rest.initialize();

        return rest;
    }

    private static HttpServletRequest httpRequest(String user, RangerPdpStats stats) {
        Map<String, Object> attrs = new HashMap<>();

        if (user != null) {
            attrs.put(RangerPdpConstants.ATTR_AUTHENTICATED_USER, user);
        }

        ServletContext servletContext = servletContextWithStats(stats);

        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getAttribute":
                            return attrs.get(args[0]);
                        case "setAttribute":
                            attrs.put((String) args[0], args[1]);
                            return null;
                        case "getServletContext":
                            return servletContext;
                        default:
                            return null;
                    }
                });
    }

    private static ServletContext servletContextWithStats(RangerPdpStats stats) {
        Map<String, Object> attrs = new HashMap<>();

        attrs.put(RangerPdpConstants.SERVLET_CTX_ATTR_RUNTIME_STATE, stats);

        return (ServletContext) Proxy.newProxyInstance(
                ServletContext.class.getClassLoader(),
                new Class<?>[] {ServletContext.class},
                (proxy, method, args) -> {
                    if ("getAttribute".equals(method.getName())) {
                        return attrs.get(args[0]);
                    } else if ("setAttribute".equals(method.getName())) {
                        attrs.put((String) args[0], args[1]);
                        return null;
                    }
                    return null;
                });
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);

        field.setAccessible(true);
        field.set(target, value);
    }

    private static Properties defaultConfig() {
        Properties ret = new Properties();

        ret.setProperty(PROP_PDP_SERVICE_PREFIX +  "svc1" + PROP_SUFFIX_DELEGATION_USERS, "alice");

        return ret;
    }

    private static class TestConfig extends RangerPdpConfig {
        private final Properties props;

        TestConfig(Properties props) {
            this.props = props;
        }

        @Override
        public Properties getAuthzProperties() {
            return new Properties(props);
        }
    }

    private static class TestAuthorizer extends RangerAuthorizer {
        private final RangerAuthzResult         authzResult;
        private final RangerMultiAuthzResult    multiAuthzResult;
        private final RangerResourcePermissions permissions;
        private final RangerAuthzException      authzException;

        TestAuthorizer() {
            this(null, null, null, null);
        }

        TestAuthorizer(RangerAuthzResult result) {
            this(result, null, null, null);
        }

        TestAuthorizer(RangerMultiAuthzResult result) {
            this(null, result, null, null);
        }

        TestAuthorizer(RangerResourcePermissions permissions) {
            this(null, null, permissions, null);
        }

        TestAuthorizer(RangerAuthzException excp) {
            this(null, null, null, excp);
        }

        TestAuthorizer(RangerAuthzResult result, RangerMultiAuthzResult multiResult, RangerResourcePermissions permissions, RangerAuthzException excp) {
            super(new Properties());

            this.authzResult      = result;
            this.multiAuthzResult = multiResult;
            this.permissions      = permissions;
            this.authzException   = excp;
        }

        @Override
        public void init() {
        }

        @Override
        public void close() {
        }

        @Override
        public RangerAuthzResult authorize(RangerAuthzRequest request) throws RangerAuthzException {
            if (authzException != null) {
                throw authzException;
            }

            return authzResult != null ? authzResult : new RangerAuthzResult();
        }

        @Override
        public RangerMultiAuthzResult authorize(RangerMultiAuthzRequest request) throws RangerAuthzException {
            if (authzException != null) {
                throw authzException;
            }

            return multiAuthzResult != null ? multiAuthzResult : new RangerMultiAuthzResult();
        }

        @Override
        public RangerResourcePermissions getResourcePermissions(RangerResourcePermissionsRequest request) throws RangerAuthzException {
            if (authzException != null) {
                throw authzException;
            }

            return permissions != null ? permissions : new RangerResourcePermissions();
        }
    }
}
