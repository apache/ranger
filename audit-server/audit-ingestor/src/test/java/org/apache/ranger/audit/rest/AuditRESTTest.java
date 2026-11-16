/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.audit.rest;

import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.producer.AuditDestinationMgr;
import org.apache.ranger.plugin.util.ServiceAccountUsersProvider;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AuditRESTTest {
    private static final String SERVICE = "dynamic-service";
    private static final String USER    = "dynamic-user";

    @Test
    void testServiceAccountUserCanPostAudits() throws Exception {
        TestDestination      destination = new TestDestination();
        List<AuthzAuditEvent> audits     = Collections.singletonList(new AuthzAuditEvent());
        AuditREST             rest       = createRest(destination, provider(true));

        Response response = rest.logAccessAudit(SERVICE, "app", audits, request(USER));

        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertTrue(destination.called);
    }

    @Test
    void testUnknownUserCannotPostAudits() throws Exception {
        TestDestination       destination = new TestDestination();
        List<AuthzAuditEvent> audits      = Collections.singletonList(new AuthzAuditEvent());
        AuditREST             rest        = createRest(destination, provider(false));

        Response response = rest.logAccessAudit(SERVICE, "app", audits, request(USER));

        assertEquals(Response.Status.FORBIDDEN.getStatusCode(), response.getStatus());
        assertFalse(destination.called);
    }

    private static AuditREST createRest(AuditDestinationMgr destination, ServiceAccountUsersProvider provider) {
        AuditREST ret = new AuditREST();

        ret.auditDestinationMgr        = destination;
        ret.serviceAccountUsersProvider = provider;

        return ret;
    }

    private static HttpServletRequest request(String user) {
        Principal principal = () -> user;

        return (HttpServletRequest) Proxy.newProxyInstance(
                HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class},
                (proxy, method, args) -> "getUserPrincipal".equals(method.getName()) ? principal : null);
    }

    private static ServiceAccountUsersProvider provider(boolean allowed) throws Exception {
        ServiceAccountUsersProvider provider = new ServiceAccountUsersProvider(false, new Properties());
        Map<String, Set<String>>     users    = allowed
                ? Collections.singletonMap(SERVICE, Collections.singleton(USER))
                : Collections.emptyMap();
        Field field = ServiceAccountUsersProvider.class.getDeclaredField("serviceAccountUsers");

        field.setAccessible(true);
        field.set(provider, users);

        return provider;
    }

    private static class TestDestination extends AuditDestinationMgr {
        private boolean called;

        @Override
        public boolean logBatch(Collection<? extends AuditEventBase> events, String appId) {
            called = true;

            return true;
        }
    }
}
