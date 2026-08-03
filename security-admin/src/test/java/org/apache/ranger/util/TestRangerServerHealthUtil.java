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

package org.apache.ranger.util;

import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.entity.XXAuthSession;
import org.apache.ranger.plugin.model.RangerServerHealth;
import org.apache.ranger.security.web.filter.RangerAuthenticationToken;
import org.apache.ranger.service.RangerServiceDefService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.DOWN;
import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.INITIALIZATION_FAILURE;
import static org.mockito.ArgumentMatchers.isNull;

@ExtendWith(MockitoExtension.class)
public class TestRangerServerHealthUtil {
    @InjectMocks
    RangerServerHealthUtil  rangerServerHealthUtil = new RangerServerHealthUtil();
    @Mock
    RangerBizUtil           bizUtil;
    @Mock
    RangerServiceDefService serviceDefService;
    @Mock
    RESTErrorUtil           restErrorUtil;

    @Test
    public void testGetRangerServerHealth() {
        RangerServerHealth rangerServerHealth = rangerServerHealthUtil.getRangerServerHealth("21.3c");
        Assertions.assertEquals(DOWN, rangerServerHealth.getStatus(), "RangerHealth.down()");
        Assertions.assertEquals(1, rangerServerHealth.getDetails().size(), "RangerHealth.getDetails()");
        Assertions.assertEquals(1, ((Map<?, ?>) rangerServerHealth.getDetails().get("components")).size(), "RangerHealth.getDetails('component')");
    }

    @Test
    public void testServiceUpWithAvailableServiceDefNames() {
        RangerServerHealth health = rangerServerHealthUtil.serviceUpWithAvailableServiceDefs(Arrays.asList("hdfs", "hive"));
        Assertions.assertEquals(RangerServerHealth.RangerServerStatus.UP, health.getStatus());
        Assertions.assertEquals(Arrays.asList("hdfs", "hive"),
                ((Map<?, ?>) health.getDetails().get("components")).get("service-defs"));
    }

    @Test
    public void testServiceInitFailure() {
        RangerServerHealth health = rangerServerHealthUtil.serviceInitFailure();
        Assertions.assertEquals(INITIALIZATION_FAILURE, health.getStatus());
    }

    @Test
    public void testGetServiceDefNames_AllowedForHealthCheckUser() {
        List<String> expected = Arrays.asList("hdfs", "hive");
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(RangerConstants.HEALTH_CHECK_USERNAME);
        Mockito.when(bizUtil.isHealthCheckUser(RangerConstants.HEALTH_CHECK_USERNAME)).thenReturn(true);
        Mockito.when(serviceDefService.getAllServiceDefNames()).thenReturn(expected);

        List<String> ret = rangerServerHealthUtil.getServiceDefNames();

        Assertions.assertEquals(expected, ret);
        Mockito.verify(serviceDefService).getAllServiceDefNames();
    }

    @Test
    public void testGetServiceDefNames_ForbiddenForOtherUser() {
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn("admin");
        Mockito.when(bizUtil.isHealthCheckUser("admin")).thenReturn(false);
        Mockito.when(restErrorUtil.createRESTException(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
                Mockito.eq("Only the healthcheck user may query service-def names via this path."),
                Mockito.eq(true))).thenReturn(new WebApplicationException(HttpServletResponse.SC_FORBIDDEN));

        Assertions.assertThrows(WebApplicationException.class, () -> rangerServerHealthUtil.getServiceDefNames());

        Mockito.verify(restErrorUtil).createRESTException(HttpServletResponse.SC_FORBIDDEN,
                "Only the healthcheck user may query service-def names via this path.", true);
        Mockito.verify(serviceDefService, Mockito.never()).getAllServiceDefNames();
    }

    @Test
    public void testGetServiceDefNames_ForbiddenWhenUnauthenticated() {
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(null);
        Mockito.when(bizUtil.isHealthCheckUser(isNull())).thenReturn(false);
        SecurityContextHolder.clearContext();
        Mockito.when(restErrorUtil.createRESTException(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
                Mockito.eq("Only the healthcheck user may query service-def names via this path."),
                Mockito.eq(true))).thenReturn(new WebApplicationException(HttpServletResponse.SC_FORBIDDEN));

        Assertions.assertThrows(WebApplicationException.class, () -> rangerServerHealthUtil.getServiceDefNames());

        Mockito.verify(serviceDefService, Mockito.never()).getAllServiceDefNames();
    }

    @Test
    public void testGetServiceDefNames_AllowedForHealthCheckUserViaSpringSecurity() {
        List<String> expected = Arrays.asList("hdfs", "hive");

        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(null);
        Mockito.when(bizUtil.isHealthCheckUser(RangerConstants.HEALTH_CHECK_USERNAME)).thenReturn(true);
        SecurityContextHolder.getContext().setAuthentication(
                trustedProxyAuth(RangerConstants.HEALTH_CHECK_USERNAME));
        Mockito.when(serviceDefService.getAllServiceDefNames()).thenReturn(expected);

        try {
            List<String> ret = rangerServerHealthUtil.getServiceDefNames();
            Assertions.assertEquals(expected, ret);
            Mockito.verify(serviceDefService).getAllServiceDefNames();
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    public void testGetServiceDefNames_ForbiddenForOtherUserViaSpringSecurity() {
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(null);
        Mockito.when(bizUtil.isHealthCheckUser("admin")).thenReturn(false);
        SecurityContextHolder.getContext().setAuthentication(trustedProxyAuth("admin"));
        Mockito.when(restErrorUtil.createRESTException(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
                Mockito.eq("Only the healthcheck user may query service-def names via this path."),
                Mockito.eq(true))).thenReturn(new WebApplicationException(HttpServletResponse.SC_FORBIDDEN));

        try {
            Assertions.assertThrows(WebApplicationException.class, () -> rangerServerHealthUtil.getServiceDefNames());
            Mockito.verify(serviceDefService, Mockito.never()).getAllServiceDefNames();
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    public void testGetServiceDefNames_ForbiddenForNonTrustedProxyAuth() {
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(null);
        Mockito.when(bizUtil.isHealthCheckUser(isNull())).thenReturn(false);
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(RangerConstants.HEALTH_CHECK_USERNAME, "", Collections.emptyList()));
        Mockito.when(restErrorUtil.createRESTException(Mockito.eq(HttpServletResponse.SC_FORBIDDEN),
                Mockito.eq("Only the healthcheck user may query service-def names via this path."),
                Mockito.eq(true))).thenReturn(new WebApplicationException(HttpServletResponse.SC_FORBIDDEN));

        try {
            Assertions.assertThrows(WebApplicationException.class, () -> rangerServerHealthUtil.getServiceDefNames());
            Mockito.verify(serviceDefService, Mockito.never()).getAllServiceDefNames();
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    private RangerAuthenticationToken trustedProxyAuth(String username) {
        return new RangerAuthenticationToken(new User(username, "", Collections.emptyList()),
                Collections.emptyList(), XXAuthSession.AUTH_TYPE_TRUSTED_PROXY);
    }
}
