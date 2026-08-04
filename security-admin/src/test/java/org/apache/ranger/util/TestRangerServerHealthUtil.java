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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.DOWN;
import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.INITIALIZATION_FAILURE;

@ExtendWith(MockitoExtension.class)
public class TestRangerServerHealthUtil {
    @InjectMocks
    RangerServerHealthUtil  rangerServerHealthUtil = new RangerServerHealthUtil();
    @Mock
    RangerBizUtil           bizUtil;
    @Mock
    RangerServiceDefService serviceDefService;

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
    public void testGetServiceDefNames() {
        List<String> expected = Arrays.asList("hdfs", "hive");
        Mockito.when(serviceDefService.getAllServiceDefNames()).thenReturn(expected);

        List<String> ret = rangerServerHealthUtil.getServiceDefNames();

        Assertions.assertEquals(expected, ret);
        Mockito.verify(serviceDefService).getAllServiceDefNames();
    }

    @Test
    public void testResolveAuthenticatedLoginId_FromUserSession() {
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(RangerBizUtil.HEALTHCHECK_USERNAME);

        Assertions.assertEquals(RangerBizUtil.HEALTHCHECK_USERNAME, rangerServerHealthUtil.resolveAuthenticatedLoginId());
    }

    @Test
    public void testResolveAuthenticatedLoginId_FromTrustedProxyToken() {
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(null);
        SecurityContextHolder.getContext().setAuthentication(
                trustedProxyAuth(RangerBizUtil.HEALTHCHECK_USERNAME));

        try {
            Assertions.assertEquals(RangerBizUtil.HEALTHCHECK_USERNAME, rangerServerHealthUtil.resolveAuthenticatedLoginId());
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    @Test
    public void testResolveAuthenticatedLoginId_Unauthenticated() {
        Mockito.when(bizUtil.getCurrentUserLoginId()).thenReturn(null);
        SecurityContextHolder.getContext().setAuthentication(
                new UsernamePasswordAuthenticationToken(RangerBizUtil.HEALTHCHECK_USERNAME, "", Collections.emptyList()));

        try {
            Assertions.assertNull(rangerServerHealthUtil.resolveAuthenticatedLoginId());
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

    private RangerAuthenticationToken trustedProxyAuth(String username) {
        return new RangerAuthenticationToken(new User(username, "", Collections.emptyList()),
                Collections.emptyList(), XXAuthSession.AUTH_TYPE_TRUSTED_PROXY);
    }
}
