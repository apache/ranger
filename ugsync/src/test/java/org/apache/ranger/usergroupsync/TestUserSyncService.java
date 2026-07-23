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

package org.apache.ranger.usergroupsync;

import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.apache.ranger.unixusersync.ha.UserSyncHAInitializerImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestUserSyncService {
    @Test
    public void userSyncServiceClassIsLoadable() {
        assertNotNull(UserSyncService.class);
    }

    @Test
    public void run_stopsHaWhenSyncThreadEnds() throws Exception {
        UserSyncService           svc = new UserSyncService();
        UserSyncHAInitializerImpl ha  = mock(UserSyncHAInitializerImpl.class);

        setField(svc, "userSyncHAInitializerImpl", ha);

        try (MockedStatic<UserGroupSyncConfig> ugscStatic = mockStatic(UserGroupSyncConfig.class)) {
            UserGroupSyncConfig ugsc = mock(UserGroupSyncConfig.class);
            ugscStatic.when(UserGroupSyncConfig::getInstance).thenReturn(ugsc);
            when(ugsc.isUserSyncMetricsEnabled()).thenReturn(false);

            try (MockedConstruction<UserGroupSync> syncConstruction = mockConstruction(UserGroupSync.class)) {
                svc.run();

                verify(ha).stop();
                assertEquals(1, syncConstruction.constructed().size());
            }
        }
    }

    @Test
    public void startUserGroupSyncProcess_metricsEnabledAndDisabled() throws Exception {
        UserSyncService svc    = new UserSyncService();
        Method          method = UserSyncService.class.getDeclaredMethod("startUserGroupSyncProcess");
        method.setAccessible(true);

        try (MockedStatic<UserGroupSyncConfig> ugscStatic = mockStatic(UserGroupSyncConfig.class)) {
            UserGroupSyncConfig ugsc = mock(UserGroupSyncConfig.class);
            ugscStatic.when(UserGroupSyncConfig::getInstance).thenReturn(ugsc);

            when(ugsc.isUserSyncMetricsEnabled()).thenReturn(true);
            try (MockedConstruction<Thread> threadConstruction = mockConstruction(Thread.class, (mockThread, ctx) -> {
                doNothing().when(mockThread).setName(any());
                doNothing().when(mockThread).setDaemon(false);
                doNothing().when(mockThread).start();
            })) {
                method.invoke(svc);
                assertEquals(2, threadConstruction.constructed().size());
            }

            when(ugsc.isUserSyncMetricsEnabled()).thenReturn(false);
            try (MockedConstruction<Thread> threadConstruction = mockConstruction(Thread.class, (mockThread, ctx) -> {
                doNothing().when(mockThread).setName(any());
                doNothing().when(mockThread).setDaemon(false);
                doNothing().when(mockThread).start();
            })) {
                method.invoke(svc);
                assertEquals(1, threadConstruction.constructed().size());
            }
        }
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = UserSyncService.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
