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

package org.apache.ranger.plugin.util;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestServiceAccountUsersProvider {
    @Test
    public void testProviderLoadsDefensiveSnapshot() throws Exception {
        Set<String>              users    = new HashSet<>(Collections.singleton("alice"));
        Map<String, Set<String>> source   = new HashMap<>();
        ServiceAccountUsersProvider provider = new ServiceAccountUsersProvider(false, (Configuration) null);
        Method                      setUsers = ServiceAccountUsersProvider.class.getDeclaredMethod("setServiceAccountUsers", Map.class);

        source.put("svc", users);

        setUsers.setAccessible(true);
        setUsers.invoke(provider, source);
        users.add("bob");

        assertTrue(provider.isServiceAccountUser("svc", "alice"));
        assertFalse(provider.isServiceAccountUser("svc", "bob"));

        setUsers.invoke(provider, Collections.emptyMap());
        assertFalse(provider.isServiceAccountUser("svc", "alice"));
    }

    @Test
    public void testRefresherReplacesSnapshotAndRetainsItOnFailure() {
        AtomicInteger                           calls    = new AtomicInteger();
        AtomicReference<Map<String, Set<String>>> received = new AtomicReference<>();
        ServiceAccountUsersRefresher refresher = new ServiceAccountUsersRefresher(() -> {
            if (calls.getAndIncrement() == 0) {
                return Collections.singletonMap("svc", Collections.singleton("alice"));
            }

            throw new Exception("expected");
        }, received::set, 60_000L);

        refresher.refresh();
        Map<String, Set<String>> snapshot = received.get();
        refresher.refresh();

        assertSame(snapshot, received.get());
    }

    @Test
    public void testDisabledProviderIsNoOp() {
        ServiceAccountUsersProvider provider = new ServiceAccountUsersProvider(false, (Configuration) null);

        assertDoesNotThrow(provider::start);
        assertFalse(provider.isServiceAccountUser("svc", "alice"));
        assertDoesNotThrow(provider::stop);
    }
}
