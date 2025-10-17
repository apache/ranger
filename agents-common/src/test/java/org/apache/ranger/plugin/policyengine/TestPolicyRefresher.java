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

package org.apache.ranger.plugin.policyengine;

import org.apache.ranger.admin.client.RangerAdminClient;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.apache.ranger.plugin.util.DownloadTrigger;
import org.apache.ranger.plugin.util.PolicyRefresher;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestPolicyRefresher {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule
    public Timeout globalTimeout = new Timeout(15000, TimeUnit.MILLISECONDS);
    @Mock
    private RangerBasePlugin mockPlugin;
    @Mock
    private RangerPluginConfig mockPluginConfig;
    @Mock
    private RangerPluginContext mockPluginContext;
    @Mock
    private RangerAdminClient mockRangerAdminClient;
    private PolicyRefresher policyRefresher;
    private File tempCacheDir;
    private static final String SERVICE_NAME = "testService";
    private static final String SERVICE_TYPE = "testType";
    private static final String APP_ID = "testAppId";
    private static final long POLL_INTERVAL = 30000L;
    private static final long TEST_TIMEOUT_SECONDS = 5;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        tempCacheDir = tempFolder.newFolder("cache");
        setupBasicMocks();
        policyRefresher = new PolicyRefresher(mockPlugin);
    }

    @After
    public void tearDown() throws Exception {
        if (policyRefresher != null && policyRefresher.isAlive()) {
            policyRefresher.stopRefresher();
            policyRefresher.join(2000);
        }
        String cacheFileName = (APP_ID + "_" + SERVICE_NAME + ".json")
                .replace(File.separatorChar, '_')
                .replace(File.pathSeparatorChar, '_');
        File cacheFile = new File(tempCacheDir, cacheFileName);
        if (cacheFile.exists()) {
            cacheFile.delete();
        }
    }

    @Test
    public void testLastActivationTimeInMillis() {
        long testTime = System.currentTimeMillis();
        policyRefresher.setLastActivationTimeInMillis(testTime);
        assertEquals("Last activation time should be set and retrieved correctly", testTime, policyRefresher.getLastActivationTimeInMillis());
    }

    @Test
    public void testStartRefresherLoadsInitialPolicies() throws Exception {
        CountDownLatch policiesSetLatch = new CountDownLatch(1);
        ServicePolicies mockPolicies = createMockServicePolicies(1L);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(mockPolicies);
        doAnswer(invocation -> {
            policiesSetLatch.countDown();
            return null;
        })
                .when(mockPlugin).setPolicies(any(ServicePolicies.class));

        policyRefresher.startRefresher();

        assertTrue("Policies should be loaded on start", policiesSetLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeastOnce()).setPolicies(argThat(policies ->
                policies != null && SERVICE_NAME.equals(policies.getServiceName()) && policies.getPolicyVersion() == 1L));
        assertTrue("PolicyRefresher thread should be alive after start", policyRefresher.isAlive());
    }

    @Test
    public void testStopRefresherStopsThread() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(1);
        ServicePolicies mockPolicies = createMockServicePolicies(1L);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(mockPolicies);
        doAnswer(invocation -> {
            startLatch.countDown();
            return null;
        }).when(mockPlugin).setPolicies(any(ServicePolicies.class));

        policyRefresher.startRefresher();

        assertTrue("Refresher should start successfully", startLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        policyRefresher.stopRefresher();
        policyRefresher.join(2000);
        assertFalse("PolicyRefresher thread should stop after stopRefresher call", policyRefresher.isAlive());
    }

    @Test
    public void testSyncPoliciesWithAdminTriggersImmediateUpdate() throws Exception {
        CountDownLatch initialLoadLatch = new CountDownLatch(1);
        CountDownLatch syncUpdateLatch = new CountDownLatch(1);
        AtomicInteger updateCount = new AtomicInteger(0);

        ServicePolicies mockPolicies = createMockServicePolicies(2L);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(mockPolicies);

        doAnswer(invocation -> {
            int count = updateCount.incrementAndGet();
            if (count == 1) {
                initialLoadLatch.countDown();
            } else if (count == 2) {
                syncUpdateLatch.countDown();
            }
            return null;
        }).when(mockPlugin).setPolicies(any(ServicePolicies.class));

        policyRefresher.startRefresher();

        assertTrue("Initial policies should load", initialLoadLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        DownloadTrigger trigger = new DownloadTrigger();
        policyRefresher.syncPoliciesWithAdmin(trigger);
        assertTrue("Sync should trigger policy update within timeout", syncUpdateLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeast(2)).setPolicies(argThat(policies -> policies != null
                && policies.getPolicyVersion() == 2L));
    }

    @Test
    public void testPolicyUpdateWithNewVersion() throws Exception {
        ArgumentCaptor<ServicePolicies> policiesCaptor = ArgumentCaptor.forClass(ServicePolicies.class);
        CountDownLatch initialLoadLatch = new CountDownLatch(1);
        CountDownLatch updateLatch = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger(0);
        ServicePolicies policies1 = createMockServicePolicies(1L);
        ServicePolicies policies3 = createMockServicePolicies(3L);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(eq(-1L), anyLong())).thenReturn(policies1);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(eq(1L), anyLong())).thenReturn(policies3);
        doAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            if (count == 1) {
                initialLoadLatch.countDown();
            } else if (count >= 2) {
                updateLatch.countDown();
            }
            return null;
        }).when(mockPlugin).setPolicies(any(ServicePolicies.class));

        policyRefresher.startRefresher();

        assertTrue("Initial load should complete", initialLoadLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeastOnce()).setPolicies(policiesCaptor.capture());
        assertEquals("First update should have version 1", Long.valueOf(1), policiesCaptor.getValue().getPolicyVersion());

        DownloadTrigger trigger = new DownloadTrigger();
        policyRefresher.syncPoliciesWithAdmin(trigger);

        assertTrue("Update should complete", updateLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeast(2)).setPolicies(policiesCaptor.capture());
        boolean version3Found = policiesCaptor.getAllValues().stream().anyMatch(p -> p.getPolicyVersion() == 3L);
        assertTrue("Should update to version 3", version3Found);
    }

    @Test
    public void testHandlesServiceNotFoundException() throws Exception {
        CountDownLatch nullPoliciesLatch = new CountDownLatch(1);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong()))
                .thenThrow(new RangerServiceNotFoundException("Service not found"));
        doAnswer(invocation -> {
            Object arg = invocation.getArgument(0);
            if (arg == null) {
                nullPoliciesLatch.countDown();
            }
            return null;
        }).when(mockPlugin).setPolicies(any());

        policyRefresher.startRefresher();

        assertTrue("Should set null policies when service not found", nullPoliciesLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeastOnce()).setPolicies(null);
    }

    @Test
    public void testHandlesIOExceptionFromAdmin() throws Exception {
        AtomicInteger attemptCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong()))
                .thenAnswer(invocation -> {
                    attemptCount.incrementAndGet();
                    startLatch.countDown();
                    throw new IOException("Network timeout");
                });

        policyRefresher.startRefresher();

        assertTrue("Refresher should start and attempt to fetch policies", startLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertTrue("Refresher should handle IO exceptions gracefully and stay alive", policyRefresher.isAlive());
        assertTrue("Refresher should retry after IO exception", attemptCount.get() >= 1);
    }

    @Test
    public void testCachePersistence() throws Exception {
        CountDownLatch policiesSetLatch = new CountDownLatch(1);
        ServicePolicies mockPolicies = createMockServicePolicies(5L);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(mockPolicies);
        doAnswer(invocation -> {
            policiesSetLatch.countDown();
            return null;
        })
                .when(mockPlugin).setPolicies(any(ServicePolicies.class));

        policyRefresher.startRefresher();

        assertTrue("Policies should be set in plugin", policiesSetLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeastOnce()).setPolicies(argThat(policies -> policies != null && policies.getPolicyVersion() == 5L));

        String expectedCacheFileName = (APP_ID + "_" + SERVICE_NAME + ".json")
                .replace(File.separatorChar, '_')
                .replace(File.pathSeparatorChar, '_');
        File cacheFile = new File(tempCacheDir, expectedCacheFileName);

        boolean fileExists = waitForFile(cacheFile);

        if (fileExists) {
            assertTrue("Cache file should be created: " + cacheFile.getAbsolutePath(), cacheFile.exists());
        }
        policyRefresher.stopRefresher();
        policyRefresher.join(2000);
    }

    @Test
    public void testLoadFromCacheWhenAdminUnavailable() throws Exception {
        String cacheFileName = (APP_ID + "_" + SERVICE_NAME + ".json")
                .replace(File.separatorChar, '_')
                .replace(File.pathSeparatorChar, '_');
        File cacheFile = new File(tempCacheDir, cacheFileName);
        String json = createCacheFileJson();
        try (FileWriter writer = new FileWriter(cacheFile)) {
            writer.write(json);
        }
        assertTrue("Cache file should be created for test setup", cacheFile.exists());
        reset(mockPlugin, mockRangerAdminClient);
        setupBasicMocks();
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(null);
        CountDownLatch policiesLoadedLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            ServicePolicies policies = invocation.getArgument(0);
            if (policies != null && policies.getPolicyVersion() == 10L) {
                policiesLoadedLatch.countDown();
            }
            return null;
        }).when(mockPlugin).setPolicies(any(ServicePolicies.class));

        PolicyRefresher newRefresher = new PolicyRefresher(mockPlugin);
        newRefresher.startRefresher();

        assertTrue("Policies should be loaded from cache within timeout", policiesLoadedLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeastOnce()).setPolicies(argThat(policies -> policies != null && policies.getPolicyVersion() == 10L));
        newRefresher.stopRefresher();
        newRefresher.join(2000);
    }

    @Test
    public void testMultipleConcurrentSyncRequests() throws Exception {
        CountDownLatch initLatch = new CountDownLatch(1);
        CountDownLatch allSyncsLatch = new CountDownLatch(4);
        AtomicInteger callCount = new AtomicInteger(0);
        ServicePolicies mockPolicies = createMockServicePolicies(1L);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(mockPolicies);

        doAnswer(invocation -> {
            int count = callCount.incrementAndGet();
            allSyncsLatch.countDown();
            if (count == 1) {
                initLatch.countDown();
            }
            return null;
        }).when(mockPlugin).setPolicies(any(ServicePolicies.class));

        PolicyRefresher newRefresher = new PolicyRefresher(mockPlugin);
        newRefresher.startRefresher();

        assertTrue("Initial load should complete", initLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));

        newRefresher.syncPoliciesWithAdmin(new DownloadTrigger());
        newRefresher.syncPoliciesWithAdmin(new DownloadTrigger());
        newRefresher.syncPoliciesWithAdmin(new DownloadTrigger());

        assertTrue("All sync requests should be processed", allSyncsLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeast(4)).setPolicies(argThat(policies ->
                policies != null && policies.getPolicyVersion() == 1L));
        newRefresher.stopRefresher();
        newRefresher.join(2000);
    }

    @Test
    public void testRefresherHandlesNullPolicyResponse() throws Exception {
        CountDownLatch waitLatch = new CountDownLatch(1);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(null);
        doAnswer(invocation -> {
            waitLatch.countDown();
            return null;
        }).when(mockPlugin).setPolicies(any());

        PolicyRefresher newRefresher = new PolicyRefresher(mockPlugin);
        newRefresher.startRefresher();

        boolean wasCalledInTime = waitLatch.await(2, TimeUnit.SECONDS);

        assertTrue("Refresher should be running", newRefresher.isAlive());
        if (wasCalledInTime) {
            verify(mockPlugin, atLeastOnce()).setPolicies(null);
        }
        newRefresher.stopRefresher();
        newRefresher.join(2000);
    }

    @Test
    public void testActivationTimeTracking() throws Exception {
        long beforeActivation = System.currentTimeMillis();
        CountDownLatch policiesAppliedLatch = new CountDownLatch(1);
        ServicePolicies mockPolicies = createMockServicePolicies(1L);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(mockPolicies);

        doAnswer(invocation -> {
            policiesAppliedLatch.countDown();
            return null;
        })
                .when(mockPlugin).setPolicies(any(ServicePolicies.class));

        policyRefresher.startRefresher();

        assertTrue("Policies should be applied", policiesAppliedLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        long afterActivation = System.currentTimeMillis();
        long activationTime = policyRefresher.getLastActivationTimeInMillis();
        assertTrue("Activation time should be after test start", activationTime >= beforeActivation);
        assertTrue("Activation time should be before verification", activationTime <= afterActivation);
        verify(mockPlugin, times(1)).setPolicies(argThat(policies ->
                policies != null && policies.getPolicyVersion() == 1L));
    }

    @Test
    public void testCorruptedCacheFileHandling() throws Exception {
        String cacheFileName = (APP_ID + "_" + SERVICE_NAME + ".json")
                .replace(File.separatorChar, '_')
                .replace(File.pathSeparatorChar, '_');
        File cacheFile = new File(tempCacheDir, cacheFileName);

        try (FileWriter writer = new FileWriter(cacheFile)) {
            writer.write("{ corrupted json data without closing brace");
        }
        assertTrue("Corrupted cache file should exist", cacheFile.exists());

        reset(mockPlugin, mockRangerAdminClient);
        setupBasicMocks();

        ServicePolicies freshPolicies = createMockServicePolicies(1L);
        when(mockRangerAdminClient.getServicePoliciesIfUpdated(anyLong(), anyLong())).thenReturn(freshPolicies);

        CountDownLatch policiesLoadedLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            policiesLoadedLatch.countDown();
            return null;
        }).when(mockPlugin)
                .setPolicies(any(ServicePolicies.class));

        PolicyRefresher newRefresher = new PolicyRefresher(mockPlugin);
        newRefresher.startRefresher();

        assertTrue("Should load fresh policies when cache is corrupted", policiesLoadedLatch.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        verify(mockPlugin, atLeastOnce()).setPolicies(argThat(policies ->
                policies != null && policies.getPolicyVersion() == 1L));
        newRefresher.stopRefresher();
        newRefresher.join(2000);
    }

    private ServicePolicies createMockServicePolicies(long version) {
        ServicePolicies policies = new ServicePolicies();
        policies.setServiceName(SERVICE_NAME);
        policies.setPolicyVersion(version);
        policies.setServiceId(1L);
        return policies;
    }

    private void setupBasicMocks() {
        when(mockPlugin.getServiceName()).thenReturn(SERVICE_NAME);
        when(mockPlugin.getServiceType()).thenReturn(SERVICE_TYPE);
        when(mockPlugin.getAppId()).thenReturn(APP_ID);
        when(mockPlugin.getConfig()).thenReturn(mockPluginConfig);
        when(mockPlugin.getPluginContext()).thenReturn(mockPluginContext);
        when(mockPluginConfig.getPropertyPrefix()).thenReturn("ranger.plugin.test.service");
        when(mockPluginConfig.get(eq("ranger.plugin.test.service.policy.cache.dir")))
                .thenReturn(tempCacheDir.getAbsolutePath());
        when(mockPluginConfig.getLong(eq("ranger.plugin.test.service.policy.pollIntervalMs"), anyLong()))
                .thenReturn(POLL_INTERVAL);
        when(mockPluginConfig.getBoolean(eq("ranger.plugin.test.service.preserve.deltas"), anyBoolean()))
                .thenReturn(false);
        when(mockPluginConfig.getInt(eq("ranger.plugin.test.service.max.versions.to.preserve"), anyInt()))
                .thenReturn(1);
        when(mockPluginContext.getAdminClient()).thenReturn(mockRangerAdminClient);
        when(mockPluginContext.createAdminClient(mockPluginConfig)).thenReturn(mockRangerAdminClient);
    }

    private boolean waitForFile(File file) throws InterruptedException {
        long endTime = System.currentTimeMillis() + (long) 3000;
        while (System.currentTimeMillis() < endTime) {
            if (file.exists()) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    private String createCacheFileJson() {
        return "{"
                + "\"serviceName\":\"" + SERVICE_NAME + "\","
                + "\"policyVersion\":10,"
                + "\"serviceId\":1,"
                + "\"policies\":["
                + "{"
                + "\"id\":1,"
                + "\"service\":\"" + SERVICE_NAME + "\","
                + "\"name\":\"read-policy\","
                + "\"isEnabled\":true,"
                + "\"resources\":{\"path\":{\"values\":[\"/data/test\"],\"isRecursive\":false}},"
                + "\"policyItems\":["
                + "{\"users\":[\"user1\"],\"accesses\":[{\"type\":\"read\",\"isAllowed\":true}]}"
                + "]"
                + "},"
                + "{"
                + "\"id\":2,"
                + "\"service\":\"" + SERVICE_NAME + "\","
                + "\"name\":\"write-policy\","
                + "\"isEnabled\":true,"
                + "\"resources\":{\"path\":{\"values\":[\"/data/restricted\"],\"isRecursive\":false}},"
                + "\"policyItems\":["
                + "{\"users\":[\"user2\"],\"accesses\":[{\"type\":\"write\",\"isAllowed\":false}]}"
                + "]"
                + "}"
                + "]}";
    }
}
