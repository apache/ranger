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

package org.apache.ranger.plugin.service;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestRangerBasePluginBatching {
    @Test
    void isAccessAllowed_batchesRequestsOverThreshold() throws Exception {
        RangerBasePlugin plugin       = new RangerBasePlugin("hbase", "hbase");
        RangerPolicyEngine policyMock = Mockito.mock(RangerPolicyEngine.class);
        List<RangerAccessRequest> requests = createRequests(2500);

        setPolicyEngine(plugin, policyMock);

        Mockito.when(policyMock.evaluatePolicies(any(Collection.class), eq(RangerPolicy.POLICY_TYPE_ACCESS), isNull()))
                .thenAnswer(invocation -> {
                    Collection<RangerAccessRequest> reqBatch = invocation.getArgument(0);
                    List<RangerAccessResult> ret = new ArrayList<>();

                    for (RangerAccessRequest req : reqBatch) {
                        ret.add(createResult(req));
                    }

                    return ret;
                });

        Collection<RangerAccessResult> results = plugin.isAccessAllowed(requests, null);

        assertEquals(2500, results.size());

        ArgumentCaptor<Collection> batches = ArgumentCaptor.forClass(Collection.class);
        verify(policyMock, times(3)).evaluatePolicies(batches.capture(), eq(RangerPolicy.POLICY_TYPE_ACCESS), isNull());

        assertEquals(1000, batches.getAllValues().get(0).size());
        assertEquals(1000, batches.getAllValues().get(1).size());
        assertEquals(500, batches.getAllValues().get(2).size());
    }

    @Test
    void isAccessAllowed_preservesResultOrderAcrossBatches() throws Exception {
        RangerBasePlugin plugin       = new RangerBasePlugin("hbase", "hbase");
        RangerPolicyEngine policyMock = Mockito.mock(RangerPolicyEngine.class);
        List<RangerAccessRequest> requests = createRequests(1200);
        List<RangerAccessResult> expected = new ArrayList<>();

        setPolicyEngine(plugin, policyMock);

        Mockito.when(policyMock.evaluatePolicies(any(Collection.class), eq(RangerPolicy.POLICY_TYPE_ACCESS), isNull()))
                .thenAnswer(invocation -> {
                    Collection<RangerAccessRequest> reqBatch = invocation.getArgument(0);
                    List<RangerAccessResult> ret = new ArrayList<>();

                    for (RangerAccessRequest req : reqBatch) {
                        RangerAccessResult result = createResult(req);
                        expected.add(result);
                        ret.add(result);
                    }

                    return ret;
                });

        Collection<RangerAccessResult> results = plugin.isAccessAllowed(requests, null);

        assertEquals(expected.size(), results.size());
        assertEquals(expected, new ArrayList<>(results));
    }

    @Test
    void isAccessAllowed_usesSingleBatchAtOrBelowThreshold() throws Exception {
        RangerBasePlugin plugin       = new RangerBasePlugin("hbase", "hbase");
        RangerPolicyEngine policyMock = Mockito.mock(RangerPolicyEngine.class);
        List<RangerAccessRequest> requests = createRequests(1000);

        setPolicyEngine(plugin, policyMock);

        Mockito.when(policyMock.evaluatePolicies(any(Collection.class), eq(RangerPolicy.POLICY_TYPE_ACCESS), isNull()))
                .thenReturn(Collections.emptyList());

        plugin.isAccessAllowed(requests, null);

        verify(policyMock, times(1)).evaluatePolicies(any(Collection.class), eq(RangerPolicy.POLICY_TYPE_ACCESS), isNull());
    }

    @Test
    void isAccessAllowed_emptyRequestsSkipsPolicyEngine() throws Exception {
        RangerBasePlugin plugin       = new RangerBasePlugin("hbase", "hbase");
        RangerPolicyEngine policyMock = Mockito.mock(RangerPolicyEngine.class);

        setPolicyEngine(plugin, policyMock);

        Collection<RangerAccessResult> results = plugin.isAccessAllowed(Collections.emptyList(), null);

        assertTrue(results.isEmpty());
        verify(policyMock, never()).evaluatePolicies(any(Collection.class), eq(RangerPolicy.POLICY_TYPE_ACCESS), isNull());
    }

    private static void setPolicyEngine(RangerBasePlugin plugin, RangerPolicyEngine policyEngine) throws Exception {
        Field field = RangerBasePlugin.class.getDeclaredField("policyEngine");
        field.setAccessible(true);
        field.set(plugin, policyEngine);
    }

    private static List<RangerAccessRequest> createRequests(int count) {
        List<RangerAccessRequest> ret = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            ret.add(Mockito.mock(RangerAccessRequest.class));
        }

        return ret;
    }

    private static RangerAccessResult createResult(RangerAccessRequest request) {
        RangerAccessResult ret = Mockito.mock(RangerAccessResult.class);
        Mockito.when(ret.getAccessRequest()).thenReturn(request);
        return ret;
    }
}
