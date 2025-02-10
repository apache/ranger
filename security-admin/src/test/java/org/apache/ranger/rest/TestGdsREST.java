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
package org.apache.ranger.rest;

import org.apache.ranger.biz.GdsDBStore;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerGrant;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPrincipal;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.util.SearchFilter;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestGdsREST {
    private final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    @Mock
    RangerSearchUtil searchUtil;
    @InjectMocks
    private GdsREST gdsREST;
    @InjectMocks
    GdsDBStore gdsDBStore;
    @Mock
    RESTErrorUtil restErrorUtil;

    @Test
    public void testAddDataSetGrants() {
        RangerGds.RangerDataset rangerDataset = createRangerDataSet();
        RangerPolicy            policy        = createPolicyForDataSet(rangerDataset);

        List<RangerPolicy.RangerPolicyItem> policyItems  = new ArrayList<>(policy.getPolicyItems());
        List<RangerGrant>                   rangerGrants = createAndGetSampleGrantData();

        policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

        List<RangerPolicy.RangerPolicyItem> updatedPolicyItems = policy.getPolicyItems();

        assertNotEquals(policyItems, updatedPolicyItems);

        assertEquals(policyItems.size() + rangerGrants.size(), updatedPolicyItems.size());

        List<RangerPolicy.RangerPolicyItem> filteredPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
        assertEquals(filteredPolicyItems, updatedPolicyItems);
    }

    @Test
    public void testUpdateDataSetGrants() {
        RangerGds.RangerDataset rangerDataset = createRangerDataSet();
        RangerPolicy            policy        = createPolicyForDataSet(rangerDataset);

        List<RangerGrant> rangerGrants = createAndGetSampleGrantData();
        policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

        String[] requestedPrincipals = {"group:hdfs"};
        when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(requestedPrincipals);

        List<RangerPolicy.RangerPolicyItem> hdfsPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

        RangerGrant grant3 = new RangerGrant(new RangerPrincipal(RangerPrincipal.PrincipalType.GROUP, "hdfs"), Collections.singletonList("_READ"), Collections.emptyList());
        policy = gdsREST.updatePolicyWithModifiedGrants(policy, Collections.singletonList(grant3));

        List<RangerPolicy.RangerPolicyItem> updatedHdfsPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

        assertNotNull(updatedHdfsPolicyItems);
        assertEquals(hdfsPolicyItems.size(), updatedHdfsPolicyItems.size());
        assertNotEquals(hdfsPolicyItems, updatedHdfsPolicyItems);
    }

    @Test
    public void testRemoveDataSetGrants() {
        RangerGds.RangerDataset rangerDataset = createRangerDataSet();
        RangerPolicy            policy        = createPolicyForDataSet(rangerDataset);
        List<RangerGrant>       rangerGrants  = createAndGetSampleGrantData();

        policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);
        List<RangerPolicy.RangerPolicyItem> newPolicyItems = policy.getPolicyItems();

        String[] requestedPrincipals = {"group:hdfs"};
        when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(requestedPrincipals);

        List<RangerPolicy.RangerPolicyItem> existingHdfsPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

        RangerGrant grant4 = new RangerGrant(new RangerPrincipal(RangerPrincipal.PrincipalType.GROUP, "hdfs"), Collections.emptyList(), Collections.emptyList());
        policy = gdsREST.updatePolicyWithModifiedGrants(policy, Collections.singletonList(grant4));

        List<RangerPolicy.RangerPolicyItem> updatedHdfsPolicyItems = gdsREST.filterPolicyItemsByRequest(policy, request);

        assertNotEquals(existingHdfsPolicyItems, updatedHdfsPolicyItems);
        assertTrue("Grants for " + Arrays.toString(requestedPrincipals) + " should be empty", updatedHdfsPolicyItems.isEmpty());
    }

    @Test
    public void testGetAllDataSetGrants() {
        RangerGds.RangerDataset rangerDataset = createRangerDataSet();
        RangerPolicy            policy        = createPolicyForDataSet(rangerDataset);
        List<RangerGrant>       rangerGrants  = createAndGetSampleGrantData();

        policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

        List<RangerPolicy.RangerPolicyItem> policyItems         = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
        List<RangerGrant>                   policyItemsAsGrants = gdsREST.transformPolicyItemsToGrants(policyItems);

        assertEquals(rangerGrants, policyItemsAsGrants);
    }

    @Test
    public void testGetDataSetGrantsByPrincipal() {
        RangerGds.RangerDataset rangerDataset = createRangerDataSet();
        RangerPolicy            policy        = createPolicyForDataSet(rangerDataset);
        List<RangerGrant>       rangerGrants  = createAndGetSampleGrantData();

        policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

        String[] existingRequestedPrincipals = {"user:hive"};
        when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(existingRequestedPrincipals);

        List<RangerPolicy.RangerPolicyItem> filteredPolicyItemsByPrincipal = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

        assertEquals(1, filteredPolicyItemsByPrincipal.size());
        assertTrue(filteredPolicyItemsByPrincipal.get(0).getUsers().contains("hive"));

        String[] nonexistentRequestedPrincipals = {"user:hadoop"};
        when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(nonexistentRequestedPrincipals);

        filteredPolicyItemsByPrincipal = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
        assertEquals("Grants for Principals: " + Arrays.toString(nonexistentRequestedPrincipals) + " should be empty", 0, filteredPolicyItemsByPrincipal.size());
    }

    @Test
    public void testGetDataSetGrantsByAccessType() {
        RangerGds.RangerDataset rangerDataset = createRangerDataSet();
        RangerPolicy            policy        = createPolicyForDataSet(rangerDataset);
        List<RangerGrant>       rangerGrants  = createAndGetSampleGrantData();

        policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

        String[] requestedAccessTypes = {"_MANAGE"};
        when(searchUtil.getParamMultiValues(request, "accessType")).thenReturn(requestedAccessTypes);

        List<RangerPolicy.RangerPolicyItem> policyItemsByAccessType = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

        assertEquals(1, policyItemsByAccessType.size());
        assertTrue(policyItemsByAccessType.get(0).getAccesses().stream().anyMatch(x -> Arrays.asList(requestedAccessTypes).contains(x.getType())));

        String[] nonexistentRequestedAccessTypes = {"_DELETE"};
        when(searchUtil.getParamMultiValues(request, "accessType")).thenReturn(nonexistentRequestedAccessTypes);

        List<RangerPolicy.RangerPolicyItem> updatedPolicyItemsByAccessType = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
        assertTrue("Grants for AccessTypes: " + Arrays.toString(nonexistentRequestedAccessTypes) + " should be empty", updatedPolicyItemsByAccessType.isEmpty());
    }

    @Test
    public void testGetDataSetGrantsByPrincipalAndAccessType() {
        RangerGds.RangerDataset rangerDataset = createRangerDataSet();
        RangerPolicy            policy        = createPolicyForDataSet(rangerDataset);
        List<RangerGrant>       rangerGrants  = createAndGetSampleGrantData();

        policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

        String[] requestedPrincipals  = {"user:hive"};
        String[] requestedAccessTypes = {"_READ"};

        when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(requestedPrincipals);
        when(searchUtil.getParamMultiValues(request, "accessType")).thenReturn(requestedAccessTypes);

        List<RangerPolicy.RangerPolicyItem> filteredPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

        assertEquals("Grants for Principals: " + Arrays.toString(requestedPrincipals) + " and AccessTypes: " + Arrays.toString(requestedAccessTypes) + " should exist", 1, filteredPolicyItems.size());
        assertTrue("Grants for Principals: " + Arrays.toString(requestedPrincipals) + "should exist", filteredPolicyItems.get(0).getUsers().contains("hive"));
        assertTrue("Grants for AccessTypes: " + Arrays.toString(requestedAccessTypes) + "should exist", filteredPolicyItems.get(0).getAccesses().stream().anyMatch(x -> Arrays.asList(requestedAccessTypes).contains(x.getType())));

        String[] nonexistentRequestedAccessTypes = {"_DELETE"};
        when(searchUtil.getParamMultiValues(request, "accessType")).thenReturn(nonexistentRequestedAccessTypes);

        List<RangerPolicy.RangerPolicyItem> updatedPolicyItemsByAccessType = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
        assertTrue("Grants for Principals: " + Arrays.toString(requestedPrincipals) + " and AccessTypes: " + Arrays.toString(nonexistentRequestedAccessTypes) + " should be empty", updatedPolicyItemsByAccessType.isEmpty());
    }

    @Test
    public void testSearchDataSetsByValidityPeriod() {
        List<RangerGds.RangerDataset> rangerDatasets = new ArrayList<>();

        RangerGds.RangerDataset rangerDataset1 = createRangerDataSet();
        updateDatasetValiditySchedule(rangerDataset1, -5, -1);

        RangerGds.RangerDataset rangerDataset2 = createRangerDataSet();
        updateDatasetValiditySchedule(rangerDataset2, -5, 5);

        RangerGds.RangerDataset rangerDataset3 = createRangerDataSet();
        updateDatasetValiditySchedule(rangerDataset3, -10, 2);

        RangerGds.RangerDataset rangerDataset4 = createRangerDataSet();
        updateDatasetValiditySchedule(rangerDataset4, -2, 15);

        RangerGds.RangerDataset rangerDataset5 = createRangerDataSet();
        updateDatasetValiditySchedule(rangerDataset5, 5, 15);

        RangerGds.RangerDataset rangerDataset6 = createRangerDataSet();
        updateDatasetValiditySchedule(rangerDataset6, -15, -5);

        rangerDatasets.addAll(Arrays.asList(rangerDataset1, rangerDataset2, rangerDataset3, rangerDataset4, rangerDataset5, rangerDataset6));
        List<RangerGds.RangerDataset> actualDatasets = new ArrayList<>(rangerDatasets);

        SearchFilter filter = new SearchFilter();
        filter.setParam(SearchFilter.VALIDITY_TIME_ZONE, SearchFilter.DEFAULT_TIME_ZONE);

        //ValiditySchedule Filter criteria-1
        String startTime = getFormattedDateString(-10);
        String endTime   = getFormattedDateString(-2);
        filter.setParam(SearchFilter.VALIDITY_EXPIRY_START, startTime);
        filter.setParam(SearchFilter.VALIDITY_EXPIRY_END, endTime);

        List<RangerGds.RangerDataset> expectedDatasets = Arrays.asList(rangerDataset6);

        gdsDBStore.filterDatasetsByValidityExpiration(filter, actualDatasets);

        assertEquals("Datasets expiry count mismatch between " + startTime + " and " + endTime, expectedDatasets.size(), actualDatasets.size());

        assertTrue("Mismatch in datasets returned for expiry between " + startTime + " and " + endTime,
                actualDatasets.containsAll(expectedDatasets));

        //ValiditySchedule Filter criteria-2
        actualDatasets.clear();
        actualDatasets.addAll(rangerDatasets);

        startTime = getFormattedDateString(-4);
        endTime   = getFormattedDateString(20);
        filter.setParam(SearchFilter.VALIDITY_EXPIRY_START, startTime);
        filter.setParam(SearchFilter.VALIDITY_EXPIRY_END, endTime);

        expectedDatasets = Arrays.asList(rangerDataset1, rangerDataset2, rangerDataset3, rangerDataset4, rangerDataset5);

        gdsDBStore.filterDatasetsByValidityExpiration(filter, actualDatasets);

        assertEquals("Datasets expiry count mismatch between " + startTime + " and " + endTime, expectedDatasets.size(), actualDatasets.size());

        assertTrue("Mismatch in datasets returned for expiry between " + startTime + " and " + endTime,
                actualDatasets.containsAll(expectedDatasets));

        //ValiditySchedule Filter criteria-3
        actualDatasets.clear();
        actualDatasets.addAll(rangerDatasets);

        startTime = getFormattedDateString(-15);
        endTime   = getFormattedDateString(0);
        filter.setParam(SearchFilter.VALIDITY_EXPIRY_START, startTime);
        filter.setParam(SearchFilter.VALIDITY_EXPIRY_END, endTime);

        expectedDatasets = Arrays.asList(rangerDataset1, rangerDataset6);

        gdsDBStore.filterDatasetsByValidityExpiration(filter, actualDatasets);

        assertEquals("Datasets expiry count mismatch between " + startTime + " and " + endTime, expectedDatasets.size(), actualDatasets.size());

        assertTrue("Mismatch in datasets returned for expiry between " + startTime + " and " + endTime,
                actualDatasets.containsAll(expectedDatasets));

        //ValiditySchedule Filter criteria-4 with invalid date
        actualDatasets.clear();
        actualDatasets.addAll(rangerDatasets);

        startTime = getInvalidDateString(-5);
        endTime   = getFormattedDateString(16);
        filter.setParam(SearchFilter.VALIDITY_EXPIRY_START, startTime);
        filter.setParam(SearchFilter.VALIDITY_EXPIRY_END, endTime);

        Mockito.when(restErrorUtil.createRESTException(Mockito.anyString())).thenThrow(new WebApplicationException());

        assertThrows(WebApplicationException.class, () -> {
            gdsDBStore.filterDatasetsByValidityExpiration(filter, actualDatasets);
        });

        //ValiditySchedule Filter criteria-5 without start and end time
        actualDatasets.clear();
        actualDatasets.addAll(rangerDatasets);

        filter.removeParam(SearchFilter.VALIDITY_EXPIRY_START);
        filter.removeParam(SearchFilter.VALIDITY_EXPIRY_END);

        expectedDatasets = rangerDatasets;

        gdsDBStore.filterDatasetsByValidityExpiration(filter, actualDatasets);

        assertEquals("Datasets expiry count mismatch with empty start and end time", expectedDatasets.size(), actualDatasets.size());

        assertTrue("Mismatch in datasets returned with empty expiry time range", actualDatasets.containsAll(expectedDatasets));
    }

    private RangerGds.RangerDataset createRangerDataSet() {
        long                    id      = new Random().nextInt(100);
        RangerGds.RangerDataset dataset = new RangerGds.RangerDataset();
        dataset.setId(id);
        dataset.setName("dataset-" + id);
        dataset.setGuid(UUID.randomUUID().toString());

        return dataset;
    }

    private RangerGds.RangerDataset updateDatasetValiditySchedule(RangerGds.RangerDataset dataset, int pastDaysToStart, int futureDaysToEnd) {
        String start      = getFormattedDateString(pastDaysToStart);
        String end        = getFormattedDateString(futureDaysToEnd);
        String timezone   = SearchFilter.DEFAULT_TIME_ZONE;
        RangerValiditySchedule validitySchedule = new RangerValiditySchedule(start, end, timezone, null);
        dataset.setValiditySchedule(validitySchedule);

        return dataset;
    }

    private String getFormattedDateString(int days) {
        return getDateString(days, RangerValiditySchedule.VALIDITY_SCHEDULE_DATE_STRING_SPECIFICATION);
    }

    private String getInvalidDateString(int days) {
        return getDateString(days, "yyyy/MM/dd");
    }

    private String getDateString(int days, String pattern) {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(pattern);
        LocalDateTime     current       = LocalDateTime.now();

        return current.plusDays(days).format(dateFormatter);
    }

    private RangerPolicy createPolicyForDataSet(RangerGds.RangerDataset dataset) {
        RangerPolicy policy = new RangerPolicy();
        policy.setName("DATASET: " + dataset.getName() + "@" + System.currentTimeMillis());
        policy.setDescription("Policy for dataset: " + dataset.getName());
        policy.setServiceType("gds");
        policy.setService("_gds");
        policy.setZoneName(null);
        policy.setResources(Collections.singletonMap("dataset-id", new RangerPolicy.RangerPolicyResource(dataset.getId().toString())));
        policy.setPolicyType(RangerPolicy.POLICY_TYPE_ACCESS);
        policy.setPolicyPriority(RangerPolicy.POLICY_PRIORITY_NORMAL);
        policy.setAllowExceptions(Collections.emptyList());
        policy.setDenyPolicyItems(Collections.emptyList());
        policy.setDenyExceptions(Collections.emptyList());
        policy.setDataMaskPolicyItems(Collections.emptyList());
        policy.setRowFilterPolicyItems(Collections.emptyList());
        policy.setIsDenyAllElse(Boolean.FALSE);

        return policy;
    }

    private List<RangerGrant> createAndGetSampleGrantData() {
        List<RangerGrant.Condition> conditions = new ArrayList<>();

        RangerGrant.Condition condition1  = new RangerGrant.Condition(null, null);
        condition1.setType("expression");
        condition1.setValues(Arrays.asList("IS_ACCESSED_BEFORE('2024/12/12')", "_STATE == 'CA'"));
        conditions.add(condition1);

        RangerGrant.Condition condition2  = new RangerGrant.Condition(null, null);
        condition2.setType("validitySchedule");
        condition2.setValues(Arrays.asList("{\"startTime\":\"1970/01/01 00:00:00\",\"endTime\":\"2025/03/08 00:35:28\",\"timeZone\":\"UTC\"}"));
        conditions.add(condition2);

        RangerGrant grant1 = new RangerGrant(new RangerPrincipal(RangerPrincipal.PrincipalType.USER, "hive"), Collections.singletonList("_READ"), conditions);
        RangerGrant grant2 = new RangerGrant(new RangerPrincipal(RangerPrincipal.PrincipalType.GROUP, "hdfs"), Collections.singletonList("_MANAGE"), Collections.emptyList());

        return Arrays.asList(grant1, grant2);
    }
}
