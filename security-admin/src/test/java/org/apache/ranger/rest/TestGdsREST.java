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
import org.apache.ranger.common.RangerSearchUtil;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerGrant;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPrincipal;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;



import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestGdsREST {

	@InjectMocks
	private GdsREST gdsREST = new GdsREST();

	@Mock
	RangerSearchUtil searchUtil;

	private final HttpServletRequest request = Mockito.mock(HttpServletRequest.class);;

	@Test
	public void testAddDataSetGrants() throws Exception {

		RangerGds.RangerDataset rangerDataset = createRangerDataSet();
		RangerPolicy policy = createPolicyForDataSet(rangerDataset);

		List<RangerPolicy.RangerPolicyItem> policyItems = new ArrayList<>(policy.getPolicyItems());
		List<RangerGrant> rangerGrants = createAndGetSampleGrantData();

		policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

		List<RangerPolicy.RangerPolicyItem> updatedPolicyItems = policy.getPolicyItems();

		assertNotEquals(policyItems, updatedPolicyItems);

		assertEquals(policyItems.size() + rangerGrants.size(), updatedPolicyItems.size());

		List<RangerPolicy.RangerPolicyItem> filteredPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
		assertEquals(filteredPolicyItems, updatedPolicyItems);
	}

	@Test
	public void testUpdateDataSetGrants() throws Exception {
		RangerGds.RangerDataset rangerDataset = createRangerDataSet();
		RangerPolicy policy = createPolicyForDataSet(rangerDataset);

		List<RangerGrant> rangerGrants = createAndGetSampleGrantData();
		policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

		String[] requestedPrincipals = {"group:hdfs"};
		when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(requestedPrincipals);

		List<RangerPolicy.RangerPolicyItem> hdfsPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

		RangerGrant grant3 = new RangerGrant(new RangerPrincipal(RangerPrincipal.PrincipalType.GROUP, "hdfs"),
				Arrays.asList("_READ"), Collections.emptyList());
		policy = gdsREST.updatePolicyWithModifiedGrants(policy, Arrays.asList(grant3));

		List<RangerPolicy.RangerPolicyItem> updatedHdfsPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

		assertNotNull(updatedHdfsPolicyItems);
		assertEquals(hdfsPolicyItems.size(), updatedHdfsPolicyItems.size());
		assertNotEquals(hdfsPolicyItems, updatedHdfsPolicyItems);
	}

	@Test
	public void testRemoveDataSetGrants() throws Exception {
		RangerGds.RangerDataset rangerDataset = createRangerDataSet();
		RangerPolicy policy = createPolicyForDataSet(rangerDataset);
		List<RangerGrant> rangerGrants = createAndGetSampleGrantData();

		policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);
		List<RangerPolicy.RangerPolicyItem> newPolicyItems = policy.getPolicyItems();


		String[] requestedPrincipals = {"group:hdfs"};
		when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(requestedPrincipals);

		List<RangerPolicy.RangerPolicyItem> existingHdfsPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

		RangerGrant grant4 = new RangerGrant(new RangerPrincipal(RangerPrincipal.PrincipalType.GROUP, "hdfs"),
				Collections.emptyList(), Collections.emptyList());
		policy = gdsREST.updatePolicyWithModifiedGrants(policy, Arrays.asList(grant4));

		List<RangerPolicy.RangerPolicyItem> updatedHdfsPolicyItems = gdsREST.filterPolicyItemsByRequest(policy, request);

		assertNotEquals(existingHdfsPolicyItems, updatedHdfsPolicyItems);
		assertTrue( "Grants for "+ Arrays.toString(requestedPrincipals) +" should be empty", updatedHdfsPolicyItems.isEmpty());
	}

	@Test
	public void testGetAllDataSetGrants() {
		RangerGds.RangerDataset rangerDataset = createRangerDataSet();
		RangerPolicy policy = createPolicyForDataSet(rangerDataset);
		List<RangerGrant> rangerGrants = createAndGetSampleGrantData();

		policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

		List<RangerPolicy.RangerPolicyItem> policyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
		List<RangerGrant> policyItemsAsGrants = gdsREST.transformPolicyItemsToGrants(policyItems);

		assertEquals(rangerGrants, policyItemsAsGrants);
	}

	@Test
	public void testGetDataSetGrantsByPrincipal() throws Exception {
		RangerGds.RangerDataset rangerDataset = createRangerDataSet();
		RangerPolicy policy = createPolicyForDataSet(rangerDataset);
		List<RangerGrant> rangerGrants = createAndGetSampleGrantData();

		policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

		String[] existingRequestedPrincipals = {"user:hive"};
		when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(existingRequestedPrincipals);

		List<RangerPolicy.RangerPolicyItem> filteredPolicyItemsByPrincipal = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

		assertTrue(filteredPolicyItemsByPrincipal.size() == 1);
		assertTrue(filteredPolicyItemsByPrincipal.get(0).getUsers().contains("hive"));

		String[] nonexistentRequestedPrincipals = {"user:hadoop"};
		when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(nonexistentRequestedPrincipals);

		filteredPolicyItemsByPrincipal = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
		assertTrue("Grants for Principals: "+ Arrays.toString(nonexistentRequestedPrincipals) +" should be empty", filteredPolicyItemsByPrincipal.size() == 0);
	}

	@Test
	public void testGetDataSetGrantsByAccessType() throws Exception {
		RangerGds.RangerDataset rangerDataset = createRangerDataSet();
		RangerPolicy policy = createPolicyForDataSet(rangerDataset);
		List<RangerGrant> rangerGrants = createAndGetSampleGrantData();

		policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

		String[] requestedAccessTypes = {"_MANAGE"};
		when(searchUtil.getParamMultiValues(request, "accessType")).thenReturn(requestedAccessTypes);

		List<RangerPolicy.RangerPolicyItem> policyItemsByAccessType = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

		assertTrue(policyItemsByAccessType.size() == 1);
		assertTrue(policyItemsByAccessType.get(0).getAccesses().stream().anyMatch(x -> Arrays.asList(requestedAccessTypes).contains(x.getType())));

		String[] nonexistentRequestedAccessTypes = {"_DELETE"};
		when(searchUtil.getParamMultiValues(request, "accessType")).thenReturn(nonexistentRequestedAccessTypes);

		List<RangerPolicy.RangerPolicyItem>  updatedPolicyItemsByAccessType = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
		assertTrue( "Grants for AccessTypes: "+ Arrays.toString(nonexistentRequestedAccessTypes) +" should be empty", updatedPolicyItemsByAccessType.isEmpty());
	}

	@Test
	public void testGetDataSetGrantsByPrincipalAndAccessType() throws Exception {
		RangerGds.RangerDataset rangerDataset = createRangerDataSet();
		RangerPolicy policy = createPolicyForDataSet(rangerDataset);
		List<RangerGrant> rangerGrants = createAndGetSampleGrantData();

		policy = gdsREST.updatePolicyWithModifiedGrants(policy, rangerGrants);

		String[] requestedPrincipals = {"user:hive"};
		String[] requestedAccessTypes = {"_READ"};

		when(searchUtil.getParamMultiValues(request, "principal")).thenReturn(requestedPrincipals);
		when(searchUtil.getParamMultiValues(request, "accessType")).thenReturn(requestedAccessTypes);

		List<RangerPolicy.RangerPolicyItem> filteredPolicyItems = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));

		assertTrue("Grants for Principals: "+ Arrays.toString(requestedPrincipals) + " and AccessTypes: "+ Arrays.toString(requestedAccessTypes) +" should exist", filteredPolicyItems.size() == 1);
		assertTrue("Grants for Principals: "+ Arrays.toString(requestedPrincipals) + "should exist", filteredPolicyItems.get(0).getUsers().contains("hive"));
		assertTrue("Grants for AccessTypes: "+ Arrays.toString(requestedAccessTypes) + "should exist", filteredPolicyItems.get(0).getAccesses().stream().anyMatch(x -> Arrays.asList(requestedAccessTypes).contains(x.getType())));

		String[] nonexistentRequestedAccessTypes = {"_DELETE"};
		when(searchUtil.getParamMultiValues(request, "accessType")).thenReturn(nonexistentRequestedAccessTypes);

		List<RangerPolicy.RangerPolicyItem>  updatedPolicyItemsByAccessType = new ArrayList<>(gdsREST.filterPolicyItemsByRequest(policy, request));
		assertTrue( "Grants for Principals: "+ Arrays.toString(requestedPrincipals) + " and AccessTypes: "+ Arrays.toString(nonexistentRequestedAccessTypes) +" should be empty", updatedPolicyItemsByAccessType.isEmpty());
	}

	private RangerGds.RangerDataset createRangerDataSet() {
		long id = new Random().nextInt(100);
		RangerGds.RangerDataset dataset = new RangerGds.RangerDataset();
		dataset.setId(id);
		dataset.setName("dataset-" + id);
		dataset.setGuid(UUID.randomUUID().toString());

		return dataset;
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
		RangerGrant grant1 = new RangerGrant(new RangerPrincipal(RangerPrincipal.PrincipalType.USER, "hive"),
				Arrays.asList("_READ"), Arrays.asList("IS_ACCESSED_BEFORE('2024/12/12')"));
		RangerGrant grant2 = new RangerGrant(new RangerPrincipal(RangerPrincipal.PrincipalType.GROUP, "hdfs"),
				Arrays.asList("_MANAGE"), Collections.emptyList());

		return Arrays.asList(grant1, grant2);
	}
}
