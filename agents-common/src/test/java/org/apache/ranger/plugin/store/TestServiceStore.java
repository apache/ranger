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

package org.apache.ranger.plugin.store;

import static org.junit.Assert.*;

import java.io.File;
import java.util.List;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.file.ServiceFileStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestServiceStore {
	static ServiceStore svcStore = null;
	static SearchFilter filter   = null;

	static final String sdName      = "svcDef-unit-test-TestServiceStore";
	static final String serviceName = "svc-unit-test-TestServiceStore";
	static final String policyName  = "testPolicy-1";

	@BeforeClass
	public static void setupTest() throws Exception {
		
		
		File file = File.createTempFile("fileStore", "dir") ;
		
		if (file.exists()) {
			file.delete() ;
		}
		
		file.deleteOnExit(); 
		
		file.mkdirs() ;
		
		String fileStoreDir =  file.getAbsolutePath() ;
		
		System.out.println("Using fileStoreDirectory as [" + fileStoreDir + "]") ;

		svcStore = new ServiceFileStore(fileStoreDir);
		svcStore.init();

		// cleanup if the test service and service-def if they already exist
		List<RangerService> services = svcStore.getServices(filter);
		for(RangerService service : services) {
			if(service.getName().startsWith(serviceName)) {
				svcStore.deleteService(service.getId());
			}
		}

		List<RangerServiceDef> serviceDefs = svcStore.getServiceDefs(filter);
		for(RangerServiceDef serviceDef : serviceDefs) {
			if(serviceDef.getName().startsWith(sdName)) {
				svcStore.deleteServiceDef(serviceDef.getId());
			}
		}
	}

	@Test
	public void testServiceStore() throws Exception {
		String updatedName, updatedDescription;

		List<RangerServiceDef> sds = svcStore.getServiceDefs(filter);

		int initSdCount = sds == null ? 0 : sds.size();

		RangerServiceDef sd = new RangerServiceDef(sdName, "org.apache.ranger.services.TestService", "TestService", "test servicedef description", null, null, null, null, null, null, null);

		RangerServiceDef createdSd = svcStore.createServiceDef(sd);
		assertNotNull("createServiceDef() failed", createdSd != null);

		sds = svcStore.getServiceDefs(filter);
		assertEquals("createServiceDef() failed", initSdCount + 1, sds == null ? 0 : sds.size());

		updatedDescription = sd.getDescription() + ": updated";
		createdSd.setDescription(updatedDescription);
		RangerServiceDef updatedSd = svcStore.updateServiceDef(createdSd);
		assertNotNull("updateServiceDef(updatedDescription) failed", updatedSd);
		assertEquals("updateServiceDef(updatedDescription) failed", updatedDescription, updatedSd.getDescription());

		sds = svcStore.getServiceDefs(filter);
		assertEquals("updateServiceDef(updatedDescription) failed", initSdCount + 1, sds == null ? 0 : sds.size());

		/*
		updatedName = sd.getName() + "-Renamed";
		updatedSd.setName(updatedName);
		updatedSd = sdMgr.update(updatedSd);
		assertNotNull("updateServiceDef(updatedName) failed", updatedSd);
		assertEquals("updateServiceDef(updatedName) failed", updatedName, updatedSd.getName());

		sds = getAllServiceDef();
		assertEquals("updateServiceDef(updatedName) failed", initSdCount + 1, sds == null ? 0 : sds.size());
		*/

		List<RangerService> services = svcStore.getServices(filter);

		int initServiceCount = services == null ? 0 : services.size();

		RangerService svc = new RangerService(sdName, serviceName, "test service description", null, null);

		RangerService createdSvc = svcStore.createService(svc);
		assertNotNull("createService() failed", createdSvc);

		services = svcStore.getServices(filter);
		assertEquals("createServiceDef() failed", initServiceCount + 1, services == null ? 0 : services.size());

		updatedDescription = createdSvc.getDescription() + ": updated";
		createdSvc.setDescription(updatedDescription);
		RangerService updatedSvc = svcStore.updateService(createdSvc);
		assertNotNull("updateService(updatedDescription) failed", updatedSvc);
		assertEquals("updateService(updatedDescription) failed", updatedDescription, updatedSvc.getDescription());

		services = svcStore.getServices(filter);
		assertEquals("updateService(updatedDescription) failed", initServiceCount + 1, services == null ? 0 : services.size());

		updatedName = serviceName + "-Renamed";
		updatedSvc.setName(updatedName);
		updatedSvc = svcStore.updateService(updatedSvc);
		assertNotNull("updateService(updatedName) failed", updatedSvc);
		assertEquals("updateService(updatedName) failed", updatedName, updatedSvc.getName());

		services = svcStore.getServices(filter);
		assertEquals("updateService(updatedName) failed", initServiceCount + 1, services == null ? 0 : services.size());

		List<RangerPolicy> policies = svcStore.getPolicies(filter);

		int initPolicyCount = policies == null ? 0 : policies.size();

		RangerPolicy policy = new RangerPolicy(updatedSvc.getName(), policyName, 0, "test policy description", null, null, null);
		policy.getResources().put("path", new RangerPolicyResource("/demo/test/finance", Boolean.FALSE, Boolean.TRUE));

		RangerPolicyItem item1 = new RangerPolicyItem();
		item1.getAccesses().add(new RangerPolicyItemAccess("read"));
		item1.getAccesses().add(new RangerPolicyItemAccess("write"));
		item1.getAccesses().add(new RangerPolicyItemAccess("execute"));
		item1.getUsers().add("admin");
		item1.getGroups().add("finance");

		RangerPolicyItem item2 = new RangerPolicyItem();
		item2.getAccesses().add(new RangerPolicyItemAccess("read"));
		item2.getGroups().add("public");

		policy.getPolicyItems().add(item1);
		policy.getPolicyItems().add(item2);

		RangerPolicy createdPolicy = svcStore.createPolicy(policy);
		assertNotNull(createdPolicy);
		assertNotNull(createdPolicy.getPolicyItems());
		assertEquals(createdPolicy.getPolicyItems().size(), 2);

		RangerPolicyItem createItem1 = createdPolicy.getPolicyItems().get(0);
		RangerPolicyItem createItem2 = createdPolicy.getPolicyItems().get(1);

		assertNotNull(createItem1.getAccesses());
		assertEquals(createItem1.getAccesses().size(), 3);
		assertNotNull(createItem1.getUsers());
		assertEquals(createItem1.getUsers().size(), 1);
		assertNotNull(createItem1.getGroups());
		assertEquals(createItem1.getGroups().size(), 1);

		assertNotNull(createItem2.getAccesses());
		assertEquals(createItem2.getAccesses().size(), 1);
		assertNotNull(createItem2.getUsers());
		assertEquals(createItem2.getUsers().size(), 0);
		assertNotNull(createItem2.getGroups());
		assertEquals(createItem2.getGroups().size(), 1);

		policies = svcStore.getPolicies(filter);
		assertEquals("createPolicy() failed", initPolicyCount + 1, policies == null ? 0 : policies.size());

		updatedDescription = policy.getDescription() + ":updated";
		createdPolicy.setDescription(updatedDescription);
		RangerPolicy updatedPolicy = svcStore.updatePolicy(createdPolicy);
		assertNotNull("updatePolicy(updatedDescription) failed", updatedPolicy != null);

		policies = svcStore.getPolicies(filter);
		assertEquals("updatePolicy(updatedDescription) failed", initPolicyCount + 1, policies == null ? 0 : policies.size());

		updatedName = policyName + "-Renamed";
		updatedPolicy.setName(updatedName);
		updatedPolicy = svcStore.updatePolicy(updatedPolicy);
		assertNotNull("updatePolicy(updatedName) failed", updatedPolicy);

		policies = svcStore.getPolicies(filter);
		assertEquals("updatePolicy(updatedName) failed", initPolicyCount + 1, policies == null ? 0 : policies.size());

		// rename the service; all the policies for this service should reflect the new service name
		updatedName = serviceName + "-Renamed2";
		updatedSvc.setName(updatedName);
		updatedSvc = svcStore.updateService(updatedSvc);
		assertNotNull("updateService(updatedName2) failed", updatedSvc);
		assertEquals("updateService(updatedName2) failed", updatedName, updatedSvc.getName());

		services = svcStore.getServices(filter);
		assertEquals("updateService(updatedName2) failed", initServiceCount + 1, services == null ? 0 : services.size());

		updatedPolicy = svcStore.getPolicy(createdPolicy.getId());
		assertNotNull("updateService(updatedName2) failed", updatedPolicy);
		assertEquals("updateService(updatedName2) failed", updatedPolicy.getService(), updatedSvc.getName());

		ServicePolicies svcPolicies = svcStore.getServicePoliciesIfUpdated(updatedSvc.getName(), 0l);
		assertNotNull("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies);
		assertNotNull("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies.getPolicies());
		assertEquals("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies.getServiceName(), updatedSvc.getName());
		assertEquals("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies.getServiceId(), updatedSvc.getId());
		assertEquals("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies.getPolicyVersion(), updatedSvc.getPolicyVersion());
		assertEquals("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies.getPolicyUpdateTime(), updatedSvc.getPolicyUpdateTime());
		assertEquals("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies.getServiceDef().getId(), updatedSd.getId());
		assertEquals("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies.getPolicies().size(), 1);
		assertEquals("getServicePolicies(" + updatedSvc.getName() + ") failed", svcPolicies.getPolicies().get(0).getName(), updatedPolicy.getName());

		ServicePolicies updatedPolicies = svcStore.getServicePoliciesIfUpdated(updatedSvc.getName(), svcPolicies.getPolicyVersion());
		assertNull(updatedPolicies);

		filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, policyName);
		policies = svcStore.getPolicies(filter);
		assertEquals("getPolicies(filter=origPolicyName) failed", 0, policies == null ? 0 : policies.size());
		filter = null;

		filter = new SearchFilter();
		filter.setParam(SearchFilter.POLICY_NAME, updatedPolicy.getName());
		policies = svcStore.getPolicies(filter);
		assertEquals("getPolicies(filter=origPolicyName) failed", 1, policies == null ? 0 : policies.size());
		filter = null;
		
		String osName = System.getProperty("os.name") ;
		boolean windows = (osName != null && osName.toLowerCase().startsWith("windows")) ;

		if (! windows ) {

			svcStore.deletePolicy(policy.getId());
			
			policies = svcStore.getPolicies(filter);
			
			assertEquals("deletePolicy() failed", initPolicyCount, policies == null ? 0 : policies.size());
			
	
			svcStore.deleteService(svc.getId());
			services = svcStore.getServices(filter);
			assertEquals("deleteService() failed", initServiceCount, services == null ? 0 : services.size());
	
			svcStore.deleteServiceDef(sd.getId());
			sds = svcStore.getServiceDefs(filter);
			assertEquals("deleteServiceDef() failed", initSdCount, sds == null ? 0 : sds.size());
		
		}
	}
}
