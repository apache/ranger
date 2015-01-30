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

package org.apache.ranger.plugin.util;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngineImpl;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.ServiceStoreFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestPolicyRefresher {
	static RangerPolicyEngineImpl policyEngine = null;
	static ServiceStore           svcStore     = null;
	static PolicyRefresher        refresher    = null;

	static final long   pollingIntervalInMs = 5 * 1000;
	static final long   sleepTimeInMs       = pollingIntervalInMs + (5 * 1000);
	static final String sdName              = "hbase";
	static final String svcName             = "svc-unit-test-TestPolicyRefresher";

	static RangerService svc     = null;
	static RangerPolicy  policy1 = null;
	static RangerPolicy  policy2 = null;

	static boolean       isPolicyRefreshed = false;
	static long          policyCount       = 0;


	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		svcStore = ServiceStoreFactory.instance().getServiceStore();
		
		// cleanup if the test service already exists
		svc = svcStore.getServiceByName(svcName);
		if(svc != null) {
			svcStore.deleteService(svc.getId());
		}

		policyEngine = new RangerPolicyEngineImpl() {
			@Override
			public void setPolicies(String serviceName, RangerServiceDef serviceDef, List<RangerPolicy> policies) {
				isPolicyRefreshed = true;
				policyCount       = policies != null ? policies.size() : 0;
				
				super.setPolicies(serviceName, serviceDef, policies);
			}
		};

		refresher = new PolicyRefresher(policyEngine, sdName, svcName, svcStore, pollingIntervalInMs, null);
		refresher.start();

		// create a service
		svc = new RangerService(sdName, svcName, "test service description", Boolean.TRUE, null);

		svc = svcStore.createService(svc);
		assertNotNull("createService(" + svcName + ") failed", svc);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		if(refresher != null) {
			refresher.stopRefresher();
		}

		if(svcStore != null) {
			if(policy1 != null) {
				svcStore.deletePolicy(policy1.getId());
			}
	
			if(policy2 != null) {
				svcStore.deletePolicy(policy2.getId());
			}
	
			if(svc != null) {
				svcStore.deleteService(svc.getId());
			}
		}
	}

	@Test
	public void testRefresher() throws Exception {
		assertEquals("policy count - initial", 0, policyCount);

		RangerPolicy policy = new RangerPolicy(svc.getName(), "policy1", "test policy description", Boolean.TRUE, null, null);
		policy.getResources().put("table", new RangerPolicyResource("employee", Boolean.FALSE, Boolean.TRUE));
		policy.getResources().put("column-family", new RangerPolicyResource("personal", Boolean.FALSE, Boolean.TRUE));
		policy.getResources().put("column", new RangerPolicyResource("ssn", Boolean.FALSE, Boolean.TRUE));

		RangerPolicyItem item1 = new RangerPolicyItem();
		item1.getAccesses().add(new RangerPolicyItemAccess("admin"));
		item1.getUsers().add("admin");
		item1.getGroups().add("hr");

		RangerPolicyItem item2 = new RangerPolicyItem();
		item2.getAccesses().add(new RangerPolicyItemAccess("read"));
		item2.getGroups().add("public");

		policy.getPolicyItems().add(item1);
		policy.getPolicyItems().add(item2);

		policy1 = svcStore.createPolicy(policy);

		policy = new RangerPolicy(svc.getName(), "policy2", "test policy description", Boolean.TRUE, null, null);
		policy.getResources().put("table", new RangerPolicyResource("employee", Boolean.FALSE, Boolean.TRUE));
		policy.getResources().put("column-family", new RangerPolicyResource("finance", Boolean.FALSE, Boolean.TRUE));
		policy.getResources().put("column", new RangerPolicyResource("balance", Boolean.FALSE, Boolean.TRUE));

		item1 = new RangerPolicyItem();
		item1.getAccesses().add(new RangerPolicyItemAccess("admin"));
		item1.getUsers().add("admin");
		item1.getGroups().add("finance");

		policy.getPolicyItems().add(item1);

		policy2 = svcStore.createPolicy(policy);

		Thread.sleep(sleepTimeInMs);
		assertTrue("policy refresh - after two new policies", isPolicyRefreshed);
		assertEquals("policy count - after two new policies", 2, policyCount);
		isPolicyRefreshed = false;

		Thread.sleep(sleepTimeInMs);
		assertFalse("policy refresh - after no new policies", isPolicyRefreshed);
		assertEquals("policy count - after no new policies", 2, policyCount);
		isPolicyRefreshed = false;

		item2 = new RangerPolicyItem();
		item2.getAccesses().add(new RangerPolicyItemAccess("read"));
		item2.getGroups().add("public");
		policy2.getPolicyItems().add(item2);

		policy2 = svcStore.updatePolicy(policy2);

		Thread.sleep(sleepTimeInMs);
		assertTrue("policy refresh - after update policy", isPolicyRefreshed);
		assertEquals("policy count - after update policy", 2, policyCount);
		isPolicyRefreshed = false;

		svcStore.deletePolicy(policy2.getId());

		Thread.sleep(sleepTimeInMs);
		assertTrue("policy refresh - after delete policy", isPolicyRefreshed);
		assertEquals("policy count - after delete policy", 1, policyCount);
		isPolicyRefreshed = false;
		policy2 = null;
	}

}
