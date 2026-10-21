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
package org.apache.ranger.biz;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.RangerBaseService;
import org.apache.ranger.plugin.service.RangerDefaultService;
import org.apache.ranger.plugin.store.ServiceStore;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestServiceMgr {
	@Test
	public void test11_getRangerServiceByService_rejectsImplClassNotAssignableToRangerBaseService() throws Exception {
		ServiceMgr       mgr   = new ServiceMgr();
		ServiceStore     store = Mockito.mock(ServiceStore.class);
		RangerService    svc   = new RangerService();
		RangerServiceDef def   = new RangerServiceDef();

		svc.setType("evil");
		svc.setName("s");
		def.setName("evil");
		def.setImplClass(Thread.class.getName());
		Mockito.when(store.getServiceDefByName("evil")).thenReturn(def);

		RangerBaseService built = mgr.getRangerServiceByService(svc, store);

		Assert.assertNotNull(built);
		Assert.assertEquals(RangerDefaultService.class, built.getClass());
		Assert.assertEquals("s", built.getServiceName());
	}

	@Test
	public void test12_getRangerServiceByService_acceptsImplClassAssignableToRangerBaseService() throws Exception {
		// Sanity check: a legitimate implClass that does extend RangerBaseService still works.
		ServiceMgr       mgr   = new ServiceMgr();
		ServiceStore     store = Mockito.mock(ServiceStore.class);
		RangerService    svc   = new RangerService();
		RangerServiceDef def   = new RangerServiceDef();

		svc.setType("legit");
		svc.setName("s2");
		def.setName("legit");
		def.setImplClass(RangerDefaultService.class.getName());
		Mockito.when(store.getServiceDefByName("legit")).thenReturn(def);

		RangerBaseService built = mgr.getRangerServiceByService(svc, store);

		Assert.assertNotNull(built);
		Assert.assertEquals(RangerDefaultService.class, built.getClass());
	}
}
