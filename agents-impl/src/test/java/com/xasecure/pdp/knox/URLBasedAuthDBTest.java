/**
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

package com.xasecure.pdp.knox;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.xasecure.pdp.model.Policy;
import com.xasecure.pdp.model.PolicyContainer;
import com.xasecure.pdp.model.RolePermission;

// FIXME: this is just a starting stub
public class URLBasedAuthDBTest {

	@Test
	public void testBasic() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("127.127.*"));
		pdp.setPolicyContainer(policyContainer);
		
		Set<String> userGroups = asSet("sales");
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				userGroups, 
				"127.127.127.127");
		System.out.println("allowed: " + allowed);
		Assert.assertTrue("not good", allowed);

	}

	private static PolicyContainer buildPolicyContainer(String topologies,
			String services, List<String> accessTypes, List<String> users,
			List<String> groups, List<String> ipAddresses) {

		PolicyContainer policyContainer = new PolicyContainer();
		policyContainer.setRepositoryName("knoxdev");

		List<Policy> policies = new ArrayList<Policy>();

		Policy policy = new Policy();
		policy.setTopologies(topologies);
		policy.setServices(services);

		List<RolePermission> rolePermissions = new ArrayList<RolePermission>();
		
		RolePermission rolePermission =  new RolePermission();
		
		rolePermissions.add(rolePermission);
		rolePermission.setAccess(accessTypes);
		rolePermission.setUsers(users);
		rolePermission.setGroups(groups);
		rolePermission.setIpAddress(ipAddresses);
		
		policy.setPermissions(rolePermissions);
		
		policies.add(policy);

		policyContainer.setAcl(policies);

		return policyContainer;
	}

	private static Set<String> asSet(String... a) {
		Set<String> vals = new HashSet<String>();
		for (String s : a) {
			vals.add(s);
		}
		return vals;
	}

	private static List<String> asList(String... a) {
		List<String> vals = new ArrayList<String>();
		for (String s : a) {
			vals.add(s);
		}
		return vals;
	}
	
}
