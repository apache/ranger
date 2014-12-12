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

package org.apache.ranger.pdp.knox;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.ranger.pdp.knox.URLBasedAuthDB;
import org.apache.ranger.pdp.model.Policy;
import org.apache.ranger.pdp.model.PolicyContainer;
import org.apache.ranger.pdp.model.RolePermission;
import org.junit.Assert;
import org.junit.Test;

public class URLBasedAuthDBTest {

	@Test
	public void testPolicyEnabled() {
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null, // ipAddress
				true, // policyEnabled
				true); // auditEnabled
		pdp.setPolicyContainer(policyContainer);
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testPolicyEnabled allowed: " + allowed);
		Assert.assertTrue("Access denied while policy is enabled", allowed);
	}
	
	@Test
	public void testPolicyNotEnabled() {
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null, // ipAddress
				false, // policyEnabled
				true); // auditEnabled
		pdp.setPolicyContainer(policyContainer);
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testPolicyNotEnabled allowed: " + allowed);
		Assert.assertFalse("Access allowed while policy is disabled", allowed);
	}
	
	@Test
	public void testPolicyEnabledAuditOnTmSm() {
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null, // ipAddress
				true, // policyEnabled
				true); // auditEnabled
		pdp.setPolicyContainer(policyContainer);
		
		boolean auditEnabled = pdp.isAuditEnabled(
				"xa", 
				"WEBHDFS"
				);
		System.out.println("testPolicyEnabledAuditOnTmSm: " + auditEnabled);
		Assert.assertTrue("Audit not ebabled while policy is matched", auditEnabled);
	}
	
	@Test
	public void testPolicyEnabledAuditOnTnmSm() {
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null, // ipAddress
				true, // policyEnabled
				true); // auditEnabled
		pdp.setPolicyContainer(policyContainer);
		
		boolean auditEnabled = pdp.isAuditEnabled(
				"yxa", 
				"WEBHDFS"
				);
		System.out.println("testPolicyEnabledAuditOnTnmSm auditEnabled: " + auditEnabled);
		Assert.assertFalse("Audit ebabled with a non matching topology", auditEnabled);
	}
	
	@Test
	public void testPolicyEnabledAuditOnTmSnm() {
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null, // ipAddress
				true, // policyEnabled
				true); // auditEnabled
		pdp.setPolicyContainer(policyContainer);
		
		boolean auditEnabled = pdp.isAuditEnabled(
				"xa", 
				"yWEBHDFS"
				);
		System.out.println("testPolicyEnabledAuditOnTmSnm auditEnabled: " + auditEnabled);
		Assert.assertFalse("Audit ebabled with a non matching service", auditEnabled);
	}
	
	@Test
	public void testPolicyEnabledAuditOff() {
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null, // ipAddress
				true, // policyEnabled
				false); // auditEnabled
		pdp.setPolicyContainer(policyContainer);
		
		boolean auditEnabled = pdp.isAuditEnabled(
				"xa", 
				"WEBHDFS"
				);
		System.out.println("testPolicyEnabledAuditOff auditEnabled: " + auditEnabled);
		Assert.assertFalse("Audit ebabled with policy disabling audit", auditEnabled);
	}
	
	@Test
	public void testPolicyNotEnabledAuditOn() {
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null, // ipAddress
				false, // policyEnabled
				true); // auditEnabled
		pdp.setPolicyContainer(policyContainer);
		
		boolean auditEnabled = pdp.isAuditEnabled(
				"xa", 
				"WEBHDFS"
				);
		System.out.println("testPolicyNotEnabledAuditOn auditEnabled: " + auditEnabled);
		Assert.assertFalse("Audit ebabled with policy not enabled", auditEnabled);
	}
	
	@Test
	public void testPolicyNotEnabledAuditOff() {
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null, // ipAddress
				false, // policyEnabled
				true); // auditEnabled
		pdp.setPolicyContainer(policyContainer);
		
		boolean auditEnabled = pdp.isAuditEnabled(
				"xa", 
				"WEBHDFS"
				);
		System.out.println("testPolicyNotEnabledAuditOff auditEnabled: " + auditEnabled);
		Assert.assertFalse("Audit ebabled with policy not enabled and audit off", auditEnabled);
	}
	
	@Test
	public void testSimpleTopologyAccessAllowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testSimpleTopologyAccessAllowed allowed: " + allowed);
		Assert.assertTrue("Access denied for a simple allow policy", allowed);

	}
	
	@Test
	public void testSimpleTopologyAccessDenied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		boolean allowed = pdp.isAccessGranted(
				"ya", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testSimpleTopologyAccessDenied allowed: " + allowed);
		Assert.assertFalse("Access allwed for a simple deny policy", allowed);

	}
	
	@Test
	public void testWildTopologyAccessAllowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"x*", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testWildTopologyAccessAllowed allowed: " + allowed);
		Assert.assertTrue("Access denied for a matching wild topology policy", allowed);

	}

	@Test
	public void testWildTopologyAccessDenied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"x*", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		boolean allowed = pdp.isAccessGranted(
				"ya", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testWildTopologyAccessDenied allowed: " + allowed);
		Assert.assertFalse("Access allowed for non matching wild topology policy", allowed);

	}
	
	@Test
	public void testSimpleServiceAccessAllowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testSimpleServiceAccessAllowed allowed: " + allowed);
		Assert.assertTrue("Access denied for a policy matching service", allowed);

	}
	
	@Test
	public void testSimpleServiceAccessDenied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"yWEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testSimpleServiceAccessDenied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a policy with no matching service", allowed);

	}
	
	@Test
	public void testWildServiceAccessAllowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEB*",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testWildServiceAccessAllowed allowed: " + allowed);
		Assert.assertTrue("Access denied for policy with matching wild service", allowed);

	}

	@Test
	public void testWildServiceAccessDenied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEB*",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"yWEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testWildServiceAccessDenied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a policy with a non matching wild service", allowed);

	}
	
	@Test
	public void testAccessTypeAccessAllowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testAccessTypeAccessAllowed allowed: " + allowed);
		Assert.assertTrue("Access denied for a policy with matching accessTyoe", allowed);

	}
	
	@Test
	public void testAccessTypeAccessDenied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"yallow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testAccessTypeAccessDenied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a non matching accessType", allowed);

	}
	
	@Test
	public void testUserAccessAllowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testUserAccessAllowed allowed: " + allowed);
		Assert.assertTrue("Access denied for matching user", allowed);

	}
	
	@Test
	public void testUserAccessDenied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("yguest"), 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"yallow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testUserAccessDenied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a non matching user", allowed);

	}
	
	@Test
	public void testGroupAccessAllowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				null, 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"sam", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testGroupAccessAllowe allowed: " + allowed);
		Assert.assertTrue("Access denied for a matching user group", allowed);

	}
	
	@Test
	public void testGroupAccessDenied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				null, 
				asList("sales"),
				null);
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"yallow",
				"guest", 
				asSet("sales"), 
				"127.127.127.127");
		System.out.println("testGroupAccessDenied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a non matching user group", allowed);

	}
	
	
	
	@Test
	public void testSimpleIP4Allowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132.133.134.135"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132.133.134.135");
		System.out.println("testSimpleIP4Allowed allowed: " + allowed);
		Assert.assertTrue("Access denied for a matching IP4 request ip", allowed);

	}
	
	@Test
	public void testSimpleIP4Denied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132.133.134.135"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132.133.134.136");
		System.out.println("testSimpleIP4Denied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a non matching IP4 request ip", allowed);

	}
	
	@Test
	public void testWildIP4Allowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132.133.134.*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132.133.134.135");
		System.out.println("testWildIP4Allowed allowed: " + allowed);
		Assert.assertTrue("Access denied for a request ip matching wild IP4", allowed);

	}
	
	@Test
	public void testWildIP4Denied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132.133.134.*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132.133.234.136");
		System.out.println("testWildIP4Denied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a request ip not matching wild IP4", allowed);

	}
	
	@Test
	public void testWilderIP4Allowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132.133.*.*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132.133.234.235");
		System.out.println("testWilderIP4Allowed: " + allowed);
		Assert.assertTrue("Access denied for a request ip matching wilder IP4", allowed);

	}
	
	@Test
	public void testWilderIP4Denied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132.133.*.*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132.233.234.136");
		System.out.println("testWilderIP4Denied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a request ip not matching wilder IP4", allowed);

	}
	
	@Test
	public void testWildIP6Allowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132:133:134:*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132:133:134:135");
		System.out.println("testWildIP6Allowed allowed: " + allowed);
		Assert.assertTrue("Access denied for a request ip matching wild IP6", allowed);

	}
	
    @Test
	public void testWildIP6AllowedMixedCase() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132:133:Db8:*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132:133:dB8:135");
		System.out.println("testWildIP6AllowedMixedCase: " + allowed);
		Assert.assertTrue("Access denied for a request ip matching wild IP6 with mixed case", allowed);

	}
	
	@Test
	public void testWildIP6Denied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132:133:134:*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132:133:234:136");
		System.out.println("testWildIP6Denied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a request ip not matching wild IP6", allowed);

	}
	
	@Test
	public void testWilderIP6Allowed() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132:133:*:*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132:133:234:235");
		System.out.println("testWilderIP6Allowed allowed: " + allowed);
		Assert.assertTrue("Access denied for a request ip matching wilder IP6", allowed);

	}
	
	@Test
	public void testWilderIP6Denied() {
		
		URLBasedAuthDB pdp = URLBasedAuthDB.getInstanceWithBackEndMocked();
		
		PolicyContainer policyContainer = buildPolicyContainer(
				"xa", 
				"WEBHDFS",
				asList("allow"), 
				asList("guest"), 
				asList("sales"),
				asList("132:133:*:*"));
		pdp.setPolicyContainer(policyContainer);
		
		
		boolean allowed = pdp.isAccessGranted(
				"xa", 
				"WEBHDFS", 
				"allow",
				"guest", 
				asSet("sales"), 
				"132:233:234:136");
		System.out.println("testWilderIP6Denied allowed: " + allowed);
		Assert.assertFalse("Access allowed for a request ip not matching wilder IP6", allowed);

	}
	
	private static PolicyContainer buildPolicyContainer(String topologies,
			String services, List<String> accessTypes, List<String> users,
			List<String> groups, List<String> ipAddresses) {
		return buildPolicyContainer( topologies,  services, accessTypes,
				users, groups, ipAddresses, true, true);
		
	}
	
	private static PolicyContainer buildPolicyContainer(String topologies,
			String  services, List<String> accessTypes, List<String> users,
			List<String> groups, List<String> ipAddresses,
			boolean policyEnabled, boolean auditEnabled) {

		PolicyContainer policyContainer = new PolicyContainer();
		policyContainer.setRepositoryName("knoxdev");

		List<Policy> policies = new ArrayList<Policy>();

		Policy policy = new Policy();
		policy.setTopologies(topologies);
		policy.setServices(services);
		policy.setPolicyStatus(policyEnabled ? "Enabled" : "NotEnabled");
		policy.setAuditInd(auditEnabled ? 1 : 0);

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
