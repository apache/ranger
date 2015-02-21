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
package org.apache.ranger.authorization.hbase;



import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.hbase.security.User;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.junit.Assert;
import org.junit.Test;

public class AuthorizationSessionTest {

	@Test
	public void testAuthorizationSession() {
//		fail("Not yet implemented");
	}

	@Test
	public void testOperation() {
//		fail("Not yet implemented");
	}

	@Test
	public void testOtherInformation() {
//		fail("Not yet implemented");
	}

	@Test
	public void testAccess() {
//		fail("Not yet implemented");
	}

	@Test
	public void testUser() {
//		fail("Not yet implemented");
	}

	@Test
	public void testTable() {
//		fail("Not yet implemented");
	}

	@Test
	public void testColumnFamily() {
//		fail("Not yet implemented");
	}

	@Test
	public void testColumn() {
//		fail("Not yet implemented");
	}

	@Test
	public void testIsBuildable() {
		RangerBasePlugin plugin = new RangerBasePlugin("hbase", "hbase");
		AuthorizationSession session = new AuthorizationSession(plugin);
		try {
			session.verifyBuildable();
			Assert.fail("Should have thrown exception");
		} catch (IllegalStateException e) { }
		// user and access are the only required ones.
		User user = mock(User.class);
		when(user.getGroupNames()).thenReturn(new String[] { "groups", "group2" });
		session.access(" ");
		session.user(user);
		try {
			session.verifyBuildable();
		} catch (IllegalStateException e) {
			fail("Shouldn't have thrown an exception!");
		}
		// setting column-family without table is a problem
		session.columnFamily("family");
		try {
			session.verifyBuildable();
			fail("Should have thrown an exception");
		} catch (IllegalStateException e) { }
		
		session.table("table");
		try {
			session.verifyBuildable();
		} catch (IllegalStateException e) {
			fail("Shouldn't have thrown an exception!");
		}
		// setting column without column-family is a problem
		session.columnFamily(null);
		session.column("col");
		try {
			session.verifyBuildable();
			fail("Should have thrown an exception");
		} catch (IllegalStateException e) { }
		session.columnFamily("family");
		try {
			session.verifyBuildable();
		} catch (IllegalStateException e) { 
			fail("Should have thrown an exception");
		}
	}

	@Test
	public void testZapAuthorizationState() {
//		fail("Not yet implemented");
	}

	@Test
	public void testIsProvided() {
		AuthorizationSession session = new AuthorizationSession(null);
		assertFalse(session.isProvided(null));
		assertFalse(session.isProvided(""));
		assertTrue(session.isProvided(" "));
		assertTrue(session.isProvided("xtq"));
	}

	@Test
	public void testBuildRequest() {
//		fail("Not yet implemented");
	}

	@Test
	public void testAuthorize() {
		RangerBasePlugin plugin = new RangerBasePlugin("hbase", "hbase");
		
		User user = mock(User.class);
		when(user.getShortName()).thenReturn("user1");
		when(user.getGroupNames()).thenReturn(new String[] { "users" } );
		AuthorizationSession session = new AuthorizationSession(plugin);
		session.access("read")
			.user(user)
			.table(":meta:")
			.buildRequest()
			.authorize();
	}

	@Test
	public void testPublishResults() {
//		fail("Not yet implemented");
	}

	@Test
	public void testIsAuthorized() {
//		fail("Not yet implemented");
	}

	@Test
	public void testGetDenialReason() {
//		fail("Not yet implemented");
	}

	@Test
	public void testGetResourceType() {
//		fail("Not yet implemented");
	}

	@Test
	public void testRequestToString() {
//		fail("Not yet implemented");
	}

	@Test
	public void testAudit() {
//		fail("Not yet implemented");
	}

	@Test
	public void testGetPrintableValue() {
//		fail("Not yet implemented");
	}

	@Test
	public void testBuildAccessDeniedMessage() {
//		fail("Not yet implemented");
	}

	@Test
	public void testBuildAccessDeniedMessageString() {
//		fail("Not yet implemented");
	}

	@Test
	public void testKnownPatternAllowedNotAudited() {
//		fail("Not yet implemented");
	}

	@Test
	public void testKnownPatternDisallowedNotAudited() {
//		fail("Not yet implemented");
	}

	@Test
	public void testAuditHandler() {
//		fail("Not yet implemented");
	}

	@Test
	public void testBuildResult() {
//		fail("Not yet implemented");
	}
}
