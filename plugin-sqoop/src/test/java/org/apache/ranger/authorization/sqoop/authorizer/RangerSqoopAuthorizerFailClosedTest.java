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

package org.apache.ranger.authorization.sqoop.authorizer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;

import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies authorization when the Ranger plugin is unavailable or returns no decision.
 */
public class RangerSqoopAuthorizerFailClosedTest {
	private static final String TEST_USER = "test-user";

	private RangerSqoopAuthorizer authorizer;

	private RangerSqoopPlugin sqoopPluginBefore;

	@Before
	public void setUp() throws Exception {
		sqoopPluginBefore = (RangerSqoopPlugin) getSqoopPluginField().get(null);
		authorizer        = new RangerSqoopAuthorizer();
	}

	@After
	public void tearDown() throws Exception {
		getSqoopPluginField().set(null, sqoopPluginBefore);
	}

	@Test(expected = SqoopException.class)
	public void checkPrivilegesDeniesWhenPluginReturnsNull() throws Exception {
		RangerSqoopPlugin plugin = mock(RangerSqoopPlugin.class);

		when(plugin.isAccessAllowed(any(RangerAccessRequest.class))).thenReturn(null);

		setSqoopPlugin(plugin);

		authorizer.checkPrivileges(buildUserPrincipal(), buildReadConnectorPrivilege());
	}

	@Test(expected = SqoopException.class)
	public void checkPrivilegesDeniesWhenPluginIsNull() throws Exception {
		setSqoopPlugin(null);

		authorizer.checkPrivileges(buildUserPrincipal(), buildReadConnectorPrivilege());
	}

	@Test(expected = SqoopException.class)
	public void checkPrivilegesDeniesWhenPluginReturnsNotAllowed() throws Exception {
		RangerSqoopPlugin  plugin = mock(RangerSqoopPlugin.class);
		RangerAccessResult result = mock(RangerAccessResult.class);

		when(result.getIsAllowed()).thenReturn(false);
		when(plugin.isAccessAllowed(any(RangerAccessRequest.class))).thenReturn(result);

		setSqoopPlugin(plugin);

		authorizer.checkPrivileges(buildUserPrincipal(), buildReadConnectorPrivilege());
	}

	@Test
	public void checkPrivilegesAllowsWhenPluginReturnsAllowed() throws Exception {
		RangerSqoopPlugin  plugin = mock(RangerSqoopPlugin.class);
		RangerAccessResult result = mock(RangerAccessResult.class);

		when(result.getIsAllowed()).thenReturn(true);
		when(plugin.isAccessAllowed(any(RangerAccessRequest.class))).thenReturn(result);

		setSqoopPlugin(plugin);

		authorizer.checkPrivileges(buildUserPrincipal(), buildReadConnectorPrivilege());
	}

	private static Field getSqoopPluginField() throws NoSuchFieldException {
		Field field = RangerSqoopAuthorizer.class.getDeclaredField("sqoopPlugin");

		field.setAccessible(true);

		return field;
	}

	private static void setSqoopPlugin(RangerSqoopPlugin plugin) throws Exception {
		getSqoopPluginField().set(null, plugin);
	}

	private static MPrincipal buildUserPrincipal() {
		MPrincipal principal = mock(MPrincipal.class);

		when(principal.getType()).thenReturn(MPrincipal.TYPE.USER.name());
		when(principal.getName()).thenReturn(TEST_USER);

		return principal;
	}

	private static List<MPrivilege> buildReadConnectorPrivilege() {
		MResource resource = mock(MResource.class);

		when(resource.getType()).thenReturn(MResource.TYPE.CONNECTOR.name());
		when(resource.getName()).thenReturn("hdfs-connector");

		MPrivilege privilege = mock(MPrivilege.class);

		when(privilege.getResource()).thenReturn(resource);
		when(privilege.getAction()).thenReturn("read");

		return Collections.singletonList(privilege);
	}
}
