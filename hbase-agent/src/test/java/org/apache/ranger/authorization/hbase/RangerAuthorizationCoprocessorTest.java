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
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.security.User;
import org.junit.Test;

public class RangerAuthorizationCoprocessorTest {

	@Test
	public void test_canBeNewed() {
		RangerAuthorizationCoprocessor _coprocessor = new RangerAuthorizationCoprocessor();
		assertNotNull(_coprocessor);
	}
	
	@Test
	public void test_getColumnFamilies_happypath() {
		
	}

	@Test
	public void test_getColumnFamilies_firewalling() {
		// passing null collection should return back an empty map
		RangerAuthorizationCoprocessor _coprocessor = new RangerAuthorizationCoprocessor();
		Map<String, Set<String>> result = _coprocessor.getColumnFamilies(null);
		assertNotNull(result);
		assertTrue(result.isEmpty());
		// same for passing in an empty collection
//		result = _coprocessor.getColumnFamilies(new HashMap<byte[], ? extends Collection<?>>());
	}

	@Test
	public void test_isSpecialTable_and_metadataRead() throws Exception {
		RangerAuthorizationCoprocessor cp = new RangerAuthorizationCoprocessor();
		assertTrue(cp.isSpecialTable("hbase:meta"));
		assertFalse(cp.isSpecialTable("normal"));
		assertFalse(cp.isAccessForMetadataRead("read", "hbase:acl", null));
		assertFalse(cp.isAccessForMetadataRead("write", "hbase:acl", null));

		// Test for system user bypass on hbase:acl
		User systemUser = mock(User.class);
		when(systemUser.getShortName()).thenReturn(User.getCurrent().getShortName());
		assertTrue(cp.isAccessForMetadataRead("read", "hbase:acl", systemUser));

		// Test for super user bypass on hbase:acl
		User superUser = mock(User.class);
		when(superUser.getShortName()).thenReturn("some_super_user");
		HbaseUserUtils userUtils = mock(HbaseUserUtils.class);
		lenient().when(userUtils.isSuperUser(superUser)).thenReturn(true);
		Field userUtilsField = RangerAuthorizationCoprocessor.class.getDeclaredField("_userUtils");
		userUtilsField.setAccessible(true);
		userUtilsField.set(cp, userUtils);
		assertTrue(cp.isAccessForMetadataRead("read", "hbase:acl", superUser));

		// Test for normal user on hbase:acl (should be denied)
		User normalUser = mock(User.class);
		when(normalUser.getShortName()).thenReturn("normal_user");
		lenient().when(userUtils.isSuperUser(normalUser)).thenReturn(false);
		assertFalse(cp.isAccessForMetadataRead("read", "hbase:acl", normalUser));

		// Test for normal user on hbase:meta (should be allowed)
		assertTrue(cp.isAccessForMetadataRead("read", "hbase:meta", normalUser));
	}
}
