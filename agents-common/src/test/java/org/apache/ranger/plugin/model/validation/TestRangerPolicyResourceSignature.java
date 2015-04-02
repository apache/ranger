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

package org.apache.ranger.plugin.model.validation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.validation.RangerPolicyResourceSignature.RangerPolicyResourceView;
import org.junit.Test;

public class TestRangerPolicyResourceSignature {

	@Test
	public void test_RangerPolicyResourceView_toString() {
		// null resource
		RangerPolicyResource policyResource = null;
		RangerPolicyResourceView policyResourceView = new RangerPolicyResourceView(policyResource);
		assertEquals("{}", policyResourceView.toString());
		
		// non-null policy resource with null values/recursive flag
		policyResource = createPolicyResource(null, null, null);
		policyResourceView = new RangerPolicyResourceView(policyResource);
		assertEquals("{values=,excludes=false,recursive=false}", policyResourceView.toString());
		
		// valid values in non-asending order
		policyResource = createPolicyResource(new String[]{"b", "a", "d", "c"}, true, false);
		policyResourceView = new RangerPolicyResourceView(policyResource);
		assertEquals("{values=[a, b, c, d],excludes=false,recursive=true}", policyResourceView.toString());
		
		// recursive flag is false and different variation of values to show lexicographic ordering
		policyResource = createPolicyResource(new String[]{"9", "A", "e", "_"}, false, true);
		policyResourceView = new RangerPolicyResourceView(policyResource);
		assertEquals("{values=[9, A, _, e],excludes=true,recursive=false}", policyResourceView.toString());
	}
	
	RangerPolicyResource createPolicyResource(String[] values, Boolean recursive, Boolean excludes) {
		
		RangerPolicyResource resource = mock(RangerPolicyResource.class);
		if (values == null) {
			when(resource.getValues()).thenReturn(null);
		} else {
			when(resource.getValues()).thenReturn(Arrays.asList(values));
		}
		when(resource.getIsRecursive()).thenReturn(recursive);
		when(resource.getIsExcludes()).thenReturn(excludes);
		
		return resource;
	}

	@Test
	public void test_isPolicyValidForResourceSignatureComputation() {
		// null policy is invalid
		RangerPolicyResourceSignature utils = new RangerPolicyResourceSignature((String)null);
		RangerPolicy rangerPolicy = null;
		assertFalse("policy==null", utils.isPolicyValidForResourceSignatureComputation(rangerPolicy));

		// null resource map is invalid
		rangerPolicy = mock(RangerPolicy.class);
		when(rangerPolicy.getResources()).thenReturn(null);
		assertFalse("policy.getResources()==null", utils.isPolicyValidForResourceSignatureComputation(rangerPolicy));
		
		// empty resources map is ok!
		Map<String, RangerPolicyResource> policyResources = new HashMap<String, RangerPolicyResource>();
		when(rangerPolicy.getResources()).thenReturn(policyResources);
		assertTrue("policy.getResources().isEmpty()", utils.isPolicyValidForResourceSignatureComputation(rangerPolicy));
		
		// but having a resource map with null key is not ok!
		RangerPolicyResource aPolicyResource = mock(RangerPolicyResource.class);
		policyResources.put(null, aPolicyResource);
		assertFalse("policy.getResources().contains(null)", utils.isPolicyValidForResourceSignatureComputation(rangerPolicy));
	}
	
	@Test
	public void test_RangerPolicyResourceSignature() {
		// String rep of a null policy is an empty string! and its hash is sha of empty string!
		RangerPolicyResourceSignature signature = new RangerPolicyResourceSignature((String)null);
		assertEquals("", signature.asString());
		assertEquals(DigestUtils.md5Hex(""), signature.asHashHex());
	}

	/*
	 * Format of data expected by the utility function which uses this is:
	 * { "resource-name", "values" "isExcludes", "isRecursive" }
	 */
	Object[][] first = new Object[][] {
			{ "table", new String[] { "tbl3", "tbl1", "tbl2"}, true,  false},
			{ "db",    new String[] { "db1", "db2"},           false, null},
			{ "col",   new String[] { "col2", "col1", "col3"}, null,  true},
	};

	Object[][] first_recursive_null_or_false = new Object[][] {
			{ "table", new String[] { "tbl3", "tbl1", "tbl2"}, true,  null}, // recursive flag is false in first
			{ "db",    new String[] { "db1", "db2"},           false, null},
			{ "col",   new String[] { "col2", "col1", "col3"}, null,  true},
	};

	Object[][] first_recursive_flag_different = new Object[][] {
			{ "table", new String[] { "tbl3", "tbl1", "tbl2"}, true,  false},
			{ "db",    new String[] { "db1", "db2"},           false, null},
			{ "col",   new String[] { "col2", "col1", "col3"}, null,  false}, // recursive flag is true in first
	};

	Object[][] first_excludes_null_or_false = new Object[][] {
			{ "table", new String[] { "tbl3", "tbl1", "tbl2"}, true,  false},
			{ "db",    new String[] { "db1", "db2"},           false, null}, // excludes flag is null in first
			{ "col",   new String[] { "col2", "col1", "col3"}, false, true},
	};

	Object[][] first_excludes_flag_different = new Object[][] {
			{ "table", new String[] { "tbl3", "tbl1", "tbl2"}, true,  false},
			{ "db",    new String[] { "db1", "db2"},           false, null},
			{ "col",   new String[] { "col2", "col1", "col3"}, true,  true}, // excludes flag is false in first
	};

	Object[][] data_second = new Object[][] {
			{ "db",    new String[] { "db2", "db1"},           false, null},
			{ "table", new String[] { "tbl2", "tbl3", "tbl1"}, true, false},
			{ "col",   new String[] { "col1", "col3", "col2"}, null, true},
	};

	@Test
	public void test_getResourceSignature_happyPath() {
		// null policy returns signature of empty resource
		RangerPolicy policy = null;
		RangerPolicyResourceSignature sig = new RangerPolicyResourceSignature(policy);
		assertEquals(null, sig.getResourceString(policy));
		
		policy = mock(RangerPolicy.class);
		Map<String, RangerPolicyResource> policyResources = _utils.createPolicyResourceMap(first);
		when(policy.getResources()).thenReturn(policyResources);
		String expected = "{" +
			"col={values=[col1, col2, col3],excludes=false,recursive=true}, " + 
			"db={values=[db1, db2],excludes=false,recursive=false}, " +
			"table={values=[tbl1, tbl2, tbl3],excludes=true,recursive=false}" +
		"}"; 
		assertEquals(expected, sig.getResourceString(policy));

		// order of values should not matter
		policyResources = _utils.createPolicyResourceMap(data_second);
		when(policy.getResources()).thenReturn(policyResources);
		assertEquals(expected, sig.getResourceString(policy));
	}
	
	
	@Test
	public void test_nullRecursiveFlagIsSameAsFlase() {
		// create two policies with resources that differ only in the recursive flag such that flags are null in one and false in another
		RangerPolicy policy1 = createPolicy(first);
		RangerPolicy policy2 = createPolicy(first_recursive_null_or_false);
		RangerPolicyResourceSignature signature = new RangerPolicyResourceSignature((String)null);
		assertEquals("null is same as false", signature.getResourceString(policy1), signature.getResourceString(policy2));
	}
	
	@Test
	public void test_onlyDifferByRecursiveFlag() {
		// create two policies with resources that differ only in the recursive flag, i.e. null/false in one and true in another
		RangerPolicy policy1 = createPolicy(first);
		RangerPolicy policy2 = createPolicy(first_recursive_flag_different);
		RangerPolicyResourceSignature signature = new RangerPolicyResourceSignature((String)null);
		assertFalse("Resources differ only by recursive flag true vs false/null", signature.getResourceString(policy1).equals(signature.getResourceString(policy2)));
	}
	
	@Test
	public void test_nullExcludesFlagIsSameAsFlase() {
		// create two policies with resources that differ only in the excludes flag such that flags are null in one and false in another
		RangerPolicy policy1 = createPolicy(first);
		RangerPolicy policy2 = createPolicy(first_excludes_null_or_false);
		RangerPolicyResourceSignature signature = new RangerPolicyResourceSignature((String)null);
		assertEquals("null is same as false", signature.getResourceString(policy1), signature.getResourceString(policy2));
	}
	
	@Test
	public void test_onlyDifferByExcludesFlag() {
		// create two policies with resources that differ only in the excludes flag, i.e. null/false in one and true in another
		RangerPolicy policy1 = createPolicy(first);
		RangerPolicy policy2 = createPolicy(first_excludes_flag_different);
		RangerPolicyResourceSignature signature = new RangerPolicyResourceSignature((String)null);
		assertFalse("Resources differ only by recursive flag true vs false/null", signature.getResourceString(policy1).equals(signature.getResourceString(policy2)));
	}
	
	RangerPolicy createPolicy(Object[][] data) {
		RangerPolicy policy = mock(RangerPolicy.class);
		Map<String, RangerPolicyResource> resources = _utils.createPolicyResourceMap(data);
		when(policy.getResources()).thenReturn(resources);
		return policy;
	}

	@Test
	public void test_integration() {
		// setup two policies with resources that are structurally different but semantically the same.
		RangerPolicy aPolicy = mock(RangerPolicy.class);
		Map<String, RangerPolicyResource> resources = _utils.createPolicyResourceMap(first);
		when(aPolicy.getResources()).thenReturn(resources);
		RangerPolicyResourceSignature signature = new RangerPolicyResourceSignature(aPolicy);
		
		RangerPolicy anotherPolicy = mock(RangerPolicy.class);
		resources = _utils.createPolicyResourceMap(data_second);
		when(anotherPolicy.getResources()).thenReturn(resources);
		RangerPolicyResourceSignature anotherSignature = new RangerPolicyResourceSignature(anotherPolicy);
		assertTrue(signature.equals(anotherSignature));
		assertTrue(anotherSignature.equals(signature));
	}
	
	ValidationTestUtils _utils = new ValidationTestUtils();
}
