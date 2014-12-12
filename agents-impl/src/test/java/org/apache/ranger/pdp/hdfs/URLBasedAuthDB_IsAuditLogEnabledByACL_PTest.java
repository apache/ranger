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
package org.apache.ranger.pdp.hdfs;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.pdp.hdfs.URLBasedAuthDB;
import org.apache.ranger.pdp.model.Policy;
import org.apache.ranger.pdp.model.PolicyContainer;
import org.apache.ranger.pdp.model.ResourcePath;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class URLBasedAuthDB_IsAuditLogEnabledByACL_PTest {

	static class PolicyIs {
		static final boolean wildcard = true;
		static final boolean audited = true;
		static final boolean recursive = true;

		static final boolean notWildcard = false;
		static final boolean notAudited = false;
		static final boolean notRecursive = false;
	}
	
	static final class PolicyPath {
		static final String path1 = "aPath";
		static final String path1Child1 = PolicyPath.path1 + "/" + "child1";
		static final String path1Child2 = PolicyPath.path1 + "/" + "child2";

		static final String path2 = "anotherPath";
	}
	static final class TestPath {
		static final String path1 = PolicyPath.path1;
		static final String beginsWithPath1 = PolicyPath.path1 + "_";
		static final String path1Child1 = PolicyPath.path1Child1;
		static final String path1Child2 = PolicyPath.path1Child2;
		static final String path1GrandChild1 = String.format("%s/%s/%s", path1, path1Child1, "grandChild1");
		static final String path1GrandChild2 = String.format("%s/%s/%s", path1, path1Child1, "grandChild2");

		static final String path2 = PolicyPath.path2;
		static final String beginsWithPath2 = PolicyPath.path2 + "_";
		static final String path2Child1 = PolicyPath.path2 + "/" + "child1";
		static final String path2Child2 = PolicyPath.path2 + "/" + "child2";
	}

	static class ExpectedResult {
		static final class AuditEnabled {
			static final boolean yes = true;
			static final boolean no = false;
		}
	}
	
	static class TestDataIndex {
		static final int ExpectedResult = 6;
		static final int Audited = 3;
		public static final int TestName = 0;
		public static final int wildCard = 2;
	}
	
	
	/**
	 * ASSUMPTION: set of tests passed as such that they require wildcard flag to be set for them to return audit enabled.
	 * So turn wildcard flag of them off to assert that they no-longer work.  Of course, those that don't work even with wildcard
	 * should also continue to not work when wildcard is turned off!
	 */
	private static List<Object[]> turnWildcardOffForTestsThatRequireWildcard(List<Object[]> tests) {
		
		// in the worse case we would generate one test for each existing test
		List<Object[]> newTests = new ArrayList<Object[]>(tests.size());
		for (Object[] aTest: tests) {
			boolean isPolicyWildcard = (Boolean) aTest[TestDataIndex.wildCard];
			if (isPolicyWildcard == PolicyIs.wildcard) {
				Object[] newTest = Arrays.copyOf(aTest, aTest.length);
				// Change the policy of this test so that Audit is disabled at policy level and accordingly change the expected result
				newTest[TestDataIndex.wildCard] = PolicyIs.notWildcard;
				newTest[TestDataIndex.ExpectedResult] = ExpectedResult.AuditEnabled.no;
				// for debugging purposes alter the test description, too
				String testName = (String) newTest[TestDataIndex.TestName];
				newTest[TestDataIndex.TestName] = "[Wildcard-ed base test with wildcard flag turned off] " + testName;
				newTests.add(newTest);
			}
		}
		return newTests;
	}
	
	/**
	 * wildcard - policy flag says wildcard by the policy path itself does not have any wildcards worth expanding.
	 * This should work exactly the same as if wildcard was turned off!
	 */
	private static List<Object[]> turnWildcardOnForNonWildcardTests(List<Object[]> tests) {
		
		// in the worse case we would generate one test for each existing test
		List<Object[]> newTests = new ArrayList<Object[]>(tests.size());
		/*
		 * If a test currently does not have wildcard set on it, then expectation is changing wildcard flag
		 * true shouldn't change the result.  ASSUMPTION here, of course, is that "base tests" don't use any
		 * wild-card characters in their resource paths that would make an otherwise disabled audit to return enabled. 
		 */
		for (Object[] aTest: tests) {
			boolean isPolicyWildcard = (Boolean) aTest[TestDataIndex.wildCard];
			if (isPolicyWildcard == PolicyIs.notWildcard) {
				Object[] newTest = Arrays.copyOf(aTest, aTest.length);
				// Change the policy of this test so that Audit is disabled at policy level and accordingly change the expected result
				newTest[TestDataIndex.wildCard] = PolicyIs.wildcard;
				// for debugging purposes alter the test description, too
				String testName = (String) newTest[TestDataIndex.TestName];
				newTest[TestDataIndex.TestName] = "[Base test with wildcard enabled] " + testName;
				newTests.add(newTest);
			}
		}
		return newTests;
	}
	
	/**
	 * Disabled audit on every test that expects result to be yes to ensure that no matter what answer should be false if policy says that audit is disabled!
	 */
	private static List<Object[]> disableAuditForBaseTests(List<Object[]> tests) {

		List<Object[]> newTests = new ArrayList<Object[]>(tests.size());
		
		for (Object[] aTest : tests) {
			boolean expectedResult = (Boolean) aTest[TestDataIndex.ExpectedResult];
			boolean isPolicyAuditEnabled = (Boolean) aTest[TestDataIndex.Audited];
			
			if (expectedResult == ExpectedResult.AuditEnabled.yes 
					&& isPolicyAuditEnabled == PolicyIs.audited) {
				Object[] newTest = Arrays.copyOf(aTest, aTest.length);
				// Change the policy of this test so that Audit is disabled at policy level and accordingly change the expected result
				newTest[TestDataIndex.Audited] = PolicyIs.notAudited;
				newTest[TestDataIndex.ExpectedResult] = ExpectedResult.AuditEnabled.no;
				// for debugging purposes alter the test description, too
				String testName = (String) newTest[TestDataIndex.TestName];
				newTest[TestDataIndex.TestName] = "[Base tests with audit disabled] " + testName;
				newTests.add(newTest);
			}
		}
		
		return newTests;
	}

	@Parameters
	public static Collection<Object[]> data() {
		Object[][] baseTestData = new Object[][] {

				// no-recursive paths - return true if paths match
				{"policypath(path1) == testpath(path1) => yes", 
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.notRecursive, TestPath.path1, ExpectedResult.AuditEnabled.yes},
				{"policypath(path2) == testpath(path2) => yes", 
					PolicyPath.path2, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.notRecursive, TestPath.path2, ExpectedResult.AuditEnabled.yes},

				// no-recursive paths - return false if paths don't match!
				{"policypath(path1) != testPath(path2) => no", 
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.notRecursive, TestPath.path2, ExpectedResult.AuditEnabled.no},
				{"policypath(path2) != testPath(path1) => no", 
					PolicyPath.path2, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.notRecursive, TestPath.path1, ExpectedResult.AuditEnabled.no},
				
				// recursive path policy - should work at least as well as non-recursive, i.e. match when same and not otherwise!
				{"recursive, policypath(path1) == testpath(path1)",
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path1, ExpectedResult.AuditEnabled.yes}, 
				{"recursive, policypath(path2) == testpath(path2)", 
					PolicyPath.path2, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path2, ExpectedResult.AuditEnabled.yes}, 
				{"recursive, policypath(path1) == testpath(path2)",
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path2, ExpectedResult.AuditEnabled.no},
				{"recursive, policypath(path1) == testpath(path2)",
					PolicyPath.path2, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path1, ExpectedResult.AuditEnabled.no}, 

				// recursive path policy - should match children
				{"recursive, policypath(path1) == testpath(path1/child1)", 
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path1Child1, ExpectedResult.AuditEnabled.yes}, 
				{"recursive, policypath(path1) == testpath(path1/child2)", 
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path1Child2, ExpectedResult.AuditEnabled.yes}, 
				{"recursive, policypath(path1) == testpath(path1/child1)", 
					PolicyPath.path2, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path2Child1, ExpectedResult.AuditEnabled.yes}, 
				{"recursive, policypath(path1) == testpath(path1/child2)", 
					PolicyPath.path2, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path2Child2, ExpectedResult.AuditEnabled.yes}, 

				// recursive path policy - should match grand children, too!
				{"recursive, policypath(path1) == testpath(path1/child1/grandChild1)", 
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path1GrandChild1, ExpectedResult.AuditEnabled.yes}, 
				{"recursive, policypath(path1) == testpath(path1/child1/grandChild2)", 
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path1GrandChild2, ExpectedResult.AuditEnabled.yes}, 

				// recursive path policy - shouldn't match child in some other directory
				{"recursive, policypath(path1) == testpath(path1/child1)", 
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path2Child1, ExpectedResult.AuditEnabled.no}, 
				{"recursive, policypath(path1) == testpath(path1/child2)", 
					PolicyPath.path1, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path2Child2, ExpectedResult.AuditEnabled.no}, 
				{"recursive, policypath(path1) == testpath(path1/child1)", 
					PolicyPath.path2, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path1Child1, ExpectedResult.AuditEnabled.no}, 
				{"recursive, policypath(path1) == testpath(path1/child2)", 
					PolicyPath.path2, PolicyIs.notWildcard, PolicyIs.audited, PolicyIs.recursive, TestPath.path1Child2, ExpectedResult.AuditEnabled.no}, 

		};
		
		Object[][] wildCardTestData = new Object[][] {
				// Pattern contains exact substring
				{"Wildcard, Pattern contains substring of tested path - 1", 
					"aPath*", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "aPath", ExpectedResult.AuditEnabled.yes}, 
				{"Wildcard, Pattern contains substring of tested path - 2",
					"*aPath", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "aPath", ExpectedResult.AuditEnabled.yes}, 
				{"Wildcard, Pattern contains substring of tested path - 3",
					"aPa*th", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "aPath", ExpectedResult.AuditEnabled.yes}, 
				{"Wildcard, Pattern contains substring of tested path - 4",
					"aP*at*h", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "aPath", ExpectedResult.AuditEnabled.yes},

				// Pattern should match
				{"Wildcard, Pattern should match - 1",
					"aPath*", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "aPath_", ExpectedResult.AuditEnabled.yes},
				{"Wildcard, Pattern should match - 2",
					"aPath*", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "aPath_longSuffix", ExpectedResult.AuditEnabled.yes},
				{"Wildcard, Pattern should match - 3",
					"*aPath", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "_aPath", ExpectedResult.AuditEnabled.yes},
				{"Wildcard, Pattern should match - 4",
					"*aPath", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "longPrefix_aPath", ExpectedResult.AuditEnabled.yes},
				{"Wildcard, Pattern should match - 5",
					"*aPath", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "_aPath", ExpectedResult.AuditEnabled.yes},
				{"Wildcard, Pattern should match - 6",
					"*aPath", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "longPrefix_aPath", ExpectedResult.AuditEnabled.yes},
				{"Wildcard, Pattern should match - 5",
					"a*Path", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "a___Path", ExpectedResult.AuditEnabled.yes},
				{"Wildcard, Pattern should match - 6",
					"a*Path", PolicyIs.wildcard, PolicyIs.audited, PolicyIs.recursive, "aMiddlePath", ExpectedResult.AuditEnabled.yes},
		};
		
		// in the worst case all tests have a corresponding audit disabled test 
		List<Object[]> baseTests = Arrays.asList(baseTestData);
		List<Object[]> result = new ArrayList<Object[]>(baseTests);
		
		// answer is false no matter what if policy is set to not audit
		List<Object[]> additionalTests = disableAuditForBaseTests(baseTests);
		result.addAll(additionalTests);
		
		// turning wildcard flag on when policy path itself does not have wildcard characters in it shouldn't change the result!
		additionalTests = turnWildcardOnForNonWildcardTests(baseTests);
		result.addAll(additionalTests);
		
		List<Object[]> wildcardBaseTests = Arrays.asList(wildCardTestData);
		result.addAll(wildcardBaseTests);
		
		additionalTests = turnWildcardOffForTestsThatRequireWildcard(wildcardBaseTests);
		result.addAll(additionalTests);
		return result;
	}

	public URLBasedAuthDB_IsAuditLogEnabledByACL_PTest(String testName, String policyPath, boolean wildCard, boolean audited, boolean recursive, String testPath, boolean expectedResult) {
		_testName = testName;
		_policyPath = policyPath;
		_policyPathWildcard = wildCard;
		_policyAudited = audited;
		_policyRecursive = recursive;
		_testPath = testPath;
		_expectedResult = expectedResult;
	}
	
	private final String _testName;
	private final String _policyPath;
	private final boolean _policyPathWildcard;
	private final boolean _policyAudited;
	private final boolean _policyRecursive;
	private final String _testPath;
	private final boolean _expectedResult;
	
	@Test
	public void testIsAuditLogEnabledByACL() {
		
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Test: %sPolicy Path: %s, isWildcard: %b, isAudited: %b, isRecursive: %b, TestPath: %s",
					_testName, _policyPath, _policyPathWildcard, _policyAudited, _policyRecursive, _testPath));
		}

		// A policy can have several paths, so let's first stuff our path into a collection
		ResourcePath path = mock(ResourcePath.class);
		when(path.getPath()).thenReturn(_policyPath);
		when(path.isWildcardPath()).thenReturn(_policyPathWildcard);
		List<ResourcePath> resourcePaths = new ArrayList<ResourcePath>();
		resourcePaths.add(path);
		
		// wire it into the policy and set other aspects of the policy
		Policy aPolicy = mock(Policy.class);
		when(aPolicy.getResourceList()).thenReturn(resourcePaths);
		
		int recursiveIndicator = _policyRecursive ? 1 : 0;
		when(aPolicy.getRecursiveInd()).thenReturn(recursiveIndicator);
		
		int auditedIndicator = _policyAudited ? 1 : 0;
		when(aPolicy.getAuditInd()).thenReturn(auditedIndicator);

		// a container can have several policies to first we stuff our policy into a container
		List<Policy> policies = new ArrayList<Policy>();
		policies.add(aPolicy);
		// now wire the policy into the container
		PolicyContainer policyContainer = mock(PolicyContainer.class);
		when(policyContainer.getAcl()).thenReturn(policies);

		// finally wire the policy container into the authdb
		URLBasedAuthDB spy = spy(mAuthDB);
		when(spy.getPolicyContainer()).thenReturn(policyContainer);
		
		// assert the result
		boolean result = spy.isAuditLogEnabledByACL(_testPath);
		assertThat(_testName, result, is(_expectedResult));
		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format(", Expected Result (Audit enabled?): %b Result: %b\n", _expectedResult, result));
		}
	}

	private final URLBasedAuthDB mAuthDB = URLBasedAuthDB.getInstance();
	private static final Log LOG = LogFactory.getLog(URLBasedAuthDB_IsAuditLogEnabledByACL_PTest.class) ;
}
