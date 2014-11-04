package com.xasecure.pdp.hdfs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.xasecure.pdp.model.Policy;
import com.xasecure.pdp.model.PolicyContainer;
import com.xasecure.pdp.model.ResourcePath;

public class URLBasedAuthDBTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testIsAuditLogEnabledByACL() {

		// if authdb isn't initialized then return false
		URLBasedAuthDB authDB = URLBasedAuthDB.getInstance();
		assertFalse(authDB.isAuditLogEnabledByACL("blah"));
		
		// Policy container with empty acl list is the same!
		URLBasedAuthDB spy = spy(authDB);
		PolicyContainer policyContainer = mock(PolicyContainer.class);
		List<Policy> policies = new ArrayList<Policy>();
		when(policyContainer.getAcl()).thenReturn(policies);
		
		when(spy.getPolicyContainer()).thenReturn(policyContainer);
		assertFalse(spy.isAuditLogEnabledByACL("blah"));
		
		// or a non-empty acl with empty resource lists!
		Policy aPolicy = mock(Policy.class);
		when(aPolicy.getResourceList()).thenReturn(new ArrayList<ResourcePath>());
		policies.add(aPolicy);
		assertFalse(spy.isAuditLogEnabledByACL("blah"));
		
		// setup a resource non-recursive path
		ResourcePath path = mock(ResourcePath.class);
		when(path.getPath()).thenReturn("aPath");
		when(path.isWildcardPath()).thenReturn(false);
		
		// build a resource list with this path
		List<ResourcePath> resourcePaths = new ArrayList<ResourcePath>();
		resourcePaths.add(path);
		when(aPolicy.getResourceList()).thenReturn(resourcePaths);
		// let the ACL not be recursive either
		when(aPolicy.getRecursiveInd()).thenReturn(0);
		when(aPolicy.getAuditInd()).thenReturn(1);
		assertFalse(spy.isAuditLogEnabledByACL("blah"));
		// right path matches
		assertTrue(spy.isAuditLogEnabledByACL("aPath"));
	}
	
}
