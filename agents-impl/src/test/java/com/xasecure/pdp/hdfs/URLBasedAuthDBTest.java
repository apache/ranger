package com.xasecure.pdp.hdfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.xasecure.pdp.model.Policy;
import com.xasecure.pdp.model.PolicyContainer;
import com.xasecure.pdp.model.ResourcePath;

public class URLBasedAuthDBTest {

	@Test
	public void testIsAuditLogEnabledByACL_emptyPolicyContainer() {

		// audit can't be enabled if authdb isn't initialized 
		assertFalse(mAuthDB.isAuditLogEnabledByACL("blah"));
		
		// or if the policy container in is null!
		URLBasedAuthDB spy = spy(mAuthDB);
		when(spy.getPolicyContainer()).thenReturn(null);
		assertFalse(mAuthDB.isAuditLogEnabledByACL("blah"));
		
		// of if policy container is empty, i.e. has no policies!
		List<Policy> policies = new ArrayList<Policy>();
		PolicyContainer policyContainer = mock(PolicyContainer.class);
		when(policyContainer.getAcl()).thenReturn(policies);
		when(spy.getPolicyContainer()).thenReturn(policyContainer);
		assertFalse(mAuthDB.isAuditLogEnabledByACL("blah"));
		
		// or if all policies are empty, i.e. no acls!
		Policy aPolicy = mock(Policy.class);
		when(aPolicy.getResourceList()).thenReturn(new ArrayList<ResourcePath>());
		policies.add(aPolicy);
		when(policyContainer.getAcl()).thenReturn(policies);
		when(spy.getPolicyContainer()).thenReturn(policyContainer);
		assertFalse(spy.isAuditLogEnabledByACL("blah"));
	}
	
	private final URLBasedAuthDB mAuthDB = URLBasedAuthDB.getInstance();	
}
