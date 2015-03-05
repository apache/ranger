package org.apache.ranger.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.rest.RangerValidator.Action;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestRangerPolicyValidator {

	@Before
	public void setUp() throws Exception {
		_store = mock(ServiceStore.class);
		_policy = mock(RangerPolicy.class);
		_validator = new RangerPolicyValidator(_store);
		_serviceDef = mock(RangerServiceDef.class);
	}
	
	final Action[] cu = new Action[] { Action.CREATE, Action.UPDATE };
	final Object[] policyItemsData = new Object[] {
			ImmutableMap.of(  // all good
				"users", new String[] {"user1" ," user2"},
				"groups", new String[] {"group1", "group2"},
				"accesses", new String[] { "r", "w" },
				"isAllowed", new Boolean[] { true, true }),
			ImmutableMap.of(   // no users, access type different case
				"groups", new String[] {"group3", "group4"},
				"accesses", new String[]{"W", "x"}, 
				"isAllowed", new Boolean[] { true, true }),
			ImmutableMap.of(   // no groups
				"users", new String[] {"user3" ," user4"}, 
				"accesses", new String[] { "r", "x" },
				"isAllowed", new Boolean[] { true, true }),
			ImmutableMap.of( // isallowed on access types is null, case is different from that in definition
				"users", new String[] {"user7" ," user6"},
				"accesses", new String[] { "a" },
				"isAllowed", new Boolean[] { null, null })
	};
	String[] accessTypes = new String[] { "r", "w", "x", "A" };  // mix of lower and upper case
	String[] accessTypes_bad = new String[] { "r", "w", "xx", }; // two missing (x, a), one new that isn't on bad (xx)
	
	final Object[][] resourceDefData = new Object[][] {
			{ "db", true, "db\\d+" },        // valid: db1, db22, db983, etc.; invalid: db, db12x, ttx11, etc.
			{ "tbl", true, null },           // anything goes
			{ "col", false, "col\\d{1,2}" }  // valid: col1, col47, etc.; invalid: col, col238, col1, etc.
	};
	
	final Map<String, String[]> policyResourceMap_good = ImmutableMap.of(
			"db", new String[] { "db1", "db2" },
			"TBL", new String[] { "tbl1", "tbl2" } ); // case should not matter
	
	final Map<String, String[]> policyResourceMap_bad = ImmutableMap.of(
			"db", new String[] { "db1", "db2" },            // mandatory "tbl" missing
			"col", new String[] { "col12", "col 1" },       // wrong format of value for "col"
			"extra", new String[] { "extra1", "extra2" } ); // spurious "extra" specified

	@Test
	public final void testIsValid_long() throws Exception {
		// this validation should be removed if we start supporting other than delete action
		assertFalse(_validator.isValid(3L, Action.CREATE, _failures));
		_utils.checkFailureForInternalError(_failures);
		
		// should fail with appropriate error message if id is null
		_failures.clear(); _failures.clear(); assertFalse(_validator.isValid((Long)null, Action.DELETE, _failures));
		_utils.checkFailureForMissingValue(_failures, "id");
		
		// should fail with appropriate error message if policy can't be found for the specified id
		when(_store.getPolicy(1L)).thenReturn(null);
		when(_store.getPolicy(2L)).thenThrow(new Exception());
		RangerPolicy existingPolicy = mock(RangerPolicy.class);
		when(_store.getPolicy(3L)).thenReturn(existingPolicy);
		_failures.clear(); assertFalse(_validator.isValid(1L, Action.DELETE, _failures));
		_utils.checkFailureForSemanticError(_failures, "id");
		_failures.clear(); assertFalse(_validator.isValid(2L, Action.DELETE, _failures));
		_utils.checkFailureForSemanticError(_failures, "id");

		// if policy exists then delete validation should pass 
		assertTrue(_validator.isValid(3L, Action.DELETE, _failures));
	}
	
	@Test
	public final void testIsValid_happyPath() throws Exception {
		// valid policy has valid non-empty name and service name 
		when(_policy.getService()).thenReturn("service-name");
		// service name exists
		RangerService service = mock(RangerService.class);
		when(service.getType()).thenReturn("service-type");
		when(_store.getServiceByName("service-name")).thenReturn(service);
		// service points to a valid service-def
		_serviceDef = _utils.createServiceDefWithAccessTypes(accessTypes);
		when(_store.getServiceDefByName("service-type")).thenReturn(_serviceDef);
		// a matching policy should exist for create when checked by id and not exist when checked by name.
		when(_store.getPolicy(7L)).thenReturn(null);
		RangerPolicy existingPolicy = mock(RangerPolicy.class);
		when(existingPolicy.getId()).thenReturn(8L);
		when(_store.getPolicy(8L)).thenReturn(existingPolicy);
		SearchFilter createFilter = new SearchFilter();
		createFilter.setParam(SearchFilter.POLICY_NAME, "service-type");
		createFilter.setParam(SearchFilter.POLICY_NAME, "policy-name-1"); // this name would be used for create
		when(_store.getPolicies(createFilter)).thenReturn(new ArrayList<RangerPolicy>());
		// a matching policy should not exist for update.
		SearchFilter updateFilter = new SearchFilter();
		updateFilter.setParam(SearchFilter.POLICY_NAME, "service-type");
		updateFilter.setParam(SearchFilter.POLICY_NAME, "policy-name-2"); // this name would be used for update
		List<RangerPolicy> existingPolicies = new ArrayList<RangerPolicy>();
		existingPolicies.add(existingPolicy);
		when(_store.getPolicies(updateFilter)).thenReturn(existingPolicies);
		// valid policy can have empty set of policy items if audit is turned on
		// null value for audit is treated as audit on.
		for (Action action : cu) {
			for (Boolean auditEnabled : new Boolean[] { null, true } ) {
				when(_policy.getIsAuditEnabled()).thenReturn(auditEnabled);
				if (action == Action.CREATE) {
					when(_policy.getId()).thenReturn(7L);
					when(_policy.getName()).thenReturn("policy-name-1");
					assertTrue("" + action + ", " + auditEnabled, _validator.isValid(_policy, action, _failures));
					assertTrue(_failures.isEmpty());
				} else {
					// update should work both when by-name is found or not, since nothing found by-name means name is being updated.
					when(_policy.getId()).thenReturn(8L);
					when(_policy.getName()).thenReturn("policy-name-1");
					assertTrue("" + action + ", " + auditEnabled, _validator.isValid(_policy, action, _failures));
					assertTrue(_failures.isEmpty());

					when(_policy.getName()).thenReturn("policy-name-2");
					assertTrue("" + action + ", " + auditEnabled, _validator.isValid(_policy, action, _failures));
					assertTrue(_failures.isEmpty());
				}
			}
		}
		// if audit is disabled then policy should have policy items and all of them should be valid
		List<RangerPolicyItem> policyItems = _utils.createPolicyItems(policyItemsData);
		when(_policy.getPolicyItems()).thenReturn(policyItems);
		when(_policy.getIsAuditEnabled()).thenReturn(false);
		for (Action action : cu) {
			if (action == Action.CREATE) {
				when(_policy.getId()).thenReturn(7L);
				when(_policy.getName()).thenReturn("policy-name-1");
			} else {
				when(_policy.getId()).thenReturn(8L);
				when(_policy.getName()).thenReturn("policy-name-2");
			}
			assertTrue("" + action , _validator.isValid(_policy, action, _failures));
			assertTrue(_failures.isEmpty());
		}
		
		// above succeeded as service def did not have any resources on it, mandatory or otherwise.
		// policy should have all mandatory resources specified, and they should conform to the validation pattern in resource definition
		List<RangerResourceDef> resourceDefs = _utils.createResourceDefs(resourceDefData);
		when(_serviceDef.getResources()).thenReturn(resourceDefs);
		Map<String, RangerPolicyResource> resourceMap = _utils.createPolicyResourceMap(policyResourceMap_good);
		when(_policy.getResources()).thenReturn(resourceMap);

		for (Action action : cu) {
			if (action == Action.CREATE) {
				when(_policy.getId()).thenReturn(7L);
				when(_policy.getName()).thenReturn("policy-name-1");
			} else {
				when(_policy.getId()).thenReturn(8L);
				when(_policy.getName()).thenReturn("policy-name-2");
			}
			assertTrue("" + action , _validator.isValid(_policy, action, _failures));
			assertTrue(_failures.isEmpty());
		}
	}
	
	void checkFailure_isValid(Action action, String errorType, String field) {
		checkFailure_isValid(action, errorType, field, null);
	}
	
	void checkFailure_isValid(Action action, String errorType, String field, String subField) {
		_failures.clear();
		assertFalse(_validator.isValid(_policy, action, _failures));
		switch (errorType) {
		case "missing":
			_utils.checkFailureForMissingValue(_failures, field, subField);
			break;
		case "semantic":
			_utils.checkFailureForSemanticError(_failures, field, subField);
			break;
		case "internal error":
			_utils.checkFailureForInternalError(_failures);
			break;
		default:
			fail("Unsupported errorType[" + errorType + "]");
			break;
		}
	}
	
	@Test
	public final void testIsValid_failures() throws Exception {
		for (Action action : cu) {
			// passing in a null policy should fail with appropriate failure reason
			_policy = null;
			checkFailure_isValid(action, "missing", "policy");
			
			// policy must have a name on it
			_policy = mock(RangerPolicy.class);
			for (String name : new String[] { null, "  " }) {
				when(_policy.getName()).thenReturn(name);
				checkFailure_isValid(action, "missing", "name");
			}
			
			// for update id is required!
			if (action == Action.UPDATE) {
				when(_policy.getId()).thenReturn(null);
				checkFailure_isValid(action, "missing", "id");
			}
		}
		/*
		 * Id is ignored for Create but name should not belong to an existing policy.  For update, policy should exist for its id and should match its name.
		 */
		when(_policy.getName()).thenReturn("policy-name");
		when(_policy.getService()).thenReturn("service-name");

		RangerPolicy existingPolicy = mock(RangerPolicy.class);
		when(existingPolicy.getId()).thenReturn(7L);
		List<RangerPolicy> existingPolicies = new ArrayList<RangerPolicy>();
		existingPolicies.add(existingPolicy);
		SearchFilter filter = new SearchFilter();
		filter.setParam(SearchFilter.SERVICE_NAME, "service-name");
		filter.setParam(SearchFilter.POLICY_NAME, "policy-name");
		when(_store.getPolicies(filter)).thenReturn(existingPolicies);
		checkFailure_isValid(Action.CREATE, "semantic", "name");
		
		// update : does not exist for id
		when(_policy.getId()).thenReturn(7L);
		when(_store.getPolicy(7L)).thenReturn(null);
		checkFailure_isValid(Action.UPDATE, "semantic", "id");

		// Update: name should not point to an existing different policy, i.e. with a different id
		when(_store.getPolicy(7L)).thenReturn(existingPolicy);
		RangerPolicy anotherExistingPolicy = mock(RangerPolicy.class);
		when(anotherExistingPolicy.getId()).thenReturn(8L);
		existingPolicies.clear();
		existingPolicies.add(anotherExistingPolicy);
		when(_store.getPolicies(filter)).thenReturn(existingPolicies);
		checkFailure_isValid(Action.UPDATE, "semantic", "id/name");

		// more than one policies with same name is also an internal error
		when(_policy.getName()).thenReturn("policy-name");
		when(_store.getPolicies(filter)).thenReturn(existingPolicies);
		existingPolicies.add(existingPolicy);
		existingPolicy = mock(RangerPolicy.class);
		existingPolicies.add(existingPolicy);
		_failures.clear(); assertFalse(_validator.isValid(_policy, Action.UPDATE, _failures));
		_utils.checkFailureForInternalError(_failures);
		
		// policy must have service name on it and it should be valid
		when(_policy.getName()).thenReturn("policy-name");
		when(_store.getServiceByName("service-name")).thenReturn(null);
		when(_store.getServiceByName("another-service-name")).thenThrow(new Exception());

		for (Action action : cu) {
			when(_policy.getService()).thenReturn("service-name");
			_failures.clear(); assertFalse(_validator.isValid(_policy, action, _failures));
			_utils.checkFailureForMissingValue(_failures, "service");

			when(_policy.getService()).thenReturn("another-service-name");
			_failures.clear(); assertFalse(_validator.isValid(_policy, action, _failures));
			_utils.checkFailureForMissingValue(_failures, "service");
		}
		
		// policy must contain at least one policy item
		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicy.RangerPolicyItem>();
		when(_policy.getService()).thenReturn("service-name");
		RangerService service = mock(RangerService.class);
		when(_store.getServiceByName("service-name")).thenReturn(service);
		for (Action action : cu) {
			// when it is null
			when(_policy.getPolicyItems()).thenReturn(null);
			_failures.clear(); assertFalse(_validator.isValid(_policy, action, _failures));
			_utils.checkFailureForMissingValue(_failures, "policy items");
			// or when it is not null but empty.
			when(_policy.getPolicyItems()).thenReturn(policyItems);
			_failures.clear(); assertFalse(_validator.isValid(_policy, action, _failures));
			_utils.checkFailureForMissingValue(_failures, "policy items");
		}
		
		// these are known good policy items -- same as used above in happypath
		policyItems = _utils.createPolicyItems(policyItemsData);
		when(_policy.getPolicyItems()).thenReturn(policyItems);
		// policy item check requires that service def should exist
		when(service.getType()).thenReturn("service-type");
		when(_store.getServiceDefByName("service-type")).thenReturn(null);
		for (Action action : cu) {
			_failures.clear(); assertFalse(_validator.isValid(_policy, action, _failures));
			_utils.checkFailureForInternalError(_failures, "policy service def");
		}
		
		// service-def should contain the right access types on it.
		_serviceDef = _utils.createServiceDefWithAccessTypes(accessTypes_bad);
		when(_store.getServiceDefByName("service-type")).thenReturn(_serviceDef);
		for (Action action : cu) {
			_failures.clear(); assertFalse(_validator.isValid(_policy, action, _failures));
			_utils.checkFailureForSemanticError(_failures, "policy item access type");
		}

		// create the right service def with right resource defs - this is the same as in the happypath test above.
		_serviceDef = _utils.createServiceDefWithAccessTypes(accessTypes);
		when(_store.getPolicies(filter)).thenReturn(null);
		List<RangerResourceDef> resourceDefs = _utils.createResourceDefs(resourceDefData);
		when(_serviceDef.getResources()).thenReturn(resourceDefs);
		when(_store.getServiceDefByName("service-type")).thenReturn(_serviceDef);
		// one mandtory is missing (tbl) and one unknown resource is specified (extra), and values of option resource don't conform to validation pattern (col)
		Map<String, RangerPolicyResource> policyResources = _utils.createPolicyResourceMap(policyResourceMap_bad);
		when(_policy.getResources()).thenReturn(policyResources);
		for (Action action : cu) {
			_failures.clear(); assertFalse(_validator.isValid(_policy, action, _failures));
			_utils.checkFailureForMissingValue(_failures, "resources", "tbl"); // for missing resource: tbl
			_utils.checkFailureForSemanticError(_failures, "resources", "extra"); // for spurious resource: "extra"
			_utils.checkFailureForSemanticError(_failures, "resource-values", "col"); // for spurious resource: "extra"
		}
	}
	
	@Test
	public void test_isValidResourceValues() {
		List<RangerResourceDef> resourceDefs = _utils.createResourceDefs(resourceDefData);
		when(_serviceDef.getResources()).thenReturn(resourceDefs);
		Map<String, RangerPolicyResource> policyResources = _utils.createPolicyResourceMap(policyResourceMap_bad);
		assertFalse(_validator.isValidResourceValues(policyResources, _failures, _serviceDef));
		_utils.checkFailureForSemanticError(_failures, "resource-values", "col");
		
		policyResources = _utils.createPolicyResourceMap(policyResourceMap_good);
		assertTrue(_validator.isValidResourceValues(policyResources, _failures, _serviceDef));
	}
	
	@Test
	public void test_isValidPolicyItems_failures() {
		// null/empty list is good because there is nothing
		assertTrue(_validator.isValidPolicyItems(null, _failures, _serviceDef));
		_failures.isEmpty();

		List<RangerPolicyItem> policyItems = new ArrayList<RangerPolicy.RangerPolicyItem>();
		assertTrue(_validator.isValidPolicyItems(policyItems, _failures, _serviceDef));
		_failures.isEmpty();
		
		// null elements in the list are flagged
		policyItems.add(null);
		assertFalse(_validator.isValidPolicyItems(policyItems, _failures, _serviceDef));
		_utils.checkFailureForMissingValue(_failures, "policy item");
	}
	
	@Test
	public void test_isValidPolicyItem_failures() {

		// empty access collections are invalid
		RangerPolicyItem policyItem = mock(RangerPolicyItem.class);
		when(policyItem.getAccesses()).thenReturn(null);
		_failures.clear(); assertFalse(_validator.isValidPolicyItem(policyItem, _failures, _serviceDef));
		_utils.checkFailureForMissingValue(_failures, "policy item accesses");

		List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicy.RangerPolicyItemAccess>();
		when(policyItem.getAccesses()).thenReturn(accesses);
		_failures.clear(); assertFalse(_validator.isValidPolicyItem(policyItem, _failures, _serviceDef));
		_utils.checkFailureForMissingValue(_failures, "policy item accesses");
		
		// both user and groups can't be null
		RangerPolicyItemAccess access = mock(RangerPolicyItemAccess.class);
		accesses.add(access);
		when(policyItem.getUsers()).thenReturn(null);
		when(policyItem.getGroups()).thenReturn(new ArrayList<String>());
		_failures.clear(); assertFalse(_validator.isValidPolicyItem(policyItem, _failures, _serviceDef));
		_utils.checkFailureForMissingValue(_failures, "policy item users/user-groups");
	}
	
	@Test
	public void test_isValidItemAccesses_happyPath() {
		
		// happy path
		Object[][] data = new Object[][] {
				{ "a", null }, // valid
				{ "b", true }, // valid
				{ "c", true }, // valid
		};
		List<RangerPolicyItemAccess> accesses = _utils.createItemAccess(data);
		_serviceDef = _utils.createServiceDefWithAccessTypes(new String[] { "a", "b", "c", "d" });
		assertTrue(_validator.isValidItemAccesses(accesses, _failures, _serviceDef));
		assertTrue(_failures.isEmpty());
	}
	
	@Test
	public void test_isValidItemAccesses_failure() {

		// null policy item access values are an error
		List<RangerPolicyItemAccess> accesses = new ArrayList<RangerPolicyItemAccess>();
		accesses.add(null);
		_failures.clear(); assertFalse(_validator.isValidItemAccesses(accesses, _failures, _serviceDef));
		_utils.checkFailureForMissingValue(_failures, "policy item access");

		// all items must be valid for this call to be valid
		Object[][] data = new Object[][] {
				{ "a", null }, // valid
				{ null, null }, // invalid - name can't be null
				{ "c", true }, // valid
		};
		accesses = _utils.createItemAccess(data);
		_serviceDef = _utils.createServiceDefWithAccessTypes(new String[] { "a", "b", "c", "d" });
		_failures.clear(); assertFalse(_validator.isValidItemAccesses(accesses, _failures, _serviceDef));
	}
	
	@Test
	public void test_isValidPolicyItemAccess_happyPath() {
		
		RangerPolicyItemAccess access = mock(RangerPolicyItemAccess.class);
		when(access.getType()).thenReturn("an-Access"); // valid

		Set<String> validAccesses = Sets.newHashSet(new String[] { "an-access", "another-access" });  // valid accesses should be lower-cased
		
		// both null or true access types are the same and valid
		for (Boolean allowed : new Boolean[] { null, true } ) {
			when(access.getIsAllowed()).thenReturn(allowed);
			assertTrue(_validator.isValidPolicyItemAccess(access, _failures, validAccesses));
			assertTrue(_failures.isEmpty());
		}
	}
	
	@Test
	public void test_isValidPolicyItemAccess_failures() {
		
		Set<String> validAccesses = Sets.newHashSet(new String[] { "anAccess", "anotherAccess" });
		// null/empty names are invalid
		RangerPolicyItemAccess access = mock(RangerPolicyItemAccess.class);
		when(access.getIsAllowed()).thenReturn(null); // valid since null == true
		for (String type : new String[] { null, " 	"}) {
			when(access.getType()).thenReturn(type); // invalid
			// null/empty validAccess set skips all checks
			assertTrue(_validator.isValidPolicyItemAccess(access, _failures, null));
			assertTrue(_validator.isValidPolicyItemAccess(access, _failures, new HashSet<String>()));
			_failures.clear(); assertFalse(_validator.isValidPolicyItemAccess(access, _failures, validAccesses));
			_utils.checkFailureForMissingValue(_failures, "policy item access type");
		}
		
		when(access.getType()).thenReturn("anAccess"); // valid
		when(access.getIsAllowed()).thenReturn(false); // invalid
		_failures.clear();assertFalse(_validator.isValidPolicyItemAccess(access, _failures, validAccesses));
		_utils.checkFailureForSemanticError(_failures, "policy item access type allowed");
		
		when(access.getType()).thenReturn("newAccessType"); // invalid
		_failures.clear(); assertFalse(_validator.isValidPolicyItemAccess(access, _failures, validAccesses));
		_utils.checkFailureForSemanticError(_failures, "policy item access type");
	}

	private ValidationTestUtils _utils = new ValidationTestUtils();
	private List<ValidationFailureDetails> _failures = new ArrayList<ValidationFailureDetails>();
	private ServiceStore _store;
	private RangerPolicy _policy;
	private RangerPolicyValidator _validator;
	private RangerServiceDef _serviceDef;
}
