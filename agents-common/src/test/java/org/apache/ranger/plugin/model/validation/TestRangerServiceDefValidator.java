package org.apache.ranger.plugin.model.validation;


import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.validation.RangerServiceDefValidator;
import org.apache.ranger.plugin.model.validation.ValidationFailureDetails;
import org.apache.ranger.plugin.model.validation.RangerValidator.Action;
import org.apache.ranger.plugin.store.ServiceStore;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class TestRangerServiceDefValidator {

	@Before
	public void setUp() throws Exception {
		_store = mock(ServiceStore.class);
		_validator = new RangerServiceDefValidator(_store);
		_failures = new ArrayList<ValidationFailureDetails>();
		_serviceDef = mock(RangerServiceDef.class);
	}

	final Action[] cu = new Action[] { Action.CREATE, Action.UPDATE };
	
	final Object[][] accessTypes_good = new Object[][] {
			{ "read",  null },                                // ok, null implied grants
			{ "write", new String[] {   } },                  // ok, empty implied grants
			{ "admin", new String[] { "READ",  "write" } }    // ok, admin access implies read/write, access types are case-insensitive
	};

	final Map<String, String[]> enums_good = ImmutableMap.of(
			"authentication-type", new String[] { "simple", "kerberos" },
			"time-unit", new String[] { "day", "hour", "minute" }
	);
	
	@Test
	public final void test_isValid_happyPath_create() throws Exception {
		
		// setup access types with implied access and couple of enums
		List<RangerAccessTypeDef> accessTypeDefs = _utils.createAccessTypeDefs(accessTypes_good);
		when(_serviceDef.getAccessTypes()).thenReturn(accessTypeDefs);
		List<RangerEnumDef> enumDefs = _utils.createEnumDefs(enums_good);
		when(_serviceDef.getEnums()).thenReturn(enumDefs);

		// create: id is not relevant, name should not conflict 
		when(_serviceDef.getId()).thenReturn(null); // id is not relevant for create
		when(_serviceDef.getName()).thenReturn("aServiceDef"); // service has a name
		when(_store.getServiceDefByName("aServiceDef")).thenReturn(null); // no name collision
		assertTrue(_validator.isValid(_serviceDef, Action.CREATE, _failures));
		assertTrue(_failures.isEmpty());
		
		// update: id should match existing service, name should not point to different service def
		when(_serviceDef.getId()).thenReturn(5L);
		RangerServiceDef existingServiceDef = mock(RangerServiceDef.class);
		when(_store.getServiceDef(5L)).thenReturn(existingServiceDef);
		assertTrue(_validator.isValid(_serviceDef, Action.UPDATE, _failures));
		assertTrue(_failures.isEmpty());
		
		// update: if name points to a service that it's id should be the same
		RangerServiceDef anotherExistingServiceDef = mock(RangerServiceDef.class);
		when(anotherExistingServiceDef.getId()).thenReturn(5L);
		when(_store.getServiceDefByName("aServiceDef")).thenReturn(anotherExistingServiceDef);
		assertTrue(_validator.isValid(_serviceDef, Action.UPDATE, _failures));
		assertTrue(_failures.isEmpty());
	}
	
	@Test
	public final void testIsValid_Long_failures() throws Exception {
		Long id = null;
		// passing in wrong action type 
		boolean result = _validator.isValid((Long)null, Action.CREATE, _failures);
		assertFalse(result);
		_utils.checkFailureForInternalError(_failures);
		// passing in null id is an error
		_failures.clear(); assertFalse(_validator.isValid((Long)null, Action.DELETE, _failures));
		_utils.checkFailureForMissingValue(_failures, "id");
		// a service def with that id should exist, else it is an error
		id = 3L;
		when(_store.getServiceDef(id)).thenReturn(null);
		_failures.clear(); assertFalse(_validator.isValid(id, Action.DELETE, _failures));
		_utils.checkFailureForSemanticError(_failures, "id");
		// happypath
		when(_store.getServiceDef(id)).thenReturn(_serviceDef);
		_failures.clear(); assertTrue(_validator.isValid(id, Action.DELETE, _failures));
		assertTrue(_failures.isEmpty());
	}

	@Test
	public final void testIsValid_failures_name() throws Exception {
		// null service def and bad service def name
		for (Action action : cu) {
			// passing in null service def is an error
			assertFalse(_validator.isValid((RangerServiceDef)null, action, _failures));
			_utils.checkFailureForMissingValue(_failures, "service def");
			// name should be valid
			for (String name : new String[] { null, "", "  " }) {
				when(_serviceDef.getName()).thenReturn(name);
				_failures.clear(); assertFalse(_validator.isValid(_serviceDef, action, _failures));
				_utils.checkFailureForMissingValue(_failures, "name");
			}
		}
	}
	
	@Test
	public final void testIsValid_failures_id() throws Exception {
		// id is required for update
		when(_serviceDef.getId()).thenReturn(null);
		assertFalse(_validator.isValid(_serviceDef, Action.UPDATE, _failures));
		_utils.checkFailureForMissingValue(_failures, "id");
		
		// update: service should exist for the passed in id
		Long id = 7L;
		when(_serviceDef.getId()).thenReturn(id);
		when(_store.getServiceDef(id)).thenReturn(null);
		assertFalse(_validator.isValid(_serviceDef, Action.UPDATE, _failures));
		_utils.checkFailureForSemanticError(_failures, "id");

		when(_store.getServiceDef(id)).thenThrow(new Exception());
		assertFalse(_validator.isValid(_serviceDef, Action.UPDATE, _failures));
		_utils.checkFailureForSemanticError(_failures, "id");
	}
	
	@Test
	public final void testIsValid_failures_nameId_create() throws Exception {
		// service shouldn't exist with the name
		RangerServiceDef existingServiceDef = mock(RangerServiceDef.class);
		when(_store.getServiceDefByName("existing-service")).thenReturn(existingServiceDef);
		when(_serviceDef.getName()).thenReturn("existing-service");
		_failures.clear(); assertFalse(_validator.isValid(_serviceDef, Action.CREATE, _failures));
		_utils.checkFailureForSemanticError(_failures, "name");
	}
	
	@Test
	public final void testIsValid_failures_nameId_update() throws Exception {
		
		// update: if service exists with the same name then it can't point to a different service
		Long id = 7L;
		when(_serviceDef.getId()).thenReturn(id);
		RangerServiceDef existingServiceDef = mock(RangerServiceDef.class);
		when(existingServiceDef.getId()).thenReturn(id);
		when(_store.getServiceDef(id)).thenReturn(existingServiceDef);
		
		String name = "aServiceDef";
		when(_serviceDef.getName()).thenReturn(name);
		RangerServiceDef anotherExistingServiceDef = mock(RangerServiceDef.class);
		Long anotherId = 49L;
		when(anotherExistingServiceDef.getId()).thenReturn(anotherId);
		when(_store.getServiceDefByName(name)).thenReturn(anotherExistingServiceDef);
		
		assertFalse(_validator.isValid(_serviceDef, Action.UPDATE, _failures));
		_utils.checkFailureForSemanticError(_failures, "id/name");
	}

	final Object[][] accessTypes_bad_unknownType = new Object[][] {
			{ "read",  null },                                // ok, null implied grants
			{ "write", new String[] {   } },                  // ok, empty implied grants
			{ "admin", new String[] { "ReaD",  "execute" } }  // non-existent access type (execute), read is good (case should not matter)
	};

	final Object[][] accessTypes_bad_selfReference = new Object[][] {
			{ "read",  null },                                // ok, null implied grants
			{ "write", new String[] {   } },                  // ok, empty implied grants
			{ "admin", new String[] { "write", "admin" } }  // non-existent access type (execute)
	};

	@Test
	public final void test_isValidAccessTypes_happyPath() {
		List<RangerAccessTypeDef> input = _utils.createAccessTypeDefs(accessTypes_good);
		assertTrue(_validator.isValidAccessTypes(input, _failures));
		assertTrue(_failures.isEmpty());
	}
	
	@Test
	public final void test_isValidAccessTypes_failures() {
		// sending in empty null access type defs is ok
		assertTrue(_validator.isValidAccessTypes(null, _failures));
		assertTrue(_failures.isEmpty());
		
		List<RangerAccessTypeDef> input = new ArrayList<RangerAccessTypeDef>();
		_failures.clear(); assertTrue(_validator.isValidAccessTypes(input, _failures));
		assertTrue(_failures.isEmpty());

		// null/empty access types
		List<RangerAccessTypeDef> accessTypeDefs = _utils.createAccessTypeDefs(new String[] { null, "", "		" });
		_failures.clear(); assertFalse(_validator.isValidAccessTypes(accessTypeDefs, _failures));
		_utils.checkFailureForMissingValue(_failures, "access type name");
		
		// duplicate access types
		accessTypeDefs = _utils.createAccessTypeDefs(new String[] { "read", "write", "execute", "read" } );
		_failures.clear(); assertFalse(_validator.isValidAccessTypes(accessTypeDefs, _failures));
		_utils.checkFailureForSemanticError(_failures, "access type name", "read");
		
		// duplicate access types - case-insensitive
		accessTypeDefs = _utils.createAccessTypeDefs(new String[] { "read", "write", "execute", "READ" } );
		_failures.clear(); assertFalse(_validator.isValidAccessTypes(accessTypeDefs, _failures));
		_utils.checkFailureForSemanticError(_failures, "access type name", "READ");
		
		// unknown access type in implied grants list
		accessTypeDefs = _utils.createAccessTypeDefs(accessTypes_bad_unknownType);
		_failures.clear(); assertFalse(_validator.isValidAccessTypes(accessTypeDefs, _failures));
		_utils.checkFailureForSemanticError(_failures, "implied grants", "execute");
		
		// access type with implied grant referring to itself
		accessTypeDefs = _utils.createAccessTypeDefs(accessTypes_bad_selfReference);
		_failures.clear(); assertFalse(_validator.isValidAccessTypes(accessTypeDefs, _failures));
		_utils.checkFailureForSemanticError(_failures, "implied grants", "admin");
	}
	
	final Map<String, String[]> enums_bad_enumName_null = ImmutableMap.of(
			"authentication-type", new String[] { "simple", "kerberos" },
			"time-unit", new String[] { "day", "hour", "minute" },
			"null", new String[] { "foo", "bar", "tar" } // null enum-name -- "null" is a special value that leads to a null enum name
	);
	
	final Map<String, String[]> enums_bad_enumName_blank = ImmutableMap.of(
			"authentication-type", new String[] { "simple", "kerberos" },
			"time-unit", new String[] { "day", "hour", "minute" },
			"  ", new String[] { "foo", "bar", "tar" } // enum name is all spaces
	);
	
	final Map<String, String[]> enums_bad_Elements_empty = ImmutableMap.of(
			"authentication-type", new String[] { "simple", "kerberos" },
			"time-unit", new String[] { "day", "hour", "minute" },
			"anEnum", new String[] { } // enum elements collection is empty
	);
	
	final Map<String, String[]> enums_bad_enumName_duplicate_exact = ImmutableMap.of(
			"authentication-type", new String[] { "simple", "kerberos" },
			"time-unit", new String[] { "day", "hour", "minute" }
	);
	
	final Map<String, String[]> enums_bad_enumName_duplicate_differentCase = ImmutableMap.of(
			"authentication-type", new String[] { "simple", "kerberos" },
			"time-unit", new String[] { "day", "hour", "minute" },
			"Authentication-Type", new String[] { } // duplicate enum-name different in case
	);
	
	@Test
	public final void test_isValidEnums_happyPath() {
		List<RangerEnumDef> input = _utils.createEnumDefs(enums_good);
		assertTrue(_validator.isValidEnums(input, _failures));
		assertTrue(_failures.isEmpty());
	}
	
	@Test
	public final void test_isValidEnums_failures() {
		// null elements in enum def list are a failure
		List<RangerEnumDef> input = _utils.createEnumDefs(enums_good);
		input.add(null);
		assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForMissingValue(_failures, "enum def");
		
		// enum names should be valid
		input = _utils.createEnumDefs(enums_bad_enumName_null);
		_failures.clear(); assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForMissingValue(_failures, "enum def name");

		input = _utils.createEnumDefs(enums_bad_enumName_blank);
		_failures.clear(); assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForMissingValue(_failures, "enum def name");
		
		// enum elements collection should not be null or empty
		input = _utils.createEnumDefs(enums_good);
		RangerEnumDef anEnumDef = mock(RangerEnumDef.class);
		when(anEnumDef.getName()).thenReturn("anEnum");
		when(anEnumDef.getElements()).thenReturn(null);
		input.add(anEnumDef);
		_failures.clear(); assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForMissingValue(_failures, "enum values", "anEnum");

		input = _utils.createEnumDefs(enums_bad_Elements_empty);
		_failures.clear(); assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForMissingValue(_failures, "enum values", "anEnum");
	
		// enum names should be distinct -- exact match
		input = _utils.createEnumDefs(enums_good);
		// add an element with same name as the first element
		String name = input.iterator().next().getName();
		when(anEnumDef.getName()).thenReturn(name);
		List<RangerEnumElementDef> elementDefs = _utils.createEnumElementDefs(new String[] {"val1", "val2"}); 
		when(anEnumDef.getElements()).thenReturn(elementDefs);
		input.add(anEnumDef);
		_failures.clear(); assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForSemanticError(_failures, "enum def name", name);

		// enum names should be distinct -- case insensitive
		input = _utils.createEnumDefs(enums_bad_enumName_duplicate_differentCase);
		_failures.clear(); assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForSemanticError(_failures, "enum def name", "Authentication-Type");
	
		// enum default index should be right
		input = _utils.createEnumDefs(enums_good);
		// set the index of 1st on to be less than 0
		when(input.iterator().next().getDefaultIndex()).thenReturn(-1);
		_failures.clear(); assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForSemanticError(_failures, "enum default index", "authentication-type");
		// set the index to be more than number of elements
		when(input.iterator().next().getDefaultIndex()).thenReturn(2);
		_failures.clear(); assertFalse(_validator.isValidEnums(input, _failures));
		_utils.checkFailureForSemanticError(_failures, "enum default index", "authentication-type");
	}

	@Test
	public final void test_isValidEnumElements_happyPath() {
		List<RangerEnumElementDef> input = _utils.createEnumElementDefs(new String[] { "simple", "kerberos" });
		assertTrue(_validator.isValidEnumElements(input, _failures, "anEnum"));
		assertTrue(_failures.isEmpty());
	}

	@Test
	public final void test_isValidEnumElements_failures() {
		// enum element collection should not have nulls in it
		List<RangerEnumElementDef> input = _utils.createEnumElementDefs(new String[] { "simple", "kerberos" });
		input.add(null);
		assertFalse(_validator.isValidEnumElements(input, _failures, "anEnum"));
		_utils.checkFailureForMissingValue(_failures, "enum element", "anEnum");

		// element names can't be null/empty
		input = _utils.createEnumElementDefs(new String[] { "simple", "kerberos", null });
		_failures.clear(); assertFalse(_validator.isValidEnumElements(input, _failures, "anEnum"));
		_utils.checkFailureForMissingValue(_failures, "enum element name", "anEnum");

		input = _utils.createEnumElementDefs(new String[] { "simple", "kerberos", "		" }); // two tabs
		_failures.clear(); assertFalse(_validator.isValidEnumElements(input, _failures, "anEnum"));
		_utils.checkFailureForMissingValue(_failures, "enum element name", "anEnum");
		
		// element names should be distinct - case insensitive
		input = _utils.createEnumElementDefs(new String[] { "simple", "kerberos", "kerberos" }); // duplicate name - exact match
		_failures.clear(); assertFalse(_validator.isValidEnumElements(input, _failures, "anEnum"));
		_utils.checkFailureForSemanticError(_failures, "enum element name", "anEnum");
		
		input = _utils.createEnumElementDefs(new String[] { "simple", "kerberos", "kErbErOs" }); // duplicate name - different case
		_failures.clear(); assertFalse(_validator.isValidEnumElements(input, _failures, "anEnum"));
		_utils.checkFailureForSemanticError(_failures, "enum element name", "anEnum");
	}
	
	private ValidationTestUtils _utils = new ValidationTestUtils();
	RangerServiceDef _serviceDef;
	List<ValidationFailureDetails> _failures;
	ServiceStore _store;
	RangerServiceDefValidator _validator;
}
