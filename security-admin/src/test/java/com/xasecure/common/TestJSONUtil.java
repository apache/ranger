package com.xasecure.common;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;
import org.springframework.transaction.annotation.Transactional;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestJSONUtil {

	@Autowired
	JSONUtil jsonUtil = new JSONUtil();

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testJsonToMapNull() {
		String jsonStr = null;
		Map<String, String> dbMap = jsonUtil.jsonToMap(jsonStr);
		Assert.assertEquals(dbMap.get(jsonStr), jsonStr);
	}

	@Test
	public void testJsonToMapIsEmpty() {
		String jsonStr = "";
		Map<String, String> dbMap = jsonUtil.jsonToMap(jsonStr);
		boolean isEmpty = dbMap.isEmpty();
		Assert.assertTrue(isEmpty);
	}

	@Test
	public void testJsonToMap() {
		String jsonStr = "{\"username\":\"admin\",\"password\":\"admin\",\"fs.default.name\":\"defaultnamevalue\",\"hadoop.security.authorization\":\"authvalue\",\"hadoop.security.authentication\":\"authenticationvalue\",\"hadoop.security.auth_to_local\":\"localvalue\",\"dfs.datanode.kerberos.principal\":\"principalvalue\",\"dfs.namenode.kerberos.principal\":\"namenodeprincipalvalue\",\"dfs.secondary.namenode.kerberos.principal\":\"secprincipalvalue\",\"commonNameForCertificate\":\"certificatevalue\"}";
		Map<String, String> dbMap = jsonUtil.jsonToMap(jsonStr);
	    Assert.assertNotNull(dbMap);
	}

	@Test
	public void testReadMapToString() {		
		Map<?, ?> map = new HashMap<Object, Object>();
		String value = jsonUtil.readMapToString(map);
		Assert.assertNotNull(value);
	}	
}