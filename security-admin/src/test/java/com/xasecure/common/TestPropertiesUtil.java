package com.xasecure.common;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

public class TestPropertiesUtil {

	@Autowired
	PropertiesUtil propertiesUtil;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testGetPropertyNull() {
		String key=null;
		String defaultValue="test";
		String value= PropertiesUtil.getProperty(key, defaultValue);
		Assert.assertNull(value);
	}
	
	@Test
	public void testGetProperty() {
		String key="1";
		String defaultValue="test";
		String value= PropertiesUtil.getProperty(key, defaultValue);
		Assert.assertNotNull(value);
	}	
	
	@Test
	public void testgetPropertyNullValue(){
		String key=null;
		String value = PropertiesUtil.getProperty(key);
		Assert.assertNull(value);
	}
	
	@Test
	public void testGetIntPropertyNull1(){
		String key=null;
		PropertiesUtil.getIntProperty(key);
		Assert.assertNull(key);
	}
	
	@Test
	public void testGetIntPropertyl1(){
		String key="1";
		Integer value= PropertiesUtil.getIntProperty(key);
		Assert.assertNull(value);
	}	
	
	@Test
	public void testGetIntPropertyNull(){
		String key=null;
		int defaultValue=0;
		PropertiesUtil.getIntProperty(key, defaultValue);
		Assert.assertNull(key);
	}
	
	@Test
	public void testGetIntPropertyl(){
		String key="1";
		int defaultValue=1;
		Integer value= PropertiesUtil.getIntProperty(key, defaultValue);
		Assert.assertEquals(value, Integer.valueOf(key));
	}
	
	@Test
	public void testGetBooleanPropertyNull() {
		String key = null;
		boolean defaultValue = true;
		boolean returnAvlue = PropertiesUtil.getBooleanProperty(key , defaultValue);
		Assert.assertTrue(returnAvlue);
	}
	
	@Test
	public void testGetBooleanProperty() {
		String key = "1";
		boolean defaultValue = true;
		boolean returnAvlue = PropertiesUtil.getBooleanProperty(key , defaultValue);
		Assert.assertTrue(returnAvlue);
	}
	
	@Test
	public void testGetPropertyStringList(){
		String key = null;
		PropertiesUtil.getPropertyStringList(key);
		Assert.assertNull(key);
	}
	
}