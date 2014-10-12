package com.xasecure.common;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

public class TestXAConfigUtil {

	@Autowired
	XAConfigUtil xaConfigUtil = new XAConfigUtil();	

	@Test
	public void testGetDefaultMaxRows() {
		int maxrow = xaConfigUtil.getDefaultMaxRows();
		Assert.assertEquals(maxrow,xaConfigUtil.defaultMaxRows );
	}
	
	@Test
	public void testIsAccessFilterEnabled() {
		boolean value = xaConfigUtil.isAccessFilterEnabled();
        Assert.assertTrue(value);
	}
	
	@Test
	public void testGetWebAppRootURL(){
		String returnValue = xaConfigUtil.getWebAppRootURL();
		Assert.assertEquals(returnValue,xaConfigUtil.webappRootURL);
	}
	
	@Test
	public void testGetRoles(){
		String[] str=xaConfigUtil.getRoles();
		Assert.assertArrayEquals(str, xaConfigUtil.roles);
	}
}