package org.apache.hadoop.hbase.security.access;

import java.io.IOException;

import org.apache.hadoop.hbase.master.MasterServices;
import org.junit.After;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class XaAccessControlListsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testInit() {
		IOException exceptionFound = null ;
		try {
			MasterServices service = null ;
			XaAccessControlLists.init(service) ;
		} catch (IOException e) {
			exceptionFound = e ;
		}
		Assert.assertFalse("Expected to get a NullPointerExecution after init method Execution - Found [" + exceptionFound + "]",  (!(exceptionFound != null && exceptionFound.getCause() instanceof NullPointerException))) ;
	}

}
