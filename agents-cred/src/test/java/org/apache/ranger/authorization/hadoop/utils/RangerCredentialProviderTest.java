/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.authorization.hadoop.utils;

import java.io.File;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialShell;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.ranger.authorization.hadoop.utils.RangerCredentialProvider;

public class RangerCredentialProviderTest {
	
	private final File ksFile =  new File(System.getProperty("user.home")+"/testkeystore.jceks") ;
	private final String keystoreFile = ksFile.toURI().getPath();
	private String[] argsCreate = {"create", "TestCredential001", "-value", "PassworD123", "-provider", "jceks://file@/" + keystoreFile};
	private String[] argsDelete = {"delete", "TestCredential001", "-provider", "jceks://file@/" + keystoreFile};
	private String url = "jceks://file@/" + keystoreFile;
	RangerCredentialProvider cp = null;
	List<CredentialProvider> providers = null;
	
	@Before
	public void setup() throws Exception {
		int ret;
		//
		// adding a delete before creating a keystore
		//
		try {
			if (ksFile != null) {
				if (ksFile.exists()) {
					ksFile.delete() ;
				}
			}
		}
		catch(Throwable t) {
		}
		
		Configuration conf = new Configuration();
		CredentialShell cs = new CredentialShell();
		cs.setConf(conf);
		try {
			 ret =cs.run(argsCreate);
		} catch (Exception e) {
			throw e;
		}
		assertEquals(0,ret);
	}
	
	@Test
	public void testCredentialProvider() {
		//test credential provider is registered and return credential providers.
		cp = new RangerCredentialProvider();
		providers = cp.getCredentialProviders(url);
		if (providers != null) {
			assertTrue(url.equals(providers.get(0).toString()));
		}
	}
	
	@Test
	public void testCredentialString() {
		//test credential provider created is returning the correct credential string.
		cp = new RangerCredentialProvider();
		providers = cp.getCredentialProviders(url);
		if (providers != null) {
			assertTrue("PassworD123".equals(new String(cp.getCredentialString(url,"TestCredential001"))));
		}
	}

	
	@After
	public void teardown() throws Exception {
		int ret;
		Configuration conf = new Configuration();
		CredentialShell cs = new CredentialShell();
		cs.setConf(conf);
		try {
			 ret =cs.run(argsDelete);
		} catch (Exception e) {
			throw e;
		}
		assertEquals(0,ret);	
	}
}

