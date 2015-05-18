package org.apache.ranger.services.hdfs;

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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.services.hdfs.RangerServiceHdfs;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestRangerServiceHdfs {
	static final String 	sdName		  =  "svcDef-Hdfs";
	static final String 	serviceName   =  "Hdfsdev";
	HashMap<String, Object> responseData  =  null;
	Map<String, String> 	configs 	  =  null;
	RangerServiceHdfs		svcHdfs		  =  null;
	RangerServiceDef 		sd 			  =  null;
	RangerService			svc			  =  null;
	ResourceLookupContext   lookupContext =  null;
	
	
	@Before
	public void setup() {
		configs 	= new HashMap<String,String>();
		lookupContext = new ResourceLookupContext();
		
		buildHdfsConnectionConfig();
		buildLookupContext();

		sd		 = new RangerServiceDef(sdName, "org.apache.ranger.service.hdfs.RangerServiceHdfs", "TestService", "test servicedef description", null, null, null, null, null, null);
		svc   	 = new RangerService(sdName, serviceName, "unit test hdfs resource lookup and validateConfig", null, configs);
		svcHdfs = new RangerServiceHdfs();
		svcHdfs.init(sd, svc);
	}
	
	@Test
	public void testValidateConfig() {

		/* TODO: does this test require a live HDFS environment?
		 *
		HashMap<String,Object> ret = null;
		String errorMessage = null;
		
		try { 
			ret = svcHdfs.validateConfig();
		}catch (Exception e) {
			errorMessage = e.getMessage();
		}
		System.out.println(errorMessage);
		if ( errorMessage != null) {
			assertTrue(errorMessage.contains("listFilesInternal"));
		} else {
			assertNotNull(ret);
		}
		*
		*/
	}
	
	
	@Test
	public void	testLookUpResource() {
		/* TODO: does this test require a live HDFS environment?
		 *
		List<String> ret 	= new ArrayList<String>();
		String errorMessage = null;
		try {
			ret = svcHdfs.lookupResource(lookupContext);
		}catch (Exception e) {
			errorMessage = e.getMessage();
		}
		System.out.println(errorMessage);
		if ( errorMessage != null) {
			assertNotNull(errorMessage);
		} else {
			assertNotNull(ret);
		}
		*
		*/
	}
	
	public void buildHdfsConnectionConfig() {
		configs.put("username", "hdfsuser");
		configs.put("password", "*******");
		configs.put("fs.default.name", "hdfs://localhost:8020");
		configs.put("hadoop.security.authorization","");
		configs.put("hadoop.security.auth_to_local","");
		configs.put("dfs.datanode.kerberos.principa","");
		configs.put("dfs.namenode.kerberos.principal","");
		configs.put("dfs.secondary.namenode.kerberos.principal","");
		configs.put("commonNameForCertificate","");
		configs.put("isencrypted","true");
	}

	public void buildLookupContext() {
		Map<String, List<String>> resourceMap = new HashMap<String,List<String>>();
		resourceMap.put(null, null);
		lookupContext.setUserInput("app");
		lookupContext.setResourceName(null);
		lookupContext.setResources(resourceMap);
	}
	
			
	@After
	public void tearDown() {
		sd  = null;
		svc = null;
	}
	
}

