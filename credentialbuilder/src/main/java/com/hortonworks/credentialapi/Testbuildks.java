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
package com.hortonworks.credentialapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class Testbuildks {
  private final String keystoreFile =System.getProperty("user.home")+"/testkeystore.jceks";  
  @Test
  public void testBuildKSsuccess() throws Exception {   
	buildks buildksOBJ=new buildks();
    String[] argsCreateCommand = {"create", "TestCredential1", "-value", "PassworD123", "-provider", "jceks://file" + keystoreFile};
    int rc1=buildksOBJ.createCredential(argsCreateCommand); 
    assertEquals( 0, rc1);
    assertTrue(rc1==0);
   
    String[] argsListCommand = {"list", "-provider","jceks://file" + keystoreFile};
    int rc2=buildksOBJ.listCredential(argsListCommand);
    assertEquals(0, rc2);
    assertTrue(rc2==0);

    String[] argsDeleteCommand = {"delete", "TestCredential1", "-provider", "jceks://file" +keystoreFile };
    int rc3=buildksOBJ.deleteCredential(argsDeleteCommand);
    assertEquals(0, rc3);
    assertTrue(rc3==0);
   
    if(rc1==rc2 && rc2==rc3 && rc3==0){
    	System.out.println("Test Case has been completed successfully..");    	
    }
  }

  @Test
  public void testInvalidProvider() throws Exception {
	buildks buildksOBJ=new buildks(); 
	String[] argsCreateCommand = {"create", "TestCredential1", "-value", "PassworD123", "-provider", "jksp://file"+keystoreFile};    
    int rc1=buildksOBJ.createCredential(argsCreateCommand);   
    assertEquals(-1, rc1);
    assertTrue(rc1==-1);
  } 
  
  @Test
  public void testInvalidCommand() throws Exception {
	buildks buildksOBJ=new buildks(); 
	String[] argsCreateCommand = {"creat", "TestCredential1", "-value", "PassworD123", "-provider", "jksp://file"+keystoreFile};    
    int rc1=buildksOBJ.createCredential(argsCreateCommand);   
    assertEquals(-1, rc1);
    assertTrue(rc1==-1);
  } 
  /*public static void main(String args[]) throws Exception{
	  Testbuildks tTestbuildks=new Testbuildks();
	  tTestbuildks.testBuildKSsuccess();
  }*/  
  
}
