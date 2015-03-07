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
package org.apache.ranger.credentialapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;

import org.apache.hadoop.security.alias.CredentialShell;
import org.junit.Before;
import org.junit.Test;

public class TestCredentialReader {
  private final String keystoreFile = new File(System.getProperty("user.home")+"/testkeystore.jceks").toURI().getPath();
  @Before
  public void setup() throws Exception {   
	buildks buildksOBJ=new buildks();	
    String[] argsCreateCommand = {"create", "TestCredential2", "-value", "PassworD123", "-provider", "jceks://file@/" + keystoreFile};
    int rc2=buildksOBJ.createCredential(argsCreateCommand); 
    assertEquals( 0, rc2);
    assertTrue(rc2==0);  
  }

  @Test
  public void testPassword() throws Exception {  	
    String password=CredentialReader.getDecryptedString(keystoreFile, "TestCredential2");
    assertEquals( "PassworD123", password);
    assertTrue(password,"PassworD123".equals(password));
    //delete after use
    
    String[] argsdeleteCommand = new String[] {"delete", "TestCredential2", "-provider", "jceks://file@/" + keystoreFile};
    
	buildks buildksOBJ=new buildks();
	buildksOBJ.deleteCredential(argsdeleteCommand, true);
    
  }
  
}
