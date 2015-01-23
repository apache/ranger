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
package org.apache.ranger.authorization.hadoop.agent;

import java.io.IOException;
import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.security.ProtectionDomain;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

public class HadoopAuthClassTransformer implements ClassFileTransformer {

	byte[] transformedClassByteCode = null ;
	
	@Override
	public byte[] transform(ClassLoader aClassLoader, String aClassName, Class<?> aClassBeingRedefined, ProtectionDomain aProtectionDomain, byte[] aClassFileBuffer) throws IllegalClassFormatException {

		byte[] byteCode = aClassFileBuffer;
		if (aClassName.equals("org/apache/hadoop/hdfs/server/namenode/FSPermissionChecker")) {
			System.out.println("Injection code is Invoked in JVM [" + Runtime.getRuntime() + "] for class [" + aClassBeingRedefined + "] ....");
			try {
				if (transformedClassByteCode == null) {
					ClassPool cp = ClassPool.getDefault();
					String curClassName = aClassName.replaceAll("/", ".");
					CtClass curClass = cp.get(curClassName);
					
					
					CtClass inodeClass = null, snapShotClass = null, fsActionClass = null  ;
					String paramClassName = null ;
					
					try {
						paramClassName = "org.apache.hadoop.hdfs.server.namenode.INode" ;
						inodeClass = cp.get(paramClassName) ;
					} catch (javassist.NotFoundException nfe) {
						System.err.println("Unable to find Class for [" + paramClassName + "]" + nfe) ;
						inodeClass = null ;
					}


					try {
						paramClassName = "org.apache.hadoop.fs.permission.FsAction" ;
						fsActionClass = cp.get(paramClassName) ;
					} catch (javassist.NotFoundException nfe) {
						System.err.println("Unable to find Class for [" + paramClassName + "]" + nfe) ;
						fsActionClass = null ;
					}
					
					try {
						paramClassName = "org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot" ;
						snapShotClass = cp.get(paramClassName) ;
					} catch (javassist.NotFoundException nfe) {
						System.err.println("Unable to find Class for [" + paramClassName + "]" + nfe) ;
						snapShotClass = null ;
					}
					
					boolean injected = false ;
					boolean injected_cm = false ;
					boolean withIntParamInMiddle = false ;

					
					try {
						
						CtClass[] paramArgs = null ;
						
						if (inodeClass != null && fsActionClass != null) {

							CtMethod checkMethod = null ;
							
							if (snapShotClass != null) {
								paramArgs = new CtClass[] { inodeClass, snapShotClass, fsActionClass } ;
								try {
									checkMethod = curClass.getDeclaredMethod("check", paramArgs);
								}
								catch(NotFoundException SSnfe) {
									System.out.println("Unable to find check method with snapshot class. Trying to find check method without snapshot support.") ;
									snapShotClass = null;
									paramArgs = new CtClass[] { inodeClass, CtClass.intType,  fsActionClass } ;
									checkMethod = curClass.getDeclaredMethod("check", paramArgs);
									withIntParamInMiddle = true ;
									System.out.println("Found method check() - without snapshot support") ;
								}
							}
							else {
								System.out.println("Snapshot class was already null ... Trying to find check method") ;
								paramArgs = new CtClass[] { inodeClass, fsActionClass } ;
								checkMethod = curClass.getDeclaredMethod("check", paramArgs);
								System.out.println("Found method check() - without snapshot support") ;
							}
						
							if (checkMethod != null) {
								checkMethod.insertAfter("org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.logHadoopEvent($1,true) ;");
								CtClass throwable = ClassPool.getDefault().get("java.lang.Throwable");
								checkMethod.addCatch("{ org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.logHadoopEvent($1,false) ; throw $e; }", throwable);

								if (snapShotClass == null && (!withIntParamInMiddle)) {
									checkMethod.insertBefore("{ if ( org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.check(ugi,$1,$2) ) { return ; } }");
								}
								else {
									checkMethod.insertBefore("{ if ( org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.check(ugi,$1,$3) ) { return ; } }");
								}

								System.out.println("Injection of code is successfull ....");
							}
							else {
								System.out.println("Injection failed. Unable to identify check() method on class: [" + curClass.getName() + "]. Continue without Injection ...") ; 
							}
							
							injected = true ;
						}
					} catch (NotFoundException nfex) {
						nfex.printStackTrace();
						System.out.println("Unable to find the check() method with expected params in [" + aClassName + "] ....");
						for (CtMethod m : curClass.getDeclaredMethods()) {
							System.err.println("Found Method: " + m);
						}
					}
					
					
					try {
						
						CtMethod checkMethod = curClass.getDeclaredMethod("checkPermission");
						
						if (checkMethod != null) {
							checkMethod.insertAfter("org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.checkPermissionPost($1) ;");
							CtClass throwable = ClassPool.getDefault().get("org.apache.hadoop.security.AccessControlException");
							checkMethod.addCatch("{ org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.checkPermissionPost($1); throw $e; }", throwable);	
							checkMethod.insertBefore("org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.checkPermissionPre($1) ;");
							injected_cm = true ;
						}

					} catch (NotFoundException nfe) {
						nfe.printStackTrace();
						System.out.println("Unable to find the checkPermission() method with expected params in [" + aClassName + "] ....");
						for (CtMethod m : curClass.getDeclaredMethods()) {
							System.err.println("Found Method: " + m);
						}
					}
					
					System.out.println("Injected: " + injected + ", Injected_CheckMethod: " + injected_cm ) ;
					
					if (injected) {
						byteCode = curClass.toBytecode();
						if (transformedClassByteCode == null) {
							synchronized(HadoopAuthClassTransformer.class) {
								byte[] temp = transformedClassByteCode ;
								if (temp == null) {
									transformedClassByteCode = byteCode;
								}
							}
						}
					}
					
				}
				else {
					byteCode = transformedClassByteCode;
					System.out.println("Injection of code (using existing bytecode) is successfull ....");
				}
			} catch (NotFoundException e) {
				System.err.println("Class Not Found Exception for class Name: " + aClassName + " Exception: " + e);
				e.printStackTrace();
			} catch (CannotCompileException e) {
				System.err.println("Can not compile Exception for class Name: " + aClassName + " Exception: " + e);
				e.printStackTrace();
			} catch (IOException e) {
				System.err.println("IO Exception for class Name: " + aClassName + " Exception: " + e);
				e.printStackTrace();
			}
		
		}
		
		return byteCode;
	}

}
