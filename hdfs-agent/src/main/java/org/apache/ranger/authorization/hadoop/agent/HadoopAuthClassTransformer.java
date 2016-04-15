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
	volatile byte[] transformedClassByteCode = null;
	
	@Override
	public byte[] transform(ClassLoader aClassLoader, String aClassName, Class<?> aClassBeingRedefined, ProtectionDomain aProtectionDomain, byte[] aClassFileBuffer) throws IllegalClassFormatException {
		byte[] ret = aClassFileBuffer;

		if (aClassName.equals("org/apache/hadoop/hdfs/server/namenode/FSPermissionChecker")) {
            byte[] result = transformedClassByteCode;
            if (result == null) {

				byte[] injectedClassCode = injectFSPermissionCheckerHooks(aClassName);

				if(injectedClassCode != null) {
                    synchronized (HadoopAuthClassTransformer.class) {
                        result = transformedClassByteCode;
                        if (result == null) {
                            transformedClassByteCode = result = injectedClassCode;
                        }
                    }
                }
			}

			if(result != null) {
				ret = result;
			}
		}

		return ret;
	}

	private static byte[] injectFSPermissionCheckerHooks(String aClassName) throws IllegalClassFormatException {
		byte[] ret = null;

		System.out.println("Injection code is Invoked in JVM [" + Runtime.getRuntime() + "] for class [" + aClassName + "] ....");
		try {
			CtClass curClass          = getCtClass(aClassName.replaceAll("/", "."));
			CtClass stringClass       = getCtClass("java.lang.String");
			CtClass throwable         = getCtClass("java.lang.Throwable");
			CtClass fsActionClass     = getCtClass("org.apache.hadoop.fs.permission.FsAction");
			CtClass fsDirClass        = getCtClass("org.apache.hadoop.hdfs.server.namenode.FSDirectory");
			CtClass inodeClass        = getCtClass("org.apache.hadoop.hdfs.server.namenode.INode");
			CtClass inodesInPathClass = getCtClass("org.apache.hadoop.hdfs.server.namenode.INodesInPath");
			CtClass accCtrlExcp       = getCtClass("org.apache.hadoop.security.AccessControlException");

			boolean is3ParamsCheckMethod = false;

			CtMethod checkMethod           = null;
			CtMethod checkPermissionMethod = null;

			if (checkMethod == null && curClass != null && inodeClass != null && fsActionClass != null) {
				try {
					System.out.print("looking for check(INode, int, FsAction)...");

					CtClass[] paramArgs = new CtClass[] { inodeClass, CtClass.intType, fsActionClass };

					checkMethod = curClass.getDeclaredMethod("check", paramArgs);

					is3ParamsCheckMethod = true;

					System.out.println("found");
				} catch(NotFoundException nfe) {
					System.out.println("not found");
				}
			}

			if (checkMethod == null && curClass != null && inodeClass != null && fsActionClass != null) {
				try {
					System.out.print("looking for check(INode, FsAction)...");

					CtClass[] paramArgs = new CtClass[] { inodeClass, fsActionClass };

					checkMethod = curClass.getDeclaredMethod("check", paramArgs);

					is3ParamsCheckMethod = false;

					System.out.println("found");
				} catch(NotFoundException nfe) {
					System.out.println("not found");
				}
			}

			if(checkPermissionMethod == null && curClass != null && inodesInPathClass != null && fsActionClass != null) {
				try {
					System.out.print("looking for checkPermission(INodesInPath, boolean, FsAction, FsAction, FsAction, FsAction, boolean)...");

					CtClass[] paramArgs = new CtClass[] { inodesInPathClass,
														  CtClass.booleanType,
														  fsActionClass,
														  fsActionClass,
														  fsActionClass,
														  fsActionClass,
														  CtClass.booleanType
														};

					checkPermissionMethod = curClass.getDeclaredMethod("checkPermission", paramArgs);

					System.out.println("found");
				} catch (NotFoundException nfe) {
					System.out.println("not found");
				}
			}

			if(checkPermissionMethod == null && curClass != null && stringClass != null && fsDirClass != null && fsActionClass != null) {
				try {
					System.out.print("looking for checkPermission(String, FSDirectory, boolean, FsAction, FsAction, FsAction, FsAction, boolean, boolean)...");

					CtClass[] paramArgs = new CtClass[] { stringClass,
														  fsDirClass,
														  CtClass.booleanType,
														  fsActionClass,
														  fsActionClass,
														  fsActionClass,
														  fsActionClass,
														  CtClass.booleanType,
														  CtClass.booleanType
														};

					checkPermissionMethod = curClass.getDeclaredMethod("checkPermission", paramArgs);

					System.out.println("found");
				} catch(NotFoundException nfe) {
					System.out.println("not found");
				}
			}

			if (curClass != null) {
				if (checkMethod != null) {
					System.out.print("injecting check() hooks...");

					checkMethod.insertAfter("org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.logHadoopEvent($1,true);");
					checkMethod.addCatch("{ org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.logHadoopEvent($1,false); throw $e; }", throwable);

					if (is3ParamsCheckMethod) {
						checkMethod.insertBefore("{ if ( org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.check(user,groups,$1,$3) ) { return; } }");
					} else {
						checkMethod.insertBefore("{ if ( org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.check(user,groups,$1,$2) ) { return; } }");
					}

					System.out.println("done");

					if (checkPermissionMethod != null) {
						System.out.print("injecting checkPermission() hooks...");

						checkPermissionMethod.insertAfter("org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.checkPermissionPost($1);");
						checkPermissionMethod.addCatch("{ org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.checkPermissionPost($1); throw $e; }", accCtrlExcp);
						checkPermissionMethod.insertBefore("org.apache.hadoop.hdfs.server.namenode.RangerFSPermissionChecker.checkPermissionPre($1);");

						System.out.println("done");
					}

					ret = curClass.toBytecode();
				} else {
					System.out.println("Unable to identify check() method on class: [" + aClassName + "]. Found following methods:");

					for (CtMethod m : curClass.getDeclaredMethods()) {
						System.err.println("  found Method: " + m);
					}

					System.out.println("Injection failed. Continue without Injection");
				}
			}
		} catch (CannotCompileException e) {
			System.err.println("Can not compile Exception for class Name: " + aClassName + " Exception: " + e);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("IO Exception for class Name: " + aClassName + " Exception: " + e);
			e.printStackTrace();
		}

		return ret;
	}

	private static CtClass getCtClass(String className) {
		CtClass ret = null;

		try {
			ClassPool cp = ClassPool.getDefault();

			ret = cp.get(className);
		} catch (NotFoundException nfe) {
			System.err.println("Unable to find Class for [" + className + "]" + nfe);
			ret = null;
		}

		return ret;
	}
}
