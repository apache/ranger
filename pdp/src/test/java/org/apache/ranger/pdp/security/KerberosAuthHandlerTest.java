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

package org.apache.ranger.pdp.security;

import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KerberosAuthHandlerTest {
    @AfterEach
    public void restoreDefaultRules() throws Exception {
        KerberosName.setRules("DEFAULT");
    }

    @Test
    public void testApplyNameRules_usesConfiguredRules() throws Exception {
        KerberosAuthHandler handler = new KerberosAuthHandler();

        invokePrivateVoid(handler, "initializeKerberosNameRules", new Class<?>[] {String.class}, "DEFAULT");

        String shortName = (String) invokePrivate(handler, "applyNameRules", new Class<?>[] {String.class}, "alice@EXAMPLE.COM");

        assertEquals("alice", shortName);
    }

    @Test
    public void testApplyNameRules_fallsBackForInvalidPrincipal() throws Exception {
        KerberosAuthHandler handler = new KerberosAuthHandler();

        invokePrivateVoid(handler, "initializeKerberosNameRules", new Class<?>[] {String.class}, "DEFAULT");

        String shortName = (String) invokePrivate(handler, "applyNameRules", new Class<?>[] {String.class}, "svc/host");

        assertEquals("svc", shortName);
    }

    private static void invokePrivateVoid(Object target, String methodName, Class<?>[] paramTypes, Object... args) throws Exception {
        invokePrivate(target, methodName, paramTypes, args);
    }

    private static Object invokePrivate(Object target, String methodName, Class<?>[] paramTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, paramTypes);

        method.setAccessible(true);

        return method.invoke(target, args);
    }
}
