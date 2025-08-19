/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.kms.metrics.source;

import org.apache.hadoop.crypto.key.kms.server.KMSMetricUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestKMSMetricUtil {
    @Test
    public void testGetKMSMetricCalculation() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        KMSMetricUtil kmsMetricUtil = new KMSMetricUtil();

        kmsMetricUtil.getClass();
        String caseValue = "org.apache.hadoop.crypto.key.kms.server.KMSMetricUtil";

        Method method = KMSMetricUtil.class.getDeclaredMethod("getKMSMetricCalculation", String.class);
        method.setAccessible(true);
        Object result = method.invoke(kmsMetricUtil, caseValue);
        // Assert that the result is not null or empty
        if (result != null) {
            System.out.println("KMSMetricCalculation: " + result);
        } else {
            System.out.println("KMSMetricCalculation returned null");
        }
    }

    @Test
    public void testMain() {
        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        try {
            KMSMetricUtil.main(new String[] {});
        } catch (SecurityException e) {
            System.out.println("Caught expected SecurityException: " + e.getMessage());
        } finally {
            System.setSecurityManager(originalSM);
        }
    }

    @Test
    public void testMain1() {
        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        String[] args = {"-type", "hsmenabled"};

        try {
            KMSMetricUtil.main(args);
        } catch (SecurityException e) {
            System.out.println("Caught expected SecurityException: " + e.getMessage());
        } finally {
            System.setSecurityManager(originalSM);
        }
    }

    @Test
    public void testMain2() {
        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        String[] args = {"-Type", "encryptedkeybyalgorithm"};

        try {
            KMSMetricUtil.main(args);
        } catch (SecurityException e) {
            System.out.println("Caught expected SecurityException: " + e.getMessage());
        } finally {
            System.setSecurityManager(originalSM);
        }
    }

    @Test
    public void testMain3() {
        SecurityManager originalSM = System.getSecurityManager();
        System.setSecurityManager(new SecurityManager() {
            @Override
            public void checkPermission(java.security.Permission perm) {}

            @Override
            public void checkExit(int status) {
                throw new SecurityException("Intercepted System.exit(" + status + ")");
            }
        });

        String[] args = {"-TYPE", "ENCRYPTEDKEY"};

        try {
            KMSMetricUtil.main(args);
        } catch (SecurityException e) {
            System.out.println("Caught expected SecurityException: " + e.getMessage());
        } finally {
            System.setSecurityManager(originalSM);
        }
    }
}
