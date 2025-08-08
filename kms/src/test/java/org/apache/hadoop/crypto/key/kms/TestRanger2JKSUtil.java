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
package org.apache.hadoop.crypto.key.kms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.Ranger2JKSUtil;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestRanger2JKSUtil {
    @Test
    public void testShowUsage() {
        Ranger2JKSUtil ranger2JKSUtil = new Ranger2JKSUtil();
        Ranger2JKSUtil.showUsage();
    }

    @Test
    public void testDoExportKeysFromJKS() throws NoSuchMethodException {
        Ranger2JKSUtil ranger2JKSUtil = new Ranger2JKSUtil();
        String[]       args           = {"-jks", "test.jks", "-out", "output.txt"};

        Method method = Ranger2JKSUtil.class.getDeclaredMethod(
                "doExportKeysFromJKS", String.class, String.class);
        method.setAccessible(true);
        try {
            method.invoke(ranger2JKSUtil, args[1], args[3]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetFromJceks() throws NoSuchMethodException {
        Ranger2JKSUtil ranger2JKSUtil = new Ranger2JKSUtil();
        Configuration  conf           = new Configuration();
        String         path           = "test.jceks";
        String         alias          = "testAlias";
        String         key            = "testKey";

        Method method = Ranger2JKSUtil.class.getDeclaredMethod(
                "getFromJceks", Configuration.class, String.class, String.class, String.class);

        method.setAccessible(true);
        try {
            method.invoke(ranger2JKSUtil, conf, path, alias, key);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Method invocation failed: " + e.getMessage());
        }
    }
}
