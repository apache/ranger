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
package org.apache.hadoop.crypto.key.kms.server;

import org.apache.hadoop.crypto.key.KeyProvider;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.util.List;

import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
@Disabled
public class TestKMSServerJSONUtils {
    @Test
    public void testToJSON() {
        KMSServerJSONUtils           jsonUtils   = mock(KMSServerJSONUtils.class);
        List<KeyProvider.KeyVersion> keyVersions = mock(List.class);
        try {
            Method method = KMSServerJSONUtils.class.getDeclaredMethod("toJSON", List.class);
            method.setAccessible(true);
            method.invoke(jsonUtils, keyVersions);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testToJSON1() {
        KMSServerJSONUtils   jsonUtils = mock(KMSServerJSONUtils.class);
        String               keyName   = "testKey";
        KeyProvider.Metadata meta      = mock(KeyProvider.Metadata.class);

        try {
            Method method = KMSServerJSONUtils.class.getDeclaredMethod("toJSON", String.class, KeyProvider.Metadata.class);
            method.setAccessible(true);
            method.invoke(jsonUtils, keyName, meta);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testToJSON2() {
        KMSServerJSONUtils jsonUtils = mock(KMSServerJSONUtils.class);
        String[]           keyNames  = {"key1", "key2"};
        KeyProvider.Metadata[] metas = {
                mock(KeyProvider.Metadata.class),
                mock(KeyProvider.Metadata.class)
        };

        try {
            Method method = KMSServerJSONUtils.class.getDeclaredMethod("toJSON", String[].class, KeyProvider.Metadata[].class);
            method.setAccessible(true);
            method.invoke(jsonUtils, keyNames, metas);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
