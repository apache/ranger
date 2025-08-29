/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ranger.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class TestPropertiesUtil {
    @Autowired
    PropertiesUtil propertiesUtil;

    @Test
    public void testGetPropertyNull() {
        String key          = null;
        String defaultValue = "test";
        String value        = PropertiesUtil.getProperty(key, defaultValue);
        Assertions.assertNull(value);
    }

    @Test
    public void testGetProperty() {
        String key          = "1";
        String defaultValue = "test";
        String value        = PropertiesUtil.getProperty(key, defaultValue);
        Assertions.assertNotNull(value);
    }

    @Test
    public void testgetPropertyNullValue() {
        String key   = null;
        String value = PropertiesUtil.getProperty(key);
        Assertions.assertNull(value);
    }

    @Test
    public void testGetIntPropertyNull1() {
        String  key   = null;
        Integer value = PropertiesUtil.getIntProperty(key);
        Assertions.assertNull(value);
    }

    @Test
    public void testGetIntPropertyl1() {
        String  key   = "1";
        Integer value = PropertiesUtil.getIntProperty(key);
        Assertions.assertNull(value);
    }

    @Test
    public void testGetIntPropertyNull() {
        String  key          = null;
        int     defaultValue = 0;
        Integer value        = PropertiesUtil.getIntProperty(key, defaultValue);
        Assertions.assertNotNull(value);
        Assertions.assertEquals(value.intValue(), defaultValue);
    }

    @Test
    public void testGetIntPropertyl() {
        String  key          = "1";
        int     defaultValue = 1;
        Integer value        = PropertiesUtil.getIntProperty(key, defaultValue);
        Assertions.assertEquals(value, Integer.valueOf(defaultValue));
    }

    @Test
    public void testGetLongProperty() {
        String key          = "longKey";
        long   defaultValue = 23L;
        Long   value        = PropertiesUtil.getLongProperty(key, defaultValue);
        Assertions.assertNotNull(value);
        Assertions.assertEquals(defaultValue, value.intValue());
    }

    @Test
    public void testGetBooleanPropertyNull() {
        String  key          = null;
        boolean defaultValue = true;
        boolean returnValue  = PropertiesUtil.getBooleanProperty(key, defaultValue);
        Assertions.assertTrue(returnValue);
    }

    @Test
    public void testGetBooleanProperty() {
        String  key          = "1";
        boolean defaultValue = true;
        boolean returnValue  = PropertiesUtil.getBooleanProperty(key, defaultValue);
        Assertions.assertTrue(returnValue);
    }

    @Test
    public void testGetPropertyStringListForNull() {
        String key = null;
        PropertiesUtil.getPropertyStringList(key);
        Assertions.assertNull(key);
    }

    @Test
    public void testGetPropertyStringList() {
        String key = "ranger.users.roles.list";

        PropertiesUtil.getPropertiesMap().put("ranger.users.roles.list", "read,write,access");
        String[] actualRoles = PropertiesUtil.getPropertyStringList(key);

        Assertions.assertEquals("read", actualRoles[0]);
        Assertions.assertEquals("write", actualRoles[1]);
        Assertions.assertEquals("access", actualRoles[2]);
    }

    @Test
    public void testGetPropsContainsInsertedEntries() {
        PropertiesUtil.getPropertiesMap().put("prop.alpha", "A");
        PropertiesUtil.getPropertiesMap().put("prop.beta", "B");
        java.util.Properties props = PropertiesUtil.getProps();
        Assertions.assertEquals("A", props.getProperty("prop.alpha"));
        Assertions.assertEquals("B", props.getProperty("prop.beta"));
    }

    @Test
    public void testGetConfigMapWithPrefix() {
        PropertiesUtil.getPropertiesMap().put("abc.key1", "v1");
        PropertiesUtil.getPropertiesMap().put("abc.key2", "v2");
        PropertiesUtil.getPropertiesMap().put("xyz.key3", "v3");
        java.util.Map<String, String> res = PropertiesUtil.getConfigMapWithPrefix("abc.");
        Assertions.assertEquals(2, res.size());
        Assertions.assertEquals("v1", res.get("abc.key1"));
        Assertions.assertEquals("v2", res.get("abc.key2"));
    }

    @Test
    public void testGetLongPropertyWithDefaultWhenKeyMissing() {
        long def = 123L;
        long val = PropertiesUtil.getLongProperty("missing.long.key", def);
        Assertions.assertEquals(def, val);
    }

    @Test
    public void testGetPropertyStringListEmptyWhenKeyMissing() {
        String[] arr = PropertiesUtil.getPropertyStringList("no.such.key");
        Assertions.assertNotNull(arr);
        Assertions.assertEquals(0, arr.length);
    }
}
