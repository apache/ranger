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

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

public class TestStringUtil {
    @Autowired
    StringUtil stringUtil = new StringUtil();

    @Test
    public void testToCamelCaseAllWords() {
        String camelcase      = "hello world";
        String camelCaseWords = stringUtil.toCamelCaseAllWords(camelcase);
        Assert.assertEquals("Hello World", camelCaseWords);
    }

    @Test
    public void testNullValidatePassword() {
        String[] invalidValues = {"aa", "bb", "aa12345dd"};
        boolean  value         = stringUtil.validatePassword(null, invalidValues);
        Assert.assertFalse(value);
    }

    @Test
    public void testValidatePassword() {
        String   password      = "Aa1234ddas12";
        String[] invalidValues = {"aa", "bb", "aa12345dd"};
        boolean  value         = stringUtil.validatePassword(password, invalidValues);
        Assert.assertTrue(password.length() >= 8);
        Assert.assertTrue(value);
    }

    @Test
    public void testNotValidatePassword() {
        String   password      = "aassasavcvcvc";
        String[] invalidValues = {"aa", "bb", "aa12345dd"};
        boolean  value         = stringUtil.validatePassword(password, invalidValues);
        Assert.assertTrue(password.length() >= 8);
        Assert.assertFalse(value);
    }

    @Test
    public void testIsEmptyValue() {
        String  str   = "";
        boolean value = stringUtil.isEmpty(str);
        Assert.assertTrue(value);
    }

    @Test
    public void testIsNullValue() {
        boolean value = stringUtil.isEmpty((String) null);
        Assert.assertTrue(value);
    }

    @Test
    public void testIsWithValue() {
        String  str   = "test value";
        boolean value = stringUtil.isEmpty(str);
        Assert.assertFalse(value);
    }

    @Test
    public void testEquals() {
        String  str1  = "test";
        String  str2  = "test";
        boolean value = stringUtil.equals(str1, str2);
        Assert.assertTrue(value);
    }

    @Test
    public void testNormalizeEmail() {
        String  email     = "test.Demo@test.COM";
        String  lowercase = stringUtil.normalizeEmail(email);
        String  emailId   = email.toLowerCase();
        boolean value     = emailId.equals(lowercase);
        Assert.assertTrue(value);
    }

    @Test
    public void testNormalizeEmailIdNull() {
        String lowercase = stringUtil.normalizeEmail(null);
        Assert.assertNull(lowercase);
    }

    @Test
    public void testSplit() {
        String   str1        = "Test1";
        String   str2        = "Test2";
        String   str3        = "Test3";
        String   value       = str1 + "," + str2 + "," + str3;
        String[] stringArray = stringUtil.split(value);
        Assert.assertEquals(3, stringArray.length);
        Assert.assertEquals(str1, stringArray[0]);
        Assert.assertEquals(str2, stringArray[1]);
        Assert.assertEquals(str3, stringArray[2]);
    }

    @Test
    public void testTrim() {
        String str        = "test";
        String dataString = StringUtil.trim(str);
        Assert.assertEquals(str, dataString);
    }

    @Test
    public void testValidateEmailId() {
        String  email = "rangerqa@apache.org";
        boolean value = stringUtil.validateEmail(email);
        Assert.assertTrue(email.length() < 128);
        Assert.assertTrue(value);
    }

    @Test
    public void testNullEmailId() {
        boolean value = stringUtil.validateEmail(null);
        Assert.assertFalse(value);
    }

    @Test
    public void testValidateString() {
        String  regExStr = "^[\\w]([\\-\\.\\w])+[\\w]+@[\\w]+[\\w\\-]+[\\w]*\\.([\\w]+[\\w\\-]+[\\w]*(\\.[a-z][a-z|0-9]*)?)$";
        String  str      = "test.test@gmail.com";
        boolean value    = stringUtil.validateString(regExStr, str);
        Assert.assertTrue(value);
    }

    @Test
    public void testNotValidateString() {
        String  regExStr = "^[\\w]([\\-\\.\\w])+[\\w]+[\\w]*\\.([\\w]+[\\w\\-]+[\\w]*(\\.[a-z][a-z|0-9]*)?)$";
        String  str      = "test.test@gmail.com";
        boolean value    = stringUtil.validateString(regExStr, str);
        Assert.assertFalse(value);
    }

    @Test
    public void testIsListEmpty() {
        List<String> list      = new ArrayList<>();
        boolean      listValue = stringUtil.isEmpty(list);
        Assert.assertTrue(listValue);
    }

    @Test
    public void testIsListNotEmpty() {
        List<String> list = new ArrayList<>();
        list.add("a");
        list.add("b");
        boolean listValue = stringUtil.isEmpty(list);
        Assert.assertFalse(listValue);
    }

    @Test
    public void testIsValidName() {
        String  name  = "test";
        boolean value = stringUtil.isValidName(name);
        Assert.assertTrue(value);
    }

    @Test
    public void testIsValidNameNull() {
        boolean value = stringUtil.isValidName(null);
        Assert.assertFalse(value);
    }
}
