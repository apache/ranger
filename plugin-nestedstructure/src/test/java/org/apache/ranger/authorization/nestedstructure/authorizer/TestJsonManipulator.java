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

package org.apache.ranger.authorization.nestedstructure.authorizer;

import com.google.gson.JsonParser;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.CUSTOM;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_DATE_SHOW_YEAR;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_HASH;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_NONE;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_NULL;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_SHOW_FIRST_4;
import static org.apache.ranger.authorization.nestedstructure.authorizer.MaskTypes.MASK_SHOW_LAST_4;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestJsonManipulator {
    static final String testString1 = "{\n" +
            "    \"customerRelationshipId\": \"42207ad4\",\n" +
            "    \"accountnumber\": \"12345678\",\n" +
            "    \"partner\": \"dance\",\n" +
            "    \"acquisitionDate\": \"2018-02-01\",\n" +
            "    \"customerSubtype\": [\n" +
            "      \"type1\",\n" +
            "      \"type2\"\n" +
            "    ],\n" +
            "    \"address\": {\n" +
            "      \"addressLine1\": \"123 Main St\",\n" +
            "      \"addressLine2\": \"Apt1\",\n" +
            "      \"city\": \"philadelphia\",\n" +
            "      \"state\": \"PA\",\n" +
            "      \"zipCode\": \"19019\",\n" +
            "      \"zipCodePlus4\": \"1111\",\n" +
            "      \"country\": \"USA\"\n" +
            "    }\n" +
            "}\n";

    static final String bigTester = "{" +
            "    \"someString\": \"42207ad4-590e-4d5d-a65f-6a4ccddca9e3002\"," +
            "    \"someNumber\": 12345678," +
            "    \"someBoolean\": true," +
            "    \"stringArray\": [\"thing1\", \"thing2\"]," +
            "    \"numberArray\": [1, 2, 3]," +
            "    \"booleanArray\": [true, false, true]," +
            "    \"aMap\": {" +
            "      \"mapString\": \"123 Main St\"," +
            "      \"mapBoolean\": false," +
            "      \"mapNumber\": 987," +
            "      \"mapStrinArray\": [\"one\", \"two\"]," +
            "      \"mapMap\": {\"mapMapString\": \"19019\"}" +
            "    }\n" +
            "}\n";

    @Test
    public void testFieldNames1() {
        JsonManipulator js = new JsonManipulator("{foo: 1}");

        assertEquals(js.getFields().size(), 1);
    }

    @Test
    public void testFieldNames2() {
        JsonManipulator js = new JsonManipulator("{foo: 1, bar: 2}");

        assertEquals(js.getFields().size(), 2);
    }

    @Test
    public void testFieldNamesBigger() {
        JsonManipulator js = new JsonManipulator(testString1);

        System.out.println(js.getFields());

        Set<String> fields = js.getFields();

        assertTrue(fields.contains("customerRelationshipId"));
        assertTrue(fields.contains("partner"));
        assertTrue(fields.contains("acquisitionDate"));
        assertTrue(fields.contains("customerSubtype.*"));
        assertTrue(fields.contains("accountnumber"));
        assertTrue(fields.contains("address.addressLine1"));
        assertTrue(fields.contains("address.addressLine2"));
        assertTrue(fields.contains("address.city"));
        assertTrue(fields.contains("address.zipCode"));
        assertTrue(fields.contains("address.zipCodePlus4"));
        assertTrue(fields.contains("address.state"));
        assertTrue(fields.contains("address.country"));
        assertEquals(js.getFields().size(), 12);
    }

    @Test
    public void invalidJson() {
        assertThrows(MaskingException.class, () -> {
            JsonManipulator js = new JsonManipulator("{foo:\"bar\"");
            js.getFields().size();
        });
    }

    @Test
    public void testToString() {
        String          condensedString = JsonParser.parseString(testString1).toString();
        JsonManipulator man             = new JsonManipulator(condensedString);

        assertEquals(man.getJsonString(), condensedString);
    }

    @Test
    public void testBigTesterFieldNames() {
        JsonManipulator man = new JsonManipulator(bigTester);

        assertEquals(man.getFields().size(), 11);
    }

    public static Object[][] simpleMasks() {
        return new Object[][] {
                //basic string masking
                {"{\"string1\":\"value1\"}",
                        Collections.singletonList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_NULL, "customValue")),
                        "{\"string1\":null}"},
                {"{\"string1\":\"value1\"}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, CUSTOM, "customValue")),
                        "{\"string1\":\"customValue\"}"},
                {"{\"string1\":\"value1\"}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_NONE, "customValue")),
                        "{\"string1\":\"value1\"}"},
                {"{\"string1\":\"value1\"}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_SHOW_FIRST_4, "customValue")),
                        "{\"string1\":\"valuxx\"}"},
                {"{\"string1\":\"value1\"}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_SHOW_LAST_4, "customValue")),
                        "{\"string1\":\"xxlue1\"}"},
                {"{\"string1\":\"value1\"}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_HASH, "customValue")),
                        "{\"string1\":\"3c9683017f9e4bf33d0fbedd26bf143fd72de9b9dd145441b75f0604047ea28e\"}"},
                {"{\"string1\":\"2021-12-25\"}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_DATE_SHOW_YEAR, "customValue")),
                        "{\"string1\":\"2021\"}"},

                //basic number masking
                {"{\"string1\":123456}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_NULL, "100")),
                        "{\"string1\":null}"},
                {"{\"string1\":123456}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, CUSTOM, "100")),
                        "{\"string1\":100}"},
                {"{\"string1\":123456}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_NONE, "100")),
                        "{\"string1\":123456}"},
                {"{\"string1\":123456}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK, "100")),
                        "{\"string1\":-11111}"},

                //basic boolean masking
                {"{\"string1\":true}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_NULL, "true")),
                        "{\"string1\":null}"},
                {"{\"string1\":true}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, CUSTOM, "false")),
                        "{\"string1\":false}"},
                {"{\"string1\":true}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK_NONE, "true")),
                        "{\"string1\":true}"},
                {"{\"string1\":true}",
                        Arrays.asList(
                                new FieldLevelAccess("string1", true, 1L, true, MASK, "true")),
                        "{\"string1\":false}"},

                //array string masking
                {"{\"string1\":[\"aaaaaaa\",\"bbbbbbb\"]}",
                        Arrays.asList(
                                new FieldLevelAccess("string1.*", true, 1L, true, MASK_NULL, "true")),
                        "{\"string1\":[null,null]}"},
                {"{\"string1\":[\"aaaaaaa\",\"bbbbbbb\"]}",
                        Arrays.asList(
                                new FieldLevelAccess("string1.*", true, 1L, true, CUSTOM, "false")),
                        "{\"string1\":[\"false\",\"false\"]}"},
                {"{\"string1\":[\"aaaaaaa\",\"bbbbbbb\"]}",
                        Arrays.asList(
                                new FieldLevelAccess("string1.*", true, 1L, true, MASK_NONE, "true")),
                        "{\"string1\":[\"aaaaaaa\",\"bbbbbbb\"]}"},
                {"{\"string1\":[\"aaaaaaa\",\"bbbbbbbbbb\"]}",
                        Arrays.asList(
                                new FieldLevelAccess("string1.*", true, 1L, true, MASK, "true")),
                        "{\"string1\":[\"*******\",\"**********\"]}"},
        };
    }

    public static Object[][] complexMasks() {
        return new Object[][] {
                //test masking two fields
                {bigTester,
                        Arrays.asList(
                                new FieldLevelAccess("someNumber", true, 1L, true, CUSTOM, "555"),
                                new FieldLevelAccess("someBoolean", true, 1L, true, CUSTOM, "false")),
                        "someNumber", "555"},
                {bigTester,
                        Arrays.asList(
                                new FieldLevelAccess("someNumber", true, 1L, true, CUSTOM, "555"),
                                new FieldLevelAccess("someBoolean", true, 1L, true, CUSTOM, "false")),
                        "someBoolean", "false"},

                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("someString", true, 1L, true, CUSTOM, "555")),
                        "someString", "555"},
                {bigTester,
                        Arrays.asList(
                                new FieldLevelAccess("someNumber", true, 1L, true, CUSTOM, "555")),
                        "someNumber", "555"},
                {bigTester,
                        Arrays.asList(
                                new FieldLevelAccess("stringArray.*", true, 1L, true, CUSTOM, "555")),
                        "stringArray.*", "[\"555\",\"555\"]"},
                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("booleanArray.*", true, 1L, true, CUSTOM, "false")),
                        "booleanArray.*", "[false,false,false]"},

                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("aMap.mapString", true, 1L, true, CUSTOM, "foo")),
                        "aMap.mapString", "foo"},
                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("aMap.mapBoolean", true, 1L, true, CUSTOM, "true")),
                        "aMap.mapBoolean", "true"},
                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("aMap.mapNumber", true, 1L, true, CUSTOM, "444")),
                        "aMap.mapNumber", "444"},
                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("aMap.mapStrinArray.*", true, 1L, true, CUSTOM, "baa")),
                        "aMap.mapStrinArray.*", "[\"baa\",\"baa\"]"},
                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("aMap.mapMap.mapMapString", true, 1L, true, CUSTOM, "444qqq")),
                        "aMap.mapMap.mapMapString", "444qqq"},

                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("aMap.mapMap.mapMapString", false, 1L, true, CUSTOM, "444qqq")),
                        "aMap.mapMap.mapMapString", "19019"},
                {bigTester,
                        Collections.singletonList(
                                new FieldLevelAccess("aMap.mapMap.mapMapString", true, 1L, false, CUSTOM, "444qqq")),
                        "aMap.mapMap.mapMapString", "19019"},
        };
    }

    @Test
    void testSimpleMasks() {
        for (Object[] row : simpleMasks()) {
            String json = (String) row[0];
            @SuppressWarnings("unchecked")
            List<FieldLevelAccess> fieldAccess = (List<FieldLevelAccess>) row[1];
            String outputJson = (String) row[2];

            JsonManipulator man = new JsonManipulator(json);
            man.maskFields(fieldAccess);
            assertEquals(outputJson, man.getJsonString());
        }
    }

    @Test
    void testComplexMasks() {
        for (Object[] row : complexMasks()) {
            String json = (String) row[0];
            @SuppressWarnings("unchecked")
            List<FieldLevelAccess> fieldAccess = (List<FieldLevelAccess>) row[1];
            String fieldName = (String) row[2];
            String value = (String) row[3];

            JsonManipulator man = new JsonManipulator(json);
            man.maskFields(fieldAccess);
            assertEquals(value, man.readString(fieldName));
        }
    }

    @Test
    void testRecordsInArray() {
        String json = "{\n" +
                "  \"modifiedTimestamp\": \"2000-01-23T04:56:07.000Z\",\n" +
                "  \"source\": [\n" +
                "    {\n" +
                "      \"sourceId\": \"123456\",\n" +
                "     \"sourceType\": \"a type\",\n" +
                "      \"sourceType2\": \"type two\",\n" +
                "      \"sourceSystem\": \"Source System\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"channel\": \"channel 4\",\n" +
                "  \"transactionStatus\": \"SUCCESS\",\n" +
                "  \"customAttributes\": [\n" +
                "    {\n" +
                "      \"key\": \"new\",\n" +
                "      \"value\": \"value1\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"modifiedBy\": \"batchJob\"\n" +
                "}";
        JsonManipulator man = new JsonManipulator(json);
        assertEquals(man.getFields().size(), 10);
    }

    @Test
    void testRecordsInArray2() {
        String json = "{\n" +
                "  \"modifiedTimestamp\": \"2000-01-23T04:56:07.000Z\",\n" +
                "  \"source\": [\n" +
                "    {\n" +
                "      \"sourceId\": \"123456\",\n" +
                "     \"sourceType\": \"a type\",\n" +
                "      \"sourceType2\": \"type two\",\n" +
                "      \"sourceSystem\": \"Source System\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"channel\": \"channel 4\",\n" +
                "  \"transactionStatus\": \"SUCCESS\",\n" +
                "  \"customAttributes\": [\n" +
                "    {\n" +
                "      \"key\": \"new\",\n" +
                "      \"value\": \"value1\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"key\": \"new22\",\n" +
                "      \"value\": \"value22\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"modifiedBy\": \"batchJob\"\n" +
                "}";
        JsonManipulator man = new JsonManipulator(json);
        assertEquals(man.getFields().size(), 10);

        FieldLevelAccess fieldAccess = new FieldLevelAccess("customAttributes.*.key", true, 1L, true, CUSTOM, "THEMASK");
        man.maskFields(Collections.singletonList(fieldAccess));
        assertEquals(man.readString("customAttributes.[0].key"), "THEMASK");
    }
}
