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

package org.apache.ranger.plugin.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtilsV2 {
    private static final ThreadLocal<ObjectMapper>          MAPPER                     = ThreadLocal.withInitial(ObjectMapper::new);
    private static final TypeReference<Map<String, String>> TYPE_REF_MAP_STRING_STRING = new TypeReference<Map<String, String>>() {};

    private JsonUtilsV2() {
        // to block instantiation
    }

    public static ObjectMapper getMapper() {
        return MAPPER.get();
    }

    public static Map<String, String> jsonToMap(String jsonStr) throws IOException {
        return (jsonStr == null || jsonStr.isEmpty()) ? new HashMap<>() : getMapper().readValue(jsonStr, TYPE_REF_MAP_STRING_STRING);
    }

    public static String mapToJson(Map<?, ?> map) throws IOException {
        return getMapper().writeValueAsString(map);
    }

    public static String listToJson(List<?> list) throws IOException {
        return getMapper().writeValueAsString(list);
    }

    public static String objToJson(Serializable obj) throws IOException {
        return getMapper().writeValueAsString(obj);
    }

    public static <T> T jsonToObj(String json, Class<T> tClass) throws IOException {
        return getMapper().readValue(json, tClass);
    }

    public static <T> T jsonToObj(String json, TypeReference<T> typeRef) throws IOException {
        return getMapper().readValue(json, typeRef);
    }

    public static void writeValue(Writer writer, Object obj) throws IOException {
        getMapper().writeValue(writer, obj);
    }

    public static <T> T readValue(Reader reader, Class<T> tClass) throws IOException {
        return getMapper().readValue(reader, tClass);
    }

    public static String nonSerializableObjToJson(Object obj) throws IOException {
        return getMapper().writeValueAsString(obj);
    }
}
