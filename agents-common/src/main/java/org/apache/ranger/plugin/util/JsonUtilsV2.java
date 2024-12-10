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
import com.sun.jersey.api.client.ClientResponse;

import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtilsV2 {
    private static final ThreadLocal<ObjectMapper> mapper = ThreadLocal.withInitial(ObjectMapper::new);

    private JsonUtilsV2() {
        // to block instantiation
    }

    public static ObjectMapper getMapper() {
        return mapper.get();
    }

    public static Map<String, String> jsonToMap(String jsonStr) throws Exception {
        final Map<String, String> ret;

        if (jsonStr == null || jsonStr.isEmpty()) {
            ret = new HashMap<>();
        } else {
            ret = getMapper().readValue(jsonStr, new TypeReference<Map<String, String>>() {});
        }

        return ret;
    }

    public static String mapToJson(Map<?, ?> map) throws Exception {
        return getMapper().writeValueAsString(map);
    }

    public static String listToJson(List<?> list) throws Exception {
        return getMapper().writeValueAsString(list);
    }

    public static String objToJson(Serializable obj) throws Exception {
        return getMapper().writeValueAsString(obj);
    }

    public static <T> T jsonToObj(String json, Class<T> tClass) throws Exception {
        return getMapper().readValue(json, tClass);
    }

    public static <T> T jsonToObj(String json, TypeReference<T> typeRef) throws Exception {
        return getMapper().readValue(json, typeRef);
    }

    public static void writeValue(Writer writer, Object obj) throws Exception {
        getMapper().writeValue(writer, obj);
    }

    public static <T> T readValue(Reader reader, Class<T> tClass) throws Exception {
        return getMapper().readValue(reader, tClass);
    }

    public static String nonSerializableObjToJson(Object obj) throws Exception {
        return getMapper().writeValueAsString(obj);
    }

    public static <T> T readResponse(ClientResponse response, Class<T> cls) throws Exception {
        String jsonStr = response.getEntity(String.class);

        return jsonToObj(jsonStr, cls);
    }

    public static <T> T readResponse(ClientResponse response, TypeReference<T> cls) throws Exception {
        String jsonStr = response.getEntity(String.class);

        return jsonToObj(jsonStr, cls);
    }
}
