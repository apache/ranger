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

package org.apache.ranger.audit.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);
    private static final ThreadLocal<ObjectMapper> mapper = new ThreadLocal<ObjectMapper>() {
        @Override
        protected ObjectMapper initialValue() {
            return new ObjectMapper();
        }
    };
    private static final ThreadLocal<Gson> gson = new ThreadLocal<Gson>() {
        @Override
        protected Gson initialValue() {
            return new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").create();
        }
    };

    private JsonUtils() {
        //Block Instantiation
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

    public static String objectToJson(Object object) {
        String ret = null;

        if (object != null) {
            try {
                ret = gson.get().toJson(object);
            } catch (Exception excp) {
                LOG.warn("objectToJson() failed to convert object to Json", excp);
            }
        }

        return ret;
    }

    public static <T> T jsonToObject(String jsonStr, Class<T> clz) {
        T ret = null;

        if (StringUtils.isNotEmpty(jsonStr)) {
            try {
                ret = gson.get().fromJson(jsonStr, clz);
            } catch (Exception excp) {
                LOG.warn("jsonToObject() failed to convert json to object: {}", jsonStr, excp);
            }
        }

        return ret;
    }

    public static Map<String, String> jsonToMapStringString(String jsonStr) {
        Map<String, String> ret = null;

        if (StringUtils.isNotEmpty(jsonStr)) {
            try {
                Type mapType = new TypeToken<Map<String, String>>() {}.getType();
                ret = gson.get().fromJson(jsonStr, mapType);
            } catch (Exception excp) {
                LOG.warn("jsonToObject() failed to convert json to object: {}", jsonStr, excp);
            }
        }

        return ret;
    }
}
