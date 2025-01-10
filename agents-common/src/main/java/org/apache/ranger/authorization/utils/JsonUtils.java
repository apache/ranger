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

package org.apache.ranger.authorization.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.AuditFilter;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPrincipal;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerValidityRecurrence;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

public class JsonUtils {
    private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

    private static final TypeReference<Map<String, String>>               TYPE_MAP_STRING_STRING               = new TypeReference<Map<String, String>>() {};
    private static final TypeReference<Set<String>>                       TYPE_SET_STRING                      = new TypeReference<Set<String>>() {};
    private static final TypeReference<List<String>>                      TYPE_LIST_STRING                     = new TypeReference<List<String>>() {};
    private static final TypeReference<List<RangerValiditySchedule>>      TYPE_LIST_RANGER_VALIDITY_SCHEDULE   = new TypeReference<List<RangerValiditySchedule>>() {};
    private static final TypeReference<List<AuditFilter>>                 TYPE_LIST_AUDIT_FILTER               = new TypeReference<List<AuditFilter>>() {};
    private static final TypeReference<List<RangerValidityRecurrence>>    TYPE_LIST_RANGER_VALIDITY_RECURRENCE = new TypeReference<List<RangerValidityRecurrence>>() {};
    private static final TypeReference<List<RangerPrincipal>>             TYPE_LIST_RANGER_PRINCIPAL           = new TypeReference<List<RangerPrincipal>>() {};
    private static final TypeReference<List<RangerGdsMaskInfo>>           TYPE_LIST_RANGER_GDS_MASK_INFO       = new TypeReference<List<RangerGdsMaskInfo>>() {};
    private static final TypeReference<Map<String, RangerPolicyResource>> TYPE_MAP_RANGER_POLICY_RESOURCE      = new TypeReference<Map<String, RangerPolicyResource>>() {};
    private static final TypeReference<List<RangerTag>>                   TYPE_LIST_RANGER_TAG                 = new TypeReference<List<RangerTag>>() {};

    private static final ThreadLocal<ObjectMapper> MAPPER = ThreadLocal.withInitial(() -> {
        ObjectMapper objectMapper = new ObjectMapper();

        objectMapper.setDateFormat(new SimpleDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z"));
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        objectMapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

        return objectMapper;
    });

    private JsonUtils() {
        // to block instantiation
    }

    public static ObjectMapper getMapper() {
        return MAPPER.get();
    }

    public static String mapToJson(Map<?, ?> map) {
        String ret = null;

        if (MapUtils.isNotEmpty(map)) {
            try {
                ret = getMapper().writeValueAsString(map);
            } catch (Exception e) {
                LOG.error("Invalid input data: ", e);
            }
        }

        return ret;
    }

    public static String listToJson(List<?> list) {
        String ret = null;

        if (CollectionUtils.isNotEmpty(list)) {
            try {
                ret = getMapper().writeValueAsString(list);
            } catch (Exception e) {
                LOG.error("Invalid input data: ", e);
            }
        }

        return ret;
    }

    public static String setToJson(Set<?> set) {
        String ret = null;

        if (CollectionUtils.isNotEmpty(set)) {
            try {
                ret = getMapper().writeValueAsString(set);
            } catch (Exception e) {
                LOG.error("Invalid input data: ", e);
            }
        }

        return ret;
    }

    public static String objectToJson(Object object) {
        String ret = null;

        if (object != null) {
            try {
                ret = getMapper().writeValueAsString(object);
            } catch (Exception excp) {
                LOG.warn("objectToJson() failed to convert object to Json", excp);
            }
        }

        return ret;
    }

    public static <T> T jsonToObject(Reader reader, Class<T> clz) {
        T ret = null;

        if (null != reader) {
            try {
                ret = getMapper().readValue(reader, clz);
            } catch (Exception excp) {
                LOG.warn("jsonToObject() failed to convert json to object: class {} reader ", clz, excp);
            }
        }

        return ret;
    }

    public static <T> void objectToWriter(Writer writer, T object) {
        if (null != writer) {
            try {
                getMapper().writeValue(writer, object);
            } catch (Exception excp) {
                LOG.warn("objectToWriter() failed to write object to writer: class {} writer ", object, excp);
            }
        }
    }

    public static <T> T jsonToObject(String jsonStr, Class<T> clz) {
        T ret = null;

        if (StringUtils.isNotEmpty(jsonStr)) {
            try {
                ret = getMapper().readValue(jsonStr, clz);
            } catch (Exception excp) {
                LOG.warn("jsonToObject() failed to convert json to object: class {} JSON {}", clz, jsonStr, excp);
            }
        }

        return ret;
    }

    public static <T> T jsonToObject(String jsonStr, TypeReference<T> valueTypeRef) throws JsonProcessingException {
        T ret = null;

        if (StringUtils.isNotEmpty(jsonStr)) {
            try {
                ret = getMapper().readValue(jsonStr, valueTypeRef);
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
                ret = getMapper().readValue(jsonStr, TYPE_MAP_STRING_STRING);
            } catch (Exception excp) {
                LOG.warn("jsonToMapStringString() failed to convert json to object: {}", jsonStr, excp);
            }
        }

        return ret;
    }

    public static Set<String> jsonToSetString(String jsonStr) {
        Set<String> ret = null;

        if (StringUtils.isNotEmpty(jsonStr)) {
            try {
                ret = getMapper().readValue(jsonStr, TYPE_SET_STRING);
            } catch (Exception excp) {
                LOG.warn("jsonToSetString() failed to convert json to object: {}", jsonStr, excp);
            }
        }

        return ret;
    }

    public static List<String> jsonToListString(String jsonStr) {
        List<String> ret = null;

        if (StringUtils.isNotEmpty(jsonStr)) {
            try {
                ret = getMapper().readValue(jsonStr, TYPE_LIST_STRING);
            } catch (Exception excp) {
                LOG.warn("jsonToListString() failed to convert json to object: {}", jsonStr, excp);
            }
        }

        return ret;
    }

    public static List<RangerValiditySchedule> jsonToRangerValiditySchedule(String jsonStr) {
        try {
            return getMapper().readValue(jsonStr, TYPE_LIST_RANGER_VALIDITY_SCHEDULE);
        } catch (Exception e) {
            LOG.error("Cannot get List<RangerValiditySchedule> from {}", jsonStr, e);

            return null;
        }
    }

    public static List<AuditFilter> jsonToAuditFilterList(String jsonStr) {
        try {
            return getMapper().readValue(jsonStr, TYPE_LIST_AUDIT_FILTER);
        } catch (Exception e) {
            LOG.error("Cannot get List<AuditFilter> from {}", jsonStr, e);

            return null;
        }
    }

    public static List<RangerValidityRecurrence> jsonToRangerValidityRecurringSchedule(String jsonStr) {
        try {
            return getMapper().readValue(jsonStr, TYPE_LIST_RANGER_VALIDITY_RECURRENCE);
        } catch (Exception e) {
            LOG.error("Cannot get List<RangerValidityRecurrence> from {}", jsonStr, e);

            return null;
        }
    }

    public static List<RangerPrincipal> jsonToRangerPrincipalList(String jsonStr) {
        try {
            return getMapper().readValue(jsonStr, TYPE_LIST_RANGER_PRINCIPAL);
        } catch (Exception e) {
            LOG.error("Cannot get List<RangerPrincipal> from {}", jsonStr, e);

            return null;
        }
    }

    public static List<RangerTag> jsonToRangerTagList(String jsonStr) {
        try {
            return getMapper().readValue(jsonStr, TYPE_LIST_RANGER_TAG);
        } catch (Exception e) {
            LOG.error("Cannot get List<RangerTag> from {}", jsonStr, e);

            return null;
        }
    }

    public static List<RangerGdsMaskInfo> jsonToListGdsMaskInfo(String jsonStr) {
        try {
            return getMapper().readValue(jsonStr, TYPE_LIST_RANGER_GDS_MASK_INFO);
        } catch (Exception e) {
            LOG.error("Cannot get Map<String, RangerPolicyItemDataMaskInfo> from {}", jsonStr, e);

            return null;
        }
    }

    public static Map<String, RangerPolicyResource> jsonToMapPolicyResource(String jsonStr) {
        try {
            return getMapper().readValue(jsonStr, TYPE_MAP_RANGER_POLICY_RESOURCE);
        } catch (Exception e) {
            LOG.error("Cannot get Map<String, RangerPolicyResource> from {}", jsonStr, e);

            return null;
        }
    }
}
