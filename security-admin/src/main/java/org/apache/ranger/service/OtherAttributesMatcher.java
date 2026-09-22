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

package org.apache.ranger.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.ugsyncutil.util.UgsyncCommonConstants;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

final class OtherAttributesMatcher {
    private OtherAttributesMatcher() {
    }

    static <T> T findExactMatch(List<T> candidates, Function<T, String> otherAttributesGetter, String attrKey, String attrValue, String expectedSyncSource) {
        if (CollectionUtils.isEmpty(candidates) || StringUtils.isBlank(attrValue)) {
            return null;
        }
        for (T candidate : candidates) {
            String otherAttributes = otherAttributesGetter.apply(candidate);
            if (StringUtils.isBlank(otherAttributes)) {
                continue;
            }
            Map<String, String> attrs = JsonUtils.jsonToMapStringString(otherAttributes);
            if (attrs == null || !StringUtils.equalsIgnoreCase(attrValue, attrs.get(attrKey))) {
                continue;
            }
            String candidateSyncSource = attrs.get(UgsyncCommonConstants.SYNC_SOURCE);
            if (StringUtils.isNotBlank(expectedSyncSource) && StringUtils.isNotBlank(candidateSyncSource) && !StringUtils.equalsIgnoreCase(expectedSyncSource, candidateSyncSource)) {
                continue;
            }
            return candidate;
        }
        return null;
    }

    static String likePattern(String value) {
        if (StringUtils.isBlank(value)) {
            return null;
        }
        return "%" + value + "%";
    }
}
