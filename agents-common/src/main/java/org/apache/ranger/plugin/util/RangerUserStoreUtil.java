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

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RangerUserStoreUtil {
    public static final String CLOUD_IDENTITY_NAME = "cloud_id";

    private final RangerUserStore                  userStore;
    private final long                             userStoreVersion;
    private final Map<String, Set<String>>         userGroups;
    private final Map<String, Map<String, String>> userAttributes;
    private final Map<String, Map<String, String>> groupAttributes;

    private volatile Map<String, String> userEmailToName = null;

    public RangerUserStoreUtil(RangerUserStore userStore) {
        this.userStore = userStore;

        if (userStore != null) {
            this.userStoreVersion = userStore.getUserStoreVersion() != null ? userStore.getUserStoreVersion() : -1;
            this.userGroups       = userStore.getUserGroupMapping() != null ? userStore.getUserGroupMapping() : Collections.emptyMap();
            this.userAttributes   = userStore.getUserAttrMapping() != null ? userStore.getUserAttrMapping() : Collections.emptyMap();
            this.groupAttributes  = userStore.getGroupAttrMapping() != null ? userStore.getGroupAttrMapping() : Collections.emptyMap();
        } else {
            this.userStoreVersion = -1;
            this.userGroups       = Collections.emptyMap();
            this.userAttributes   = Collections.emptyMap();
            this.groupAttributes  = Collections.emptyMap();
            this.userEmailToName  = Collections.emptyMap();
        }
    }

    public RangerUserStore getUserStore() { return userStore; }

    public long getUserStoreVersion() { return userStoreVersion; }

    public Set<String> getUserGroups(String userName) { return userGroups.get(userName); }

    public Map<String, String> getUserAttributes(String userName) { return userAttributes.get(userName); }

    public Map<String, String> getGroupAttributes(String groupName) { return groupAttributes.get(groupName); }

    public String getUserNameFromEmail(String emailAddress) {
        Map<String, String> userEmailToName = this.userEmailToName;

        if (userEmailToName == null) {
            synchronized (this) {
                userEmailToName = this.userEmailToName;

                if (userEmailToName == null) {
                    this.userEmailToName = buildUserEmailToNameMap();

                    userEmailToName = this.userEmailToName;
                }
            }
        }

        return userEmailToName != null ? userEmailToName.get(emailAddress) : null;
    }

    private Map<String, String> buildUserEmailToNameMap() {
        final Map<String, String> ret;

        if (!userAttributes.isEmpty()) {
            ret = new HashMap<>();

            for (Map.Entry<String, Map<String, String>> entry : userAttributes.entrySet()) {
                String              userName  = entry.getKey();
                Map<String, String> userAttrs = entry.getValue();
                String              emailAddr = userAttrs != null ? userAttrs.get(RangerCommonConstants.SCRIPT_FIELD__EMAIL_ADDRESS) : null;

                if (StringUtils.isNotBlank(emailAddr)) {
                    ret.put(emailAddr, userName);
                }
            }
        } else {
            ret = Collections.emptyMap();
        }

        return ret;
    }

    public static String getPrintableOptions(Map<String, String> otherAttributes) {
        if (MapUtils.isEmpty(otherAttributes)) return "{}";
        StringBuilder ret = new StringBuilder();
        ret.append("{");
        for (Map.Entry<String, String> entry : otherAttributes.entrySet()) {
            ret.append(entry.getKey()).append(", ").append("[").append(entry.getValue()).append("]").append(",");
        }
        ret.append("}");
        return ret.toString();
    }

    public static String getAttrVal(Map<String, Map<String, String>> attrMap, String name, String attrName) {
        String ret = null;

        if (StringUtils.isNotEmpty(name) && StringUtils.isNotEmpty(attrName)) {
            Map<String, String> attrs = attrMap.get(name);
            if (MapUtils.isNotEmpty(attrs)) {
                ret = attrs.get(attrName);
            }
        }
        return ret;
    }

    public String getCloudId(Map<String, Map<String, String>> attrMap, String name) {
        return getAttrVal(attrMap, name, CLOUD_IDENTITY_NAME);
    }
}


