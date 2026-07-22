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

package org.apache.ranger.authentication.metrics.source;

import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.unixusersync.config.UserGroupSyncConfig;
import org.springframework.stereotype.Component;

import java.util.Properties;

@Component
public class RangerUserSyncSourceRoleStatus extends RangerUserSyncSourceBase {
    private static final String              IS_ROLE_ACTIVE = "IsRoleACTIVE";
    private final        UserGroupSyncConfig config         = UserGroupSyncConfig.getInstance();
    private final        Properties          prop           = config.getProperties();

    public RangerUserSyncSourceRoleStatus() {
        super("usersync", "RoleStatus");
    }

    public boolean getBoolean(String aPropertyName, boolean aDefaultValue) {
        boolean ret = aDefaultValue;
        String  val = getProperty(aPropertyName);
        if (StringUtils.isNotBlank(val)) {
            ret = Boolean.valueOf(val);
        }
        return ret;
    }

    public String getProperty(String aPropertyName) {
        String value = prop.getProperty(aPropertyName);
        if (value == null) { // config-with-prefix not found; look at System properties
            value = System.getProperty(aPropertyName);
        }
        return value;
    }

    @Override
    protected void refresh() {
        if (getBoolean(UserGroupSyncConfig.UGSYNC_SERVER_HA_ENABLED_PARAM, false)) {
            if (UserGroupSyncConfig.isUgsyncServiceActive()) {
                metricsMap.put(IS_ROLE_ACTIVE, 1L);
            } else {
                metricsMap.put(IS_ROLE_ACTIVE, 0L);
            }
        }
    }
}
