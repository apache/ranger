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

package org.apache.ranger.util;

import org.apache.commons.lang.StringUtils;
import org.apache.ranger.biz.RangerBizUtil;
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.plugin.model.RangerServerHealth;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.DOWN;
import static org.apache.ranger.plugin.model.RangerServerHealth.RangerServerStatus.UP;

@Component
public class RangerServerHealthUtil {
    private static final String COMPONENTS          = "components";
    private static final String STATUS              = "status";
    private static final String DETAILS             = "details";
    private static final String DB                  = "db";
    private static final String DB_FLAVOR           = "flavor";
    private static final String DB_VERSION          = "version";
    private static final String DB_VALIDATION_QUERY = "validationQuery";
    private static final String NOT_AVAILABLE       = "Not Available";

    /* RangerAdmin Health Check JSON Response look like
     {
       "status": "UP",
       "components": {
         "db": { "status":  "UP", "details": { "flavor": "Oracle", "version": "21.3c", "validationQuery": "SELECT banner from v$version where rownum<2" } }
       }
     }

    In the future this health check API can be extended for other components like audit store (Solr, Elastic search), plugins, usersync, tagsync, KMS, etc
     {
       "status": "UP",
       "components": {
         "db": { "status": "UP", "details": { "flavor": "Oracle", "version": "21.3c", "validationQuery": "SELECT banner from v$version where rownum<2" } },
         "audit-Elasticsearch": { "status": "UP", "details": { "provider": "Elastic Search", "providerHealthCheckEndpoint": "http://localhost:9200/_cluster/health?pretty" } }
       }
     }
    */

    public RangerServerHealth getRangerServerHealth(String dbVersion) {
        Map<String, Object> components = new HashMap<>();
        Map<String, Object> dbStatus   = getDbStatus(dbVersion);

        components.put(DB, dbStatus);

        final RangerServerHealth ret;

        if (Objects.equals(dbStatus.get(STATUS), UP)) {
            ret = RangerServerHealth.up().withDetail(COMPONENTS, components).build();
        } else {
            ret = RangerServerHealth.down().withDetail(COMPONENTS, components).build();
        }

        return ret;
    }

    private Map<String, Object> getDbStatus(String dbVersion) {
        Map<String, Object> ret      = new LinkedHashMap<>();
        int                 dbFlavor = RangerBizUtil.getDBFlavor();
        Map<String, Object> details  = new LinkedHashMap<>();

        details.put(DB_FLAVOR, RangerBizUtil.getDBFlavorType(dbFlavor));
        details.put(DB_VERSION, dbVersion);
        details.put(DB_VALIDATION_QUERY, RangerBizUtil.getDBVersionQuery(dbFlavor));

        ret.put(DETAILS, details);

        if (dbFlavor == AppConstants.DB_FLAVOR_UNKNOWN || StringUtils.contains(dbVersion, NOT_AVAILABLE)) {
            ret.put(STATUS, DOWN);
        } else {
            ret.put(STATUS, UP);
        }

        return ret;
    }
}
