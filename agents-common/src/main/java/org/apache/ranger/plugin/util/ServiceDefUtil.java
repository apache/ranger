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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class ServiceDefUtil {

    public static RangerResourceDef getResourceDef(RangerServiceDef serviceDef, String resource) {
        RangerResourceDef ret = null;

        if(serviceDef != null && resource != null && CollectionUtils.isNotEmpty(serviceDef.getResources())) {
            for(RangerResourceDef resourceDef : serviceDef.getResources()) {
                if(StringUtils.equalsIgnoreCase(resourceDef.getName(), resource)) {
                    ret = resourceDef;
                    break;
                }
            }
        }

        return ret;
    }

    public static Integer getLeafResourceLevel(RangerServiceDef serviceDef, Map<String, RangerPolicy.RangerPolicyResource> policyResource) {
        Integer ret = null;

        if(serviceDef != null && policyResource != null) {
            for(Map.Entry<String, RangerPolicy.RangerPolicyResource> entry : policyResource.entrySet()) {
                String            resource    = entry.getKey();
                RangerResourceDef resourceDef = ServiceDefUtil.getResourceDef(serviceDef, resource);

                if(resourceDef != null && resourceDef.getLevel() != null) {
                    if(ret == null) {
                        ret = resourceDef.getLevel();
                    } else if(ret < resourceDef.getLevel()) {
                        ret = resourceDef.getLevel();
                    }
                }
            }
        }

        return ret;
    }
}
