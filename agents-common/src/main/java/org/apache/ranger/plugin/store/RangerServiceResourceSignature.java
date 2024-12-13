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

package org.apache.ranger.plugin.store;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceResource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RangerServiceResourceSignature {
    private final String strValue;
    private final String hashValue;

    public RangerServiceResourceSignature(RangerServiceResource serviceResource) {
        strValue = ServiceResourceSerializer.toString(serviceResource);
        if (RangerAdminConfig.getInstance().isFipsEnabled()) {
            hashValue = DigestUtils.sha512Hex(strValue);
        } else {
            hashValue = DigestUtils.sha256Hex(strValue);
        }
    }

    public String getSignature() {
        return hashValue;
    }

    String asString() {
        return strValue;
    }

    static class ServiceResourceSerializer {
        static final int _SignatureVersion = 1;

        public static String toString(final RangerServiceResource serviceResource) {
            // invalid/empty serviceResource gets a deterministic signature as if it had an
            // empty resource string
            Map<String, RangerPolicy.RangerPolicyResource> resource  = serviceResource.getResourceElements();
            Map<String, ResourceSerializer>                resources = new TreeMap<>();
            for (Map.Entry<String, RangerPolicy.RangerPolicyResource> entry : resource.entrySet()) {
                String             resourceName = entry.getKey();
                ResourceSerializer resourceView = new ResourceSerializer(entry.getValue());
                resources.put(resourceName, resourceView);
            }
            String resourcesAsString = resources.toString();
            return String.format("{version=%d,resource=%s}", _SignatureVersion, resourcesAsString);
        }

        static class ResourceSerializer {
            final RangerPolicy.RangerPolicyResource policyResource;

            ResourceSerializer(RangerPolicy.RangerPolicyResource policyResource) {
                this.policyResource = policyResource;
            }

            @Override
            public String toString() {
                StringBuilder builder = new StringBuilder();
                builder.append("{");
                if (policyResource != null) {
                    builder.append("values=");
                    if (policyResource.getValues() != null) {
                        List<String> values = new ArrayList<>(policyResource.getValues());
                        Collections.sort(values);
                        builder.append(values);
                    }

                    builder.append(",excludes=");
                    if (policyResource.getIsExcludes() == null) { // null is same as false
                        builder.append(Boolean.FALSE);
                    } else {
                        builder.append(policyResource.getIsExcludes());
                    }

                    builder.append(",recursive=");
                    if (policyResource.getIsRecursive() == null) { // null is the same as false
                        builder.append(Boolean.FALSE);
                    } else {
                        builder.append(policyResource.getIsRecursive());
                    }
                }
                builder.append("}");
                return builder.toString();
            }
        }
    }
}
