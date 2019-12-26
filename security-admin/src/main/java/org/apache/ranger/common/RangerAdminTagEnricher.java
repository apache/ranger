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

package org.apache.ranger.common;

import org.apache.ranger.plugin.contextenricher.RangerTagEnricher;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.store.TagStore;
import org.apache.ranger.plugin.util.ServiceTags;

public class RangerAdminTagEnricher extends RangerTagEnricher {
    private static final Log LOG = LogFactory.getLog(RangerAdminTagEnricher.class);

    private static TagStore tagStore = null;

    private Long serviceId;

    public static void setTagStore(TagStore tagStore) {
        RangerAdminTagEnricher.tagStore = tagStore;
    }

    @Override
    public void init() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAdminTagEnricher.init()");
        }
        super.init();

        ServiceStore svcStore = tagStore != null ? tagStore.getServiceStore() : null;

        if (tagStore == null || svcStore == null) {
            LOG.error("ServiceDBStore/TagDBStore is not initialized!! Internal Error!");
        } else {
            try {
                RangerService service = svcStore.getServiceByName(serviceName);
                serviceId = service.getId();
            } catch (Exception e) {
                LOG.error("Cannot find service with name:[" + serviceName + "]", e);
                LOG.error("This will cause tag-enricher in Ranger-Admin to fail!!");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAdminTagEnricher.init()");
        }
    }

    @Override
    public void enrich(RangerAccessRequest request) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerAdminTagEnricher.enrich(" + request + ")");
        }

        refreshTagsIfNeeded();
        super.enrich(request);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerAdminTagEnricher.enrich(" + request + ")");
        }
    }

    private void refreshTagsIfNeeded() {
        ServiceTags serviceTags = null;
        try {
            serviceTags = RangerServiceTagsCache.getInstance().getServiceTags(serviceName, serviceId, tagStore);
        } catch (Exception e) {
            LOG.error("Could not get cached service-tags, continue to use old ones..", e);
        }

        if (serviceTags != null) {
            Long enrichedServiceTagsVersion = getServiceTagsVersion();

            if (enrichedServiceTagsVersion == null || !enrichedServiceTagsVersion.equals(serviceTags.getTagVersion())) {
                synchronized(this) {
                    enrichedServiceTagsVersion = getServiceTagsVersion();

                    if (enrichedServiceTagsVersion == null || !enrichedServiceTagsVersion.equals(serviceTags.getTagVersion())) {
                        setServiceTags(serviceTags);
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RangerAdminTagEnricher={serviceName=").append(serviceName).append(", ");
        sb.append("serviceId=").append(serviceId).append("}");
        return sb.toString();
    }
}
