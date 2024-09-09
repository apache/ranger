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
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RangerServiceTagsDeltaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RangerServiceTagsDeltaUtil.class);

    private static final Logger PERF_TAGS_DELTA_LOG = RangerPerfTracer.getPerfLogger("tags.delta");

    /*
    It should be possible to call applyDelta() multiple times with serviceTags and delta resulting from previous call to applyDelta()
    The end result should be same if called once or multiple times.
     */
    static public ServiceTags applyDelta(ServiceTags serviceTags, ServiceTags delta, boolean supportsTagsDedup) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerServiceTagsDeltaUtil.applyDelta(): serviceTags:[" + serviceTags + "], delta:[" + delta + "], supportsTagsDedup:[" + supportsTagsDedup + "]");
        }

        ServiceTags      ret  = serviceTags;
        RangerPerfTracer perf = null;

        if(RangerPerfTracer.isPerfTraceEnabled(PERF_TAGS_DELTA_LOG)) {
            perf = RangerPerfTracer.getPerfTracer(PERF_TAGS_DELTA_LOG, "RangerServiceTagsDeltaUtil.applyDelta()");
        }

        if (serviceTags != null && !serviceTags.getIsDelta() && delta != null && delta.getIsDelta()) {
            ret = new ServiceTags(serviceTags);

            ret.setServiceName(delta.getServiceName());
            ret.setTagVersion(delta.getTagVersion());
            ret.setIsTagsDeduped(delta.getIsTagsDeduped());

            int tagDefsAdded = 0, tagDefsUpdated = 0, tagDefsRemoved = 0;
            int tagsAdded    = 0, tagsUpdated    = 0, tagsRemoved    = 0;

            Map<Long, RangerTagDef> tagDefs = ret.getTagDefinitions();

            for (Iterator<Map.Entry<Long, RangerTagDef>> deltaTagDefIter = delta.getTagDefinitions().entrySet().iterator(); deltaTagDefIter.hasNext(); ) {
                Map.Entry<Long, RangerTagDef> entry         = deltaTagDefIter.next();
                Long                          deltaTagDefId = entry.getKey();
                RangerTagDef                  deltaTagDef   = entry.getValue();

                if (StringUtils.isEmpty(deltaTagDef.getName())) { // tagdef has been removed
                    RangerTagDef removedTagDef = tagDefs.remove(deltaTagDefId);

                    if (removedTagDef != null) {
                        tagDefsRemoved++;
                    }
                }

                RangerTagDef existing = tagDefs.put(deltaTagDefId, deltaTagDef);

                if (existing == null) {
                    tagDefsAdded++;
                } else if (!existing.equals(deltaTagDef)) {
                    tagDefsUpdated++;
                }
            }

            Map<Long, RangerTag> tags           = ret.getTags();
            Map<Long, Long>      replacedTagIds = new HashMap<>();

            for (Iterator<Map.Entry<Long, RangerTag>> deltaTagIter = delta.getTags().entrySet().iterator(); deltaTagIter.hasNext(); ) {
                Map.Entry<Long, RangerTag> entry      = deltaTagIter.next();
                Long                       deltaTagId = entry.getKey();
                RangerTag                  deltaTag   = entry.getValue();

                if (StringUtils.isEmpty(deltaTag.getType())) { // tag has been removed
                    if (supportsTagsDedup) {
                        boolean found   = false;

                        for (Iterator<Map.Entry<RangerTag, MutablePair<Long, Long>>> iterator = ret.cachedTags.entrySet().iterator(); iterator.hasNext(); ) {
                            MutablePair<Long, Long> value = iterator.next().getValue();
                            if (value.left.equals(deltaTagId)) {
                                if (--value.right == 0) {
                                    // This may never be true when this tag is duplicated
                                    // as the mapping between de-duplicated tags is not maintained - only the reference count is stored
                                    // So, the tag with the smallest tag-id (among duplicate tags) will never be removed
                                    if (tags.remove(deltaTagId) != null) {
                                        tagsRemoved++;
                                    }
                                    iterator.remove();
                                }
                                found = true;
                                break;
                            }
                        }

                        if (!found) {
                            if (tags.remove(deltaTagId) != null) {
                                tagsRemoved++;
                            }
                        }
                    } else {
                        if (tags.remove(deltaTagId) != null) {
                            tagsRemoved++;
                        }
                    }
                } else {
                    if (supportsTagsDedup) {
                        MutablePair<Long, Long> cachedTag = ret.cachedTags.get(deltaTag);

                        if (cachedTag == null) {
                            ret.cachedTags.put(deltaTag, new MutablePair<>(deltaTagId, 1L));
                            tags.put(deltaTagId, deltaTag);
                            tagsAdded++;
                        } else {
                            cachedTag.right++;
                            replacedTagIds.put(deltaTagId, cachedTag.left);
                        }
                    } else {
                        RangerTag existing = tags.put(deltaTagId, deltaTag);

                        if (existing == null) {
                            tagsAdded++;
                        } else if (!existing.equals(deltaTag)) {
                            tagsUpdated++;
                        }
                    }
                }
            }

            List<RangerServiceResource>      serviceResources  = ret.getServiceResources();
            Map<Long, List<Long>>            resourceToTagIds  = ret.getResourceToTagIds();
            Map<Long, RangerServiceResource> idResourceMap     = serviceResources.stream().collect(Collectors.toMap(RangerServiceResource::getId, Function.identity()));
            Map<Long, RangerServiceResource> resourcesToRemove = new HashMap<>();
            Map<Long, RangerServiceResource> resourcesToAdd    = new HashMap<>();

            for (RangerServiceResource resource : delta.getServiceResources()) {
                RangerServiceResource existingResource = idResourceMap.get(resource.getId());

                if (existingResource != null) {
                    if (StringUtils.isNotEmpty(resource.getResourceSignature())) {
                        if (!StringUtils.equals(resource.getResourceSignature(), existingResource.getResourceSignature())) {  // ServiceResource changed; replace existing instance
                            /* If the signature changed, we need to remove existing resource and add new resource */
                            resourcesToRemove.put(resource.getId(), existingResource);
                            resourcesToAdd.put(resource.getId(), resource);
                        }
                    } else { // resource deleted
                        resourcesToRemove.put(resource.getId(), existingResource);

                        resourceToTagIds.remove(existingResource.getId());
                    }
                } else { // resource added
                    if (StringUtils.isNotEmpty(resource.getResourceSignature())) {
                        resourcesToAdd.put(resource.getId(), resource);
                    }
                }
            }

            if (!resourcesToRemove.isEmpty()) {
                for (ListIterator<RangerServiceResource> iter = serviceResources.listIterator(); iter.hasNext(); ) {
                    RangerServiceResource resource          = iter.next();
                    RangerServiceResource deletedResource   = resourcesToRemove.get(resource.getId());
                    RangerServiceResource addedResource     = resourcesToAdd.get(resource.getId());

                    if (addedResource == null && deletedResource == resource) {
                        iter.remove();
                    }
                }
            }

            serviceResources.addAll(resourcesToAdd.values());

            if (!replacedTagIds.isEmpty()) {
                for (Map.Entry<Long, List<Long>> resourceEntry : delta.getResourceToTagIds().entrySet()) {
                    ListIterator<Long> listIter = resourceEntry.getValue().listIterator();

                    while (listIter.hasNext()) {
                        Long tagId = listIter.next();
                        Long replacerTagId = replacedTagIds.get(tagId);

                        if (replacerTagId != null) {
                            listIter.set(replacerTagId);
                        }
                    }
                }
            }

            resourceToTagIds.putAll(delta.getResourceToTagIds());

            if (MapUtils.isEmpty(resourceToTagIds)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("There are no resource->tag mappings!!");
                }
                if (MapUtils.isNotEmpty(ret.getTags())) {
                    LOG.warn("There are no resource->tag mappings, but there are tags in the ServiceTags!! Cleaning up");
                    ret.getTags().clear();
                }
                if (supportsTagsDedup) {
                    ret.cachedTags.clear();
                }
            }

            // Ensure that any modified service-resources are at head of list of service-resources in delta
            // So that in setServiceTags(), they get cleaned out first, and service-resource with new spec gets added

            List<RangerServiceResource> deltaServiceResources = new ArrayList<>();
            for (RangerServiceResource resourceToRemove : resourcesToRemove.values()) {
                resourceToRemove.setResourceSignature(null);
                deltaServiceResources.add(resourceToRemove);
            }
            if (!resourcesToAdd.isEmpty()) {
                deltaServiceResources.addAll(resourcesToAdd.values());
            }

            delta.setServiceResources(deltaServiceResources);

            if (LOG.isDebugEnabled()) {
                LOG.debug("RangerServiceTagsDeltaUtil.applyDelta(): delta(tagDefs={}, tags={}, resources={}), " +
                          "resources(total={}, added={}, removed={}), " +
                          "tags(total={}, added={}, updated={}, removed={}), " +
                          "tagDefs(total={}, added={}, updated={}, removed={})",
                        delta.getTagDefinitions().size(), delta.getTags().size(), delta.getServiceResources().size(),
                        serviceResources.size(), resourcesToAdd.size(), resourcesToRemove.size(),
                        tags.size(), tagsAdded, tagsUpdated, tagsRemoved,
                        tagDefs.size(), tagDefsAdded, tagDefsUpdated, tagDefsRemoved);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cannot apply deltas to service-tags as one of preconditions is violated. Returning received serviceTags without applying delta!!");
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerServiceTagsDeltaUtil.applyDelta(): serviceTags:[" + ret + "], delta:[" + delta + "], supportsTagsDedup:[" + supportsTagsDedup + "]");
        }

        RangerPerfTracer.log(perf);

        return ret;
    }

    public static void pruneUnusedAttributes(ServiceTags serviceTags) {
        if (serviceTags != null) {
            serviceTags.setTagUpdateTime(null);

            for (Map.Entry<Long, RangerTagDef> entry : serviceTags.getTagDefinitions().entrySet()) {
                pruneUnusedAttributes(entry.getValue());
            }

            for (Map.Entry<Long, RangerTag> entry : serviceTags.getTags().entrySet()) {
                pruneUnusedAttributes(entry.getValue());
            }

            for (RangerServiceResource serviceResource : serviceTags.getServiceResources()) {
                pruneUnusedAttributes(serviceResource);
            }
        }
    }

    public static void pruneUnusedAttributes(RangerTagDef tagDef) {
        tagDef.setCreatedBy(null);
        tagDef.setCreateTime(null);
        tagDef.setUpdatedBy(null);
        tagDef.setUpdateTime(null);
        tagDef.setGuid(null);
        tagDef.setVersion(null);

        if (tagDef.getAttributeDefs() != null && tagDef.getAttributeDefs().isEmpty()) {
            tagDef.setAttributeDefs(null);
        }
    }

    public static void pruneUnusedAttributes(RangerTag tag) {
        tag.setCreatedBy(null);
        tag.setCreateTime(null);
        tag.setUpdatedBy(null);
        tag.setUpdateTime(null);
        tag.setGuid(null);
        tag.setVersion(null);

        if (tag.getOwner() != null && tag.getOwner().shortValue() == RangerTag.OWNER_SERVICERESOURCE) {
            tag.setOwner(null);
        }

        if (tag.getAttributes() != null && tag.getAttributes().isEmpty()) {
            tag.setAttributes(null);
        }

        if (tag.getOptions() != null && tag.getOptions().isEmpty()) {
            tag.setOptions(null);
        }

        if (tag.getValidityPeriods() != null && tag.getValidityPeriods().isEmpty()) {
            tag.setValidityPeriods(null);
        }
    }

    public static void pruneUnusedAttributes(RangerServiceResource serviceResource) {
        serviceResource.setCreatedBy(null);
        serviceResource.setCreateTime(null);
        serviceResource.setUpdatedBy(null);
        serviceResource.setUpdateTime(null);
        serviceResource.setGuid(null);
        serviceResource.setVersion(null);

        if (serviceResource.getAdditionalInfo() != null && serviceResource.getAdditionalInfo().isEmpty()) {
            serviceResource.setAdditionalInfo(null);
        }
    }

}
