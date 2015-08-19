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

import org.apache.ranger.plugin.model.*;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;

import java.util.List;
import java.util.Map;

/**
 * Interface to backing store for the top-level TAG model objects
 */

public interface TagStore {
    void init() throws Exception;

    void setServiceStore(ServiceStore svcStore);

    RangerTagDef createTagDef(RangerTagDef tagDef) throws Exception;

    RangerTagDef updateTagDef(RangerTagDef TagDef) throws Exception;

    void deleteTagDef(String name) throws Exception;

	void deleteTagDefById(Long id) throws Exception;

	List<RangerTagDef> getTagDef(String name) throws Exception;

    RangerTagDef getTagDefById(Long id) throws Exception;

    List<RangerTagDef> getTagDefs(SearchFilter filter) throws Exception;

    PList<RangerTagDef> getPaginatedTagDefs(SearchFilter filter) throws Exception;

    RangerTag createTag(RangerTag tag) throws Exception;

    RangerTag updateTag(RangerTag tag) throws Exception;

    void deleteTagById(Long id) throws Exception;

    RangerTag getTagById(Long id) throws Exception;

    List<RangerTag> getTagsByName(String name) throws Exception;

    List<RangerTag> getTagsByExternalId(String externalId) throws Exception;

    List<RangerTag> getTags(SearchFilter filter) throws Exception;

    RangerServiceResource createServiceResource(RangerServiceResource resource) throws Exception;

    RangerServiceResource updateServiceResource(RangerServiceResource resource) throws Exception;

    void deleteServiceResourceById(Long id) throws Exception;

    List<RangerServiceResource> getServiceResourcesByExternalId(String externalId) throws Exception;

    RangerServiceResource getServiceResourceById(Long id) throws Exception;

    List<RangerServiceResource> getServiceResourcesByServiceAndResourceSpec(String serviceName, Map<String, RangerPolicy.RangerPolicyResource> resourceSpec) throws Exception;

    List<RangerServiceResource> getServiceResources(SearchFilter filter) throws Exception;

    RangerTagResourceMap createTagResourceMap(RangerTagResourceMap tagResourceMap) throws Exception;

    void deleteTagResourceMapById(Long id) throws Exception;

    List<RangerTagResourceMap> getTagResourceMap(String externalResourceId, String externalTagId) throws Exception;

    RangerTagResourceMap getTagResourceMapById(Long id) throws Exception;

    List<RangerTagResourceMap> getTagResourceMapsByTagId(Long tagId) throws Exception;

    List<RangerTagResourceMap> getTagResourceMapsByResourceId(Long resourceId) throws Exception;

    List<RangerTagResourceMap> getTagResourceMaps(SearchFilter filter) throws Exception;

    ServiceTags getServiceTagsIfUpdated(String serviceName, Long lastKnownVersion) throws Exception;

    PList<RangerTagResourceMap> getPaginatedTagResourceMaps(SearchFilter filter) throws Exception;

    List<String> getTags(String serviceName) throws Exception;

    List<String> lookupTags(String serviceName, String tagNamePattern) throws Exception;

    List<RangerTag> getTagsForServiceResource(Long resourceId) throws Exception;

    List<RangerTag> getTagsForServiceResourceByExtId(String resourceExtId) throws Exception;

    List<RangerTagDef> getTagDefsByExternalId(String extId) throws Exception;

}
