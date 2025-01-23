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

package org.apache.ranger.biz;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerAdminTagEnricher;
import org.apache.ranger.common.RangerServiceTagsCache;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXTagDao;
import org.apache.ranger.db.XXTagDefDao;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceResource;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.entity.XXTagChangeLog;
import org.apache.ranger.entity.XXTagDef;
import org.apache.ranger.entity.XXTagResourceMap;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagResourceMap;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.model.validation.RangerValidityScheduleValidator;
import org.apache.ranger.plugin.model.validation.ValidationFailureDetails;
import org.apache.ranger.plugin.store.AbstractTagStore;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.store.RangerServiceResourceSignature;
import org.apache.ranger.plugin.util.RangerCommonConstants;
import org.apache.ranger.plugin.util.RangerPerfTracer;
import org.apache.ranger.plugin.util.RangerServiceNotFoundException;
import org.apache.ranger.plugin.util.RangerServiceTagsDeltaUtil;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.service.RangerServiceResourceService;
import org.apache.ranger.service.RangerServiceResourceWithTagsService;
import org.apache.ranger.service.RangerTagDefService;
import org.apache.ranger.service.RangerTagResourceMapService;
import org.apache.ranger.service.RangerTagService;
import org.apache.ranger.view.RangerServiceResourceWithTagsList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Component
public class TagDBStore extends AbstractTagStore {
    private static final Logger LOG      = LoggerFactory.getLogger(TagDBStore.class);
    private static final Logger PERF_LOG = RangerPerfTracer.getPerfLogger("db.TagDBStore");

    public static boolean SUPPORTS_IN_PLACE_TAG_UPDATES;

    private static boolean SUPPORTS_TAG_DELTAS;
    private static boolean IS_SUPPORTS_TAG_DELTAS_INITIALIZED;
    private static boolean SUPPORTS_TAGS_DEDUP_INITIALIZED;
    private static boolean SUPPORTS_TAGS_DEDUP;

    @Autowired
    RangerTagDefService rangerTagDefService;

    @Autowired
    RangerTagService rangerTagService;

    @Autowired
    RangerServiceResourceService rangerServiceResourceService;

    @Autowired
    RangerServiceResourceWithTagsService rangerServiceResourceWithTagsService;

    @Autowired
    RangerTagResourceMapService rangerTagResourceMapService;

    @Autowired
    RangerDaoManager daoManager;

    @Autowired
    @Qualifier(value = "transactionManager")
    PlatformTransactionManager txManager;

    @Autowired
    RESTErrorUtil errorUtil;

    @Autowired
    RESTErrorUtil restErrorUtil;
    RangerAdminConfig config;

    public static boolean isSupportsTagDeltas() {
        initStatics();

        return SUPPORTS_TAG_DELTAS;
    }

    public static RangerServiceResource toRangerServiceResource(String serviceName, Map<String, String[]> resourceMap) {
        LOG.debug("==> TagDBStore.toRangerServiceResource(): serviceName={{}}", serviceName);

        Map<String, RangerPolicyResource> resourceElements = new HashMap<>();

        for (Map.Entry<String, String[]> entry : resourceMap.entrySet()) {
            String[] parts      = entry.getKey().split("\\.");
            String[] valueArray = entry.getValue();

            if (parts.length < 1 || valueArray == null) {
                continue;
            }

            String               key            = parts[0];
            RangerPolicyResource policyResource = resourceElements.get(key);

            if (policyResource == null) {
                policyResource = new RangerPolicyResource();

                resourceElements.put(key, policyResource);
            }

            if (parts.length == 1) {
                List<String> valueList = new ArrayList<>(valueArray.length);

                for (String str : valueArray) {
                    valueList.add(str.trim());
                }

                policyResource.setValues(valueList);
            } else if (parts.length == 2 && valueArray[0] != null) {
                String subKey = parts[1];
                String value  = valueArray[0];

                if (subKey.equalsIgnoreCase("isExcludes")) {
                    policyResource.setIsExcludes(Boolean.parseBoolean(value.trim()));
                } else if (subKey.equalsIgnoreCase("isRecursive")) {
                    policyResource.setIsRecursive(Boolean.parseBoolean(value.trim()));
                }
            }
        }

        RangerServiceResource ret = new RangerServiceResource(serviceName, resourceElements);

        LOG.debug("<== TagDBStore.toRangerServiceResource(): (serviceName={{}} RangerServiceResource={{}})", serviceName, ret);

        return ret;
    }

    public static boolean isSupportsTagsDedup() {
        if (!SUPPORTS_TAGS_DEDUP_INITIALIZED) {
            RangerAdminConfig config = RangerAdminConfig.getInstance();

            SUPPORTS_TAGS_DEDUP             = config.getBoolean("ranger.admin" + RangerCommonConstants.RANGER_SUPPORTS_TAGS_DEDUP, RangerCommonConstants.RANGER_SUPPORTS_TAGS_DEDUP_DEFAULT);
            SUPPORTS_TAGS_DEDUP_INITIALIZED = true;
        }
        return SUPPORTS_TAGS_DEDUP;
    }

    @PostConstruct
    public void initStore() {
        config = RangerAdminConfig.getInstance();

        RangerAdminTagEnricher.setTagStore(this);
        RangerAdminTagEnricher.setDaoManager(daoManager);
    }

    @Override
    public RangerTagDef createTagDef(RangerTagDef tagDef) {
        LOG.debug("==> TagDBStore.createTagDef({})", tagDef);

        RangerTagDef ret = rangerTagDefService.create(tagDef);

        ret = rangerTagDefService.read(ret.getId());

        LOG.debug("<== TagDBStore.createTagDef({}): id={}", tagDef, ret == null ? null : ret.getId());

        return ret;
    }

    @Override
    public RangerTagDef updateTagDef(RangerTagDef tagDef) {
        LOG.debug("==> TagDBStore.updateTagDef({})", tagDef);

        RangerTagDef existing = rangerTagDefService.read(tagDef.getId());

        if (existing == null) {
            throw errorUtil.createRESTException("failed to update tag-def [" + tagDef.getName() + "], Reason: No TagDef found with id: [" + tagDef.getId() + "]", MessageEnums.DATA_NOT_UPDATABLE);
        } else if (!existing.getName().equals(tagDef.getName())) {
            throw errorUtil.createRESTException("Cannot change tag-def name; existing-name:[" + existing.getName() + "], new-name:[" + tagDef.getName() + "]", MessageEnums.DATA_NOT_UPDATABLE);
        }

        tagDef.setCreatedBy(existing.getCreatedBy());
        tagDef.setCreateTime(existing.getCreateTime());
        tagDef.setGuid(existing.getGuid());
        tagDef.setVersion(existing.getVersion());

        RangerTagDef ret = rangerTagDefService.update(tagDef);

        ret = rangerTagDefService.read(ret.getId());

        LOG.debug("<== TagDBStore.updateTagDef({}): {}", tagDef, ret);

        return ret;
    }

    @Override
    public void deleteTagDefByName(String name) throws Exception {
        LOG.debug("==> TagDBStore.deleteTagDefByName({})", name);

        if (StringUtils.isNotBlank(name)) {
            deleteTagDef(getTagDefByName(name));
        }

        LOG.debug("<== TagDBStore.deleteTagDefByName({})", name);
    }

    @Override
    public void deleteTagDef(Long id) throws Exception {
        LOG.debug("==> TagDBStore.deleteTagDef({})", id);

        if (id != null) {
            deleteTagDef(rangerTagDefService.read(id));
        }

        LOG.debug("<== TagDBStore.deleteTagDef({})", id);
    }

    @Override
    public RangerTagDef getTagDef(Long id) {
        LOG.debug("==> TagDBStore.getTagDef({})", id);

        RangerTagDef ret = rangerTagDefService.read(id);

        LOG.debug("<== TagDBStore.getTagDef({}): {}", id, ret);

        return ret;
    }

    @Override
    public RangerTagDef getTagDefByGuid(String guid) {
        LOG.debug("==> TagDBStore.getTagDefByGuid({})", guid);

        RangerTagDef ret = rangerTagDefService.getTagDefByGuid(guid);

        LOG.debug("<== TagDBStore.getTagDefByGuid({}): {}", guid, ret);

        return ret;
    }

    @Override
    public RangerTagDef getTagDefByName(String name) {
        LOG.debug("==> TagDBStore.getTagDefByName({})", name);

        RangerTagDef ret = null;

        if (StringUtils.isNotBlank(name)) {
            ret = rangerTagDefService.getTagDefByName(name);
        }

        LOG.debug("<== TagDBStore.getTagDefByName({}): {}", name, ret);

        return ret;
    }

    @Override
    public List<RangerTagDef> getTagDefs(SearchFilter filter) {
        LOG.debug("==> TagDBStore.getTagDefs({})", filter);

        List<RangerTagDef> ret = getPaginatedTagDefs(filter).getList();

        LOG.debug("<== TagDBStore.getTagDefs({}): {}", filter, ret);

        return ret;
    }

    @Override
    public PList<RangerTagDef> getPaginatedTagDefs(SearchFilter filter) {
        LOG.debug("==> TagDBStore.getPaginatedTagDefs({})", filter);

        PList<RangerTagDef> ret = rangerTagDefService.searchRangerTagDefs(filter);

        LOG.debug("<== TagDBStore.getPaginatedTagDefs({}): {}", filter, ret);

        return ret;
    }

    @Override
    public List<String> getTagTypes() {
        LOG.debug("==> TagDBStore.getTagTypes()");

        List<String> ret = daoManager.getXXTagDef().getAllNames();

        LOG.debug("<== TagDBStore.getTagTypes(): count={}", ret != null ? ret.size() : 0);

        return ret;
    }

    @Override
    public RangerTag createTag(RangerTag tag) throws Exception {
        LOG.debug("==> TagDBStore.createTag({})", tag);

        tag = validateTag(tag);

        RangerTag ret = rangerTagService.create(tag);

        ret = rangerTagService.read(ret.getId());

        LOG.debug("<== TagDBStore.createTag({}): {}", tag, ret);

        return ret;
    }

    @Override
    public RangerTag updateTag(RangerTag tag) throws Exception {
        LOG.debug("==> TagDBStore.updateTag({})", tag);

        tag = validateTag(tag);

        RangerTag existing = rangerTagService.read(tag.getId());

        if (existing == null) {
            throw errorUtil.createRESTException("failed to update tag [" + tag.getType() + "], Reason: No Tag found with id: [" + tag.getId() + "]", MessageEnums.DATA_NOT_UPDATABLE);
        }

        tag.setCreatedBy(existing.getCreatedBy());
        tag.setCreateTime(existing.getCreateTime());
        tag.setGuid(existing.getGuid());
        tag.setVersion(existing.getVersion());

        RangerTag ret = rangerTagService.update(tag);

        ret = rangerTagService.read(ret.getId());

        LOG.debug("<== TagDBStore.updateTag({}) : {}", tag, ret);

        return ret;
    }

    @Override
    public void deleteTag(Long id) {
        LOG.debug("==> TagDBStore.deleteTag({})", id);

        RangerTag tag = rangerTagService.read(id);

        rangerTagService.delete(tag);

        LOG.debug("<== TagDBStore.deleteTag({})", id);
    }

    @Override
    public RangerTag getTag(Long id) {
        LOG.debug("==> TagDBStore.getTag({})", id);

        RangerTag ret = rangerTagService.read(id);

        LOG.debug("<== TagDBStore.getTag({}): {}", id, ret);

        return ret;
    }

    @Override
    public RangerTag getTagByGuid(String guid) {
        LOG.debug("==> TagDBStore.getTagByGuid({})", guid);

        RangerTag ret = rangerTagService.getTagByGuid(guid);

        LOG.debug("<== TagDBStore.getTagByGuid({}): {}", guid, ret);

        return ret;
    }

    @Override
    public List<Long> getTagIdsForResourceId(Long resourceId) {
        LOG.debug("==> TagDBStore.getTagIdsForResourceId({})", resourceId);

        List<Long> ret = rangerTagResourceMapService.getTagIdsForResourceId(resourceId);

        LOG.debug("<== TagDBStore.getTagIdsForResourceId({}): count={}", resourceId, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public List<RangerTag> getTagsByType(String type) {
        LOG.debug("==> TagDBStore.getTagsByType({})", type);

        List<RangerTag> ret = null;

        if (StringUtils.isNotBlank(type)) {
            ret = rangerTagService.getTagsByType(type);
        }

        LOG.debug("<== TagDBStore.getTagsByType({}): count={}", type, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public List<RangerTag> getTagsForResourceId(Long resourceId) {
        LOG.debug("==> TagDBStore.getTagsForResourceId({})", resourceId);

        List<RangerTag> ret = null;

        if (resourceId != null) {
            ret = rangerTagService.getTagsForResourceId(resourceId);
        }

        LOG.debug("<== TagDBStore.getTagsForResourceId({}): count={}", resourceId, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public List<RangerTag> getTagsForResourceGuid(String resourceGuid) {
        LOG.debug("==> TagDBStore.getTagsForResourceGuid({})", resourceGuid);

        List<RangerTag> ret = null;

        if (resourceGuid != null) {
            ret = rangerTagService.getTagsForResourceGuid(resourceGuid);
        }

        LOG.debug("<== TagDBStore.getTagsForResourceGuid({}): count={}", resourceGuid, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public List<RangerTag> getTags(SearchFilter filter) throws Exception {
        LOG.debug("==> TagDBStore.getTags({})", filter);

        List<RangerTag> ret = rangerTagService.searchRangerTags(filter).getList();

        LOG.debug("<== TagDBStore.getTags({}): count={}", filter, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public PList<RangerTag> getPaginatedTags(SearchFilter filter) {
        LOG.debug("==> TagDBStore.getPaginatedTags({})", filter);

        PList<RangerTag> ret = rangerTagService.searchRangerTags(filter);

        LOG.debug("<== TagDBStore.getPaginatedTags({}): count={}", filter, ret == null ? 0 : ret.getPageSize());

        return ret;
    }

    @Override
    public RangerServiceResource createServiceResource(RangerServiceResource resource) {
        LOG.debug("==> TagDBStore.createServiceResource({})", resource);

        if (StringUtils.isEmpty(resource.getResourceSignature())) {
            RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

            resource.setResourceSignature(serializer.getSignature());
        }

        RangerServiceResource ret = rangerServiceResourceService.create(resource);

        ret = rangerServiceResourceService.read(ret.getId());

        LOG.debug("<== TagDBStore.createServiceResource({})", resource);

        return ret;
    }

    @Override
    public RangerServiceResource updateServiceResource(RangerServiceResource resource) {
        LOG.debug("==> TagDBStore.updateResource({})", resource);

        RangerServiceResource existing = rangerServiceResourceService.read(resource.getId());

        if (existing == null) {
            throw errorUtil.createRESTException("failed to update tag [" + resource.getId() + "], Reason: No resource found with id: [" + resource.getId() + "]", MessageEnums.DATA_NOT_UPDATABLE);
        }

        if (StringUtils.isEmpty(resource.getResourceSignature())) {
            RangerServiceResourceSignature serializer = new RangerServiceResourceSignature(resource);

            resource.setResourceSignature(serializer.getSignature());
        }

        resource.setCreatedBy(existing.getCreatedBy());
        resource.setCreateTime(existing.getCreateTime());
        resource.setGuid(existing.getGuid());
        resource.setVersion(existing.getVersion());

        rangerServiceResourceService.update(resource);

        RangerServiceResource ret = rangerServiceResourceService.read(existing.getId());

        LOG.debug("<== TagDBStore.updateResource({}) : {}", resource, ret);

        return ret;
    }

    @Override
    public void refreshServiceResource(Long resourceId) {
        XXServiceResource serviceResourceEntity = daoManager.getXXServiceResource().getById(resourceId);
        String            tagsText              = null;

        List<RangerTagResourceMap> tagResourceMaps = getTagResourceMapsForResourceId(resourceId);
        if (tagResourceMaps != null) {
            List<RangerTag> associatedTags = new ArrayList<>();
            for (RangerTagResourceMap element : tagResourceMaps) {
                associatedTags.add(getTag(element.getTagId()));
            }
            tagsText = JsonUtils.listToJson(associatedTags);
        }
        serviceResourceEntity.setTags(tagsText);
        daoManager.getXXServiceResource().update(serviceResourceEntity);
    }

    @Override
    public void deleteServiceResource(Long id) {
        LOG.debug("==> TagDBStore.deleteServiceResource({})", id);

        RangerServiceResource resource = getServiceResource(id);

        if (resource != null) {
            rangerServiceResourceService.delete(resource);
        }

        LOG.debug("<== TagDBStore.deleteServiceResource({})", id);
    }

    @Override
    public void deleteServiceResourceByGuid(String guid) {
        LOG.debug("==> TagDBStore.deleteServiceResourceByGuid({})", guid);

        RangerServiceResource resource = getServiceResourceByGuid(guid);

        if (resource != null) {
            rangerServiceResourceService.delete(resource);
        }

        LOG.debug("<== TagDBStore.deleteServiceResourceByGuid({})", guid);
    }

    @Override
    public RangerServiceResource getServiceResource(Long id) {
        LOG.debug("==> TagDBStore.getServiceResource({})", id);

        RangerServiceResource ret = rangerServiceResourceService.read(id);

        LOG.debug("<== TagDBStore.getServiceResource({}): {}", id, ret);

        return ret;
    }

    @Override
    public RangerServiceResource getServiceResourceByGuid(String guid) {
        LOG.debug("==> TagDBStore.getServiceResourceByGuid({})", guid);

        RangerServiceResource ret = rangerServiceResourceService.getServiceResourceByGuid(guid);

        LOG.debug("<== TagDBStore.getServiceResourceByGuid({}): {}", guid, ret);

        return ret;
    }

    @Override
    public List<RangerServiceResource> getServiceResourcesByService(String serviceName) {
        LOG.debug("==> TagDBStore.getServiceResourcesByService({})", serviceName);

        List<RangerServiceResource> ret = null;

        Long serviceId = daoManager.getXXService().findIdByName(serviceName);

        if (serviceId != null) {
            ret = rangerServiceResourceService.getByServiceId(serviceId);
        }

        LOG.debug("<== TagDBStore.getServiceResourcesByService({}): count={}", serviceName, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public List<String> getServiceResourceGuidsByService(String serviceName) {
        LOG.debug("==> TagDBStore.getServiceResourceGuidsByService({})", serviceName);

        List<String> ret = null;

        Long serviceId = daoManager.getXXService().findIdByName(serviceName);

        if (serviceId != null) {
            ret = daoManager.getXXServiceResource().findServiceResourceGuidsInServiceId(serviceId);
        }

        LOG.debug("<== TagDBStore.getServiceResourceGuidsByService({}): count={}", serviceName, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public RangerServiceResource getServiceResourceByServiceAndResourceSignature(String serviceName, String resourceSignature) {
        LOG.debug("==> TagDBStore.getServiceResourceByServiceAndResourceSignature({}, {})", serviceName, resourceSignature);

        RangerServiceResource ret = null;

        Long serviceId = daoManager.getXXService().findIdByName(serviceName);

        if (serviceId != null) {
            ret = rangerServiceResourceService.getByServiceAndResourceSignature(serviceId, resourceSignature);
        }

        LOG.debug("<== TagDBStore.getServiceResourceByServiceAndResourceSignature({}, {}): {}", serviceName, resourceSignature, ret);

        return ret;
    }

    @Override
    public List<RangerServiceResource> getServiceResources(SearchFilter filter) {
        LOG.debug("==> TagDBStore.getServiceResources({})", filter);

        List<RangerServiceResource> ret = rangerServiceResourceService.searchServiceResources(filter).getList();

        LOG.debug("<== TagDBStore.getServiceResources({}): count={}", filter, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public PList<RangerServiceResource> getPaginatedServiceResources(SearchFilter filter) {
        LOG.debug("==> TagDBStore.getPaginatedServiceResources({})", filter);

        PList<RangerServiceResource> ret = rangerServiceResourceService.searchServiceResources(filter);

        LOG.debug("<== TagDBStore.getPaginatedServiceResources({}): count={}", filter, ret == null ? 0 : ret.getPageSize());

        return ret;
    }

    @Override
    public RangerTagResourceMap createTagResourceMap(RangerTagResourceMap tagResourceMap) {
        LOG.debug("==> TagDBStore.createTagResourceMap({})", tagResourceMap);

        RangerTagResourceMap ret = rangerTagResourceMapService.create(tagResourceMap);

        // We also need to update tags stored with the resource
        refreshServiceResource(tagResourceMap.getResourceId());

        LOG.debug("<== TagDBStore.createTagResourceMap({}): {}", tagResourceMap, ret);

        return ret;
    }

    @Override
    public void deleteTagResourceMap(Long id) {
        LOG.debug("==> TagDBStore.deleteTagResourceMap({})", id);

        RangerTagResourceMap tagResourceMap = rangerTagResourceMapService.read(id);
        Long                 tagId          = tagResourceMap.getTagId();
        RangerTag            tag            = getTag(tagId);

        rangerTagResourceMapService.delete(tagResourceMap);

        if (tag.getOwner() == null || tag.getOwner() == RangerTag.OWNER_SERVICERESOURCE) {
            deleteTag(tagId);
        }
        // We also need to update tags stored with the resource
        refreshServiceResource(tagResourceMap.getResourceId());

        LOG.debug("<== TagDBStore.deleteTagResourceMap({})", id);
    }

    @Override
    public RangerTagResourceMap getTagResourceMap(Long id) {
        LOG.debug("==> TagDBStore.getTagResourceMap({})", id);

        RangerTagResourceMap ret = rangerTagResourceMapService.read(id);

        LOG.debug("<== TagDBStore.getTagResourceMap({})", id);

        return ret;
    }

    @Override
    public RangerTagResourceMap getTagResourceMapByGuid(String guid) {
        LOG.debug("==> TagDBStore.getTagResourceMapByGuid({})", guid);

        RangerTagResourceMap ret = rangerTagResourceMapService.getByGuid(guid);

        LOG.debug("<== TagDBStore.getTagResourceMapByGuid({})", guid);

        return ret;
    }

    @Override
    public List<RangerTagResourceMap> getTagResourceMapsForTagId(Long tagId) {
        LOG.debug("==> TagDBStore.getTagResourceMapsForTagId({})", tagId);

        List<RangerTagResourceMap> ret = rangerTagResourceMapService.getByTagId(tagId);

        LOG.debug("<== TagDBStore.getTagResourceMapsForTagId({}): count={}", tagId, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public List<RangerTagResourceMap> getTagResourceMapsForTagGuid(String tagGuid) {
        LOG.debug("==> TagDBStore.getTagResourceMapsForTagGuid({})", tagGuid);

        List<RangerTagResourceMap> ret = rangerTagResourceMapService.getByTagGuid(tagGuid);

        LOG.debug("<== TagDBStore.getTagResourceMapsForTagGuid({}): count={}", tagGuid, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public List<RangerTagResourceMap> getTagResourceMapsForResourceId(Long resourceId) {
        LOG.debug("==> TagDBStore.getTagResourceMapsForResourceId({})", resourceId);

        List<RangerTagResourceMap> ret = rangerTagResourceMapService.getByResourceId(resourceId);

        LOG.debug("<== TagDBStore.getTagResourceMapsForResourceId({}): count={}", resourceId, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public List<RangerTagResourceMap> getTagResourceMapsForResourceGuid(String resourceGuid) {
        LOG.debug("==> TagDBStore.getTagResourceMapsForResourceGuid({})", resourceGuid);

        List<RangerTagResourceMap> ret = rangerTagResourceMapService.getByResourceGuid(resourceGuid);

        LOG.debug("<== TagDBStore.getTagResourceMapsForResourceGuid({}): count={}", resourceGuid, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public RangerTagResourceMap getTagResourceMapForTagAndResourceId(Long tagId, Long resourceId) {
        LOG.debug("==> TagDBStore.getTagResourceMapsForTagAndResourceId({}, {})", tagId, resourceId);

        RangerTagResourceMap ret = rangerTagResourceMapService.getByTagAndResourceId(tagId, resourceId);

        LOG.debug("<== TagDBStore.getTagResourceMapsForTagAndResourceId({}, {}): {}", tagId, resourceId, ret);

        return ret;
    }

    @Override
    public RangerTagResourceMap getTagResourceMapForTagAndResourceGuid(String tagGuid, String resourceGuid) {
        LOG.debug("==> TagDBStore.getTagResourceMapForTagAndResourceGuid({}, {})", tagGuid, resourceGuid);

        RangerTagResourceMap ret = rangerTagResourceMapService.getByTagAndResourceGuid(tagGuid, resourceGuid);

        LOG.debug("<== TagDBStore.getTagResourceMapForTagAndResourceGuid({}, {}): {}", tagGuid, resourceGuid, ret);

        return ret;
    }

    @Override
    public List<RangerTagResourceMap> getTagResourceMaps(SearchFilter filter) {
        LOG.debug("==> TagDBStore.getTagResourceMaps({})", filter);

        List<RangerTagResourceMap> ret = rangerTagResourceMapService.searchRangerTaggedResources(filter).getList();

        LOG.debug("<== TagDBStore.getTagResourceMaps({}): count={}", filter, ret == null ? 0 : ret.size());

        return ret;
    }

    @Override
    public PList<RangerTagResourceMap> getPaginatedTagResourceMaps(SearchFilter filter) {
        LOG.debug("==> TagDBStore.getPaginatedTagResourceMaps({})", filter);

        PList<RangerTagResourceMap> ret = rangerTagResourceMapService.searchRangerTaggedResources(filter);

        LOG.debug("<== TagDBStore.getPaginatedTagResourceMaps({}): count={}", filter, ret == null ? 0 : ret.getPageSize());

        return ret;
    }

    @Override
    public ServiceTags getServiceTagsIfUpdated(String serviceName, Long lastKnownVersion, boolean needsBackwardCompatibility) throws Exception {
        LOG.debug("==> TagDBStore.getServiceTagsIfUpdated({}, {}, {})", serviceName, lastKnownVersion, needsBackwardCompatibility);

        ServiceTags ret       = null;
        Long        serviceId = daoManager.getXXService().findIdByName(serviceName);

        if (serviceId == null) {
            LOG.error("Requested Service not found. serviceName={}", serviceName);

            throw restErrorUtil.createRESTException(HttpServletResponse.SC_NOT_FOUND, RangerServiceNotFoundException.buildExceptionMsg(serviceName), false);
        }

        XXServiceVersionInfo serviceVersionInfoDbObj = daoManager.getXXServiceVersionInfo().findByServiceName(serviceName);

        if (serviceVersionInfoDbObj == null) {
            LOG.warn("serviceVersionInfo does not exist. name={}", serviceName);
        }

        if (lastKnownVersion == null || serviceVersionInfoDbObj == null || serviceVersionInfoDbObj.getTagVersion() == null || !lastKnownVersion.equals(serviceVersionInfoDbObj.getTagVersion())) {
            ret = RangerServiceTagsCache.getInstance().getServiceTags(serviceName, serviceId, lastKnownVersion, needsBackwardCompatibility, this);
        }

        if (ret != null && lastKnownVersion != null && lastKnownVersion.equals(ret.getTagVersion())) {
            // ServiceTags are not changed
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            RangerServiceTagsCache.getInstance().dump();
        }

        LOG.debug("<== TagDBStore.getServiceTagsIfUpdated({}, {}, {}): count={}", serviceName, lastKnownVersion, needsBackwardCompatibility, (ret == null || ret.getTags() == null) ? 0 : ret.getTags().size());

        return ret;
    }

    @Override
    public ServiceTags getServiceTags(String serviceName, Long lastKnownVersion) throws Exception {
        LOG.debug("==> TagDBStore.getServiceTags({}, {})", serviceName, lastKnownVersion);

        XXService xxService = daoManager.getXXService().findByName(serviceName);

        if (xxService == null) {
            throw new Exception("service does not exist. name=" + serviceName);
        }

        XXServiceVersionInfo serviceVersionInfoDbObj = daoManager.getXXServiceVersionInfo().findByServiceName(serviceName);

        if (serviceVersionInfoDbObj == null) {
            LOG.warn("serviceVersionInfo does not exist for service [{}]", serviceName);
        }

        RangerServiceDef serviceDef = svcStore.getServiceDef(xxService.getType());

        if (serviceDef == null) {
            throw new Exception("service-def does not exist. id=" + xxService.getType());
        }

        ServiceTags       delta = getServiceTagsDelta(xxService.getId(), serviceName, lastKnownVersion);
        final ServiceTags ret;

        if (delta != null) {
            ret = delta;
        } else {
            RangerTagDBRetriever tagDBRetriever = new RangerTagDBRetriever(daoManager, txManager, xxService);

            Map<Long, RangerTagDef>     tagDefMap        = tagDBRetriever.getTagDefs();
            Map<Long, RangerTag>        tagMap           = tagDBRetriever.getTags();
            List<RangerServiceResource> resources        = tagDBRetriever.getServiceResources();
            Map<Long, List<Long>>       resourceToTagIds = tagDBRetriever.getResourceToTagIds();

            ret = new ServiceTags();

            ret.setServiceName(xxService.getName());
            ret.setTagVersion(serviceVersionInfoDbObj == null ? null : serviceVersionInfoDbObj.getTagVersion());
            ret.setTagUpdateTime(serviceVersionInfoDbObj == null ? null : serviceVersionInfoDbObj.getTagUpdateTime());
            ret.setTagDefinitions(tagDefMap);
            ret.setTags(tagMap);
            ret.setServiceResources(resources);
            ret.setResourceToTagIds(resourceToTagIds);
            ret.setIsTagsDeduped(isSupportsTagsDedup());

            if (isSupportsTagsDedup()) {
                final int countOfDuplicateTags = ret.dedupTags();

                LOG.debug("Number of duplicate tags removed from the received serviceTags:[{}]. Number of tags in the de-duplicated serviceTags :[{}].", countOfDuplicateTags, ret.getTags().size());
            }
        }

        LOG.debug("<== TagDBStore.getServiceTags({}, {})", serviceName, lastKnownVersion);

        return ret;
    }

    @Override
    public ServiceTags getServiceTagsDelta(String serviceName, Long lastKnownVersion) throws Exception {
        LOG.debug("==> TagDBStore.getServiceTagsDelta({}, {})", serviceName, lastKnownVersion);

        final ServiceTags ret;

        if (lastKnownVersion == -1L || !isSupportsTagDeltas()) {
            LOG.debug("Returning without computing tags-deltas.., SUPPORTS_TAG_DELTAS:[{}], lastKnownVersion:[{}]", SUPPORTS_TAG_DELTAS, lastKnownVersion);

            ret = null;
        } else {
            Long serviceId = daoManager.getXXService().findIdByName(serviceName);

            if (serviceId == null) {
                throw new Exception("service does not exist. name=" + serviceName);
            }

            ret = getServiceTagsDelta(serviceId, serviceName, lastKnownVersion);
        }

        LOG.debug("<== TagDBStore.getServiceTagsDelta({}, {})", serviceName, lastKnownVersion);

        return ret;
    }

    @Override
    public Long getTagVersion(String serviceName) {
        XXServiceVersionInfo serviceVersionInfoDbObj = daoManager.getXXServiceVersionInfo().findByServiceName(serviceName);

        return serviceVersionInfoDbObj != null ? serviceVersionInfoDbObj.getTagVersion() : null;
    }

    @Override
    public void deleteAllTagObjectsForService(String serviceName) {
        LOG.debug("==> TagDBStore.deleteAllTagObjectsForService({})", serviceName);

        XXService service = daoManager.getXXService().findByName(serviceName);

        if (service != null) {
            Long                   serviceId         = service.getId();
            List<XXTag>            xxTags            = daoManager.getXXTag().findByServiceIdAndOwner(serviceId, RangerTag.OWNER_SERVICERESOURCE);
            List<XXTagResourceMap> xxTagResourceMaps = daoManager.getXXTagResourceMap().findByServiceId(serviceId);

            if (CollectionUtils.isNotEmpty(xxTagResourceMaps)) {
                for (XXTagResourceMap xxTagResourceMap : xxTagResourceMaps) {
                    try {
                        daoManager.getXXTagResourceMap().remove(xxTagResourceMap);
                    } catch (Exception e) {
                        LOG.error("Error deleting RangerTagResourceMap with id={}", xxTagResourceMap.getId(), e);

                        throw e;
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(xxTags)) {
                for (XXTag xxTag : xxTags) {
                    try {
                        daoManager.getXXTag().remove(xxTag);
                    } catch (Exception e) {
                        LOG.error("Error deleting RangerTag with id={}", xxTag.getId(), e);

                        throw e;
                    }
                }
            }

            List<XXServiceResource> xxServiceResources = daoManager.getXXServiceResource().findByServiceId(serviceId);

            if (CollectionUtils.isNotEmpty(xxServiceResources)) {
                for (XXServiceResource xxServiceResource : xxServiceResources) {
                    try {
                        daoManager.getXXServiceResource().remove(xxServiceResource);
                    } catch (Exception e) {
                        LOG.error("Error deleting RangerServiceResource with id={}", xxServiceResource.getId(), e);

                        throw e;
                    }
                }
            }
        }

        LOG.debug("<== TagDBStore.deleteAllTagObjectsForService({})", serviceName);
    }

    public boolean isInPlaceTagUpdateSupported() {
        initStatics();
        return SUPPORTS_IN_PLACE_TAG_UPDATES;
    }

    public boolean resetTagCache(final String serviceName) {
        LOG.debug("==> TagDBStore.resetTagCache({})", serviceName);

        boolean ret = RangerServiceTagsCache.getInstance().resetCache(serviceName);

        LOG.debug("<== TagDBStore.resetTagCache(): ret={}", ret);

        return ret;
    }

    public RangerServiceResourceWithTagsList getPaginatedServiceResourcesWithTags(SearchFilter filter) {
        return rangerServiceResourceWithTagsService.searchServiceResourcesWithTags(filter);
    }

    private RangerTag validateTag(RangerTag tag) throws Exception {
        List<RangerValiditySchedule> validityPeriods = tag.getValidityPeriods();

        if (CollectionUtils.isNotEmpty(validityPeriods)) {
            List<RangerValiditySchedule>   normalizedValidityPeriods = new ArrayList<>();
            List<ValidationFailureDetails> failures                  = new ArrayList<>();

            for (RangerValiditySchedule validityPeriod : validityPeriods) {
                RangerValidityScheduleValidator validator                = new RangerValidityScheduleValidator(validityPeriod);
                RangerValiditySchedule          normalizedValidityPeriod = validator.validate(failures);

                if (normalizedValidityPeriod != null && CollectionUtils.isEmpty(failures)) {
                    LOG.debug("Normalized ValidityPeriod:[{}]", normalizedValidityPeriod);

                    normalizedValidityPeriods.add(normalizedValidityPeriod);
                } else {
                    String error = "Incorrect time-specification:[" + Collections.singletonList(failures) + "]";

                    LOG.error(error);

                    throw new Exception(error);
                }
            }

            tag.setValidityPeriods(normalizedValidityPeriods);
        }

        return tag;
    }

    private ServiceTags getServiceTagsDelta(Long serviceId, String serviceName, Long lastKnownVersion) {
        LOG.debug("==> TagDBStore.getServiceTagsDelta(lastKnownVersion={})", lastKnownVersion);

        ServiceTags ret = null;

        if (lastKnownVersion == -1L || !isSupportsTagDeltas()) {
            LOG.debug("Returning without computing tags-deltas.., SUPPORTS_TAG_DELTAS:[{}], lastKnownVersion:[{}]", SUPPORTS_TAG_DELTAS, lastKnownVersion);
        } else {
            RangerPerfTracer perf = null;

            if (RangerPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = RangerPerfTracer.getPerfTracer(PERF_LOG, "TagDBStore.getServiceTagsDelta(serviceName=" + serviceName + ", lastKnownVersion=" + lastKnownVersion + ")");
            }

            List<XXTagChangeLog> changeLogRecords = daoManager.getXXTagChangeLog().findLaterThan(lastKnownVersion, serviceId);

            LOG.debug("Number of tag-change-log records found since {} :[{}] for serviceId:[{}]", lastKnownVersion, changeLogRecords == null ? 0 : changeLogRecords.size(), serviceId);

            try {
                ret = createServiceTagsDelta(changeLogRecords);

                if (ret != null) {
                    ret.setServiceName(serviceName);
                }
            } catch (Exception e) {
                LOG.error("Perhaps some tag or service-resource could not be found", e);
            }

            RangerPerfTracer.logAlways(perf);
        }

        LOG.debug("<== TagDBStore.getServiceTagsDelta(lastKnownVersion={})", lastKnownVersion);

        return ret;
    }

    private ServiceTags createServiceTagsDelta(List<XXTagChangeLog> changeLogs) {
        LOG.debug("==> TagDBStore.createServiceTagsDelta()");

        ServiceTags ret = null;

        if (CollectionUtils.isNotEmpty(changeLogs)) {
            Set<String> tagTypes           = new HashSet<>();
            Set<Long>   tagIds             = new HashSet<>();
            Set<Long>   serviceResourceIds = new HashSet<>();

            for (XXTagChangeLog record : changeLogs) {
                if (record.getChangeType().equals(ServiceTags.TagsChangeType.TAG_UPDATE.ordinal())) {
                    tagIds.add(record.getTagId());
                } else if (record.getChangeType().equals(ServiceTags.TagsChangeType.SERVICE_RESOURCE_UPDATE.ordinal())) {
                    serviceResourceIds.add(record.getServiceResourceId());
                } else if (record.getChangeType().equals(ServiceTags.TagsChangeType.TAG_RESOURCE_MAP_UPDATE.ordinal())) {
                    tagIds.add(record.getTagId());
                    serviceResourceIds.add(record.getServiceResourceId());
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Unknown changeType in tag-change-log record: [{}]", record);
                        LOG.debug("Returning without further processing");

                        tagIds.clear();
                        serviceResourceIds.clear();
                        break;
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(serviceResourceIds) || CollectionUtils.isNotEmpty(tagIds)) {
                ret = new ServiceTags();

                ret.setIsDelta(true);
                ret.setIsTagsDeduped(isSupportsTagsDedup());

                ServiceTags.TagsChangeExtent tagsChangeExtent = ServiceTags.TagsChangeExtent.TAGS;

                ret.setTagVersion(changeLogs.get(changeLogs.size() - 1).getServiceTagsVersion());

                XXTagDao tagDao = daoManager.getXXTag();

                for (Long tagId : tagIds) {
                    RangerTag tag = null;

                    try {
                        XXTag xTag = tagDao.getById(tagId);

                        if (xTag != null) {
                            tag = rangerTagService.getPopulatedViewObject(xTag);

                            tagTypes.add(tag.getType());
                        }
                    } catch (Throwable t) {
                        LOG.debug("TagDBStore.createServiceTagsDelta(): failed to read tag id={}", tagId, t);
                    } finally {
                        if (tag == null) {
                            tag = new RangerTag();

                            tag.setId(tagId);
                        }
                    }

                    RangerServiceTagsDeltaUtil.pruneUnusedAttributes(tag);

                    ret.getTags().put(tag.getId(), tag);
                }

                XXTagDefDao tagDefDao = daoManager.getXXTagDef();

                for (String tagType : tagTypes) {
                    try {
                        XXTagDef     xTagDef = tagDefDao.findByName(tagType);
                        RangerTagDef tagDef  = xTagDef != null ? rangerTagDefService.getPopulatedViewObject(xTagDef) : null;

                        if (tagDef != null) {
                            RangerServiceTagsDeltaUtil.pruneUnusedAttributes(tagDef);

                            ret.getTagDefinitions().put(tagDef.getId(), tagDef);
                        } else {
                            LOG.debug("TagDBStore.createServiceTagsDelta(): failed to load tagDef type={}", tagType);
                        }
                    } catch (Throwable t) {
                        LOG.debug("TagDBStore.createServiceTagsDelta(): failed to load tagDef type={}", tagType, t);
                    }
                }

                for (Long serviceResourceId : serviceResourceIds) {
                    // Check if serviceResourceId is part of any resource->id mapping
                    XXServiceResource xServiceResource = null;

                    try {
                        xServiceResource = daoManager.getXXServiceResource().getById(serviceResourceId);
                    } catch (Throwable t) {
                        LOG.debug("TagDBStore.createServiceTagsDelta(): failed to read serviceResource id={}", serviceResourceId, t);
                    }

                    final RangerServiceResource serviceResource;

                    if (xServiceResource == null) {
                        serviceResource = new RangerServiceResource();

                        serviceResource.setId(serviceResourceId);
                    } else {
                        serviceResource = rangerServiceResourceService.getPopulatedViewObject(xServiceResource);

                        if (StringUtils.isNotEmpty(xServiceResource.getTags())) {
                            try {
                                List<RangerTag> tags = JsonUtils.jsonToObject(xServiceResource.getTags(), RangerServiceResourceService.duplicatedDataType);

                                if (CollectionUtils.isNotEmpty(tags)) {
                                    List<Long> resourceTagIds = new ArrayList<>(tags.size());

                                    for (RangerTag tag : tags) {
                                        RangerServiceTagsDeltaUtil.pruneUnusedAttributes(tag);

                                        if (!ret.getTags().containsKey(tag.getId())) {
                                            ret.getTags().put(tag.getId(), tag);
                                        }

                                        resourceTagIds.add(tag.getId());
                                    }

                                    ret.getResourceToTagIds().put(serviceResourceId, resourceTagIds);
                                }
                            } catch (JsonProcessingException e) {
                                LOG.error("Error occurred while processing json", e);
                            }
                        }
                    }

                    RangerServiceTagsDeltaUtil.pruneUnusedAttributes(serviceResource);

                    ret.getServiceResources().add(serviceResource);

                    tagsChangeExtent = ServiceTags.TagsChangeExtent.SERVICE_RESOURCE;
                }

                ret.setTagsChangeExtent(tagsChangeExtent);
            }
        } else {
            LOG.debug("No tag-change-log records provided to createServiceTagsDelta()");
        }

        LOG.debug("<== TagDBStore.createServiceTagsDelta() : serviceTagsDelta={{}}", ret);

        return ret;
    }

    private static void initStatics() {
        if (!IS_SUPPORTS_TAG_DELTAS_INITIALIZED) {
            RangerAdminConfig config = RangerAdminConfig.getInstance();

            SUPPORTS_TAG_DELTAS                = config.getBoolean("ranger.admin" + RangerCommonConstants.RANGER_ADMIN_SUFFIX_TAG_DELTA, RangerCommonConstants.RANGER_ADMIN_SUFFIX_TAG_DELTA_DEFAULT);
            SUPPORTS_IN_PLACE_TAG_UPDATES      = SUPPORTS_TAG_DELTAS && config.getBoolean("ranger.admin" + RangerCommonConstants.RANGER_ADMIN_SUFFIX_IN_PLACE_TAG_UPDATES, RangerCommonConstants.RANGER_ADMIN_SUFFIX_IN_PLACE_TAG_UPDATES_DEFAULT);
            IS_SUPPORTS_TAG_DELTAS_INITIALIZED = true;

            LOG.info("SUPPORTS_TAG_DELTAS={}", SUPPORTS_TAG_DELTAS);
            LOG.info("SUPPORTS_IN_PLACE_TAG_UPDATES={}", SUPPORTS_IN_PLACE_TAG_UPDATES);
        }
    }

    private void deleteTagDef(RangerTagDef tagDef) throws Exception {
        if (tagDef != null) {
            LOG.debug("Deleting tag-def [name={}; id={}]", tagDef.getName(), tagDef.getId());

            List<RangerTag> tagsByType = rangerTagService.getTagsByType(tagDef.getName());

            if (CollectionUtils.isEmpty(tagsByType)) {
                rangerTagDefService.delete(tagDef);
            } else {
                throw new Exception("Cannot delete tag-def: " + tagDef.getName() + ". " + tagsByType.size() + " tag instances for this tag-def exist");
            }
        }
    }
}
