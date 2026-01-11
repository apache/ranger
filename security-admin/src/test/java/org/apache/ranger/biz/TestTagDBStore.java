/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ranger.biz;

import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerServiceTagsCache;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.db.XXServiceDao;
import org.apache.ranger.db.XXServiceResourceDao;
import org.apache.ranger.db.XXServiceVersionInfoDao;
import org.apache.ranger.db.XXTagChangeLogDao;
import org.apache.ranger.db.XXTagDao;
import org.apache.ranger.db.XXTagDefDao;
import org.apache.ranger.db.XXTagResourceMapDao;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.entity.XXServiceResource;
import org.apache.ranger.entity.XXServiceVersionInfo;
import org.apache.ranger.entity.XXTag;
import org.apache.ranger.entity.XXTagChangeLog;
import org.apache.ranger.entity.XXTagDef;
import org.apache.ranger.entity.XXTagResourceMap;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerContextEnricherDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerPolicyConditionDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.model.RangerServiceResource;
import org.apache.ranger.plugin.model.RangerServiceResourceWithTags;
import org.apache.ranger.plugin.model.RangerTag;
import org.apache.ranger.plugin.model.RangerTagDef;
import org.apache.ranger.plugin.model.RangerTagResourceMap;
import org.apache.ranger.plugin.store.PList;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceTags;
import org.apache.ranger.service.RangerServiceResourceService;
import org.apache.ranger.service.RangerServiceResourceWithTagsService;
import org.apache.ranger.service.RangerTagDefService;
import org.apache.ranger.service.RangerTagResourceMapService;
import org.apache.ranger.service.RangerTagService;
import org.apache.ranger.view.RangerServiceResourceWithTagsList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.WebApplicationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

@ExtendWith(MockitoExtension.class)
@TestMethodOrder(MethodOrderer.MethodName.class)
public class TestTagDBStore {
    private static final Long   id                = 1L;
    private static final String name              = "test";
    private static final String gId               = "1427365526516_835_0";
    private static final Long   lastKnownVersion  = 10L;
    private static final String resourceSignature = "testResourceSign";
    private static final String serviceName       = "HDFS";
    @InjectMocks
    TagDBStore tagDBStore = new TagDBStore();
    @Mock
    RangerTagDefService rangerTagDefService;
    @Mock
    RangerServiceResourceService rangerServiceResourceService;
    @Mock
    RangerServiceResourceWithTagsService rangerServiceResourceWithTagsService;
    @Mock
    RangerTagResourceMapService rangerTagResourceMapService;
    @Mock
    RESTErrorUtil errorUtil;
    @Mock
    RangerTagService rangerTagService;
    @Mock
    RangerDaoManager daoManager;
    @Mock
    ServiceDBStore svcStore;
    @Mock
    XXTagDefDao tagDefDao;
    @Mock
    XXServiceResourceDao  serviceResourceDao;

    @Test
    public void testCreateTagDef() throws Exception {
        RangerTagDef rangerTagDef = createRangerTagDef();

        Mockito.when(rangerTagDefService.create(any())).thenReturn(rangerTagDef);
        Mockito.when(rangerTagDefService.read(id)).thenReturn(rangerTagDef);

        RangerTagDef returnedRangerTagDef = tagDBStore.createTagDef(rangerTagDef);

        Assertions.assertNotNull(returnedRangerTagDef);
        Assertions.assertEquals(id, returnedRangerTagDef.getId());
        Assertions.assertEquals(name, rangerTagDef.getName());
    }

    @Test
    public void testUpdateTagDef() throws Exception {
        RangerTagDef rangerTagDef = createRangerTagDef();

        Mockito.when(rangerTagDefService.update(any())).thenReturn(rangerTagDef);
        Mockito.when(rangerTagDefService.read(id)).thenReturn(rangerTagDef);

        RangerTagDef returnedRangerTagDef = tagDBStore.updateTagDef(rangerTagDef);

        Assertions.assertNotNull(returnedRangerTagDef);
        Assertions.assertEquals(id, returnedRangerTagDef.getId());
        Assertions.assertEquals(name, rangerTagDef.getName());
    }

    @Test
    public void testUpdateTagDefWhenItIsNotAvailable() throws Exception {
        RangerTagDef rangerTagDef = createRangerTagDef();

        Mockito.when(rangerTagDefService.read(id)).thenReturn(null).thenReturn(rangerTagDef);
        Mockito.when(errorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenThrow(new WebApplicationException());

        Assertions.assertThrows(WebApplicationException.class, () -> tagDBStore.updateTagDef(rangerTagDef));
    }

    @Test
    public void testUpdateTagDefForTheSameName() throws Exception {
        RangerTagDef rangerTagDef     = createRangerTagDef();
        RangerTagDef rangerTagDefInDB = createRangerTagDef();

        rangerTagDefInDB.setName("test1");

        Mockito.when(rangerTagDefService.read(id)).thenReturn(rangerTagDefInDB);
        Mockito.when(errorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenThrow(new WebApplicationException());

        Assertions.assertThrows(WebApplicationException.class, () -> tagDBStore.updateTagDef(rangerTagDef));
    }

    @Test
    public void testGetTagDefByName() throws Exception {
        RangerTagDef rangerTagDef = createRangerTagDef();

        Mockito.when(rangerTagDefService.getTagDefByName(any())).thenReturn(rangerTagDef);

        RangerTagDef returnedRangerTagDef = tagDBStore.getTagDefByName(rangerTagDef.getName());

        Assertions.assertNotNull(returnedRangerTagDef);
        Assertions.assertEquals(id, returnedRangerTagDef.getId());
        Assertions.assertEquals(name, rangerTagDef.getName());
    }

    @Test
    public void testGetTagDefByGuid() throws Exception {
        RangerTagDef rangerTagDef = createRangerTagDef();

        Mockito.when(rangerTagDefService.getTagDefByGuid(any())).thenReturn(rangerTagDef);

        RangerTagDef returnedRangerTagDef = tagDBStore.getTagDefByGuid(rangerTagDef.getGuid());

        Assertions.assertNotNull(returnedRangerTagDef);
        Assertions.assertEquals(id, returnedRangerTagDef.getId());
        Assertions.assertEquals(gId, returnedRangerTagDef.getGuid());
        Assertions.assertEquals(name, rangerTagDef.getName());
    }

    @Test
    public void testGetTagDefById() throws Exception {
        RangerTagDef rangerTagDef = createRangerTagDef();

        Mockito.when(rangerTagDefService.read(id)).thenReturn(rangerTagDef);

        RangerTagDef returnedRangerTagDef = tagDBStore.getTagDef(rangerTagDef.getId());

        Assertions.assertNotNull(returnedRangerTagDef);
        Assertions.assertEquals(id, returnedRangerTagDef.getId());
        Assertions.assertEquals(gId, returnedRangerTagDef.getGuid());
        Assertions.assertEquals(name, rangerTagDef.getName());
    }

    @Test
    public void testDeleteTagDefById() throws Exception {
        RangerTagDef rangerTagDef = createRangerTagDef();

        Mockito.when(rangerTagDefService.read(id)).thenReturn(rangerTagDef);
        Mockito.when(rangerTagDefService.delete(Mockito.any())).thenReturn(true);

        tagDBStore.deleteTagDef(id);
    }

    @Test
    public void testDeleteTagDefByName() throws Exception {
        RangerTagDef rangerTagDef = createRangerTagDef();

        Mockito.when(rangerTagDefService.getTagDefByName(any())).thenReturn(rangerTagDef);
        Mockito.when(rangerTagDefService.delete(Mockito.any())).thenReturn(true);

        tagDBStore.deleteTagDefByName(name);
    }

    @Test
    public void testGetTagDefs() throws Exception {
        PList<RangerTagDef> rangerTagDefPList = createRangerTagDefPList();
        SearchFilter        searchFilter      = new SearchFilter();

        Mockito.when(rangerTagDefService.searchRangerTagDefs(searchFilter)).thenReturn(rangerTagDefPList);

        List<RangerTagDef> rangerTagDefList = tagDBStore.getTagDefs(searchFilter);

        Assertions.assertNotNull(rangerTagDefList);

        RangerTagDef rangerTagDef = rangerTagDefList.get(0);

        Assertions.assertEquals(id, rangerTagDef.getId());
        Assertions.assertEquals(gId, rangerTagDef.getGuid());
        Assertions.assertEquals(name, rangerTagDef.getName());
    }

    @Test
    public void testGetPaginatedTagDefs() throws Exception {
        PList<RangerTagDef> rangerTagDefPList = createRangerTagDefPList();
        SearchFilter        searchFilter      = new SearchFilter();

        Mockito.when(rangerTagDefService.searchRangerTagDefs(searchFilter)).thenReturn(rangerTagDefPList);

        PList<RangerTagDef> returnedRangerTagDefList = tagDBStore.getPaginatedTagDefs(searchFilter);

        Assertions.assertNotNull(returnedRangerTagDefList);

        RangerTagDef rangerTagDef = returnedRangerTagDefList.getList().get(0);

        Assertions.assertEquals(1, returnedRangerTagDefList.getList().size());
        Assertions.assertEquals(id, rangerTagDef.getId());
        Assertions.assertEquals(gId, rangerTagDef.getGuid());
        Assertions.assertEquals(name, rangerTagDef.getName());
    }

    @Test
    public void testCreateTag() throws Exception {
        RangerTag rangerTag = createRangerTag();

        Mockito.when(rangerTagService.create(any())).thenReturn(rangerTag);
        Mockito.when(rangerTagService.read(id)).thenReturn(rangerTag);

        RangerTag returnedRangerTag = tagDBStore.createTag(rangerTag);

        Assertions.assertNotNull(returnedRangerTag);
        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testUpdateTag() throws Exception {
        RangerTag rangerTag = createRangerTag();

        Mockito.when(rangerTagService.update(any())).thenReturn(rangerTag);
        Mockito.when(rangerTagService.read(id)).thenReturn(rangerTag);

        RangerTag returnedRangerTag = tagDBStore.updateTag(rangerTag);

        Assertions.assertNotNull(returnedRangerTag);
        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testUpdateTagWhenItIsNotAvailable() throws Exception {
        RangerTag rangerTag = createRangerTag();

        Mockito.when(rangerTagService.read(id)).thenReturn(null).thenReturn(rangerTag);
        Mockito.when(errorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenThrow(new WebApplicationException());

        Assertions.assertThrows(WebApplicationException.class, () -> tagDBStore.updateTag(rangerTag));
    }

    @Test
    public void testDeleteTagById() throws Exception {
        RangerTag rangerTag = createRangerTag();

        Mockito.when(rangerTagService.read(id)).thenReturn(rangerTag);
        Mockito.when(rangerTagService.delete(Mockito.any())).thenReturn(true);

        tagDBStore.deleteTag(id);
    }

    @Test
    public void testGetTagById() throws Exception {
        RangerTag rangerTag = createRangerTag();

        Mockito.when(rangerTagService.read(id)).thenReturn(rangerTag);

        RangerTag returnedRangerTag = tagDBStore.getTag(id);

        Assertions.assertNotNull(returnedRangerTag);
        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testGetTagByGuid() throws Exception {
        RangerTag rangerTag = createRangerTag();

        Mockito.when(rangerTagService.getTagByGuid(gId)).thenReturn(rangerTag);

        RangerTag returnedRangerTag = tagDBStore.getTagByGuid(gId);

        Assertions.assertNotNull(returnedRangerTag);
        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testGetTagsByType() throws Exception {
        String          type       = "file";
        RangerTag       rangerTag  = createRangerTag();
        List<RangerTag> rangerTags = new ArrayList<>();

        rangerTags.add(rangerTag);

        Mockito.when(rangerTagService.getTagsByType(type)).thenReturn(rangerTags);

        List<RangerTag> returnedRangerTags = tagDBStore.getTagsByType(type);

        Assertions.assertNotNull(returnedRangerTags);

        RangerTag returnedRangerTag = returnedRangerTags.get(0);

        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testGetTagsForResourceId() throws Exception {
        RangerTag       rangerTag  = createRangerTag();
        List<RangerTag> rangerTags = new ArrayList<>();

        rangerTags.add(rangerTag);

        Mockito.when(rangerTagService.getTagsForResourceId(id)).thenReturn(rangerTags);

        List<RangerTag> returnedRangerTags = tagDBStore.getTagsForResourceId(id);

        Assertions.assertNotNull(returnedRangerTags);

        RangerTag returnedRangerTag = returnedRangerTags.get(0);

        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testGetTagsForResourceGuid() throws Exception {
        RangerTag       rangerTag  = createRangerTag();
        List<RangerTag> rangerTags = new ArrayList<>();

        rangerTags.add(rangerTag);
        Mockito.when(rangerTagService.getTagsForResourceGuid(gId)).thenReturn(rangerTags);

        List<RangerTag> returnedRangerTags = tagDBStore.getTagsForResourceGuid(gId);

        Assertions.assertNotNull(returnedRangerTags);

        RangerTag returnedRangerTag = returnedRangerTags.get(0);

        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testGetTags() throws Exception {
        SearchFilter     filter         = new SearchFilter();
        PList<RangerTag> rangerTagPList = createRangerTagPList();

        Mockito.when(rangerTagService.searchRangerTags(filter)).thenReturn(rangerTagPList);

        List<RangerTag> returnedRangerTags = tagDBStore.getTags(filter);

        Assertions.assertNotNull(returnedRangerTags);

        RangerTag returnedRangerTag = returnedRangerTags.get(0);

        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testGetPaginatedTags() throws Exception {
        SearchFilter     filter         = new SearchFilter();
        PList<RangerTag> rangerTagPList = createRangerTagPList();

        Mockito.when(rangerTagService.searchRangerTags(filter)).thenReturn(rangerTagPList);

        PList<RangerTag> returnedRangerTagPList = tagDBStore.getPaginatedTags(filter);

        Assertions.assertNotNull(returnedRangerTagPList);
        Assertions.assertEquals(1, returnedRangerTagPList.getListSize());

        RangerTag returnedRangerTag = returnedRangerTagPList.getList().get(0);

        Assertions.assertEquals(id, returnedRangerTag.getId());
        Assertions.assertEquals(gId, returnedRangerTag.getGuid());
    }

    @Test
    public void testResetTagCache() {
        RangerServiceTagsCache rangerServiceTagsCache = Mockito.mock(RangerServiceTagsCache.class);

        tagDBStore.resetTagCache(name);
    }

    @Test
    public void testCreateServiceResource() throws Exception {
        RangerServiceResource rangerServiceResource = createRangerServiceResource();

        Mockito.when(rangerServiceResourceService.create(rangerServiceResource)).thenReturn(rangerServiceResource);
        Mockito.when(rangerServiceResourceService.read(id)).thenReturn(rangerServiceResource);

        RangerServiceResource returnedRangerServiceResource = tagDBStore.createServiceResource(rangerServiceResource);

        Assertions.assertNotNull(returnedRangerServiceResource);
        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertEquals(resourceSignature, returnedRangerServiceResource.getResourceSignature());
        Assertions.assertEquals(serviceName, returnedRangerServiceResource.getServiceName());
    }

    @Test
    public void testUpdateServiceResource() throws Exception {
        RangerServiceResource rangerServiceResource = createRangerServiceResource();

        Mockito.when(rangerServiceResourceService.update(rangerServiceResource)).thenReturn(rangerServiceResource);
        Mockito.when(rangerServiceResourceService.read(id)).thenReturn(rangerServiceResource);

        RangerServiceResource returnedRangerServiceResource = tagDBStore.updateServiceResource(rangerServiceResource);

        Assertions.assertNotNull(returnedRangerServiceResource);
        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertEquals(resourceSignature, returnedRangerServiceResource.getResourceSignature());
        Assertions.assertEquals(serviceName, returnedRangerServiceResource.getServiceName());
    }

    @Test
    public void testUpdateServiceResourceWhenItIsNotAvailable() throws Exception {
        RangerServiceResource rangerServiceResource = createRangerServiceResource();

        Mockito.when(rangerServiceResourceService.read(id)).thenReturn(null).thenReturn(rangerServiceResource);
        Mockito.when(errorUtil.createRESTException(Mockito.anyString(), Mockito.any(MessageEnums.class))).thenThrow(new WebApplicationException());

        Assertions.assertThrows(WebApplicationException.class, () -> tagDBStore.updateServiceResource(rangerServiceResource));
    }

    @Test
    public void testRefreshServiceResource() throws Exception {
        RangerTagResourceMap       rangerTagResourceMap     = createRangerTagResourceMap();
        List<RangerTagResourceMap> rangerTagResourceMapList = new ArrayList<>();
        XXServiceResource          serviceResourceEntity    = createXXServiceResource();
        XXServiceResourceDao       xxServiceResourceDao     = Mockito.mock(XXServiceResourceDao.class);

        rangerTagResourceMapList.add(rangerTagResourceMap);

        Mockito.when(rangerTagResourceMapService.getByResourceId(id)).thenReturn(rangerTagResourceMapList);

        Mockito.when(daoManager.getXXServiceResource()).thenReturn(xxServiceResourceDao);
        Mockito.when(xxServiceResourceDao.getById(Mockito.any())).thenReturn(serviceResourceEntity);
        Mockito.when(xxServiceResourceDao.update(serviceResourceEntity)).thenReturn(serviceResourceEntity);

        tagDBStore.refreshServiceResource(id);
    }

    @Test
    public void testDeleteServiceResourceByGuid() throws Exception {
        RangerServiceResource rangerServiceResource = createRangerServiceResource();

        Mockito.when(rangerServiceResourceService.delete(rangerServiceResource)).thenReturn(true);
        Mockito.when(rangerServiceResourceService.getServiceResourceByGuid(gId)).thenReturn(rangerServiceResource);

        tagDBStore.deleteServiceResourceByGuid(gId);
    }

    @Test
    public void tesGetServiceResourceByGuid() throws Exception {
        RangerServiceResource rangerServiceResource = createRangerServiceResource();

        Mockito.when(rangerServiceResourceService.getServiceResourceByGuid(gId)).thenReturn(rangerServiceResource);

        RangerServiceResource returnedRangerServiceResource = tagDBStore.getServiceResourceByGuid(gId);

        Assertions.assertNotNull(returnedRangerServiceResource);
        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertEquals(resourceSignature, returnedRangerServiceResource.getResourceSignature());
        Assertions.assertEquals(serviceName, returnedRangerServiceResource.getServiceName());
    }

    @Test
    public void tesGetServiceResourceById() throws Exception {
        RangerServiceResource rangerServiceResource = createRangerServiceResource();

        Mockito.when(rangerServiceResourceService.read(id)).thenReturn(rangerServiceResource);

        RangerServiceResource returnedRangerServiceResource = tagDBStore.getServiceResource(id);

        Assertions.assertNotNull(returnedRangerServiceResource);
        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertEquals(resourceSignature, returnedRangerServiceResource.getResourceSignature());
        Assertions.assertEquals(serviceName, returnedRangerServiceResource.getServiceName());
    }

    @Test
    public void tesGetServiceResourcesByService() throws Exception {
        RangerServiceResource       rangerServiceResource     = createRangerServiceResource();
        List<RangerServiceResource> rangerServiceResourceList = new ArrayList<>();

        rangerServiceResourceList.add(rangerServiceResource);

        XXServiceDao xxServiceDao = Mockito.mock(XXServiceDao.class);

        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(xxServiceDao.findIdByName(serviceName)).thenReturn(id);
        Mockito.when(rangerServiceResourceService.getByServiceId(id)).thenReturn(rangerServiceResourceList);

        List<RangerServiceResource> returnedRangerServiceResourceList = tagDBStore.getServiceResourcesByService(serviceName);

        Assertions.assertNotNull(returnedRangerServiceResourceList);

        RangerServiceResource returnedRangerServiceResource = returnedRangerServiceResourceList.get(0);

        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertEquals(resourceSignature, returnedRangerServiceResource.getResourceSignature());
        Assertions.assertEquals(serviceName, returnedRangerServiceResource.getServiceName());
    }

    @Test
    public void tesGetServiceResourceGuidsByService() {
        RangerServiceResource rangerServiceResource = createRangerServiceResource();
        List<String>          result                = new ArrayList<>();
        XXServiceResourceDao  xxServiceResourceDao  = Mockito.mock(XXServiceResourceDao.class);
        XXServiceDao          xxServiceDao          = Mockito.mock(XXServiceDao.class);

        result.add(rangerServiceResource.getGuid());

        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(daoManager.getXXServiceResource()).thenReturn(xxServiceResourceDao);
        Mockito.when(xxServiceDao.findIdByName(serviceName)).thenReturn(id);
        Mockito.when(xxServiceResourceDao.findServiceResourceGuidsInServiceId(id)).thenReturn(result);

        List<String> returnedServiceResourceGuidsInServiceId = tagDBStore.getServiceResourceGuidsByService(serviceName);

        Assertions.assertNotNull(returnedServiceResourceGuidsInServiceId);
        Assertions.assertEquals(gId, returnedServiceResourceGuidsInServiceId.get(0));
    }

    @Test
    public void tesGetServiceResourceByServiceAndResourceSignature() throws Exception {
        RangerServiceResource rangerServiceResource = createRangerServiceResource();
        XXServiceDao          xxServiceDao          = Mockito.mock(XXServiceDao.class);

        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(xxServiceDao.findIdByName(serviceName)).thenReturn(id);
        Mockito.when(rangerServiceResourceService.getByServiceAndResourceSignature(id, resourceSignature)).thenReturn(rangerServiceResource);

        RangerServiceResource returnedRangerServiceResource = tagDBStore.getServiceResourceByServiceAndResourceSignature(serviceName, resourceSignature);

        Assertions.assertNotNull(returnedRangerServiceResource);
        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertEquals(resourceSignature, returnedRangerServiceResource.getResourceSignature());
    }

    @Test
    public void tesGetServiceResources() throws Exception {
        PList<RangerServiceResource> rangerServiceResourcePList = createRangerServiceResourcePList();
        SearchFilter                 searchFilter               = new SearchFilter();

        Mockito.when(rangerServiceResourceService.searchServiceResources(searchFilter)).thenReturn(rangerServiceResourcePList);

        List<RangerServiceResource> returnedRangerServiceResourceList = tagDBStore.getServiceResources(searchFilter);

        Assertions.assertNotNull(returnedRangerServiceResourceList);
        Assertions.assertEquals(1, returnedRangerServiceResourceList.size());

        RangerServiceResource returnedRangerServiceResource = returnedRangerServiceResourceList.get(0);

        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertEquals(resourceSignature, returnedRangerServiceResource.getResourceSignature());
    }

    @Test
    public void tesGetPaginatedServiceResources() throws Exception {
        PList<RangerServiceResource> rangerServiceResourcePList = createRangerServiceResourcePList();
        SearchFilter                 searchFilter               = new SearchFilter();

        Mockito.when(rangerServiceResourceService.searchServiceResources(searchFilter)).thenReturn(rangerServiceResourcePList);

        PList<RangerServiceResource> returnedRangerServiceResourcePList = tagDBStore.getPaginatedServiceResources(searchFilter);

        Assertions.assertNotNull(returnedRangerServiceResourcePList);
        Assertions.assertEquals(1, returnedRangerServiceResourcePList.getList().size());

        RangerServiceResource returnedRangerServiceResource = returnedRangerServiceResourcePList.getList().get(0);

        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertEquals(resourceSignature, returnedRangerServiceResource.getResourceSignature());
    }

    @Test
    public void tesCreateTagResourceMap() throws Exception {
        RangerTagResourceMap       rangerTagResourceMap     = createRangerTagResourceMap();
        List<RangerTagResourceMap> rangerTagResourceMapList = new ArrayList<>();
        XXServiceResource          serviceResourceEntity    = createXXServiceResource();
        XXServiceResourceDao       xxServiceResourceDao     = Mockito.mock(XXServiceResourceDao.class);

        rangerTagResourceMapList.add(rangerTagResourceMap);

        Mockito.when(rangerTagResourceMapService.create(rangerTagResourceMap)).thenReturn(rangerTagResourceMap);

        Mockito.when(daoManager.getXXServiceResource()).thenReturn(xxServiceResourceDao);
        Mockito.when(xxServiceResourceDao.getById(Mockito.any())).thenReturn(serviceResourceEntity);
        Mockito.when(xxServiceResourceDao.update(serviceResourceEntity)).thenReturn(serviceResourceEntity);

        RangerTagResourceMap returnedRangerTagResourceMap = tagDBStore.createTagResourceMap(rangerTagResourceMap);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void testDeleteTagResourceMap() throws Exception {
        RangerTagResourceMap       rangerTagResourceMap     = createRangerTagResourceMap();
        List<RangerTagResourceMap> rangerTagResourceMapList = new ArrayList<>();
        XXServiceResource          serviceResourceEntity    = createXXServiceResource();
        XXServiceResourceDao       xxServiceResourceDao     = Mockito.mock(XXServiceResourceDao.class);

        rangerTagResourceMapList.add(rangerTagResourceMap);

        Mockito.when(rangerTagResourceMapService.getByResourceId(id)).thenReturn(rangerTagResourceMapList);
        Mockito.when(daoManager.getXXServiceResource()).thenReturn(xxServiceResourceDao);
        Mockito.when(xxServiceResourceDao.getById(Mockito.any())).thenReturn(serviceResourceEntity);
        Mockito.when(xxServiceResourceDao.update(serviceResourceEntity)).thenReturn(serviceResourceEntity);

        RangerTag rangerTag = createRangerTag();

        Mockito.when(rangerTagService.read(id)).thenReturn(rangerTag);
        Mockito.when(rangerTagResourceMapService.read(id)).thenReturn(rangerTagResourceMap);
        Mockito.when(rangerTagResourceMapService.delete(rangerTagResourceMap)).thenReturn(true);

        tagDBStore.deleteTagResourceMap(id);
    }

    @Test
    public void tesGetTagResourceMap() throws Exception {
        RangerTagResourceMap rangerTagResourceMap = createRangerTagResourceMap();

        Mockito.when(rangerTagResourceMapService.read(id)).thenReturn(rangerTagResourceMap);

        RangerTagResourceMap returnedRangerTagResourceMap = tagDBStore.getTagResourceMap(id);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void tesGetTagResourceMapByGuid() throws Exception {
        RangerTagResourceMap rangerTagResourceMap = createRangerTagResourceMap();

        Mockito.when(rangerTagResourceMapService.getByGuid(gId)).thenReturn(rangerTagResourceMap);

        RangerTagResourceMap returnedRangerTagResourceMap = tagDBStore.getTagResourceMapByGuid(gId);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void tesGetTagResourceMapsForTagId() throws Exception {
        RangerTagResourceMap       rangerTagResourceMap     = createRangerTagResourceMap();
        List<RangerTagResourceMap> rangerTagResourceMapList = new ArrayList<>();

        rangerTagResourceMapList.add(rangerTagResourceMap);

        Mockito.when(rangerTagResourceMapService.getByTagId(id)).thenReturn(rangerTagResourceMapList);

        List<RangerTagResourceMap> returnedRangerTagResourceMapList = tagDBStore.getTagResourceMapsForTagId(id);
        RangerTagResourceMap       returnedRangerTagResourceMap     = returnedRangerTagResourceMapList.get(0);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void tesGetTagResourceMapsForTagGuid() throws Exception {
        RangerTagResourceMap       rangerTagResourceMap     = createRangerTagResourceMap();
        List<RangerTagResourceMap> rangerTagResourceMapList = new ArrayList<>();

        rangerTagResourceMapList.add(rangerTagResourceMap);

        Mockito.when(rangerTagResourceMapService.getByTagGuid(gId)).thenReturn(rangerTagResourceMapList);

        List<RangerTagResourceMap> returnedRangerTagResourceMapList = tagDBStore.getTagResourceMapsForTagGuid(gId);
        RangerTagResourceMap       returnedRangerTagResourceMap     = returnedRangerTagResourceMapList.get(0);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void tesGetTagIdsForResourceId() throws Exception {
        List<Long> tagIds = new ArrayList<>();

        tagIds.add(id);

        Mockito.when(rangerTagResourceMapService.getTagIdsForResourceId(id)).thenReturn(tagIds);

        List<Long> returnedTagIdsList = tagDBStore.getTagIdsForResourceId(id);

        Assertions.assertNotNull(returnedTagIdsList);
        Assertions.assertEquals(1, returnedTagIdsList.size());
        Assertions.assertEquals(id, returnedTagIdsList.get(0));
    }

    @Test
    public void testGetTagResourceMapsForResourceId() throws Exception {
        RangerTagResourceMap       rangerTagResourceMap     = createRangerTagResourceMap();
        List<RangerTagResourceMap> rangerTagResourceMapList = new ArrayList<>();

        rangerTagResourceMapList.add(rangerTagResourceMap);

        Mockito.when(rangerTagResourceMapService.getByResourceId(id)).thenReturn(rangerTagResourceMapList);

        List<RangerTagResourceMap> returnedRangerTagResourceMapList = tagDBStore.getTagResourceMapsForResourceId(id);
        RangerTagResourceMap       returnedRangerTagResourceMap     = returnedRangerTagResourceMapList.get(0);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void testGetTagResourceMapsForResourceGuid() throws Exception {
        RangerTagResourceMap       rangerTagResourceMap     = createRangerTagResourceMap();
        List<RangerTagResourceMap> rangerTagResourceMapList = new ArrayList<>();

        rangerTagResourceMapList.add(rangerTagResourceMap);

        Mockito.when(rangerTagResourceMapService.getByResourceGuid(gId)).thenReturn(rangerTagResourceMapList);

        List<RangerTagResourceMap> returnedRangerTagResourceMapList = tagDBStore.getTagResourceMapsForResourceGuid(gId);
        RangerTagResourceMap       returnedRangerTagResourceMap     = returnedRangerTagResourceMapList.get(0);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void testGetTagResourceMapForTagAndResourceId() throws Exception {
        RangerTagResourceMap rangerTagResourceMap = createRangerTagResourceMap();

        Mockito.when(rangerTagResourceMapService.getByTagAndResourceId(id, id)).thenReturn(rangerTagResourceMap);

        RangerTagResourceMap returnedRangerTagResourceMap = tagDBStore.getTagResourceMapForTagAndResourceId(id, id);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void testGetTagResourceMapForTagAndResourceGuid() throws Exception {
        RangerTagResourceMap rangerTagResourceMap = createRangerTagResourceMap();

        Mockito.when(rangerTagResourceMapService.getByTagAndResourceGuid(gId, gId)).thenReturn(rangerTagResourceMap);

        RangerTagResourceMap returnedRangerTagResourceMap = tagDBStore.getTagResourceMapForTagAndResourceGuid(gId, gId);

        Assertions.assertNotNull(returnedRangerTagResourceMap);
        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void testGetPaginatedTagResourceMaps() throws Exception {
        PList<RangerTagResourceMap> rangerTagResourceMapPList = createRangerTagResourceMapPList();
        SearchFilter                searchFilter              = new SearchFilter();

        Mockito.when(rangerTagResourceMapService.searchRangerTaggedResources(searchFilter)).thenReturn(rangerTagResourceMapPList);

        PList<RangerTagResourceMap> returnedRangerTagResourceMapPList = tagDBStore.getPaginatedTagResourceMaps(searchFilter);

        Assertions.assertNotNull(returnedRangerTagResourceMapPList);
        Assertions.assertEquals(1, returnedRangerTagResourceMapPList.getList().size());

        RangerTagResourceMap returnedRangerTagResourceMap = returnedRangerTagResourceMapPList.getList().get(0);

        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void testGetTagResourceMaps() throws Exception {
        PList<RangerTagResourceMap> rangerTagResourceMapPList = createRangerTagResourceMapPList();
        SearchFilter                searchFilter              = new SearchFilter();

        Mockito.when(rangerTagResourceMapService.searchRangerTaggedResources(searchFilter)).thenReturn(rangerTagResourceMapPList);

        List<RangerTagResourceMap> returnedRangerTagResourceMapList = tagDBStore.getTagResourceMaps(searchFilter);

        Assertions.assertNotNull(returnedRangerTagResourceMapList);
        Assertions.assertEquals(1, returnedRangerTagResourceMapList.size());

        RangerTagResourceMap returnedRangerTagResourceMap = returnedRangerTagResourceMapList.get(0);

        Assertions.assertEquals(id, returnedRangerTagResourceMap.getId());
        Assertions.assertEquals(gId, returnedRangerTagResourceMap.getGuid());
    }

    @Test
    public void testGetServiceTagsIfUpdated() throws Exception {
        XXServiceVersionInfo    serviceVersionInfoDbObj = createXXServiceVersionInfo();
        XXTagDefDao             xxTagDefDao             = Mockito.mock(XXTagDefDao.class);
        XXServiceVersionInfoDao xxServiceVersionInfoDao = Mockito.mock(XXServiceVersionInfoDao.class);
        XXServiceDao            xxServiceDao            = Mockito.mock(XXServiceDao.class);
        XXServiceResourceDao    xxServiceResourceDao    = Mockito.mock(XXServiceResourceDao.class);
        XXService               xxService               = createXXService();
        RangerServiceDef        rangerServiceDef        = createRangerServiceDef();

        Mockito.when(daoManager.getXXServiceVersionInfo()).thenReturn(xxServiceVersionInfoDao);
        Mockito.when(xxServiceVersionInfoDao.findByServiceName(serviceName)).thenReturn(serviceVersionInfoDbObj);
        Mockito.when(daoManager.getXXTagDef()).thenReturn(xxTagDefDao);
        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(daoManager.getXXServiceResource()).thenReturn(xxServiceResourceDao);
        Mockito.when(xxServiceDao.findIdByName(serviceName)).thenReturn(id);
        Mockito.when(xxServiceDao.findByName(serviceName)).thenReturn(xxService);
        Mockito.when(svcStore.getServiceDef(Mockito.any())).thenReturn(rangerServiceDef);

        ServiceTags serviceTags = tagDBStore.getServiceTagsIfUpdated(serviceName, -1L, true);

        Assertions.assertNotNull(serviceTags);
        Assertions.assertEquals(lastKnownVersion, serviceTags.getTagVersion());
        Assertions.assertEquals(serviceName, serviceTags.getServiceName());
    }

    @Test
    public void testGetServiceTags() throws Exception {
        XXServiceVersionInfo    serviceVersionInfoDbObj = createXXServiceVersionInfo();
        XXTagDefDao             xxTagDefDao             = Mockito.mock(XXTagDefDao.class);
        XXServiceVersionInfoDao xxServiceVersionInfoDao = Mockito.mock(XXServiceVersionInfoDao.class);
        XXServiceDao            xxServiceDao            = Mockito.mock(XXServiceDao.class);
        XXServiceResourceDao    xxServiceResourceDao    = Mockito.mock(XXServiceResourceDao.class);
        XXService               xxService               = createXXService();
        RangerServiceDef        rangerServiceDef        = createRangerServiceDef();

        Mockito.when(daoManager.getXXServiceVersionInfo()).thenReturn(xxServiceVersionInfoDao);
        Mockito.when(xxServiceVersionInfoDao.findByServiceName(serviceName)).thenReturn(serviceVersionInfoDbObj);
        Mockito.when(daoManager.getXXTagDef()).thenReturn(xxTagDefDao);
        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(daoManager.getXXServiceResource()).thenReturn(xxServiceResourceDao);
        Mockito.when(xxServiceDao.findByName(serviceName)).thenReturn(xxService);
        Mockito.when(svcStore.getServiceDef(Mockito.any())).thenReturn(rangerServiceDef);

        ServiceTags serviceTags = tagDBStore.getServiceTags(serviceName, -1L);

        Assertions.assertNotNull(serviceTags);
        Assertions.assertEquals(lastKnownVersion, serviceTags.getTagVersion());
        Assertions.assertEquals(serviceName, serviceTags.getServiceName());
    }

    @Test
    public void testDeleteAllTagObjectsForService() throws Exception {
        XXServiceDao            xxServiceDao         = Mockito.mock(XXServiceDao.class);
        XXTagDao                xxTagDao             = Mockito.mock(XXTagDao.class);
        XXTagResourceMapDao     xxTagResourceMapDao  = Mockito.mock(XXTagResourceMapDao.class);
        XXServiceResourceDao    xxServiceResourceDao = Mockito.mock(XXServiceResourceDao.class);
        XXService               xxService            = createXXService();
        XXTag                   xxTag                = createXXTag();
        XXTagResourceMap        xxTagResourceMap     = createXXTagResourceMap();
        XXServiceResource       xxServiceResource    = createXXServiceResource();
        List<XXTag>             xxTagList            = new ArrayList<>();
        List<XXTagResourceMap>  xxTagResourceMapList = new ArrayList<>();
        List<XXServiceResource> xxServiceResources   = new ArrayList<>();

        Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
        Mockito.when(daoManager.getXXTag()).thenReturn(xxTagDao);
        Mockito.when(daoManager.getXXTagResourceMap()).thenReturn(xxTagResourceMapDao);
        Mockito.when(daoManager.getXXServiceResource()).thenReturn(xxServiceResourceDao);
        Mockito.when(xxServiceDao.findByName(serviceName)).thenReturn(xxService);

        xxTagList.add(xxTag);

        xxTagResourceMapList.add(xxTagResourceMap);

        xxServiceResources.add(xxServiceResource);

        Mockito.when(xxTagDao.findByServiceIdAndOwner(Mockito.any(), Mockito.any())).thenReturn(xxTagList);
        Mockito.when(xxServiceResourceDao.findByServiceId(Mockito.any())).thenReturn(xxServiceResources);
        Mockito.when(xxTagResourceMapDao.findByServiceId(Mockito.any())).thenReturn(xxTagResourceMapList);
        Mockito.when(xxTagDao.remove(xxTag)).thenReturn(true);
        Mockito.when(xxTagResourceMapDao.remove(xxTagResourceMap)).thenReturn(true);

        tagDBStore.deleteAllTagObjectsForService(serviceName);
    }

    @Test
    public void testGetServiceTagsDeltaWhenTagDeltaSupportsDisabled() throws Exception {
        ServiceTags serviceTags = tagDBStore.getServiceTagsDelta(serviceName, lastKnownVersion);

        Assertions.assertNull(serviceTags);
    }

    @Test
    public void testIsSupportsTagDeltas() {
        boolean isSupportsTagDeltas = TagDBStore.isSupportsTagDeltas();

        Assertions.assertFalse(isSupportsTagDeltas);
    }

    @Test
    public void testIsInPlaceTagUpdateSupported() {
        boolean isInPlaceTagUpdateSupported = tagDBStore.isInPlaceTagUpdateSupported();

        Assertions.assertFalse(isInPlaceTagUpdateSupported);
    }

    @Test
    public void testGetTagVersion() {
        XXServiceVersionInfo    serviceVersionInfoDbObj = createXXServiceVersionInfo();
        XXServiceVersionInfoDao xxServiceVersionInfoDao = Mockito.mock(XXServiceVersionInfoDao.class);

        Mockito.when(daoManager.getXXServiceVersionInfo()).thenReturn(xxServiceVersionInfoDao);
        Mockito.when(xxServiceVersionInfoDao.findByServiceName(serviceName)).thenReturn(serviceVersionInfoDbObj);

        Long tagVersion = tagDBStore.getTagVersion(serviceName);

        Assertions.assertEquals(lastKnownVersion, tagVersion);
    }

    @Test
    public void tesGetPaginatedServiceResourcesWithTags() throws Exception {
        RangerServiceResourceWithTagsList rangerServiceResourceViewList = createRangerServiceResourceWithTagsViewList();
        SearchFilter                      searchFilter                  = new SearchFilter();

        Mockito.when(rangerServiceResourceWithTagsService.searchServiceResourcesWithTags(searchFilter)).thenReturn(rangerServiceResourceViewList);

        RangerServiceResourceWithTagsList returnedRangerServiceResourcePList = tagDBStore.getPaginatedServiceResourcesWithTags(searchFilter);

        Assertions.assertNotNull(returnedRangerServiceResourcePList);
        Assertions.assertEquals(1, returnedRangerServiceResourcePList.getList().size());

        RangerServiceResourceWithTags returnedRangerServiceResource = returnedRangerServiceResourcePList.getResourceList().get(0);

        Assertions.assertEquals(id, returnedRangerServiceResource.getId());
        Assertions.assertEquals(gId, returnedRangerServiceResource.getGuid());
        Assertions.assertNotNull(returnedRangerServiceResource.getAssociatedTags());
        Assertions.assertEquals(rangerServiceResourceViewList.getResourceList().get(0).getAssociatedTags().size(), returnedRangerServiceResource.getAssociatedTags().size());
    }

    @Test
    public void testToRangerServiceResource() {
        Map<String, String[]>             resourceMap      = new HashMap<>();
        Map<String, RangerPolicyResource> resourceElements = new HashMap<>();

        resourceMap.put("database", new String[] {"db1"});
        resourceMap.put("database.isExcludes", new String[] {"false"});
        resourceMap.put("database.isRecursive", new String[] {"false"});

        resourceElements.put("database", new RangerPolicyResource("db1", false, false));

        RangerServiceResource expectedResource = new RangerServiceResource(serviceName, resourceElements);
        RangerServiceResource actualResource   = TagDBStore.toRangerServiceResource(serviceName, resourceMap);

        Assertions.assertEquals(expectedResource.getResourceElements(), actualResource.getResourceElements());
    }

    /**
     * Pre:
     *   New tag created: this event will not trigger a policy download, as it will not increment policy version.
     *
     * Current:
     *   Associate Resource: associate first resource. This event will trigger a version change. Total resources associated with Tag are before 0, after 1.
     *
     * Below method covers one delta case, when a new TAG (no resources) is associated with first resource (eg, hive table)
     */
    @Test
    public void testGetServiceTagsDeltaWhenTagDeltaAndDedupEnabledAdditionOfFirstResource() throws Exception {
        try (MockedStatic<TagDBStore> mockedTagDBStore = mockStatic(TagDBStore.class); MockedStatic<JsonUtils> mockedJsonUtil = mockStatic(JsonUtils.class)) {
            final Long   tagId             = 4L;
            final Long   tagTypeId         = 2L;
            final String tagTypeName       = "ranger_atlas_tag1";
            final String tagSource         = "Atlas";
            final String serviceName       = "HIVE_SERVICE";
            final Long   serviceId         = 6L;
            final Long   resourceId        = 4L;
            final Long   serviceTagVersion = 8L;

            /*
             * Given
             */
            TagDBStore            tagDBStoreSpy         = Mockito.spy(tagDBStore);
            XXServiceDao          xxServiceDao          = Mockito.mock(XXServiceDao.class);
            XXTagChangeLogDao     xxTagChangeLogDao     = Mockito.mock(XXTagChangeLogDao.class);
            XXTagDao              tagDao                = Mockito.mock(XXTagDao.class);
            XXTag                 xTag                  = createXXTag(tagId, 1L, tagTypeId);
            RangerTag             rangerTag             = new RangerTag("6e248648-739a-434a-9c42-a74c8277638d", tagTypeName, Collections.emptyMap(), (short) 0);
            XXTagDef              xxTagDef              = createXXTagDef(tagTypeId, 1L, tagTypeName);
            RangerTagDef          rangerTagDef          = new RangerTagDef(tagTypeName, tagSource);
            XXServiceResource     xServiceResource      = createXXServiceResource(resourceId, "8fa22999-06a4-4d5e-83e7-dc9872082dbc", 2L, serviceId);
            RangerServiceResource rangerServiceResource = new RangerServiceResource("8fa22999-06a4-4d5e-83e7-dc9872082dbc", serviceName, Collections.emptyMap());
            List<XXTagChangeLog>  changeLogRecords      = Collections.singletonList(new XXTagChangeLog(43L, 3, serviceTagVersion, resourceId, tagId));

            rangerTag.setId(tagId);
            rangerTagDef.setId(tagTypeId);
            xServiceResource.setTags(tagTypeName);
            rangerServiceResource.setId(resourceId);

            Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
            Mockito.when(daoManager.getXXTagChangeLog()).thenReturn(xxTagChangeLogDao);
            Mockito.when(xxTagChangeLogDao.findLaterThan(Mockito.anyLong(), Mockito.anyLong())).thenReturn(changeLogRecords);
            Mockito.when(daoManager.getXXTag()).thenReturn(tagDao);
            Mockito.when(tagDao.getById(Mockito.anyLong())).thenReturn(xTag);
            Mockito.when(rangerTagService.getPopulatedViewObject(Mockito.any())).thenReturn(rangerTag);

            mockedTagDBStore.when(TagDBStore::isSupportsTagsDedup).thenReturn(true);
            mockedTagDBStore.when(TagDBStore::isSupportsTagDeltas).thenReturn(true);
            mockedJsonUtil.when(() -> JsonUtils.jsonToObject(Mockito.anyString(), Mockito.any(RangerServiceResourceService.duplicatedDataType.getClass()))).thenReturn(Collections.singletonList(rangerTag));

            Mockito.when(daoManager.getXXTagDef()).thenReturn(tagDefDao);
            Mockito.when(tagDefDao.findByName(tagTypeName)).thenReturn(xxTagDef);
            Mockito.when(rangerTagDefService.getPopulatedViewObject(Mockito.any())).thenReturn(rangerTagDef);

            Mockito.when(daoManager.getXXServiceResource()).thenReturn(serviceResourceDao);
            Mockito.when(serviceResourceDao.getById(Mockito.anyLong())).thenReturn(xServiceResource);
            Mockito.when(rangerServiceResourceService.getPopulatedViewObject(Mockito.any())).thenReturn(rangerServiceResource);

            /*
             * When
             */
            ServiceTags serviceTags = tagDBStoreSpy.getServiceTagsDelta(serviceName, (serviceTagVersion - 1));

            /*
             * Then
             */
            Assertions.assertEquals("add_or_update", serviceTags.getOp());
            Assertions.assertEquals(serviceName, serviceTags.getServiceName());
            Assertions.assertEquals(serviceTagVersion, serviceTags.getTagVersion());
            Assertions.assertTrue(serviceTags.getIsDelta());
            Assertions.assertTrue(serviceTags.getIsTagsDeduped());
            Assertions.assertEquals(ServiceTags.TagsChangeExtent.SERVICE_RESOURCE, serviceTags.getTagsChangeExtent());

            // tagDefinitions
            Assertions.assertEquals(1, serviceTags.getTagDefinitions().size());
            Assertions.assertEquals(tagTypeId,  serviceTags.getTagDefinitions().get(tagTypeId).getId());
            Assertions.assertTrue(serviceTags.getTagDefinitions().get(tagTypeId).getIsEnabled());
            Assertions.assertEquals(tagTypeName,  serviceTags.getTagDefinitions().get(tagTypeId).getName());
            Assertions.assertEquals(tagSource, serviceTags.getTagDefinitions().get(tagTypeId).getSource());

            // tags
            Assertions.assertEquals(1, serviceTags.getTags().size());
            Assertions.assertEquals(tagId, serviceTags.getTags().get(tagId).getId());
            Assertions.assertTrue(serviceTags.getTags().get(tagId).getIsEnabled());
            Assertions.assertEquals(tagTypeName, serviceTags.getTags().get(tagId).getType());

            // serviceResources
            Assertions.assertEquals(1, serviceTags.getServiceResources().size());
            Assertions.assertEquals(resourceId, serviceTags.getServiceResources().get(0).getId());
            Assertions.assertTrue(serviceTags.getServiceResources().get(0).getIsEnabled());
            Assertions.assertEquals(serviceName, serviceTags.getServiceResources().get(0).getServiceName());

            // resourceToTagIds
            Assertions.assertEquals(1, serviceTags.getResourceToTagIds().size());
            Assertions.assertEquals(resourceId, serviceTags.getResourceToTagIds().get(serviceTags.getServiceResources().get(0).getId()).get(0));
        }
    }

    /**
     * Pre:
     *   New tag created: this event will not trigger a policy download, as it will not increment policy version
     *   Associate Resource: associate first resource.
     *
     * Current:
     *   Associate Resource: associate second resource. This event will trigger a version change. Total resources associated with Tag are before 1, after 2.
     *
     * Below method covers one delta case, when a TAG (with 1 resource) is associated with second resource (eg, hive table)
     */
    @Test
    public void testGetServiceTagsDeltaWhenTagDeltaAndDedupEnabledAdditionOfSecondResource() throws Exception {
        try (MockedStatic<TagDBStore> mockedTagDBStore = mockStatic(TagDBStore.class); MockedStatic<JsonUtils> mockedJsonUtil = mockStatic(JsonUtils.class)) {
            final Long   tagId             = 5L;
            final Long   tagTypeId         = 2L;
            final String tagTypeName       = "ranger_atlas_tag1";
            final String tagSource         = "Atlas";
            final String serviceName       = "HIVE_SERVICE";
            final Long   serviceId         = 6L;
            final Long   resourceId        = 5L;
            final Long   serviceTagVersion = 9L;

            /*
             * Given
             */
            TagDBStore            tagDBStoreSpy         = Mockito.spy(tagDBStore);
            XXServiceDao          xxServiceDao          = Mockito.mock(XXServiceDao.class);
            XXTagChangeLogDao     xxTagChangeLogDao     = Mockito.mock(XXTagChangeLogDao.class);
            XXTagDao              tagDao                = Mockito.mock(XXTagDao.class);
            XXTag                 xTag                  = createXXTag(tagId, 1L, tagTypeId);
            RangerTag             rangerTag             = new RangerTag("ef2401cb-f723-4663-a19e-51363efe71ea", tagTypeName, Collections.emptyMap(), (short) 0);
            XXTagDef              xxTagDef              = createXXTagDef(tagTypeId, 1L, tagTypeName);
            RangerTagDef          rangerTagDef          = new RangerTagDef(tagTypeName, tagSource);
            XXServiceResource     xServiceResource      = createXXServiceResource(resourceId, "b6ef4baa-f38e-49ce-bac3-3b2243641d8a", 2L, serviceId);
            RangerServiceResource rangerServiceResource = new RangerServiceResource("b6ef4baa-f38e-49ce-bac3-3b2243641d8a", serviceName, Collections.emptyMap());
            List<XXTagChangeLog>  changeLogRecords      = Collections.singletonList(new XXTagChangeLog(44L, 3, serviceTagVersion, resourceId, tagId));

            rangerTag.setId(tagId);
            rangerTagDef.setId(tagTypeId);
            xServiceResource.setTags(tagTypeName);
            rangerServiceResource.setId(resourceId);

            Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
            Mockito.when(daoManager.getXXTagChangeLog()).thenReturn(xxTagChangeLogDao);
            Mockito.when(xxTagChangeLogDao.findLaterThan(Mockito.anyLong(), Mockito.anyLong())).thenReturn(changeLogRecords);
            Mockito.when(daoManager.getXXTag()).thenReturn(tagDao);
            Mockito.when(tagDao.getById(Mockito.anyLong())).thenReturn(xTag);
            Mockito.when(rangerTagService.getPopulatedViewObject(Mockito.any())).thenReturn(rangerTag);

            mockedTagDBStore.when(TagDBStore::isSupportsTagsDedup).thenReturn(true);
            mockedTagDBStore.when(TagDBStore::isSupportsTagDeltas).thenReturn(true);
            mockedJsonUtil.when(() -> JsonUtils.jsonToObject(Mockito.anyString(), Mockito.any(RangerServiceResourceService.duplicatedDataType.getClass()))).thenReturn(Collections.singletonList(rangerTag));

            Mockito.when(daoManager.getXXTagDef()).thenReturn(tagDefDao);
            Mockito.when(tagDefDao.findByName(tagTypeName)).thenReturn(xxTagDef);
            Mockito.when(rangerTagDefService.getPopulatedViewObject(Mockito.any())).thenReturn(rangerTagDef);

            Mockito.when(daoManager.getXXServiceResource()).thenReturn(serviceResourceDao);
            Mockito.when(serviceResourceDao.getById(Mockito.anyLong())).thenReturn(xServiceResource);
            Mockito.when(rangerServiceResourceService.getPopulatedViewObject(Mockito.any())).thenReturn(rangerServiceResource);

            /*
             * When
             */
            ServiceTags serviceTags = tagDBStoreSpy.getServiceTagsDelta(serviceName, (serviceTagVersion - 1));

            /*
             * Then
             */
            Assertions.assertEquals("add_or_update", serviceTags.getOp());
            Assertions.assertEquals(serviceName, serviceTags.getServiceName());
            Assertions.assertEquals(serviceTagVersion, serviceTags.getTagVersion());
            Assertions.assertTrue(serviceTags.getIsDelta());
            Assertions.assertTrue(serviceTags.getIsTagsDeduped());
            Assertions.assertEquals(ServiceTags.TagsChangeExtent.SERVICE_RESOURCE, serviceTags.getTagsChangeExtent());

            // tagDefinitions
            Assertions.assertEquals(1, serviceTags.getTagDefinitions().size());
            Assertions.assertEquals(tagTypeId,  serviceTags.getTagDefinitions().get(tagTypeId).getId());
            Assertions.assertTrue(serviceTags.getTagDefinitions().get(tagTypeId).getIsEnabled());
            Assertions.assertEquals(tagTypeName,  serviceTags.getTagDefinitions().get(tagTypeId).getName());
            Assertions.assertEquals(tagSource, serviceTags.getTagDefinitions().get(tagTypeId).getSource());

            // tags
            Assertions.assertEquals(1, serviceTags.getTags().size());
            Assertions.assertEquals(tagId, serviceTags.getTags().get(tagId).getId());
            Assertions.assertTrue(serviceTags.getTags().get(tagId).getIsEnabled());
            Assertions.assertEquals(tagTypeName, serviceTags.getTags().get(tagId).getType());

            // serviceResources
            Assertions.assertEquals(1, serviceTags.getServiceResources().size());
            Assertions.assertEquals(resourceId, serviceTags.getServiceResources().get(0).getId());
            Assertions.assertTrue(serviceTags.getServiceResources().get(0).getIsEnabled());
            Assertions.assertEquals(serviceName, serviceTags.getServiceResources().get(0).getServiceName());

            // resourceToTagIds
            Assertions.assertEquals(1, serviceTags.getResourceToTagIds().size());
            Assertions.assertEquals(resourceId, serviceTags.getResourceToTagIds().get(serviceTags.getServiceResources().get(0).getId()).get(0));
        }
    }

    /**
     * Pre:
     *   New tag created: this event will not trigger a policy download, as it will not increment policy version
     *   Associate Resource: associate first resource.
     *   Associate Resource: associate second resource.
     *
     * Current:
     *   De-associate resource: de-associate first resource. This event will trigger a version change. Total resources associated with Tag are before 2, after 1.
     *
     * Below method covers one delta case, when a TAG (with 2 resource) is de-associated with first resource (eg, hive table)
     */
    @Test
    public void testGetServiceTagsDeltaWhenTagDeltaAndDedupEnabledRemovalOfFirstResource() throws Exception {
        try (MockedStatic<TagDBStore> mockedTagDBStore = mockStatic(TagDBStore.class); MockedStatic<JsonUtils> mockedJsonUtil = mockStatic(JsonUtils.class)) {
            final Long   tagId             = 4L;
            final Long   tagTypeId         = 2L;
            final String tagTypeName       = "ranger_atlas_tag1";
            final String tagSource         = "Atlas";
            final String serviceName       = "HIVE_SERVICE";
            final Long   serviceId         = 6L;
            final Long   resourceId        = 4L;
            final Long   serviceTagVersion = 10L;

            /*
             * Given
             */
            TagDBStore            tagDBStoreSpy         = Mockito.spy(tagDBStore);
            XXServiceDao          xxServiceDao          = Mockito.mock(XXServiceDao.class);
            XXTagChangeLogDao     xxTagChangeLogDao     = Mockito.mock(XXTagChangeLogDao.class);
            XXTagDao              tagDao                = Mockito.mock(XXTagDao.class);
            RangerTag             rangerTag             = new RangerTag("6e248648-739a-434a-9c42-a74c8277638d", tagTypeName, Collections.emptyMap(), (short) 0);
            RangerTagDef          rangerTagDef          = new RangerTagDef(tagTypeName, tagSource);
            XXServiceResource     xServiceResource      = createXXServiceResource(resourceId, "8fa22999-06a4-4d5e-83e7-dc9872082dbc", 2L, serviceId);
            RangerServiceResource rangerServiceResource = new RangerServiceResource("8fa22999-06a4-4d5e-83e7-dc9872082dbc", serviceName, Collections.emptyMap());
            List<XXTagChangeLog>  changeLogRecords      = Collections.singletonList(new XXTagChangeLog(45L, 3, serviceTagVersion, resourceId, tagId));

            rangerTag.setId(tagId);
            rangerTagDef.setId(tagTypeId);
            xServiceResource.setTags(tagTypeName);
            rangerServiceResource.setId(resourceId);

            Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
            Mockito.when(daoManager.getXXTagChangeLog()).thenReturn(xxTagChangeLogDao);
            Mockito.when(xxTagChangeLogDao.findLaterThan(Mockito.anyLong(), Mockito.anyLong())).thenReturn(changeLogRecords);
            Mockito.when(daoManager.getXXTag()).thenReturn(tagDao);

            mockedTagDBStore.when(TagDBStore::isSupportsTagsDedup).thenReturn(true);
            mockedTagDBStore.when(TagDBStore::isSupportsTagDeltas).thenReturn(true);
            mockedJsonUtil.when(() -> JsonUtils.jsonToObject(Mockito.anyString(), Mockito.any(RangerServiceResourceService.duplicatedDataType.getClass()))).thenReturn(Collections.singletonList(rangerTag));

            Mockito.when(daoManager.getXXTagDef()).thenReturn(tagDefDao);

            Mockito.when(daoManager.getXXServiceResource()).thenReturn(serviceResourceDao);

            /*
             * When
             */
            ServiceTags serviceTags = tagDBStoreSpy.getServiceTagsDelta(serviceName, (serviceTagVersion - 1));

            /*
             * Then
             */
            Assertions.assertEquals("add_or_update", serviceTags.getOp());
            Assertions.assertEquals(serviceName, serviceTags.getServiceName());
            Assertions.assertEquals(serviceTagVersion, serviceTags.getTagVersion());
            Assertions.assertTrue(serviceTags.getIsDelta());
            Assertions.assertTrue(serviceTags.getIsTagsDeduped());
            Assertions.assertEquals(ServiceTags.TagsChangeExtent.SERVICE_RESOURCE, serviceTags.getTagsChangeExtent());

            // tagDefinitions
            Assertions.assertEquals(0, serviceTags.getTagDefinitions().size());

            // tags
            Assertions.assertEquals(0, serviceTags.getTags().size());

            // serviceResources
            Assertions.assertEquals(1, serviceTags.getServiceResources().size());
            Assertions.assertEquals(resourceId, serviceTags.getServiceResources().get(0).getId());
            Assertions.assertTrue(serviceTags.getServiceResources().get(0).getIsEnabled());

            // resourceToTagIds
            Assertions.assertEquals(0, serviceTags.getResourceToTagIds().size());
        }
    }

    /**
     * Pre:
     *   New tag created: this event will not trigger a policy download, as it will not increment policy version
     *   Associate Resource: associate first resource.
     *   Associate Resource: associate second resource.
     *   De-associate resource: de-associate first resource.
     *
     * Current:
     *   De-associate resource: de-associate second resource. This event will trigger a version change. Total resources associated with Tag are before 1, after 0.
     *
     * Below method covers one delta case, when a TAG (with 1 resource) is de-associated with second/ last resource (eg, hive table)
     */
    @Test
    public void testGetServiceTagsDeltaWhenTagDeltaAndDedupEnabledRemovalOfSecondResource() throws Exception {
        try (MockedStatic<TagDBStore> mockedTagDBStore = mockStatic(TagDBStore.class); MockedStatic<JsonUtils> mockedJsonUtil = mockStatic(JsonUtils.class)) {
            final Long   tagId             = 5L;
            final Long   tagTypeId         = 2L;
            final String tagTypeName       = "ranger_atlas_tag1";
            final String tagSource         = "Atlas";
            final String serviceName       = "HIVE_SERVICE";
            final Long   serviceId         = 6L;
            final Long   resourceId        = 5L;
            final Long   serviceTagVersion = 11L;

            /*
             * Given
             */
            TagDBStore            tagDBStoreSpy         = Mockito.spy(tagDBStore);
            XXServiceDao          xxServiceDao          = Mockito.mock(XXServiceDao.class);
            XXTagChangeLogDao     xxTagChangeLogDao     = Mockito.mock(XXTagChangeLogDao.class);
            XXTagDao              tagDao                = Mockito.mock(XXTagDao.class);
            RangerTag             rangerTag             = new RangerTag("6e248648-739a-434a-9c42-a74c8277638d", tagTypeName, Collections.emptyMap(), (short) 0);
            RangerTagDef          rangerTagDef          = new RangerTagDef(tagTypeName, tagSource);
            XXServiceResource     xServiceResource      = createXXServiceResource(resourceId, "b6ef4baa-f38e-49ce-bac3-3b2243641d8a", 2L, serviceId);
            RangerServiceResource rangerServiceResource = new RangerServiceResource("b6ef4baa-f38e-49ce-bac3-3b2243641d8a", serviceName, Collections.emptyMap());
            List<XXTagChangeLog>  changeLogRecords      = Collections.singletonList(new XXTagChangeLog(46L, 3, serviceTagVersion, resourceId, tagId));

            rangerTag.setId(tagId);
            rangerTagDef.setId(tagTypeId);
            xServiceResource.setTags(tagTypeName);
            rangerServiceResource.setId(resourceId);

            Mockito.when(daoManager.getXXService()).thenReturn(xxServiceDao);
            Mockito.when(daoManager.getXXTagChangeLog()).thenReturn(xxTagChangeLogDao);
            Mockito.when(xxTagChangeLogDao.findLaterThan(Mockito.anyLong(), Mockito.anyLong())).thenReturn(changeLogRecords);
            Mockito.when(daoManager.getXXTag()).thenReturn(tagDao);

            mockedTagDBStore.when(TagDBStore::isSupportsTagsDedup).thenReturn(true);
            mockedTagDBStore.when(TagDBStore::isSupportsTagDeltas).thenReturn(true);
            mockedJsonUtil.when(() -> JsonUtils.jsonToObject(Mockito.anyString(), Mockito.any(RangerServiceResourceService.duplicatedDataType.getClass()))).thenReturn(Collections.singletonList(rangerTag));

            Mockito.when(daoManager.getXXTagDef()).thenReturn(tagDefDao);

            Mockito.when(daoManager.getXXServiceResource()).thenReturn(serviceResourceDao);

            /*
             * When
             */
            ServiceTags serviceTags = tagDBStoreSpy.getServiceTagsDelta(serviceName, (serviceTagVersion - 1));

            /*
             * Then
             */
            Assertions.assertEquals("add_or_update", serviceTags.getOp());
            Assertions.assertEquals(serviceName, serviceTags.getServiceName());
            Assertions.assertEquals(serviceTagVersion, serviceTags.getTagVersion());
            Assertions.assertTrue(serviceTags.getIsDelta());
            Assertions.assertTrue(serviceTags.getIsTagsDeduped());
            Assertions.assertEquals(ServiceTags.TagsChangeExtent.SERVICE_RESOURCE, serviceTags.getTagsChangeExtent());

            // tagDefinitions
            Assertions.assertEquals(0, serviceTags.getTagDefinitions().size());

            // tags
            Assertions.assertEquals(0, serviceTags.getTags().size());

            // serviceResources
            Assertions.assertEquals(1, serviceTags.getServiceResources().size());
            Assertions.assertEquals(resourceId, serviceTags.getServiceResources().get(0).getId());
            Assertions.assertTrue(serviceTags.getServiceResources().get(0).getIsEnabled());

            // resourceToTagIds
            Assertions.assertEquals(0, serviceTags.getResourceToTagIds().size());
        }
    }

    private XXServiceResource createXXServiceResource(Long id, String gId, Long lastKnownVersion, Long serviceId) {
        XXServiceResource xxServiceResource = new XXServiceResource();

        xxServiceResource.setId(id);
        xxServiceResource.setCreateTime(new Date());
        xxServiceResource.setGuid(gId);
        xxServiceResource.setVersion(lastKnownVersion);
        xxServiceResource.setResourceSignature(resourceSignature);
        xxServiceResource.setServiceId(serviceId);

        return xxServiceResource;
    }

    private XXTagDef createXXTagDef(Long id, Long version, String name) {
        XXTagDef ret = new XXTagDef();
        ret.setId(id);
        ret.setVersion(version);
        ret.setGuid("6bd466f5-1ad0-428f-8a87-b89b4f21cc65");
        ret.setName(name);
        ret.setIsEnabled(true);

        return ret;
    }

    private XXTag createXXTag(Long id, Long version, Long type) {
        XXTag xxTag = new XXTag();
        xxTag.setId(id);
        xxTag.setVersion(version);
        xxTag.setType(type);

        return xxTag;
    }

    private RangerTagDef createRangerTagDef() {
        RangerTagDef rangerTagDef = new RangerTagDef();

        rangerTagDef.setId(id);
        rangerTagDef.setName(name);
        rangerTagDef.setCreateTime(new Date());
        rangerTagDef.setGuid(gId);
        rangerTagDef.setVersion(lastKnownVersion);

        return rangerTagDef;
    }

    private PList<RangerTagDef> createRangerTagDefPList() {
        PList<RangerTagDef> rangerTagDefPList = new PList<>();
        List<RangerTagDef>  rangerTagDefList  = new ArrayList<>();
        RangerTagDef        rangerTagDef      = createRangerTagDef();

        rangerTagDefList.add(rangerTagDef);

        rangerTagDefPList.setList(rangerTagDefList);
        rangerTagDefPList.setPageSize(0);
        rangerTagDefPList.setResultSize(1);
        rangerTagDefPList.setSortBy("asc");
        rangerTagDefPList.setSortType("1");
        rangerTagDefPList.setStartIndex(0);
        rangerTagDefPList.setTotalCount(10);

        return rangerTagDefPList;
    }

    private RangerTag createRangerTag() {
        RangerTag rangerTag = new RangerTag();

        rangerTag.setId(id);
        rangerTag.setCreatedBy(name);
        rangerTag.setOwner((short) 0);
        rangerTag.setCreateTime(new Date());
        rangerTag.setGuid(gId);
        rangerTag.setVersion(lastKnownVersion);

        return rangerTag;
    }

    private PList<RangerTag> createRangerTagPList() {
        PList<RangerTag> rangerTagPList = new PList<>();
        List<RangerTag>  rangerTagList  = new ArrayList<>();
        RangerTag        rangerTag      = createRangerTag();

        rangerTagList.add(rangerTag);
        rangerTagPList.setList(rangerTagList);
        rangerTagPList.setPageSize(0);
        rangerTagPList.setResultSize(1);
        rangerTagPList.setSortBy("asc");
        rangerTagPList.setSortType("1");
        rangerTagPList.setStartIndex(0);
        rangerTagPList.setTotalCount(10);

        return rangerTagPList;
    }

    private RangerServiceResource createRangerServiceResource() {
        RangerServiceResource rangerServiceResource = new RangerServiceResource();

        rangerServiceResource.setId(id);
        rangerServiceResource.setCreateTime(new Date());
        rangerServiceResource.setGuid(gId);
        rangerServiceResource.setVersion(lastKnownVersion);
        rangerServiceResource.setResourceSignature(resourceSignature);
        rangerServiceResource.setServiceName(serviceName);

        return rangerServiceResource;
    }

    private PList<RangerServiceResource> createRangerServiceResourcePList() {
        PList<RangerServiceResource> rangerServiceResourcePList = new PList<>();
        List<RangerServiceResource>  rangerServiceResourceList  = new ArrayList<>();
        RangerServiceResource        rangerServiceResource      = new RangerServiceResource();

        rangerServiceResource.setId(id);
        rangerServiceResource.setCreateTime(new Date());
        rangerServiceResource.setGuid(gId);
        rangerServiceResource.setVersion(lastKnownVersion);
        rangerServiceResource.setResourceSignature(resourceSignature);
        rangerServiceResource.setServiceName(serviceName);

        rangerServiceResourceList.add(rangerServiceResource);

        rangerServiceResourcePList.setList(rangerServiceResourceList);
        rangerServiceResourcePList.setPageSize(0);
        rangerServiceResourcePList.setResultSize(1);
        rangerServiceResourcePList.setSortBy("asc");
        rangerServiceResourcePList.setSortType("1");
        rangerServiceResourcePList.setStartIndex(0);
        rangerServiceResourcePList.setTotalCount(10);

        return rangerServiceResourcePList;
    }

    private RangerTagResourceMap createRangerTagResourceMap() {
        RangerTagResourceMap rangerTagResourceMap = new RangerTagResourceMap();

        rangerTagResourceMap.setId(id);
        rangerTagResourceMap.setTagId(id);
        rangerTagResourceMap.setGuid(gId);
        rangerTagResourceMap.setVersion(lastKnownVersion);
        rangerTagResourceMap.setResourceId(id);

        return rangerTagResourceMap;
    }

    private XXServiceResource createXXServiceResource() {
        XXServiceResource xxServiceResource = new XXServiceResource();

        xxServiceResource.setId(id);
        xxServiceResource.setCreateTime(new Date());
        xxServiceResource.setGuid(gId);
        xxServiceResource.setVersion(lastKnownVersion);
        xxServiceResource.setResourceSignature(resourceSignature);
        xxServiceResource.setServiceId(id);

        return xxServiceResource;
    }

    private PList<RangerTagResourceMap> createRangerTagResourceMapPList() {
        PList<RangerTagResourceMap> rangerTagResourceMapPList = new PList<>();
        List<RangerTagResourceMap>  rangerTagResourceMapList  = new ArrayList<>();
        RangerTagResourceMap        rangerTagResourceMap      = new RangerTagResourceMap();

        rangerTagResourceMap.setId(id);
        rangerTagResourceMap.setGuid(gId);
        rangerTagResourceMap.setVersion(lastKnownVersion);
        rangerTagResourceMap.setResourceId(id);

        rangerTagResourceMapList.add(rangerTagResourceMap);

        rangerTagResourceMapPList.setList(rangerTagResourceMapList);
        rangerTagResourceMapPList.setPageSize(0);
        rangerTagResourceMapPList.setResultSize(1);
        rangerTagResourceMapPList.setSortBy("asc");
        rangerTagResourceMapPList.setSortType("1");
        rangerTagResourceMapPList.setStartIndex(0);
        rangerTagResourceMapPList.setTotalCount(10);

        return rangerTagResourceMapPList;
    }

    private XXServiceVersionInfo createXXServiceVersionInfo() {
        XXServiceVersionInfo serviceVersionInfoDbObj = new XXServiceVersionInfo();

        serviceVersionInfoDbObj.setId(id);
        serviceVersionInfoDbObj.setRoleVersion(lastKnownVersion);
        serviceVersionInfoDbObj.setPolicyVersion(lastKnownVersion);
        serviceVersionInfoDbObj.setTagVersion(lastKnownVersion);
        serviceVersionInfoDbObj.setGdsVersion(lastKnownVersion);

        return serviceVersionInfoDbObj;
    }

    private XXService createXXService() {
        XXService xxService = new XXService();

        xxService.setId(id);
        xxService.setName(serviceName);
        xxService.setType(5L);

        return xxService;
    }

    private RangerServiceDef createRangerServiceDef() {
        List<RangerServiceConfigDef>   configs          = new ArrayList<>();
        List<RangerResourceDef>        resources        = new ArrayList<>();
        List<RangerAccessTypeDef>      accessTypes      = new ArrayList<>();
        List<RangerPolicyConditionDef> policyConditions = new ArrayList<>();
        List<RangerContextEnricherDef> contextEnrichers = new ArrayList<>();
        List<RangerEnumDef>            enums            = new ArrayList<>();
        RangerServiceDef               rangerServiceDef = new RangerServiceDef();

        rangerServiceDef.setId(id);
        rangerServiceDef.setImplClass("RangerServiceHdfs");
        rangerServiceDef.setLabel("HDFS Repository");
        rangerServiceDef.setDescription("HDFS Repository");
        rangerServiceDef.setRbKeyDescription(null);
        rangerServiceDef.setUpdatedBy("Admin");
        rangerServiceDef.setUpdateTime(new Date());
        rangerServiceDef.setConfigs(configs);
        rangerServiceDef.setResources(resources);
        rangerServiceDef.setAccessTypes(accessTypes);
        rangerServiceDef.setPolicyConditions(policyConditions);
        rangerServiceDef.setContextEnrichers(contextEnrichers);
        rangerServiceDef.setEnums(enums);

        return rangerServiceDef;
    }

    private XXTag createXXTag() {
        XXTag xxTag = new XXTag();

        xxTag.setGuid(gId);
        xxTag.setId(id);
        xxTag.setVersion(lastKnownVersion);
        xxTag.setType(1L);

        return xxTag;
    }

    private XXTagResourceMap createXXTagResourceMap() {
        XXTagResourceMap xxTagResourceMap = new XXTagResourceMap();

        xxTagResourceMap.setTagId(id);
        xxTagResourceMap.setResourceId(id);
        xxTagResourceMap.setId(id);
        xxTagResourceMap.setGuid(gId);

        return xxTagResourceMap;
    }

    private RangerServiceResourceWithTagsList createRangerServiceResourceWithTagsViewList() {
        RangerServiceResourceWithTagsList   rangerServiceResourceViewList = new RangerServiceResourceWithTagsList();
        List<RangerServiceResourceWithTags> rangerServiceResourceList     = new ArrayList<>();
        RangerServiceResourceWithTags       rangerServiceResource         = new RangerServiceResourceWithTags();
        List<RangerTag>                     associatedTags                = new ArrayList<>();

        associatedTags.add(createRangerTag());

        rangerServiceResource.setId(id);
        rangerServiceResource.setCreateTime(new Date());
        rangerServiceResource.setGuid(gId);
        rangerServiceResource.setVersion(lastKnownVersion);
        rangerServiceResource.setServiceName(serviceName);
        rangerServiceResource.setAssociatedTags(associatedTags);

        rangerServiceResourceList.add(rangerServiceResource);

        rangerServiceResourceViewList.setResourceList(rangerServiceResourceList);
        rangerServiceResourceViewList.setPageSize(0);
        rangerServiceResourceViewList.setResultSize(1);
        rangerServiceResourceViewList.setSortBy("asc");
        rangerServiceResourceViewList.setSortType("1");
        rangerServiceResourceViewList.setStartIndex(0);
        rangerServiceResourceViewList.setTotalCount(1);

        return rangerServiceResourceViewList;
    }
}
