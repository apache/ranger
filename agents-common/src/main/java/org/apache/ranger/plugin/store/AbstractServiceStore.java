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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.hadoop.config.RangerAdminConfig;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerDataMaskDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerDataMaskTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerRowFilterDef;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.apache.ranger.services.tag.RangerServiceTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public abstract class AbstractServiceStore implements ServiceStore {
    public static final  String  COMPONENT_ACCESSTYPE_SEPARATOR                 = ":";
    public static final  String  AUTOPROPAGATE_ROWFILTERDEF_TO_TAG_PROP         = "ranger.servicedef.autopropagate.rowfilterdef.to.tag";
    public static final  boolean AUTOPROPAGATE_ROWFILTERDEF_TO_TAG_PROP_DEFAULT = false;
    private static final Logger  LOG                                            = LoggerFactory.getLogger(AbstractServiceStore.class);
    private static final int     MAX_ACCESS_TYPES_IN_SERVICE_DEF                = 1000;

    private final RangerAdminConfig config;

    protected AbstractServiceStore() {
        this.config = RangerAdminConfig.getInstance();
    }

    public static long getNextVersion(Long currentVersion) {
        return currentVersion == null ? 1L : currentVersion + 1;
    }

    @Override
    public void updateTagServiceDefForAccessTypes() throws Exception {
        LOG.debug("==> ServiceDefDBStore.updateTagServiceDefForAccessTypes()");

        List<RangerServiceDef> allServiceDefs = getServiceDefs(new SearchFilter());
        for (RangerServiceDef serviceDef : allServiceDefs) {
            if (ServiceDefUtil.getOption_enableTagBasedPolicies(serviceDef, config)) {
                updateTagServiceDefForUpdatingAccessTypes(serviceDef);
            }
        }

        LOG.debug("<== ServiceDefDBStore.updateTagServiceDefForAccessTypes()");
    }

    @Override
    public PList<RangerServiceDef> getPaginatedServiceDefs(SearchFilter filter) throws Exception {
        List<RangerServiceDef> resultList = getServiceDefs(filter);

        return CollectionUtils.isEmpty(resultList) ? new PList<>() : new PList<>(resultList, 0, resultList.size(), resultList.size(), resultList.size(), filter.getSortType(), filter.getSortBy());
    }

    @Override
    public PList<RangerService> getPaginatedServices(SearchFilter filter) throws Exception {
        List<RangerService> resultList = getServices(filter);

        return CollectionUtils.isEmpty(resultList) ? new PList<>() : new PList<>(resultList, 0, resultList.size(), resultList.size(), resultList.size(), filter.getSortType(), filter.getSortBy());
    }

    @Override
    public PList<RangerPolicy> getPaginatedPolicies(SearchFilter filter) throws Exception {
        List<RangerPolicy> resultList = getPolicies(filter);

        return CollectionUtils.isEmpty(resultList) ? new PList<>() : new PList<>(resultList, 0, resultList.size(), resultList.size(), resultList.size(), filter.getSortType(), filter.getSortBy());
    }

    @Override
    public PList<RangerPolicy> getPaginatedServicePolicies(Long serviceId, SearchFilter filter) throws Exception {
        List<RangerPolicy> resultList = getServicePolicies(serviceId, filter);

        return CollectionUtils.isEmpty(resultList) ? new PList<>() : new PList<>(resultList, 0, resultList.size(), resultList.size(), resultList.size(), filter.getSortType(), filter.getSortBy());
    }

    @Override
    public PList<RangerPolicy> getPaginatedServicePolicies(String serviceName, SearchFilter filter) throws Exception {
        List<RangerPolicy> resultList = getServicePolicies(serviceName, filter);

        return CollectionUtils.isEmpty(resultList) ? new PList<>() : new PList<>(resultList, 0, resultList.size(), resultList.size(), resultList.size(), filter.getSortType(), filter.getSortBy());
    }

    @Override
    public Long getServicePolicyVersion(String serviceName) {
        RangerService service = null;

        try {
            service = getServiceByName(serviceName);
        } catch (Exception exception) {
            LOG.error("Failed to get service object for service:{}", serviceName);
        }

        return service != null ? service.getPolicyVersion() : null;
    }

    // when a service-def is updated, the updated service-def should be made available to plugins
    //   this is achieved by incrementing policyVersion of all its services
    protected abstract void updateServicesForServiceDefUpdate(RangerServiceDef serviceDef) throws Exception;

    protected void postCreate(RangerBaseModelObject obj) throws Exception {
        if (obj instanceof RangerServiceDef) {
            updateTagServiceDefForUpdatingAccessTypes((RangerServiceDef) obj);
        }
    }

    protected void postUpdate(RangerBaseModelObject obj) throws Exception {
        if (obj instanceof RangerServiceDef) {
            RangerServiceDef serviceDef = (RangerServiceDef) obj;

            updateTagServiceDefForUpdatingAccessTypes(serviceDef);
            updateServicesForServiceDefUpdate(serviceDef);
        }
    }

    protected void postDelete(RangerBaseModelObject obj) throws Exception {
        if (obj instanceof RangerServiceDef) {
            updateTagServiceDefForDeletingAccessTypes(((RangerServiceDef) obj).getName());
        }
    }

    private RangerAccessTypeDef findAccessTypeDef(long itemId, List<RangerAccessTypeDef> accessTypeDefs) {
        RangerAccessTypeDef ret = null;

        for (RangerAccessTypeDef accessTypeDef : accessTypeDefs) {
            if (itemId == accessTypeDef.getItemId()) {
                ret = accessTypeDef;
                break;
            }
        }
        return ret;
    }

    private boolean updateTagAccessTypeDef(RangerAccessTypeDef tagAccessType, RangerAccessTypeDef svcAccessType, String prefix) {
        boolean isUpdated = false;

        if (!Objects.equals(tagAccessType.getName().substring(prefix.length()), svcAccessType.getName())) {
            isUpdated = true;
        } else if (!Objects.equals(tagAccessType.getLabel(), svcAccessType.getLabel())) {
            isUpdated = true;
        } else if (!Objects.equals(tagAccessType.getRbKeyLabel(), svcAccessType.getRbKeyLabel())) {
            isUpdated = true;
        } else {
            Collection<String> tagImpliedGrants = tagAccessType.getImpliedGrants();
            Collection<String> svcImpliedGrants = svcAccessType.getImpliedGrants();

            int tagImpliedGrantsLen = tagImpliedGrants == null ? 0 : tagImpliedGrants.size();
            int svcImpliedGrantsLen = svcImpliedGrants == null ? 0 : svcImpliedGrants.size();

            if (tagImpliedGrantsLen != svcImpliedGrantsLen) {
                isUpdated = true;
            } else if (tagImpliedGrantsLen > 0) {
                for (String svcImpliedGrant : svcImpliedGrants) {
                    if (!tagImpliedGrants.contains(prefix + svcImpliedGrant)) {
                        isUpdated = true;
                        break;
                    }
                }
            }
        }

        if (isUpdated) {
            tagAccessType.setName(prefix + svcAccessType.getName());
            tagAccessType.setLabel(svcAccessType.getLabel());
            tagAccessType.setRbKeyLabel(svcAccessType.getRbKeyLabel());

            tagAccessType.setImpliedGrants(new HashSet<>());

            if (CollectionUtils.isNotEmpty(svcAccessType.getImpliedGrants())) {
                for (String svcImpliedGrant : svcAccessType.getImpliedGrants()) {
                    tagAccessType.getImpliedGrants().add(prefix + svcImpliedGrant);
                }
            }
        }
        return isUpdated;
    }

    private boolean updateTagAccessTypeDefs(List<RangerAccessTypeDef> svcDefAccessTypes, List<RangerAccessTypeDef> tagDefAccessTypes, long itemIdOffset, String prefix) {
        List<RangerAccessTypeDef> toAdd    = new ArrayList<>();
        List<RangerAccessTypeDef> toUpdate = new ArrayList<>();
        List<RangerAccessTypeDef> toDelete = new ArrayList<>();

        for (RangerAccessTypeDef svcAccessType : svcDefAccessTypes) {
            long tagAccessTypeItemId = svcAccessType.getItemId() + itemIdOffset;

            RangerAccessTypeDef tagAccessType = findAccessTypeDef(tagAccessTypeItemId, tagDefAccessTypes);

            if (tagAccessType == null) {
                tagAccessType = new RangerAccessTypeDef();

                tagAccessType.setItemId(tagAccessTypeItemId);
                tagAccessType.setName(prefix + svcAccessType.getName());
                tagAccessType.setLabel(svcAccessType.getLabel());
                tagAccessType.setRbKeyLabel(svcAccessType.getRbKeyLabel());
                tagAccessType.setCategory(svcAccessType.getCategory());

                tagAccessType.setImpliedGrants(new HashSet<>());

                if (CollectionUtils.isNotEmpty(svcAccessType.getImpliedGrants())) {
                    for (String svcImpliedGrant : svcAccessType.getImpliedGrants()) {
                        tagAccessType.getImpliedGrants().add(prefix + svcImpliedGrant);
                    }
                }

                toAdd.add(tagAccessType);
            }
        }

        for (RangerAccessTypeDef tagAccessType : tagDefAccessTypes) {
            if (tagAccessType.getName().startsWith(prefix)) {
                long svcAccessTypeItemId = tagAccessType.getItemId() - itemIdOffset;

                RangerAccessTypeDef svcAccessType = findAccessTypeDef(svcAccessTypeItemId, svcDefAccessTypes);

                if (svcAccessType == null) { // accessType has been deleted in service
                    toDelete.add(tagAccessType);
                } else if (updateTagAccessTypeDef(tagAccessType, svcAccessType, prefix)) {
                    toUpdate.add(tagAccessType);
                }
            }
        }

        boolean updateNeeded = false;

        if (CollectionUtils.isNotEmpty(toAdd) || CollectionUtils.isNotEmpty(toUpdate) || CollectionUtils.isNotEmpty(toDelete)) {
            if (LOG.isDebugEnabled()) {
                for (RangerAccessTypeDef accessTypeDef : toDelete) {
                    LOG.debug("accessTypeDef-to-delete:[{}]", accessTypeDef);
                }

                for (RangerAccessTypeDef accessTypeDef : toUpdate) {
                    LOG.debug("accessTypeDef-to-update:[{}]", accessTypeDef);
                }

                for (RangerAccessTypeDef accessTypeDef : toAdd) {
                    LOG.debug("accessTypeDef-to-add:[{}]", accessTypeDef);
                }
            }

            tagDefAccessTypes.addAll(toAdd);
            tagDefAccessTypes.removeAll(toDelete);

            updateNeeded = true;
        }

        return updateNeeded;
    }

    private void updateTagServiceDefForUpdatingAccessTypes(RangerServiceDef serviceDef) throws Exception {
        if (StringUtils.equals(serviceDef.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME) ||
                StringUtils.equals(serviceDef.getName(), EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_GDS_NAME)) {
            return;
        }

        if (EmbeddedServiceDefsUtil.instance().getTagServiceDefId() == -1) {
            LOG.info("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes({}): tag service-def does not exist", serviceDef.getName());
        }

        RangerServiceDef tagServiceDef;

        try {
            tagServiceDef = this.getServiceDef(EmbeddedServiceDefsUtil.instance().getTagServiceDefId());
        } catch (Exception e) {
            LOG.error("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes{}): could not find TAG ServiceDef.. ", serviceDef.getName(), e);

            throw e;
        }

        if (tagServiceDef == null) {
            LOG.error("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes({}): could not find TAG ServiceDef.. ", serviceDef.getName());

            return;
        }

        String                    serviceDefName    = serviceDef.getName();
        String                    prefix            = serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR;
        List<RangerAccessTypeDef> svcDefAccessTypes = serviceDef.getAccessTypes();
        List<RangerAccessTypeDef> tagDefAccessTypes = tagServiceDef.getAccessTypes();
        long                      itemIdOffset      = serviceDef.getId() * (MAX_ACCESS_TYPES_IN_SERVICE_DEF + 1);
        boolean                   updateNeeded      = updateTagAccessTypeDefs(svcDefAccessTypes, tagDefAccessTypes, itemIdOffset, prefix);

        if (updateTagServiceDefForUpdatingDataMaskDef(tagServiceDef, serviceDef, itemIdOffset, prefix)) {
            updateNeeded = true;
        }

        if (updateTagServiceDefForUpdatingRowFilterDef(tagServiceDef, serviceDef, itemIdOffset, prefix)) {
            updateNeeded = true;
        }

        boolean resourceUpdated = updateResourceInTagServiceDef(tagServiceDef);

        updateNeeded = updateNeeded || resourceUpdated;

        if (updateNeeded) {
            try {
                updateServiceDef(tagServiceDef);

                LOG.info("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes -- updated TAG service def with {} access types", serviceDefName);
            } catch (Exception e) {
                LOG.error("AbstractServiceStore.updateTagServiceDefForUpdatingAccessTypes -- Failed to update TAG ServiceDef.. ", e);

                throw e;
            }
        }
    }

    private void updateTagServiceDefForDeletingAccessTypes(String serviceDefName) throws Exception {
        if (EmbeddedServiceDefsUtil.EMBEDDED_SERVICEDEF_TAG_NAME.equals(serviceDefName)) {
            return;
        }

        RangerServiceDef tagServiceDef;
        try {
            tagServiceDef = this.getServiceDef(EmbeddedServiceDefsUtil.instance().getTagServiceDefId());
        } catch (Exception e) {
            LOG.error("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes({}): could not find TAG ServiceDef.. ", serviceDefName, e);

            throw e;
        }

        if (tagServiceDef == null) {
            LOG.error("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes({}): could not find TAG ServiceDef.. ", serviceDefName);

            return;
        }

        List<RangerAccessTypeDef> accessTypes = new ArrayList<>();

        for (RangerAccessTypeDef accessType : tagServiceDef.getAccessTypes()) {
            if (accessType.getName().startsWith(serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR)) {
                accessTypes.add(accessType);
            }
        }

        tagServiceDef.getAccessTypes().removeAll(accessTypes);

        updateTagServiceDefForDeletingDataMaskDef(tagServiceDef, serviceDefName);

        updateTagServiceDefForDeletingRowFilterDef(tagServiceDef, serviceDefName);

        updateResourceInTagServiceDef(tagServiceDef);

        try {
            updateServiceDef(tagServiceDef);

            LOG.info("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes -- updated TAG service def with {} access types", serviceDefName);
        } catch (Exception e) {
            LOG.error("AbstractServiceStore.updateTagServiceDefForDeletingAccessTypes -- Failed to update TAG ServiceDef.. ", e);

            throw e;
        }
    }

    private boolean updateTagServiceDefForUpdatingDataMaskDef(RangerServiceDef tagServiceDef, RangerServiceDef serviceDef, long itemIdOffset, String prefix) {
        LOG.debug("==> AbstractServiceStore.updateTagServiceDefForUpdatingDataMaskDef({})", serviceDef.getName());

        boolean ret = false;

        RangerDataMaskDef           svcDataMaskDef    = serviceDef.getDataMaskDef();
        RangerDataMaskDef           tagDataMaskDef    = tagServiceDef.getDataMaskDef();
        List<RangerDataMaskTypeDef> svcDefMaskTypes   = svcDataMaskDef.getMaskTypes();
        List<RangerDataMaskTypeDef> tagDefMaskTypes   = tagDataMaskDef.getMaskTypes();
        List<RangerAccessTypeDef>   svcDefAccessTypes = svcDataMaskDef.getAccessTypes();
        List<RangerAccessTypeDef>   tagDefAccessTypes = tagDataMaskDef.getAccessTypes();
        List<RangerDataMaskTypeDef> maskTypesToAdd    = new ArrayList<>();
        List<RangerDataMaskTypeDef> maskTypesToUpdate = new ArrayList<>();
        List<RangerDataMaskTypeDef> maskTypesToDelete = new ArrayList<>();

        for (RangerDataMaskTypeDef svcMaskType : svcDefMaskTypes) {
            long                                   tagMaskTypeItemId = itemIdOffset + svcMaskType.getItemId();
            RangerDataMaskTypeDef foundTagMaskType  = null;

            for (RangerDataMaskTypeDef tagMaskType : tagDefMaskTypes) {
                if (tagMaskType.getItemId().equals(tagMaskTypeItemId)) {
                    foundTagMaskType = tagMaskType;

                    break;
                }
            }

            if (foundTagMaskType == null) {
                RangerDataMaskTypeDef tagMaskType = new RangerDataMaskTypeDef(svcMaskType);

                tagMaskType.setName(prefix + svcMaskType.getName());
                tagMaskType.setItemId(itemIdOffset + svcMaskType.getItemId());
                tagMaskType.setLabel(svcMaskType.getLabel());
                tagMaskType.setRbKeyLabel(svcMaskType.getRbKeyLabel());

                maskTypesToAdd.add(tagMaskType);
            }
        }

        for (RangerDataMaskTypeDef tagMaskType : tagDefMaskTypes) {
            if (StringUtils.startsWith(tagMaskType.getName(), prefix)) {
                RangerDataMaskTypeDef foundSvcMaskType = null;

                for (RangerDataMaskTypeDef svcMaskType : svcDefMaskTypes) {
                    long tagMaskTypeItemId = itemIdOffset + svcMaskType.getItemId();

                    if (tagMaskType.getItemId().equals(tagMaskTypeItemId)) {
                        foundSvcMaskType = svcMaskType;

                        break;
                    }
                }

                if (foundSvcMaskType == null) {
                    maskTypesToDelete.add(tagMaskType);

                    continue;
                }

                RangerDataMaskTypeDef checkTagMaskType = new RangerDataMaskTypeDef(foundSvcMaskType);

                checkTagMaskType.setName(prefix + foundSvcMaskType.getName());
                checkTagMaskType.setItemId(itemIdOffset + foundSvcMaskType.getItemId());

                if (!checkTagMaskType.equals(tagMaskType)) {
                    tagMaskType.setLabel(checkTagMaskType.getLabel());
                    tagMaskType.setDescription(checkTagMaskType.getDescription());
                    tagMaskType.setTransformer(checkTagMaskType.getTransformer());
                    tagMaskType.setDataMaskOptions(checkTagMaskType.getDataMaskOptions());
                    tagMaskType.setRbKeyLabel(checkTagMaskType.getRbKeyLabel());
                    tagMaskType.setRbKeyDescription(checkTagMaskType.getRbKeyDescription());

                    maskTypesToUpdate.add(tagMaskType);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(maskTypesToAdd) || CollectionUtils.isNotEmpty(maskTypesToUpdate) || CollectionUtils.isNotEmpty(maskTypesToDelete)) {
            ret = true;

            if (LOG.isDebugEnabled()) {
                for (RangerDataMaskTypeDef maskTypeDef : maskTypesToDelete) {
                    LOG.debug("maskTypeDef-to-delete:[{}]", maskTypeDef);
                }

                for (RangerDataMaskTypeDef maskTypeDef : maskTypesToUpdate) {
                    LOG.debug("maskTypeDef-to-update:[{}]", maskTypeDef);
                }

                for (RangerDataMaskTypeDef maskTypeDef : maskTypesToAdd) {
                    LOG.debug("maskTypeDef-to-add:[{}]", maskTypeDef);
                }
            }

            tagDefMaskTypes.removeAll(maskTypesToDelete);
            tagDefMaskTypes.addAll(maskTypesToAdd);

            tagDataMaskDef.setMaskTypes(tagDefMaskTypes);
        }

        boolean tagMaskDefAccessTypesUpdated = updateTagAccessTypeDefs(svcDefAccessTypes, tagDefAccessTypes, itemIdOffset, prefix);

        if (tagMaskDefAccessTypesUpdated) {
            tagDataMaskDef.setAccessTypes(tagDefAccessTypes);

            ret = true;
        }

        LOG.debug("<== AbstractServiceStore.updateTagServiceDefForUpdatingDataMaskDef({}) : {}", serviceDef.getName(), ret);

        return ret;
    }

    private void updateTagServiceDefForDeletingDataMaskDef(RangerServiceDef tagServiceDef, String serviceDefName) {
        LOG.debug("==> AbstractServiceStore.updateTagServiceDefForDeletingDataMaskDef({})", serviceDefName);

        RangerDataMaskDef tagDataMaskDef = tagServiceDef.getDataMaskDef();

        if (tagDataMaskDef == null) {
            return;
        }

        String                    prefix      = serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR;
        List<RangerAccessTypeDef> accessTypes = new ArrayList<>();

        for (RangerAccessTypeDef accessType : tagDataMaskDef.getAccessTypes()) {
            if (accessType.getName().startsWith(prefix)) {
                accessTypes.add(accessType);
            }
        }

        List<RangerDataMaskTypeDef> maskTypes = new ArrayList<>();

        for (RangerDataMaskTypeDef maskType : tagDataMaskDef.getMaskTypes()) {
            if (maskType.getName().startsWith(prefix)) {
                maskTypes.add(maskType);
            }
        }

        tagDataMaskDef.getAccessTypes().removeAll(accessTypes);
        tagDataMaskDef.getMaskTypes().removeAll(maskTypes);

        LOG.debug("<== AbstractServiceStore.updateTagServiceDefForDeletingDataMaskDef({})", serviceDefName);
    }

    private boolean updateTagServiceDefForUpdatingRowFilterDef(RangerServiceDef tagServiceDef, RangerServiceDef serviceDef, long itemIdOffset, String prefix) {
        LOG.debug("==> AbstractServiceStore.updateTagServiceDefForUpdatingRowFilterDef({})", serviceDef.getName());

        boolean ret                            = false;
        boolean autopropagateRowfilterdefToTag = config.getBoolean(AUTOPROPAGATE_ROWFILTERDEF_TO_TAG_PROP, AUTOPROPAGATE_ROWFILTERDEF_TO_TAG_PROP_DEFAULT);

        if (autopropagateRowfilterdefToTag) {
            RangerRowFilterDef        svcRowFilterDef   = serviceDef.getRowFilterDef();
            RangerRowFilterDef        tagRowFilterDef   = tagServiceDef.getRowFilterDef();
            List<RangerAccessTypeDef> svcDefAccessTypes = svcRowFilterDef.getAccessTypes();
            List<RangerAccessTypeDef> tagDefAccessTypes = tagRowFilterDef.getAccessTypes();

            boolean tagRowFilterAccessTypesUpdated = updateTagAccessTypeDefs(svcDefAccessTypes, tagDefAccessTypes, itemIdOffset, prefix);

            if (tagRowFilterAccessTypesUpdated) {
                tagRowFilterDef.setAccessTypes(tagDefAccessTypes);

                ret = true;
            }
        }

        LOG.debug("<== AbstractServiceStore.updateTagServiceDefForUpdatingRowFilterDef({}) : {}", serviceDef.getName(), ret);

        return ret;
    }

    private void updateTagServiceDefForDeletingRowFilterDef(RangerServiceDef tagServiceDef, String serviceDefName) {
        LOG.debug("==> AbstractServiceStore.updateTagServiceDefForDeletingRowFilterDef({})", serviceDefName);

        RangerRowFilterDef tagRowFilterDef = tagServiceDef.getRowFilterDef();

        if (tagRowFilterDef == null) {
            return;
        }

        String                    prefix      = serviceDefName + COMPONENT_ACCESSTYPE_SEPARATOR;
        List<RangerAccessTypeDef> accessTypes = new ArrayList<>();

        for (RangerAccessTypeDef accessType : tagRowFilterDef.getAccessTypes()) {
            if (accessType.getName().startsWith(prefix)) {
                accessTypes.add(accessType);
            }
        }

        tagRowFilterDef.getAccessTypes().removeAll(accessTypes);

        LOG.debug("<== AbstractServiceStore.updateTagServiceDefForDeletingRowFilterDef({})", serviceDefName);
    }

    private boolean updateResourceInTagServiceDef(RangerServiceDef tagServiceDef) {
        LOG.debug("==> AbstractServiceStore.updateResourceInTagServiceDef({})", tagServiceDef);

        boolean ret = false;

        final RangerResourceDef       accessPolicyTagResource = getResourceDefForTagResource(tagServiceDef.getResources());
        final List<RangerResourceDef> resources               = new ArrayList<>();

        if (accessPolicyTagResource == null) {
            LOG.warn("Resource with name :[{}] not found in  tag-service-definition!!", RangerServiceTag.TAG_RESOURCE_NAME);
        } else {
            resources.add(accessPolicyTagResource);
        }

        RangerDataMaskDef dataMaskDef = tagServiceDef.getDataMaskDef();

        if (dataMaskDef != null) {
            if (CollectionUtils.isNotEmpty(dataMaskDef.getAccessTypes())) {
                if (CollectionUtils.isEmpty(dataMaskDef.getResources())) {
                    dataMaskDef.setResources(resources);

                    ret = true;
                }
            } else {
                if (CollectionUtils.isNotEmpty(dataMaskDef.getResources())) {
                    dataMaskDef.setResources(null);

                    ret = true;
                }
            }
        }

        RangerRowFilterDef rowFilterDef = tagServiceDef.getRowFilterDef();

        if (rowFilterDef != null) {
            boolean autopropagateRowfilterdefToTag = config.getBoolean(AUTOPROPAGATE_ROWFILTERDEF_TO_TAG_PROP, AUTOPROPAGATE_ROWFILTERDEF_TO_TAG_PROP_DEFAULT);

            if (autopropagateRowfilterdefToTag) {
                if (CollectionUtils.isNotEmpty(rowFilterDef.getAccessTypes())) {
                    if (CollectionUtils.isEmpty(rowFilterDef.getResources())) {
                        rowFilterDef.setResources(resources);

                        ret = true;
                    }
                } else {
                    if (CollectionUtils.isNotEmpty(rowFilterDef.getResources())) {
                        rowFilterDef.setResources(null);

                        ret = true;
                    }
                }
            }
        }

        LOG.debug("<== AbstractServiceStore.updateResourceInTagServiceDef({}) : {}", tagServiceDef, ret);

        return ret;
    }

    private RangerResourceDef getResourceDefForTagResource(List<RangerResourceDef> resourceDefs) {
        RangerResourceDef ret = null;

        if (CollectionUtils.isNotEmpty(resourceDefs)) {
            for (RangerResourceDef resourceDef : resourceDefs) {
                if (resourceDef.getName().equals(RangerServiceTag.TAG_RESOURCE_NAME)) {
                    ret = resourceDef;

                    break;
                }
            }
        }

        return ret;
    }
}
