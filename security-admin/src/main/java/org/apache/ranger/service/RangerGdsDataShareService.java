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

package org.apache.ranger.service;


import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.authorization.utils.JsonUtils;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.entity.XXGdsDataShare;
import org.apache.ranger.entity.XXSecurityZone;
import org.apache.ranger.entity.XXService;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerGdsVList.RangerDataShareList;
import org.apache.ranger.view.VXMessage;
import org.apache.ranger.view.VXResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;

@Service
@Scope("singleton")
public class RangerGdsDataShareService extends RangerGdsBaseModelService<XXGdsDataShare, RangerDataShare> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsDataShareService.class);

    @Autowired
    GUIDUtil guidUtil;

    public RangerGdsDataShareService() {
        super();

        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_NAME, "obj.name",        SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_ID,   "obj.id",          SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.GUID      ,      "obj.guid",        SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.IS_ENABLED,      "obj.isEnabled",   SearchField.DATA_TYPE.BOOLEAN, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME,    "d.name",          SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL, "XXGdsDataShareInDataset dshid, XXGdsDataset d", "obj.id = dshid.dataShareId and dshid.datasetId = d.id"));
        searchFields.add(new SearchField(SearchFilter.DATASET_ID,      "dshid.datasetId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShareInDataset dshid", "obj.id = dshid.dataShareId"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME,    "p.name",          SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL, "XXGdsDataShareInDataset dshid, XXGdsDatasetInProject dip, XXGdsProject p", "obj.id = dshid.dataShareId and dshid.datasetId = dip.datasetId and dip.projectId = p.id"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_ID,      "dip.projectId",   SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL, "XXGdsDataShareInDataset dshid, XXGdsDatasetInProject dip", "obj.id = dshid.dataShareId and dshid.datasetId = dip.datasetId"));

        sortFields.add(new SortField(SearchFilter.CREATE_TIME,  "obj.createTime"));
        sortFields.add(new SortField(SearchFilter.UPDATE_TIME,  "obj.updateTime"));
        sortFields.add(new SortField(SearchFilter.DATASET_ID,   "obj.id", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.DATASET_NAME, "obj.name"));
    }

    @Override
    public RangerDataShare postCreate(XXGdsDataShare xObj) {
        RangerDataShare ret = super.postCreate(xObj);

        // TODO:

        return ret;
    }

    @Override
    public RangerDataShare postUpdate(XXGdsDataShare xObj) {
        RangerDataShare ret = super.postUpdate(xObj);

        // TODO:

        return ret;
    }

    @Override
    public XXGdsDataShare preDelete(Long id) {
        // Update ServiceVersionInfo for each service in the zone
        XXGdsDataShare ret = super.preDelete(id);

        // TODO:

        return ret;
    }

    @Override
    protected void validateForCreate(RangerDataShare vObj) {
        List<VXMessage> msgList = null;

        if (StringUtils.isBlank(vObj.getName())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "name"));
        }

        XXService xService = daoMgr.getXXService().findByName(vObj.getService());

        if (xService == null) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null, "service"));
        }

        if (!StringUtils.isBlank(vObj.getZone())) {
            XXSecurityZone xSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneName(vObj.getZone());

            if (xSecurityZone == null) {
                msgList = getOrCreateMessageList(msgList);

                msgList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null, "zone"));
            }
        }

        if (CollectionUtils.isNotEmpty(msgList)) {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
            gjResponse.setMsgDesc("Validation failure");
            gjResponse.setMessageList(msgList);

            LOG.debug("Validation failure in createDataShare({}): error={}", vObj, gjResponse);

            throw restErrorUtil.createRESTException(gjResponse);
        }

        if (StringUtils.isBlank(vObj.getGuid())) {
            vObj.setGuid(guidUtil.genGUID());
        }

        if (vObj.getIsEnabled() == null) {
            vObj.setIsEnabled(Boolean.TRUE);
        }
    }

    @Override
    protected void validateForUpdate(RangerDataShare vObj, XXGdsDataShare xObj) {
        List<VXMessage> msgList = null;

        if (StringUtils.isBlank(vObj.getName())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "name"));
        }

        XXService xService = daoMgr.getXXService().findByName(vObj.getService());

        if (xService == null || !Objects.equals(xService.getId(), xObj.getServiceId())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null, "service"));
        }

        if (!StringUtils.isBlank(vObj.getZone())) {
            XXSecurityZone xSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneName(vObj.getZone());

            if (xSecurityZone == null || !Objects.equals(xSecurityZone.getId(), xObj.getZoneId())) {
                msgList = getOrCreateMessageList(msgList);

                msgList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null, "zone"));
            }
        } else if (!Objects.equals(RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID, xObj.getZoneId())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.INVALID_INPUT_DATA.getMessage(null, "zone"));
        }

        if (CollectionUtils.isNotEmpty(msgList)) {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
            gjResponse.setMsgDesc("Validation failure");
            gjResponse.setMessageList(msgList);

            LOG.debug("Validation failure in updateDataShare({}): error={}", vObj, gjResponse);

            throw restErrorUtil.createRESTException(gjResponse);
        }

        if (vObj.getIsEnabled() == null) {
            vObj.setIsEnabled(Boolean.TRUE);
        }
    }

    @Override
    protected XXGdsDataShare mapViewToEntityBean(RangerDataShare vObj, XXGdsDataShare xObj, int OPERATION_CONTEXT) {
        XXService xService = daoMgr.getXXService().findByName(vObj.getService());

        if (xService == null) {
            throw restErrorUtil.createRESTException("No service found with name: " + vObj.getService(), MessageEnums.INVALID_INPUT_DATA);
        }

        final Long zoneId;

        if (StringUtils.isBlank(vObj.getZone())) {
            zoneId = RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID;
        } else {
            XXSecurityZone xSecurityZone = daoMgr.getXXSecurityZoneDao().findByZoneName(vObj.getZone());

            if (xSecurityZone == null) {
                throw restErrorUtil.createRESTException("No security zone found with name: " + vObj.getZone(), MessageEnums.INVALID_INPUT_DATA);
            }

            zoneId = xSecurityZone.getId();
        }

        xObj.setGuid(vObj.getGuid());
        xObj.setIsEnabled(vObj.getIsEnabled());
        xObj.setName(vObj.getName());
        xObj.setDescription(vObj.getDescription());
        xObj.setAdmins(JsonUtils.listToJson(vObj.getAdmins()));
        xObj.setServiceId(xService.getId());
        xObj.setZoneId(zoneId);
        xObj.setConditionExpr(vObj.getConditionExpr());
        xObj.setDefaultAccessTypes(JsonUtils.objectToJson(vObj.getDefaultAccessTypes()));
        xObj.setDefaultMasks(JsonUtils.objectToJson(vObj.getDefaultMasks()));
        xObj.setTermsOfUse(vObj.getTermsOfUse());
        xObj.setOptions(JsonUtils.mapToJson(vObj.getOptions()));
        xObj.setAdditionalInfo(JsonUtils.mapToJson(vObj.getAdditionalInfo()));

        return xObj;
    }

    @Override
    protected RangerDataShare mapEntityToViewBean(RangerDataShare vObj, XXGdsDataShare xObj) {
        XXService      xService      = daoMgr.getXXService().getById(xObj.getServiceId());
        XXSecurityZone xSecurityZone = daoMgr.getXXSecurityZoneDao().getById(xObj.getZoneId());

        String serviceName = xService != null ? xService.getName() : null;
        String zoneName    = xSecurityZone != null ? xSecurityZone.getName() : null;

        vObj.setGuid(xObj.getGuid());
        vObj.setIsEnabled(xObj.getIsEnabled());
        vObj.setVersion(xObj.getVersion());
        vObj.setName(xObj.getName());
        vObj.setDescription(xObj.getDescription());
        vObj.setAdmins(JsonUtils.jsonToRangerPrincipalList(xObj.getAdmins()));
        vObj.setService(serviceName);
        vObj.setZone(zoneName);
        vObj.setConditionExpr(xObj.getConditionExpr());
        vObj.setDefaultAccessTypes(JsonUtils.jsonToSetString(xObj.getDefaultAccessTypes()));
        vObj.setDefaultMasks(JsonUtils.jsonToMapMaskInfo(xObj.getDefaultMasks()));
        vObj.setTermsOfUse(xObj.getTermsOfUse());
        vObj.setOptions(JsonUtils.jsonToMapStringString(xObj.getOptions()));
        vObj.setAdditionalInfo(JsonUtils.jsonToMapStringString(xObj.getAdditionalInfo()));

        return vObj;
    }

    public RangerDataShare getPopulatedViewObject(XXGdsDataShare xObj) {
        return this.populateViewBean(xObj);
    }

    public RangerDataShareList searchDataShares(SearchFilter filter) {
        LOG.debug("==> searchDataShares({})", filter);

        RangerDataShareList  ret      = new RangerDataShareList();
        List<XXGdsDataShare> datasets = super.searchResources(filter, searchFields, sortFields, ret);

        if (datasets != null) {
            for (XXGdsDataShare dataset : datasets) {
                ret.getList().add(getPopulatedViewObject(dataset));
            }
        }

        LOG.debug("<== searchDataShares({}): ret={}", filter, ret);

        return ret;
    }
}
