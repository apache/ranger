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
import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.GUIDUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.SearchField;
import org.apache.ranger.common.SortField;
import org.apache.ranger.common.view.VTrxLogAttr;
import org.apache.ranger.entity.*;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.view.RangerGdsVList.RangerDatasetList;
import org.apache.ranger.view.VXMessage;
import org.apache.ranger.view.VXResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Scope("singleton")
public class RangerGdsDatasetService extends RangerGdsBaseModelService<XXGdsDataset, RangerDataset> {
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsDatasetService.class);

    @Autowired
    GUIDUtil guidUtil;

    public RangerGdsDatasetService() {
        super(AppConstants.CLASS_TYPE_GDS_DATASET);

        searchFields.add(new SearchField(SearchFilter.DATASET_ID,              "obj.id",            SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME,            "obj.name",          SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL));
        searchFields.add(new SearchField(SearchFilter.DATASET_NAME_PARTIAL,    "obj.name",          SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.PARTIAL));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_ID,           "dshid.dataShareId", SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL,    "XXGdsDataShareInDataset dshid",                                       "obj.id = dshid.datasetId"));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_NAME,         "dsh.name",          SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL,    "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh",                   "obj.id = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.DATA_SHARE_NAME_PARTIAL, "dsh.name",          SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh",                   "obj.id = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_ID,              "dsh.serviceId",     SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL,    "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh",                   "obj.id = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME,            "s.name",            SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL,     "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXService s",      "obj.id = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.serviceId = s.id"));
        searchFields.add(new SearchField(SearchFilter.SERVICE_NAME_PARTIAL,    "s.name",            SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL,  "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXService s",      "obj.id = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.serviceId = s.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_ID,                 "dsh.zoneId",        SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL,    "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh",                   "obj.id = dshid.datasetId and dshid.dataShareId = dsh.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_NAME,               "z.name",            SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.FULL,     "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXSecurityZone z", "obj.id = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.zoneId = z.id"));
        searchFields.add(new SearchField(SearchFilter.ZONE_NAME_PARTIAL,       "z.name",            SearchField.DATA_TYPE.STRING, SearchField.SEARCH_TYPE.PARTIAL,  "XXGdsDataShareInDataset dshid, XXGdsDataShare dsh, XXSecurityZone z", "obj.id = dshid.datasetId and dshid.dataShareId = dsh.id and dsh.zoneId = z.id"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_ID,              "dip.projectId",     SearchField.DATA_TYPE.INTEGER, SearchField.SEARCH_TYPE.FULL,    "XXGdsDatasetInProject dip",                    "obj.id = dip.datasetId"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME,            "proj.name",         SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.FULL,    "XXGdsDatasetInProject dip, XXGdsProject proj", "obj.id = dip.datasetId and dip.projectId = proj.id"));
        searchFields.add(new SearchField(SearchFilter.PROJECT_NAME_PARTIAL,    "proj.name",         SearchField.DATA_TYPE.STRING,  SearchField.SEARCH_TYPE.PARTIAL, "XXGdsDatasetInProject dip, XXGdsProject proj", "obj.id = dip.datasetId and dip.projectId = proj.id"));
        searchFields.add(new SearchField(SearchFilter.CREATED_BY,              "obj.addedByUserId", SearchField.DATA_TYPE.INTEGER,  SearchField.SEARCH_TYPE.FULL));

        sortFields.add(new SortField(SearchFilter.CREATE_TIME,  "obj.createTime"));
        sortFields.add(new SortField(SearchFilter.UPDATE_TIME,  "obj.updateTime"));
        sortFields.add(new SortField(SearchFilter.DATASET_ID,   "obj.id", true, SortField.SORT_ORDER.ASC));
        sortFields.add(new SortField(SearchFilter.DATASET_NAME, "obj.name"));

        trxLogAttrs.put("name",       new VTrxLogAttr("name", "Name", false, true));
        trxLogAttrs.put("acl",        new VTrxLogAttr("acl", "ACL"));
        trxLogAttrs.put("termsOfUse", new VTrxLogAttr("termsOfUse", "Terms of use"));
    }

    @Override
    public RangerDataset postCreate(XXGdsDataset xObj) {
        RangerDataset ret = super.postCreate(xObj);

        // TODO:

        return ret;
    }

    @Override
    public RangerDataset postUpdate(XXGdsDataset xObj) {
        RangerDataset ret = super.postUpdate(xObj);

        // TODO:

        return ret;
    }

    @Override
    public XXGdsDataset preDelete(Long id) {
        // Update ServiceVersionInfo for each service in the zone
        XXGdsDataset ret = super.preDelete(id);

        // TODO:

        return ret;
    }

    @Override
    protected void validateForCreate(RangerDataset vObj) {
        List<VXMessage> msgList = null;

        if (StringUtils.isBlank(vObj.getName())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "name"));
        }

        if (CollectionUtils.isNotEmpty(msgList)) {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
            gjResponse.setMsgDesc("Validation failure");
            gjResponse.setMessageList(msgList);

            LOG.debug("Validation failure in createDataset({}): error={}", vObj, gjResponse);

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
    protected void validateForUpdate(RangerDataset vObj, XXGdsDataset xObj) {
        List<VXMessage> msgList = null;

        if (StringUtils.isBlank(vObj.getName())) {
            msgList = getOrCreateMessageList(msgList);

            msgList.add(MessageEnums.NO_INPUT_DATA.getMessage(null, "name"));
        }

        if (CollectionUtils.isNotEmpty(msgList)) {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setStatusCode(VXResponse.STATUS_ERROR);
            gjResponse.setMsgDesc("Validation failure");
            gjResponse.setMessageList(msgList);

            LOG.debug("Validation failure in updateDataset({}): error={}", vObj, gjResponse);

            throw restErrorUtil.createRESTException(gjResponse);
        }

        if (vObj.getIsEnabled() == null) {
            vObj.setIsEnabled(Boolean.TRUE);
        }
    }

    @Override
    protected XXGdsDataset mapViewToEntityBean(RangerDataset vObj, XXGdsDataset xObj, int OPERATION_CONTEXT) {
        xObj.setGuid(vObj.getGuid());
        xObj.setIsEnabled(vObj.getIsEnabled());
        xObj.setName(vObj.getName());
        xObj.setDescription(vObj.getDescription());
        xObj.setAcl(JsonUtils.objectToJson(vObj.getAcl()));
        xObj.setTermsOfUse(vObj.getTermsOfUse());
        xObj.setOptions(JsonUtils.mapToJson(vObj.getOptions()));
        xObj.setAdditionalInfo(JsonUtils.mapToJson(vObj.getAdditionalInfo()));

        return xObj;
    }

    @Override
    protected RangerDataset mapEntityToViewBean(RangerDataset vObj, XXGdsDataset xObj) {
        vObj.setGuid(xObj.getGuid());
        vObj.setIsEnabled(xObj.getIsEnabled());
        vObj.setVersion(xObj.getVersion());
        vObj.setName(xObj.getName());
        vObj.setDescription(xObj.getDescription());
        vObj.setAcl(JsonUtils.jsonToObject(xObj.getAcl(), RangerGds.RangerGdsObjectACL.class));
        vObj.setTermsOfUse(xObj.getTermsOfUse());
        vObj.setOptions(JsonUtils.jsonToMapStringString(xObj.getOptions()));
        vObj.setAdditionalInfo(JsonUtils.jsonToMapStringString(xObj.getAdditionalInfo()));

        return vObj;
    }

    public RangerDataset getPopulatedViewObject(XXGdsDataset xObj) {
        return this.populateViewBean(xObj);
    }

    public RangerDatasetList searchDatasets(SearchFilter filter) {
        LOG.debug("==> searchDatasets({})", filter);

        RangerDatasetList  ret      = new RangerDatasetList();
        List<XXGdsDataset> datasets = super.searchResources(filter, searchFields, sortFields, ret);

        if (datasets != null) {
            for (XXGdsDataset dataset : datasets) {
                ret.getList().add(getPopulatedViewObject(dataset));
            }
        }

        LOG.debug("<== searchDatasets({}): ret={}", filter, ret);

        return ret;
    }
}
