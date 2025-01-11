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

import org.apache.ranger.common.AppConstants;
import org.apache.ranger.common.DateUtil;
import org.apache.ranger.common.JSONUtil;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.db.RangerDaoManager;
import org.apache.ranger.entity.XXDataHist;
import org.apache.ranger.plugin.model.RangerBaseModelObject;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@Scope("singleton")
public class RangerDataHistService {
    public static final String ACTION_CREATE = "Create";
    public static final String ACTION_UPDATE = "Update";
    public static final String ACTION_DELETE = "Delete";

    @Autowired
    RESTErrorUtil restErrorUtil;

    @Autowired
    RangerDaoManager daoMgr;

    @Autowired
    JSONUtil jsonUtil;

    public void createObjectDataHistory(RangerBaseModelObject baseModelObj, String action) {
        if (baseModelObj == null || action == null) {
            throw restErrorUtil.createRESTException("Error while creating DataHistory. " + "Object or Action can not be null.", MessageEnums.DATA_NOT_FOUND);
        }

        Integer    classType   = null;
        String     objectName  = null;
        String     content     = null;
        Long       objectId    = baseModelObj.getId();
        String     objectGuid  = baseModelObj.getGuid();
        Date       currentDate = DateUtil.getUTCDate();
        XXDataHist xDataHist   = new XXDataHist();

        xDataHist.setObjectId(baseModelObj.getId());
        xDataHist.setObjectGuid(objectGuid);
        xDataHist.setCreateTime(currentDate);
        xDataHist.setAction(action);
        xDataHist.setVersion(baseModelObj.getVersion());
        xDataHist.setUpdateTime(currentDate);
        xDataHist.setFromTime(currentDate);

        if (baseModelObj instanceof RangerServiceDef) {
            RangerServiceDef serviceDef = (RangerServiceDef) baseModelObj;

            objectName = serviceDef.getName();
            classType  = AppConstants.CLASS_TYPE_XA_SERVICE_DEF;
            content    = jsonUtil.writeObjectAsString(serviceDef);
        } else if (baseModelObj instanceof RangerService) {
            RangerService service = (RangerService) baseModelObj;

            objectName = service.getName();
            classType  = AppConstants.CLASS_TYPE_XA_SERVICE;
            content    = jsonUtil.writeObjectAsString(service);
        } else if (baseModelObj instanceof RangerPolicy) {
            RangerPolicy policy = (RangerPolicy) baseModelObj;

            objectName = policy.getName();
            classType  = AppConstants.CLASS_TYPE_RANGER_POLICY;

            policy.setServiceType(policy.getServiceType());

            content = jsonUtil.writeObjectAsString(policy);
        } else if (baseModelObj instanceof RangerDataset) {
            RangerDataset dataset = (RangerDataset) baseModelObj;

            objectName = dataset.getName();
            classType  = AppConstants.CLASS_TYPE_GDS_DATASET;
            content    = jsonUtil.writeObjectAsString(dataset);
        } else if (baseModelObj instanceof RangerProject) {
            RangerProject project = (RangerProject) baseModelObj;

            objectName = project.getName();
            classType  = AppConstants.CLASS_TYPE_GDS_PROJECT;
            content    = jsonUtil.writeObjectAsString(project);
        } else if (baseModelObj instanceof RangerDataShare) {
            RangerDataShare dataShare = (RangerDataShare) baseModelObj;

            objectName = dataShare.getName();
            classType  = AppConstants.CLASS_TYPE_GDS_DATA_SHARE;
            content    = jsonUtil.writeObjectAsString(dataShare);
        } else if (baseModelObj instanceof RangerSharedResource) {
            RangerSharedResource sharedResource = (RangerSharedResource) baseModelObj;

            objectName = sharedResource.getName();
            classType  = AppConstants.CLASS_TYPE_GDS_SHARED_RESOURCE;
            content    = jsonUtil.writeObjectAsString(sharedResource);
        } else if (baseModelObj instanceof RangerDataShareInDataset) {
            RangerDataShareInDataset dataShareInDataset = (RangerDataShareInDataset) baseModelObj;

            objectName = dataShareInDataset.getGuid();
            classType  = AppConstants.CLASS_TYPE_GDS_DATA_SHARE_IN_DATASET;
            content    = jsonUtil.writeObjectAsString(dataShareInDataset);
        } else if (baseModelObj instanceof RangerDatasetInProject) {
            RangerDatasetInProject datasetInProject = (RangerDatasetInProject) baseModelObj;

            objectName = datasetInProject.getGuid();
            classType  = AppConstants.CLASS_TYPE_GDS_DATASET_IN_PROJECT;
            content    = jsonUtil.writeObjectAsString(datasetInProject);
        }

        xDataHist.setObjectClassType(classType);
        xDataHist.setObjectName(objectName);
        xDataHist.setContent(content);

        daoMgr.getXXDataHist().create(xDataHist);

        if (ACTION_UPDATE.equalsIgnoreCase(action) || ACTION_DELETE.equalsIgnoreCase(action)) {
            XXDataHist prevHist = daoMgr.getXXDataHist().findLatestByObjectClassTypeAndObjectId(classType, objectId);

            if (prevHist == null) {
                throw restErrorUtil.createRESTException("Error updating DataHistory Object. ObjectName: " + objectName, MessageEnums.DATA_NOT_UPDATABLE);
            }

            prevHist.setUpdateTime(currentDate);
            prevHist.setToTime(currentDate);
            prevHist.setObjectName(objectName);

            daoMgr.getXXDataHist().update(prevHist);
        }
    }
}
