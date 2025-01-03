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

package org.apache.ranger.validation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.common.MessageEnums;
import org.apache.ranger.common.RESTErrorUtil;
import org.apache.ranger.common.RangerConstants;
import org.apache.ranger.plugin.errors.ValidationErrorCode;
import org.apache.ranger.plugin.model.RangerGds;
import org.apache.ranger.plugin.model.RangerGds.GdsPermission;
import org.apache.ranger.plugin.model.RangerGds.GdsShareStatus;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShare;
import org.apache.ranger.plugin.model.RangerGds.RangerDataShareInDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDataset;
import org.apache.ranger.plugin.model.RangerGds.RangerDatasetInProject;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsMaskInfo;
import org.apache.ranger.plugin.model.RangerGds.RangerGdsObjectACL;
import org.apache.ranger.plugin.model.RangerGds.RangerProject;
import org.apache.ranger.plugin.model.RangerGds.RangerSharedResource;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.validation.ValidationFailureDetails;
import org.apache.ranger.view.VXResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Component
public class RangerGdsValidator {
    public static final Integer GDS_ENTITIES_NAME_MAX_LENGTH = 512;
    private static final Logger LOG = LoggerFactory.getLogger(RangerGdsValidator.class);
    private final RangerGdsValidationDataProvider dataProvider;
    @Autowired
    RESTErrorUtil restErrorUtil;

    public RangerGdsValidator(RangerGdsValidationDataProvider dataProvider) {
        this.dataProvider = dataProvider;
    }

    public void validateCreate(RangerDataset dataset) {
        LOG.debug("==> validateCreate(dataset={})", dataset);

        ValidationResult result   = new ValidationResult();
        Long             existing = dataProvider.getDatasetId(dataset.getName());

        if (existing != null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_NAME_CONFLICT, "name", dataset.getName(), existing));
        }

        if (dataset.getName().length() > GDS_ENTITIES_NAME_MAX_LENGTH) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_DATASET_NAME_TOO_LONG, "name", dataset.getName()));
        }

        validateAcl(dataset.getAcl(), "acl", result);

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateCreate(dataset={})", dataset);
    }

    public void validateUpdate(RangerDataset dataset, RangerDataset existing) {
        LOG.debug("==> validateUpdate(dataset={}, existing={})", dataset, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_NAME_NOT_FOUND, "name", dataset.getName()));
        } else {
            validateDatasetAdmin(existing, result);
            validateAcl(dataset.getAcl(), "acl", result);

            boolean renamed = !StringUtils.equalsIgnoreCase(dataset.getName(), existing.getName());

            if (renamed) {
                Long existingDatasetNameId = dataProvider.getDatasetId(dataset.getName());

                if (existingDatasetNameId != null) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_NAME_CONFLICT, "name", dataset.getName(), existingDatasetNameId));
                }

                if (dataset.getName().length() > GDS_ENTITIES_NAME_MAX_LENGTH) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_DATASET_NAME_TOO_LONG, "name", dataset.getName()));
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateUpdate(dataset={}, existing={})", dataset, existing);
    }

    public void validateDelete(long datasetId, RangerDataset existing) {
        LOG.debug("==> validateDelete(datasetId={}, existing={})", datasetId, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_ID_NOT_FOUND, "id", datasetId));
        } else {
            validateDatasetAdmin(existing, result);
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateDelete(datasetId={}, existing={})", datasetId, existing);
    }

    public void validateCreate(RangerProject project) {
        LOG.debug("==> validateCreate(project={})", project);

        ValidationResult result   = new ValidationResult();
        Long             existing = dataProvider.getProjectId(project.getName());

        if (existing != null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_PROJECT_NAME_CONFLICT, "name", project.getName(), existing));
        }

        if (project.getName().length() > GDS_ENTITIES_NAME_MAX_LENGTH) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_PROJECT_NAME_TOO_LONG, "name", project.getName()));
        }

        validateAcl(project.getAcl(), "acl", result);

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateCreate(project={})", project);
    }

    public void validateUpdate(RangerProject project, RangerProject existing) {
        LOG.debug("==> validateUpdate(project={}, existing={})", project, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_PROJECT_NAME_NOT_FOUND, "name", project.getName()));
        } else {
            validateProjectAdmin(existing, result);
            validateAcl(project.getAcl(), "acl", result);

            boolean renamed = !StringUtils.equalsIgnoreCase(project.getName(), existing.getName());

            if (renamed) {
                Long existingProjectNameId = dataProvider.getProjectId(project.getName());

                if (existingProjectNameId != null) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_PROJECT_NAME_CONFLICT, "name", project.getName(), existingProjectNameId));
                }

                if (project.getName().length() > GDS_ENTITIES_NAME_MAX_LENGTH) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_PROJECT_NAME_TOO_LONG, "name", project.getName()));
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateUpdate(project={}, existing={})", project, existing);
    }

    public void validateDelete(long projectId, RangerProject existing) {
        LOG.debug("==> validateDelete(projectId={}, existing={})", projectId, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_PROJECT_ID_NOT_FOUND, "id", projectId));
        } else {
            validateProjectAdmin(existing, result);
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateDelete(projectId={}, existing={})", projectId, existing);
    }

    public void validateCreate(RangerDataShare dataShare) {
        LOG.debug("==> validateCreate(dataShare={})", dataShare);

        ValidationResult result   = new ValidationResult();
        Long             existing = dataProvider.getDataShareId(dataShare.getName());

        if (existing != null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_NAME_CONFLICT, "name", dataShare.getName(), existing));
        }

        if (dataShare.getName().length() > GDS_ENTITIES_NAME_MAX_LENGTH) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_DATASHARE_NAME_TOO_LONG, "name", dataShare.getName()));
        }

        validateServiceZoneAdmin(dataShare.getService(), dataShare.getZone(), result);

        validateAcl(dataShare.getAcl(), "acl", result);
        validateAccessTypes(dataShare.getService(), "defaultAccessTypes", dataShare.getDefaultAccessTypes(), result);
        validateMaskTypes(dataShare.getService(), "defaultTagMasks", dataShare.getDefaultTagMasks(), result);

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateCreate(dataShare={})", dataShare);
    }

    public void validateUpdate(RangerDataShare dataShare, RangerDataShare existing) {
        LOG.debug("==> validateUpdate(dataShare={}, existing={})", dataShare, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_NAME_NOT_FOUND, "name", dataShare.getName()));
        } else {
            validateDataShareAdmin(existing, result);
            validateAcl(dataShare.getAcl(), "acl", result);
            validateAccessTypes(dataShare.getService(), "defaultAccessTypes", dataShare.getDefaultAccessTypes(), result);
            validateMaskTypes(dataShare.getService(), "defaultTagMasks", dataShare.getDefaultTagMasks(), result);

            boolean renamed = !StringUtils.equalsIgnoreCase(dataShare.getName(), existing.getName());

            if (renamed) {
                Long existingDataShareNameId = dataProvider.getDataShareId(dataShare.getName());

                if (existingDataShareNameId != null) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_NAME_CONFLICT, "name", dataShare.getName(), existingDataShareNameId));
                }

                if (dataShare.getName().length() > GDS_ENTITIES_NAME_MAX_LENGTH) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_DATASHARE_NAME_TOO_LONG, "name", dataShare.getName()));
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateUpdate(dataShare={}, existing={})", dataShare, existing);
    }

    public void validateDelete(long dataShareId, RangerDataShare existing) {
        LOG.debug("==> validateDelete(dataShareId={}, existing={})", dataShareId, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_ID_NOT_FOUND, "id", dataShareId));
        } else {
            validateDataShareAdmin(existing, result);
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateDelete(dataShareId={}, existing={})", dataShareId, existing);
    }

    public void validateCreate(RangerSharedResource resource) {
        LOG.debug("==> validateCreate(resource={})", resource);

        ValidationResult result    = new ValidationResult();
        RangerDataShare  dataShare = dataProvider.getDataShare(resource.getDataShareId());

        if (dataShare == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_ID_NOT_FOUND, "dataShareId", resource.getDataShareId()));
        } else {
            Long existing = dataProvider.getSharedResourceId(resource.getDataShareId(), resource.getName());

            if (existing != null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_NAME_CONFLICT, "name", resource.getName(), dataShare.getName(), existing));
            } else if (MapUtils.isEmpty(resource.getResource())) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_RESOURCE_NULL, "resource", resource.getName()));
            } else {
                validateSharedResourceCreateAndUpdate(resource, dataShare, result);

                if (result.isSuccess()) {
                    existing = dataProvider.getSharedResourceId(resource.getDataShareId(), new RangerPolicyResourceSignature(resource));

                    if (existing != null) {
                        result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_CONFLICT, "resource", resource.getResource(), dataShare.getName()));
                    }
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateCreate(resource={})", resource);
    }

    public void validateUpdate(RangerSharedResource resource, RangerSharedResource existing) {
        LOG.debug("==> validateUpdate(resource={}, existing={})", resource, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_ID_NOT_FOUND, "id", resource.getId()));
        } else {
            RangerDataShare dataShare = dataProvider.getDataShare(resource.getDataShareId());

            if (dataShare == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_ID_NOT_FOUND, "dataShareId", resource.getDataShareId()));
            } else if (MapUtils.isEmpty(resource.getResource())) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_RESOURCE_NULL, "resource", resource.getName()));
            } else {
                validateSharedResourceCreateAndUpdate(resource, dataShare, result);

                if (result.isSuccess()) {
                    boolean renamed = !StringUtils.equalsIgnoreCase(resource.getName(), existing.getName());

                    if (renamed) {
                        Long existingSharedResourceNameId = dataProvider.getSharedResourceId(resource.getDataShareId(), resource.getName());

                        if (existingSharedResourceNameId != null) {
                            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_NAME_CONFLICT, "name", resource.getName(), dataShare.getName(), existing));
                        }
                    }

                    if (result.isSuccess()) {
                        Long existingSharedResourceNameId = dataProvider.getSharedResourceId(resource.getDataShareId(), new RangerPolicyResourceSignature(resource));

                        if (existingSharedResourceNameId != null && !existingSharedResourceNameId.equals(existing.getId())) {
                            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_CONFLICT, "resource", resource.getResource(), dataShare.getName()));
                        }
                    }
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateUpdate(resource={}, existing={})", resource, existing);
    }

    public void validateDelete(Long resourceId, RangerSharedResource existing) {
        LOG.debug("==> validateDelete(resourceId={}, existing={})", resourceId, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_ID_NOT_FOUND, "id", resourceId));
        } else {
            RangerDataShare dataShare = dataProvider.getDataShare(existing.getDataShareId());

            if (dataShare == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_ID_NOT_FOUND, "dataShareId", existing.getDataShareId()));
            } else {
                validateDataShareAdmin(dataShare, result);
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateDelete(resourceId={}, existing={})", resourceId, existing);
    }

    public void validateCreate(RangerDataShareInDataset dshid) {
        LOG.debug("==> validateCreate(dshid={})", dshid);

        ValidationResult result    = new ValidationResult();
        RangerDataShare  dataShare = dataProvider.getDataShare(dshid.getDataShareId());
        RangerDataset    dataset   = dataProvider.getDataset(dshid.getDatasetId());

        if (dataShare == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_ID_NOT_FOUND, "dataShareId", dshid.getDataShareId()));
        }

        if (dataset == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_ID_NOT_FOUND, "datasetId", dshid.getDatasetId()));
        }

        if (dataShare != null && dataset != null && !dataProvider.isAdminUser()) {
            switch (dshid.getStatus()) {
                case GRANTED:
                case DENIED:
                    validateAdmin(dataProvider.getCurrentUserLoginId(), "dataShare", dataShare.getName(), dataShare.getAcl(), result);
                    break;

                case ACTIVE:
                    validateAdmin(dataProvider.getCurrentUserLoginId(), "dataShare", dataShare.getName(), dataShare.getAcl(), result);
                    validateAdmin(dataProvider.getCurrentUserLoginId(), "dataset", dataset.getName(), dataset.getAcl(), result);
                    break;

                case REQUESTED:
                    validateAdmin(dataProvider.getCurrentUserLoginId(), "dataset", dataset.getName(), dataset.getAcl(), result);
                    break;

                case NONE:
                default:
                    break;
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateCreate(dshid={})", dshid);
    }

    public void validateUpdate(RangerDataShareInDataset dshid, RangerDataShareInDataset existing) {
        LOG.debug("==> validateUpdate(dshid={}, existing={})", dshid, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_IN_DATASET_ID_NOT_FOUND, "id", dshid.getId()));
        } else {
            RangerDataShare dataShare = dataProvider.getDataShare(existing.getDataShareId());
            RangerDataset   dataset   = dataProvider.getDataset(existing.getDatasetId());

            if (dataShare == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_ID_NOT_FOUND, "dataShareId", existing.getDataShareId()));
            }

            if (dataset == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_ID_NOT_FOUND, "datasetId", existing.getDatasetId()));
            }

            if (!Objects.equals(dshid.getDataShareId(), existing.getDataShareId())) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_UPDATE_IMMUTABLE_FIELD, "dataShareId"));

                dataShare = null;
            }

            if (!Objects.equals(dshid.getDatasetId(), existing.getDatasetId())) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_UPDATE_IMMUTABLE_FIELD, "datasetId"));

                dataset = null;
            }

            if (dataShare != null && dataset != null && !dataProvider.isAdminUser()) {
                if (!Objects.equals(existing.getStatus(), dshid.getStatus())) {
                    boolean requireDataShareAdmin = needsSharedObjectAdmin(existing.getStatus(), dshid.getStatus());
                    boolean requireDatasetAdmin   = needsReceivingObjectAdmin(existing.getStatus(), dshid.getStatus());

                    if (requireDataShareAdmin) {
                        if (!dataProvider.isServiceAdmin(dataShare.getService()) && !dataProvider.isZoneAdmin(dataShare.getZone())) {
                            validateAdmin(dataProvider.getCurrentUserLoginId(), "dataShare", dataShare.getName(), dataShare.getAcl(), result);
                        }
                    }

                    if (requireDatasetAdmin) {
                        validateAdmin(dataProvider.getCurrentUserLoginId(), "dataset", dataset.getName(), dataset.getAcl(), result);
                    }

                    if (!requireDataShareAdmin && !requireDatasetAdmin) { // must be either a dataShare admin or a dataset admin
                        String userName = dataProvider.getCurrentUserLoginId();
                        boolean isAllowed = isAdmin(userName, dataShare.getAcl()) || dataProvider.isServiceAdmin(dataShare.getService()) || dataProvider.isZoneAdmin(dataShare.getZone()) || isAdmin(userName, dataset.getAcl());

                        if (!isAllowed) {
                            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_INVALID_STATUS_CHANGE, "status", existing.getStatus(), dshid.getStatus()));
                        }
                    }
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateUpdate(dshid={}, existing={})", dshid, existing);
    }

    public void validateDelete(Long dshidId, RangerDataShareInDataset existing) {
        LOG.debug("==> validateDelete(dshidId={}, existing={})", dshidId, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_IN_DATASET_ID_NOT_FOUND, "id", dshidId));
        } else {
            RangerDataShare dataShare = dataProvider.getDataShare(existing.getDataShareId());
            RangerDataset   dataset   = dataProvider.getDataset(existing.getDatasetId());

            if (dataShare == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_ID_NOT_FOUND, "dataShareId", existing.getDataShareId()));
            }

            if (dataset == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_ID_NOT_FOUND, "datasetId", existing.getDatasetId()));
            }

            if (dataShare != null && dataset != null && !dataProvider.isAdminUser()) {  // must be either a dataset admin or a dataShare admin
                String userName = dataProvider.getCurrentUserLoginId();
                boolean isAllowed = isAdmin(userName, dataShare.getAcl()) || dataProvider.isServiceAdmin(dataShare.getService()) || dataProvider.isZoneAdmin(dataShare.getZone()) || isAdmin(userName, dataset.getAcl());

                if (!isAllowed) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_NOT_ADMIN, null, userName, "dataShareInDataset", "dataShare (name=" + dataShare.getName() + ") or dataset (name=" + dataset.getName() + ")"));
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateDelete(dshidId={}, existing={})", dshidId, existing);
    }

    public void validateCreate(RangerDatasetInProject dip) {
        LOG.debug("==> validateCreate(dip={})", dip);

        ValidationResult result  = new ValidationResult();
        RangerDataset    dataset = dataProvider.getDataset(dip.getDatasetId());
        RangerProject    project = dataProvider.getProject(dip.getProjectId());

        if (dataset == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_ID_NOT_FOUND, "datasetId", dip.getDatasetId()));
        }

        if (project == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_PROJECT_ID_NOT_FOUND, "projectId", dip.getProjectId()));
        }

        if (dataset != null && project != null && !dataProvider.isAdminUser()) {
            switch (dip.getStatus()) {
                case GRANTED:
                case DENIED:
                    validateAdmin(dataProvider.getCurrentUserLoginId(), "dataset", dataset.getName(), dataset.getAcl(), result);
                    break;

                case ACTIVE:
                    validateAdmin(dataProvider.getCurrentUserLoginId(), "dataset", dataset.getName(), dataset.getAcl(), result);
                    validateAdmin(dataProvider.getCurrentUserLoginId(), "project", project.getName(), project.getAcl(), result);
                    break;

                case NONE:
                case REQUESTED:
                default:
                    break;
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateCreate(dip={})", dip);
    }

    public void validateUpdate(RangerDatasetInProject dip, RangerDatasetInProject existing) {
        LOG.debug("==> validateUpdate(dip={}, existing={})", dip, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_IN_PROJECT_ID_NOT_FOUND, "id", dip.getId()));
        } else {
            RangerDataset dataset = dataProvider.getDataset(existing.getDatasetId());
            RangerProject project = dataProvider.getProject(existing.getProjectId());

            if (dataset == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_ID_NOT_FOUND, "datasetId", existing.getDatasetId()));
            }

            if (project == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_PROJECT_ID_NOT_FOUND, "projectId", existing.getProjectId()));
            }

            if (!Objects.equals(dip.getDatasetId(), existing.getDatasetId())) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_UPDATE_IMMUTABLE_FIELD, "datasetId"));

                dataset = null;
            }

            if (!Objects.equals(dip.getProjectId(), existing.getProjectId())) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_UPDATE_IMMUTABLE_FIELD, "projectId"));

                project = null;
            }

            if (dataset != null && project != null && !dataProvider.isAdminUser()) {
                if (!Objects.equals(existing.getStatus(), dip.getStatus())) {
                    boolean requireDatasetAdmin = needsSharedObjectAdmin(existing.getStatus(), dip.getStatus());
                    boolean requireProjectAdmin = needsReceivingObjectAdmin(existing.getStatus(), dip.getStatus());

                    if (requireDatasetAdmin) {
                        validateAdmin(dataProvider.getCurrentUserLoginId(), "dataset", dataset.getName(), dataset.getAcl(), result);
                    }

                    if (requireProjectAdmin) {
                        validateAdmin(dataProvider.getCurrentUserLoginId(), "project", project.getName(), project.getAcl(), result);
                    }

                    if (!requireDatasetAdmin && !requireProjectAdmin) { // must be either a dataset admin or a project admin
                        String  userName  = dataProvider.getCurrentUserLoginId();
                        boolean isAllowed = isAdmin(userName, dataset.getAcl()) || isAdmin(userName, project.getAcl());

                        if (!isAllowed) {
                            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_INVALID_STATUS_CHANGE, "status", existing.getStatus(), dip.getStatus()));
                        }
                    }
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateUpdate(dip={}, existing={})", dip, existing);
    }

    public void validateDelete(Long dipId, RangerDatasetInProject existing) {
        LOG.debug("==> validateDelete(dipId={}, existing={})", dipId, existing);

        ValidationResult result = new ValidationResult();

        if (existing == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_IN_PROJECT_ID_NOT_FOUND, "id", dipId));
        } else {
            RangerDataset dataset = dataProvider.getDataset(existing.getDatasetId());
            RangerProject project = dataProvider.getProject(existing.getProjectId());

            if (dataset == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATASET_ID_NOT_FOUND, "datasetId", existing.getDatasetId()));
            }

            if (project == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_PROJECT_ID_NOT_FOUND, "projectId", existing.getProjectId()));
            }

            if (dataset != null && project != null && !dataProvider.isAdminUser()) {
                String  userName  = dataProvider.getCurrentUserLoginId();
                boolean isAllowed = isAdmin(userName, dataset.getAcl()) || isAdmin(userName, project.getAcl());

                if (!isAllowed) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_NOT_ADMIN, null, userName, "datasetInProject", "dataset (name=" + dataset.getName() + ") or project (name=" + project.getName() + ")"));
                }
            }
        }

        if (!result.isSuccess()) {
            result.throwRESTException();
        }

        LOG.debug("<== validateDelete(dipId={}, existing={})", dipId, existing);
    }

    public void validateCreateOrUpdate(RangerPolicy policy) {
        LOG.debug("==> validateCreateOrUpdate(policy={})", policy);
        if (policy == null || CollectionUtils.isEmpty(policy.getPolicyItems())) {
            return;
        }

        ValidationResult       result      = new ValidationResult();
        List<RangerPolicyItem> policyItems = policy.getPolicyItems();

        validatePolicyItems(policyItems, result);

        if (!result.isSuccess()) {
            result.throwRESTException();
        }
        LOG.debug("<== validateCreateOrUpdate(policy={})", policy);
    }

    public GdsPermission getGdsPermissionForUser(RangerGds.RangerGdsObjectACL acl, String user) {
        if (dataProvider.isAdminUser()) {
            return GdsPermission.ADMIN;
        }

        GdsPermission permission = GdsPermission.NONE;

        if (acl.getUsers() != null) {
            permission = getHigherPrivilegePermission(permission, acl.getUsers().get(user));
        }

        if (acl.getGroups() != null) {
            permission = getHigherPrivilegePermission(permission, acl.getGroups().get(RangerConstants.GROUP_PUBLIC));

            Set<String> groups = dataProvider.getGroupsForUser(user);

            if (CollectionUtils.isNotEmpty(groups)) {
                for (String group : groups) {
                    permission = getHigherPrivilegePermission(permission, acl.getGroups().get(group));
                }
            }
        }

        if (acl.getRoles() != null) {
            Set<String> roles = dataProvider.getRolesForUser(user);

            if (CollectionUtils.isNotEmpty(roles)) {
                for (String role : roles) {
                    permission = getHigherPrivilegePermission(permission, acl.getRoles().get(role));
                }
            }
        }

        return permission;
    }

    public boolean hasPermission(RangerGdsObjectACL acl, GdsPermission permission) {
        boolean ret = dataProvider.isAdminUser();

        if (!ret && acl != null) {
            String userName = dataProvider.getCurrentUserLoginId();

            if (acl.getUsers() != null) {
                ret = isAllowed(acl.getUsers().get(userName), permission);
            }

            if (!ret && acl.getGroups() != null) {
                ret = isAllowed(acl.getGroups().get(RangerConstants.GROUP_PUBLIC), permission);

                if (!ret) {
                    Set<String> userGroups = dataProvider.getGroupsForUser(userName);

                    for (String userGroup : userGroups) {
                        ret = isAllowed(acl.getGroups().get(userGroup), permission);

                        if (ret) {
                            break;
                        }
                    }
                }
            }

            if (!ret && acl.getRoles() != null) {
                Set<String> userRoles = dataProvider.getRolesForUser(userName);

                if (userRoles != null) {
                    for (String userRole : userRoles) {
                        ret = isAllowed(acl.getRoles().get(userRole), permission);

                        if (ret) {
                            break;
                        }
                    }
                }
            }
        }

        return ret;
    }

    public boolean needApproverUpdate(GdsShareStatus existing, GdsShareStatus updated) {
        boolean ret = !existing.equals(updated);

        if (ret) {
            switch (updated) {
                case DENIED:
                case GRANTED:
                    break;

                case ACTIVE:
                    if (!existing.equals(GdsShareStatus.NONE) && !existing.equals(GdsShareStatus.REQUESTED)) {
                        ret = false;
                    }
                    break;

                case NONE:
                case REQUESTED:
                    ret = false;
                    break;
            }
        }

        return ret;
    }

    private void validateDatasetAdmin(RangerDataset dataset, ValidationResult result) {
        if (!dataProvider.isAdminUser()) {
            validateAdmin(dataProvider.getCurrentUserLoginId(), "dataset", dataset.getName(), dataset.getAcl(), result);
        }
    }

    private void validateProjectAdmin(RangerProject project, ValidationResult result) {
        if (!dataProvider.isAdminUser()) {
            validateAdmin(dataProvider.getCurrentUserLoginId(), "project", project.getName(), project.getAcl(), result);
        }
    }

    private void validateDataShareAdmin(RangerDataShare dataShare, ValidationResult result) {
        if (!dataProvider.isAdminUser() && !dataProvider.isServiceAdmin(dataShare.getService()) && !dataProvider.isZoneAdmin(dataShare.getZone())) {
            validateAdmin(dataProvider.getCurrentUserLoginId(), "datashare", dataShare.getName(), dataShare.getAcl(), result);
        }
    }

    private void validateSharedResourceCreateAndUpdate(RangerSharedResource resource, RangerDataShare dataShare, ValidationResult result) {
        if (!dataProvider.isAdminUser()) {
            validateAdmin(dataProvider.getCurrentUserLoginId(), "datashare", dataShare.getName(), dataShare.getAcl(), result);

            if (!dataProvider.isServiceAdmin(dataShare.getService()) && !dataProvider.isZoneAdmin(dataShare.getZone())) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_NOT_SERVICE_OR_ZONE_ADMIN, null, dataShare.getService(), dataShare.getZone()));
            }
        }
        validatePolicyResourceValuesNotEmpty(resource.getResource(), result);
    }

    private void validatePolicyResourceValuesNotEmpty(Map<String, RangerPolicyResource> resourceMap, ValidationResult result) {
        for (String resourceName : resourceMap.keySet()) {
            List<String> resourceValues = resourceMap.get(resourceName).getValues();
            if (CollectionUtils.isEmpty(resourceValues)) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_MISSING_VALUE, null, resourceName));
            } else {
                for (String value : resourceValues) {
                    if (StringUtils.isEmpty(value) || StringUtils.isBlank(value)) {
                        result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SHARED_RESOURCE_MISSING_VALUE, null, resourceName));
                        break;
                    }
                }
            }
        }
    }

    private void validatePolicyItems(List<RangerPolicyItem> policyItems, ValidationResult result) {
        if (CollectionUtils.isEmpty(policyItems)) {
            return;
        }

        for (RangerPolicyItem policyItem : policyItems) {
            if (policyItem == null) {
                addValidationFailure(result, ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM);
                continue;
            }

            boolean hasNoPrincipals  = CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups()) && CollectionUtils.isEmpty(policyItem.getRoles());
            boolean hasInvalidUsers  = policyItem.getUsers() != null && policyItem.getUsers().stream().anyMatch(StringUtils::isBlank);
            boolean hasInvalidGroups = policyItem.getGroups() != null && policyItem.getGroups().stream().anyMatch(StringUtils::isBlank);
            boolean hasInvalidRoles  = policyItem.getRoles() != null && policyItem.getRoles().stream().anyMatch(StringUtils::isBlank);

            if (hasNoPrincipals || hasInvalidUsers || hasInvalidGroups || hasInvalidRoles) {
                addValidationFailure(result, ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_USER_AND_GROUPS);
            }

            if (CollectionUtils.isEmpty(policyItem.getAccesses()) || policyItem.getAccesses().contains(null)) {
                addValidationFailure(result, ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM_ACCESS);
                continue;
            }

            boolean hasInvalidAccesses = policyItem.getAccesses().stream().anyMatch(itemAccess -> StringUtils.isBlank(itemAccess.getType()));

            if (hasInvalidAccesses) {
                addValidationFailure(result, ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM_ACCESS_TYPE);
            }
        }
    }

    private void addValidationFailure(ValidationResult result, ValidationErrorCode errorCode) {
        result.addValidationFailure(new ValidationFailureDetails(errorCode, "policy items"));
    }

    private void validateAcl(RangerGdsObjectACL acl, String fieldName, ValidationResult result) {
        if (acl != null) {
            if (MapUtils.isNotEmpty(acl.getUsers())) {
                for (String userName : acl.getUsers().keySet()) {
                    validateUser(userName, fieldName, result);
                }
            }

            if (MapUtils.isNotEmpty(acl.getGroups())) {
                for (String groupName : acl.getGroups().keySet()) {
                    validateGroup(groupName, fieldName, result);
                }
            }

            if (MapUtils.isNotEmpty(acl.getRoles())) {
                for (String roleName : acl.getRoles().keySet()) {
                    validateRole(roleName, fieldName, result);
                }
            }
        }
    }

    private void validateUser(String userName, String fieldName, ValidationResult result) {
        Long userId = dataProvider.getUserId(userName);

        if (userId == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_NON_EXISTING_USER, fieldName, userName));
        }
    }

    private void validateGroup(String groupName, String fieldName, ValidationResult result) {
        Long groupId = dataProvider.getGroupId(groupName);

        if (groupId == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_NON_EXISTING_GROUP, fieldName, groupName));
        }
    }

    private void validateRole(String roleName, String fieldName, ValidationResult result) {
        Long roleId = dataProvider.getRoleId(roleName);

        if (roleId == null) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_NON_EXISTING_ROLE, fieldName, roleName));
        }
    }

    private void validateAdmin(String userName, String objType, String objName, RangerGdsObjectACL acl, ValidationResult result) {
        boolean isAdmin = isAdmin(userName, acl);

        if (!isAdmin) {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_NOT_ADMIN, null, userName, objType, objName));
        }
    }

    private void validateServiceZoneAdmin(String serviceName, String zoneName, ValidationResult result) {
        if (StringUtils.isNotBlank(serviceName)) {
            Long serviceId = dataProvider.getServiceId(serviceName);

            if (serviceId == null) {
                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_NON_EXISTING_SERVICE, null, serviceName));
            } else {
                boolean isServiceAdmin = dataProvider.isAdminUser() || dataProvider.isServiceAdmin(serviceName);

                if (!isServiceAdmin) {
                    if (StringUtils.isNotBlank(zoneName)) {
                        Long zoneId = dataProvider.getZoneId(zoneName);

                        if (zoneId == null) {
                            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_NON_EXISTING_ZONE, null, zoneName));
                        } else {
                            boolean isZoneAdmin = dataProvider.isZoneAdmin(zoneName);

                            if (!isZoneAdmin) {
                                result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_NOT_SERVICE_OR_ZONE_ADMIN, "serviceName", serviceName, zoneName));
                            }
                        }
                    } else {
                        result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_DATA_SHARE_NOT_SERVICE_ADMIN, "serviceName", serviceName));
                    }
                }
            }
        } else {
            result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_SERVICE_NAME_MISSING, null));
        }
    }

    private void validateAccessTypes(String serviceName, String fieldName, Set<String> accessTypes, ValidationResult result) {
        if (accessTypes != null && !accessTypes.isEmpty()) {
            Set<String> validAccessTypes = dataProvider.getAccessTypes(serviceName);

            for (String accessType : accessTypes) {
                if (!validAccessTypes.contains(accessType)) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_INVALID_ACCESS_TYPE, fieldName, accessType));
                }
            }
        }
    }

    private void validateMaskTypes(String serviceName, String fieldName, List<RangerGdsMaskInfo> maskTypes, ValidationResult result) {
        if (maskTypes != null && !maskTypes.isEmpty()) {
            Set<String> validMaskTypes = dataProvider.getMaskTypes(serviceName);

            for (RangerGdsMaskInfo maskType : maskTypes) {
                RangerPolicyItemDataMaskInfo maskInfo = maskType.getMaskInfo();
                if (!validMaskTypes.contains(maskInfo.getDataMaskType())) {
                    result.addValidationFailure(new ValidationFailureDetails(ValidationErrorCode.GDS_VALIDATION_ERR_INVALID_MASK_TYPE, fieldName, maskInfo.getDataMaskType()));
                }
            }
        }
    }

    private boolean isAdmin(String userName, RangerGdsObjectACL acl) {
        boolean ret = false;

        if (acl != null) {
            if (MapUtils.isNotEmpty(acl.getUsers())) {
                ret = isAllowed(acl.getUsers().get(userName), GdsPermission.ADMIN);
            }

            if (!ret && MapUtils.isNotEmpty(acl.getGroups())) {
                ret = isAllowed(acl.getGroups().get(RangerConstants.GROUP_PUBLIC), GdsPermission.ADMIN);

                if (!ret) {
                    Set<String> userGroups = dataProvider.getGroupsForUser(userName);

                    if (userGroups != null) {
                        for (String userGroup : userGroups) {
                            ret = isAllowed(acl.getGroups().get(userGroup), GdsPermission.ADMIN);

                            if (ret) {
                                break;
                            }
                        }
                    }
                }
            }

            if (!ret && MapUtils.isNotEmpty(acl.getRoles())) {
                Set<String> userRoles = dataProvider.getRolesForUser(userName);

                if (userRoles != null) {
                    for (String userRole : userRoles) {
                        ret = isAllowed(acl.getRoles().get(userRole), GdsPermission.ADMIN);

                        if (ret) {
                            break;
                        }
                    }
                }
            }
        }

        return ret;
    }

    private boolean isAllowed(GdsPermission hasPermission, GdsPermission accessPermission) {
        final boolean ret;

        switch (accessPermission) {
            case ADMIN:
                ret = hasPermission == GdsPermission.ADMIN;
                break;

            case POLICY_ADMIN:
                ret = hasPermission == GdsPermission.POLICY_ADMIN || hasPermission == GdsPermission.ADMIN;
                break;

            case AUDIT:
                ret = hasPermission == GdsPermission.AUDIT || hasPermission == GdsPermission.POLICY_ADMIN || hasPermission == GdsPermission.ADMIN;
                break;

            case VIEW:
                ret = hasPermission == GdsPermission.VIEW || hasPermission == GdsPermission.AUDIT || hasPermission == GdsPermission.POLICY_ADMIN || hasPermission == GdsPermission.ADMIN;
                break;

            case LIST:
                ret = hasPermission == GdsPermission.LIST || hasPermission == GdsPermission.VIEW || hasPermission == GdsPermission.AUDIT || hasPermission == GdsPermission.POLICY_ADMIN || hasPermission == GdsPermission.ADMIN;
                break;

            case NONE:
                ret = false;
                break;

            default:
                ret = false;
                break;
        }

        return ret;
    }

    private GdsPermission getHigherPrivilegePermission(GdsPermission permission1, GdsPermission permission2) {
        GdsPermission ret = permission1;

        if (permission2 != null) {
            ret = permission1.compareTo(permission2) > 0 ? permission1 : permission2;
        }

        return ret;
    }

    // Shared object:
    //   DataShareInDataset => dataShare
    //   DatasetInProject   => dataset
    private static boolean needsSharedObjectAdmin(GdsShareStatus existing, GdsShareStatus updated) {
        boolean ret = false;

        if (!Objects.equals(existing, updated)) {
            switch (existing) {
                case NONE:
                case REQUESTED:
                    ret = (updated == GdsShareStatus.GRANTED) || (updated == GdsShareStatus.DENIED) || (updated == GdsShareStatus.ACTIVE); // implicit approval
                    break;

                case GRANTED:
                    ret = (updated == GdsShareStatus.DENIED);
                    break;

                case DENIED:
                    ret = (updated == GdsShareStatus.GRANTED) || (updated == GdsShareStatus.ACTIVE); // implicit approval
                    break;

                case ACTIVE:
                    ret = (updated == GdsShareStatus.GRANTED) || (updated == GdsShareStatus.DENIED);
                    break;
            }
        }

        return ret;
    }

    // Receiving object:
    //   DataShareInDataset => dataset
    //   DatasetInProject   => project
    private static boolean needsReceivingObjectAdmin(GdsShareStatus existing, GdsShareStatus updated) {
        boolean ret = false;

        if (!Objects.equals(existing, updated)) {
            switch (existing) {
                case NONE:
                    ret = (updated == GdsShareStatus.REQUESTED) || (updated == GdsShareStatus.ACTIVE);
                    break;

                case REQUESTED:
                    ret = (updated == GdsShareStatus.NONE) || (updated == GdsShareStatus.ACTIVE);
                    break;

                case GRANTED:
                case DENIED:
                    ret = (updated == GdsShareStatus.NONE) || (updated == GdsShareStatus.REQUESTED) || (updated == GdsShareStatus.ACTIVE);
                    break;

                case ACTIVE:
                    ret = (updated == GdsShareStatus.NONE) || (updated == GdsShareStatus.REQUESTED);
                    break;
            }
        }

        return ret;
    }

    public class ValidationResult {
        private final List<ValidationFailureDetails> validationFailures = new ArrayList<>();

        private ValidationResult() {
        }

        public boolean isSuccess() {
            return validationFailures.isEmpty();
        }

        public void addValidationFailure(ValidationFailureDetails validationFailure) {
            validationFailures.add(validationFailure);
        }

        public List<ValidationFailureDetails> getValidationFailures() {
            return validationFailures;
        }

        public void throwRESTException() {
            throw restErrorUtil.createRESTException(validationFailures.toString(), MessageEnums.INVALID_INPUT_DATA);
        }

        public void throwREST403Exception() {
            VXResponse gjResponse = new VXResponse();

            gjResponse.setMsgDesc(validationFailures.toString());

            throw restErrorUtil.create403RESTException(gjResponse);
        }
    }
}
