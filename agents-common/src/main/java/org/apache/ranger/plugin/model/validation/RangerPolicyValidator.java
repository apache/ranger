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

package org.apache.ranger.plugin.model.validation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ranger.plugin.errors.ValidationErrorCode;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerConstraintPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerDataMaskPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemAccess;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemConstraintInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyItemDataMaskInfo;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicy.RangerRowFilterPolicyItem;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerConstraintDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerConstraintParamDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerValiditySchedule;
import org.apache.ranger.plugin.store.ServiceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerPolicyValidator extends RangerValidator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerPolicyValidator.class);

    private static final Set<String> INVALID_POLICY_ITEM_VALUES = new HashSet<>(Arrays.asList("null", "NULL", "Null", null, ""));

    public RangerPolicyValidator(ServiceStore store) {
        super(store);
    }

    public void validate(RangerPolicy policy, Action action, boolean isAdmin) throws Exception {
        LOG.debug("==> RangerPolicyValidator.validate({}, {}, {})", policy, action, isAdmin);

        List<ValidationFailureDetails> failures = new ArrayList<>();
        boolean                        valid    = isValid(policy, action, isAdmin, failures);
        String                         message  = "";

        try {
            if (!valid) {
                message = serializeFailures(failures);

                throw new Exception(message);
            }
        } finally {
            LOG.debug("<== RangerPolicyValidator.validate({}, {}, {}): {}, reason[{}]", policy, action, isAdmin, valid, message);
        }
    }

    @Override
    boolean isValid(Long id, Action action, List<ValidationFailureDetails> failures) {
        LOG.debug("==> RangerPolicyValidator.isValid({}, {}, {})", id, action, failures);

        boolean valid = true;

        if (action != Action.DELETE) {
            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_UNSUPPORTED_ACTION;

            failures.add(new ValidationFailureDetailsBuilder()
                    .isAnInternalError()
                    .becauseOf(error.getMessage())
                    .errorCode(error.getErrorCode())
                    .build());

            valid = false;
        } else if (id == null) {
            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_FIELD;

            failures.add(new ValidationFailureDetailsBuilder()
                    .becauseOf("policy id was null/missing")
                    .field("id")
                    .isMissing()
                    .errorCode(error.getErrorCode())
                    .becauseOf(error.getMessage("id"))
                    .build());

            valid = false;
        } else if (policyExists(id)) {
            LOG.debug("No policy found for id[{}]! ok!", id);
        }

        LOG.debug("<== RangerPolicyValidator.isValid({}, {}, {}): {}", id, action, failures, valid);

        return valid;
    }

    boolean isValid(RangerPolicy policy, Action action, boolean isAdmin, List<ValidationFailureDetails> failures) {
        LOG.debug("==> RangerPolicyValidator.isValid({}, {}, {}, {})", policy, action, isAdmin, failures);

        if (!(action == Action.CREATE || action == Action.UPDATE)) {
            throw new IllegalArgumentException("isValid(RangerPolicy, ...) is only supported for create/update");
        }

        boolean valid = true;

        if (policy == null) {
            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_OBJECT;

            failures.add(new ValidationFailureDetailsBuilder()
                    .field("policy")
                    .isMissing()
                    .becauseOf(error.getMessage())
                    .errorCode(error.getErrorCode())
                    .build());

            valid = false;
        } else {
            Integer priority = policy.getPolicyPriority();

            if (priority != null) {
                if (priority < RangerPolicy.POLICY_PRIORITY_NORMAL || priority > RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_INVALID_PRIORITY;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policyPriority")
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage("out of range"))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }
            }

            Long         id             = policy.getId();
            RangerPolicy existingPolicy = null;

            if (action == Action.UPDATE) { // id is ignored for CREATE
                if (id == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_FIELD;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("id")
                            .isMissing()
                            .becauseOf(error.getMessage("id"))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }

                existingPolicy = getPolicy(id);

                if (existingPolicy == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_POLICY_ID;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("id")
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(id))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }
            }

            String policyName        = policy.getName();
            String serviceName       = policy.getService();
            String policyServicetype = policy.getServiceType();
            String zoneName          = policy.getZoneName();

            RangerService      service          = null;
            RangerSecurityZone zone             = null;
            boolean            serviceNameValid = false;

            if (StringUtils.isBlank(serviceName)) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_FIELD;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("service name")
                        .isMissing()
                        .becauseOf(error.getMessage("service name"))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            } else {
                service = getService(serviceName);

                if (service == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_SERVICE_NAME;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("service name")
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(serviceName))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    serviceNameValid = true;

                    String serviceType = service.getType();

                    if (StringUtils.isNotEmpty(serviceType) && StringUtils.isNotEmpty(policyServicetype)) {
                        if (!serviceType.equalsIgnoreCase(policyServicetype)) {
                            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_SERVICE_TYPE;

                            failures.add(new ValidationFailureDetailsBuilder()
                                    .field("service type")
                                    .isSemanticallyIncorrect()
                                    .becauseOf(error.getMessage(policyServicetype, serviceName))
                                    .errorCode(error.getErrorCode())
                                    .build());
                            valid = false;
                        }
                    }
                }
            }

            if (StringUtils.isNotEmpty(zoneName)) {
                zone = getSecurityZone(zoneName);

                if (zone == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NONEXISTANT_ZONE_NAME;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("zoneName")
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(id, zoneName))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    List<String> tagSvcList = zone.getTagServices();
                    Set<String>  svcNameSet = zone.getServices().keySet();

                    if (!svcNameSet.contains(serviceName) && !tagSvcList.contains(serviceName)) {
                        ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_SERVICE_NOT_ASSOCIATED_TO_ZONE;

                        failures.add(new ValidationFailureDetailsBuilder().field("zoneName").isSemanticallyIncorrect().becauseOf(error.getMessage(serviceName, zoneName)).errorCode(error.getErrorCode()).build());

                        valid = false;
                    }
                }
            }

            if (StringUtils.isBlank(policyName)) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_FIELD;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("name")
                        .isMissing()
                        .becauseOf(error.getMessage("name"))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            } else {
                if (service != null && (StringUtils.isEmpty(zoneName) || zone != null)) {
                    Long zoneId   = zone != null ? zone.getId() : RangerSecurityZone.RANGER_UNZONED_SECURITY_ZONE_ID;
                    Long policyId = getPolicyId(service.getId(), policyName, zoneId);

                    if (policyId != null) {
                        if (action == Action.CREATE) {
                            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_NAME_CONFLICT;

                            failures.add(new ValidationFailureDetailsBuilder()
                                    .field("policy name")
                                    .isSemanticallyIncorrect()
                                    .becauseOf(error.getMessage(policyId, serviceName))
                                    .errorCode(error.getErrorCode())
                                    .build());

                            valid = false;
                        } else if (!policyId.equals(id)) { // action == UPDATE
                            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_NAME_CONFLICT;

                            failures.add(new ValidationFailureDetailsBuilder()
                                    .field("id/name")
                                    .isSemanticallyIncorrect()
                                    .becauseOf(error.getMessage(policyId, serviceName))
                                    .errorCode(error.getErrorCode())
                                    .build());

                            valid = false;
                        }
                    }
                }
            }

            if (existingPolicy != null) {
                if (!StringUtils.equalsIgnoreCase(existingPolicy.getService(), policy.getService())) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_UPDATE_MOVE_SERVICE_NOT_ALLOWED;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("service name")
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(policy.getId(), existingPolicy.getService(), policy.getService()))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }

                int existingPolicyType = existingPolicy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : existingPolicy.getPolicyType();
                int policyType         = policy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : policy.getPolicyType();

                if (existingPolicyType != policyType) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_TYPE_CHANGE_NOT_ALLOWED;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy type")
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(policy.getId(), existingPolicyType, policyType))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }

                String existingZoneName = existingPolicy.getZoneName();

                if (StringUtils.isNotEmpty(zoneName) || StringUtils.isNotEmpty(existingZoneName)) {
                    if (!StringUtils.equals(existingZoneName, zoneName)) {
                        ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_UPDATE_ZONE_NAME_NOT_ALLOWED;

                        failures.add(new ValidationFailureDetailsBuilder()
                                .field("zoneName")
                                .isSemanticallyIncorrect()
                                .becauseOf(error.getMessage(existingZoneName, zoneName))
                                .errorCode(error.getErrorCode())
                                .build());

                        valid = false;
                    }
                }
            }

            boolean          isAuditEnabled   = getIsAuditEnabled(policy);
            String           serviceDefName;
            RangerServiceDef serviceDef       = null;
            int              policyItemsCount = 0;

            int policyType = policy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : policy.getPolicyType();

            switch (policyType) {
                case RangerPolicy.POLICY_TYPE_DATAMASK:
                    if (CollectionUtils.isNotEmpty(policy.getDataMaskPolicyItems())) {
                        policyItemsCount += policy.getDataMaskPolicyItems().size();
                    }
                    break;
                case RangerPolicy.POLICY_TYPE_ROWFILTER:
                    if (CollectionUtils.isNotEmpty(policy.getRowFilterPolicyItems())) {
                        policyItemsCount += policy.getRowFilterPolicyItems().size();
                    }
                    break;
                case RangerPolicy.POLICY_TYPE_CONSTRAINTS:
                    if (CollectionUtils.isNotEmpty(policy.getConstraintPolicyItems())) {
                        policyItemsCount += policy.getConstraintPolicyItems().size();
                    }
                    break;
                default:
                    if (CollectionUtils.isNotEmpty(policy.getPolicyItems())) {
                        policyItemsCount += policy.getPolicyItems().size();
                    }
                    if (CollectionUtils.isNotEmpty(policy.getDenyPolicyItems())) {
                        policyItemsCount += policy.getDenyPolicyItems().size();
                    }
                    break;
            }

            if (policyItemsCount == 0 && !isAuditEnabled) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_POLICY_ITEMS;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("policy items")
                        .isMissing()
                        .becauseOf(error.getMessage())
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            } else if (service != null) {
                serviceDefName = service.getType();
                serviceDef     = getServiceDef(serviceDefName);

                if (serviceDef == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_SERVICE_DEF;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy service def")
                            .isAnInternalError()
                            .becauseOf(error.getMessage(serviceDefName, serviceName))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    if (Boolean.TRUE.equals(policy.getIsDenyAllElse())) {
                        if (CollectionUtils.isNotEmpty(policy.getDenyPolicyItems()) || CollectionUtils.isNotEmpty(policy.getDenyExceptions())) {
                            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_UNSUPPORTED_POLICY_ITEM_TYPE;

                            failures.add(new ValidationFailureDetailsBuilder()
                                    .field("policy items")
                                    .becauseOf(error.getMessage())
                                    .errorCode(error.getErrorCode())
                                    .build());

                            valid = false;
                        }
                    }

                    valid = isValidPolicyItems(policy.getPolicyItems(), failures, serviceDef) && valid;
                    valid = isValidPolicyItems(policy.getDenyPolicyItems(), failures, serviceDef) && valid;
                    valid = isValidPolicyItems(policy.getAllowExceptions(), failures, serviceDef) && valid;
                    valid = isValidPolicyItems(policy.getDenyExceptions(), failures, serviceDef) && valid;

                    @SuppressWarnings("unchecked")
                    List<RangerPolicyItem> dataMaskPolicyItems = (List<RangerPolicyItem>) (List<?>) policy.getDataMaskPolicyItems();
                    valid = isValidPolicyItems(dataMaskPolicyItems, failures, serviceDef) && valid;

                    @SuppressWarnings("unchecked")
                    List<RangerPolicyItem> rowFilterPolicyItems = (List<RangerPolicyItem>) (List<?>) policy.getRowFilterPolicyItems();
                    valid = isValidPolicyItems(rowFilterPolicyItems, failures, serviceDef) && valid;

                    @SuppressWarnings("unchecked")
                    List<RangerPolicyItem> constraintPolicyItems = (List<RangerPolicyItem>) (List<?>) policy.getConstraintPolicyItems();
                    valid = isValidPolicyItems(constraintPolicyItems, failures, serviceDef) && valid;

                    valid = isValidConstraintPolicy(policy, failures, serviceDef) && valid;
                }
            }

            if (serviceNameValid) { // resource checks can't be done meaningfully otherwise
                valid = isValidValiditySchedule(policy, failures, action) && valid;
                valid = isValidResources(policy, failures, action, isAdmin, serviceDef) && valid;
                valid = isValidAccessTypeDef(policy, failures, action, isAdmin, serviceDef) && valid;
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValid({}, {}, {}, {}): {}", policy, action, isAdmin, failures, valid);

        return valid;
    }

    boolean isValidAccessTypeDef(RangerPolicy policy, final List<ValidationFailureDetails> failures, Action action, boolean isAdmin, final RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValidAccessTypeDef({}, {}, {},{},{})", policy, failures, action, isAdmin, serviceDef);

        boolean valid      = true;
        int     policyType = policy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : policy.getPolicyType();

        if (policyType == RangerPolicy.POLICY_TYPE_ROWFILTER) { //row filter policy
            List<String> rowFilterAccessTypeDefNames = new ArrayList<>();

            if (serviceDef != null && serviceDef.getRowFilterDef() != null) {
                if (!CollectionUtils.isEmpty(serviceDef.getRowFilterDef().getAccessTypes())) {
                    for (RangerAccessTypeDef rangerAccessTypeDef : serviceDef.getRowFilterDef().getAccessTypes()) {
                        rowFilterAccessTypeDefNames.add(rangerAccessTypeDef.getName().toLowerCase());
                    }

                    if (serviceDef.getMarkerAccessTypes() != null) {
                        for (RangerAccessTypeDef accessTypeDef : serviceDef.getMarkerAccessTypes()) {
                            if (accessTypeDef == null || accessTypeDef.getImpliedGrants() == null) {
                                continue;
                            }

                            if (CollectionUtils.containsAny(accessTypeDef.getImpliedGrants(), rowFilterAccessTypeDefNames)) {
                                rowFilterAccessTypeDefNames.add(accessTypeDef.getName());
                            }
                        }
                    }
                }
            }

            if (!CollectionUtils.isEmpty(policy.getRowFilterPolicyItems())) {
                for (RangerRowFilterPolicyItem rangerRowFilterPolicyItem : policy.getRowFilterPolicyItems()) {
                    if (!CollectionUtils.isEmpty(rangerRowFilterPolicyItem.getAccesses())) {
                        for (RangerPolicyItemAccess rangerPolicyItemAccess : rangerRowFilterPolicyItem.getAccesses()) {
                            if (!rowFilterAccessTypeDefNames.contains(rangerPolicyItemAccess.getType().toLowerCase())) {
                                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_ITEM_ACCESS_TYPE_INVALID;

                                failures.add(new ValidationFailureDetailsBuilder()
                                        .field("row filter policy item access type")
                                        .isSemanticallyIncorrect()
                                        .becauseOf(error.getMessage(rangerPolicyItemAccess.getType(), rowFilterAccessTypeDefNames))
                                        .errorCode(error.getErrorCode())
                                        .build());

                                valid = false;
                            }
                        }
                    }
                }
            }
        } else if (policyType == RangerPolicy.POLICY_TYPE_DATAMASK) { //data mask policy
            List<String> dataMaskAccessTypeDefNames = new ArrayList<>();

            if (serviceDef != null && serviceDef.getDataMaskDef() != null) {
                if (!CollectionUtils.isEmpty(serviceDef.getDataMaskDef().getAccessTypes())) {
                    for (RangerAccessTypeDef rangerAccessTypeDef : serviceDef.getDataMaskDef().getAccessTypes()) {
                        dataMaskAccessTypeDefNames.add(rangerAccessTypeDef.getName().toLowerCase());
                    }

                    if (serviceDef.getMarkerAccessTypes() != null) {
                        for (RangerAccessTypeDef accessTypeDef : serviceDef.getMarkerAccessTypes()) {
                            if (accessTypeDef == null || accessTypeDef.getImpliedGrants() == null) {
                                continue;
                            }

                            if (CollectionUtils.containsAny(accessTypeDef.getImpliedGrants(), dataMaskAccessTypeDefNames)) {
                                dataMaskAccessTypeDefNames.add(accessTypeDef.getName());
                            }
                        }
                    }
                }
            }

            if (!CollectionUtils.isEmpty(policy.getDataMaskPolicyItems())) {
                for (RangerDataMaskPolicyItem rangerDataMaskPolicyItem : policy.getDataMaskPolicyItems()) {
                    if (!CollectionUtils.isEmpty(rangerDataMaskPolicyItem.getAccesses())) {
                        for (RangerPolicyItemAccess rangerPolicyItemAccess : rangerDataMaskPolicyItem.getAccesses()) {
                            if (!dataMaskAccessTypeDefNames.contains(rangerPolicyItemAccess.getType().toLowerCase())) {
                                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_ITEM_ACCESS_TYPE_INVALID;

                                failures.add(new ValidationFailureDetailsBuilder()
                                        .field("data masking policy item access type")
                                        .isSemanticallyIncorrect()
                                        .becauseOf(error.getMessage(rangerPolicyItemAccess.getType(), dataMaskAccessTypeDefNames))
                                        .errorCode(error.getErrorCode())
                                        .build());

                                valid = false;
                            }
                        }
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidAccessTypeDef({}, {}, {},{},{})", policy, failures, action, isAdmin, serviceDef);

        return valid;
    }

    boolean isValidResources(RangerPolicy policy, final List<ValidationFailureDetails> failures, Action action, boolean isAdmin, final RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValidResources({}, {}, {}, {}, {})", policy, failures, action, isAdmin, serviceDef);

        boolean                           valid       = true;
        Map<String, RangerPolicyResource> resourceMap = policy.getResources();

        if (resourceMap != null) { // following checks can't be done meaningfully otherwise
            valid = isPolicyResourceUnique(policy, failures, action) && valid;

            if (serviceDef != null) { // following checks can't be done meaningfully otherwise
                valid = isValidResourceNames(policy, failures, serviceDef) && valid;
                valid = isValidResourceValues(resourceMap, failures, serviceDef) && valid;
                valid = isValidResourceFlags(resourceMap, failures, serviceDef.getResources(), serviceDef.getName(), policy.getName(), isAdmin) && valid;

                List<Map<String, RangerPolicyResource>> additionalResources = policy.getAdditionalResources();

                if (additionalResources != null) {
                    for (Map<String, RangerPolicyResource> additionalResource : additionalResources) {
                        valid = isValidResourceValues(additionalResource, failures, serviceDef) && valid;
                        valid = isValidResourceFlags(additionalResource, failures, serviceDef.getResources(), serviceDef.getName(), policy.getName(), isAdmin) && valid;
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidResources({}, {}, {}, {}, {}): {}", policy, failures, action, isAdmin, serviceDef, valid);

        return valid;
    }

    boolean isValidValiditySchedule(RangerPolicy policy, final List<ValidationFailureDetails> failures, Action action) {
        LOG.debug("==> RangerPolicyValidator.isValidValiditySchedule({}, {}, {})", policy, failures, action);

        boolean                      valid                       = true;
        List<RangerValiditySchedule> validitySchedules           = policy.getValiditySchedules();
        List<RangerValiditySchedule> normalizedValiditySchedules = null;

        for (RangerValiditySchedule entry : validitySchedules) {
            RangerValidityScheduleValidator validator                  = new RangerValidityScheduleValidator(entry);
            RangerValiditySchedule          normalizedValiditySchedule = validator.validate(failures);

            if (normalizedValiditySchedule == null) {
                valid = false;

                LOG.debug("Invalid Validity-Schedule:[{}]", entry);
            } else {
                if (normalizedValiditySchedules == null) {
                    normalizedValiditySchedules = new ArrayList<>();
                }

                normalizedValiditySchedules.add(normalizedValiditySchedule);
            }
        }

        if (valid && CollectionUtils.isNotEmpty(normalizedValiditySchedules)) {
            policy.setValiditySchedules(normalizedValiditySchedules);
        }

        LOG.debug("<== RangerPolicyValidator.isValidValiditySchedule({}, {}, {}): {}", policy, failures, action, valid);

        return valid;
    }

    boolean isPolicyResourceUnique(RangerPolicy policy, final List<ValidationFailureDetails> failures, Action action) {
        LOG.debug("==> RangerPolicyValidator.isPolicyResourceUnique({}, {}, {})", policy, failures, action);

        boolean                       valid           = true;
        RangerPolicyResourceSignature policySignature = factory.createPolicyResourceSignature(policy);
        String                        signature       = policySignature.getSignature();
        List<RangerPolicy>            policies        = getPoliciesForResourceSignature(policy.getService(), signature);

        if (CollectionUtils.isNotEmpty(policies)) {
            ValidationErrorCode error         = ValidationErrorCode.POLICY_VALIDATION_ERR_DUPLICATE_POLICY_RESOURCE;
            RangerPolicy        matchedPolicy = policies.iterator().next();

            // there shouldn't be a matching policy for create.  During update only match should be to itself
            if (action == Action.CREATE || (action == Action.UPDATE && !matchedPolicy.getId().equals(policy.getId()))) {
                failures.add(new ValidationFailureDetailsBuilder()
                        .field("resources")
                        .isSemanticallyIncorrect()
                        .becauseOf(error.getMessage(matchedPolicy.getName(), policy.getService()))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            }
        }

        LOG.debug("<== RangerPolicyValidator.isPolicyResourceUnique({}, {}, {}): {}", policy, failures, action, valid);

        return valid;
    }

    boolean isValidResourceNames(final RangerPolicy policy, final List<ValidationFailureDetails> failures, final RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValidResourceNames({}, {}, {})", policy, failures, serviceDef);

        boolean                           valid     = true;
        Map<String, RangerPolicyResource> resources = policy.getResources();

        if (resources != null) {
            boolean     isConstraintPolicy = Integer.valueOf(RangerPolicy.POLICY_TYPE_CONSTRAINTS).equals(policy.getPolicyType());
            Set<String> constraintNames    = isConstraintPolicy ? getDefinedConstraintNames(policy, serviceDef) : null;

            if (isConstraintPolicy) {
                valid = isValidConstraintResourceNames(resources, constraintNames, failures, serviceDef) && valid;
            } else {
                valid = isValidResourceNames(resources, failures, serviceDef, policy.getPolicyType()) && valid;
            }

            List<Map<String, RangerPolicyResource>> additionalResources = policy.getAdditionalResources();

            if (additionalResources != null) {
                for (Map<String, RangerPolicyResource> additionalResource : additionalResources) {
                    if (isConstraintPolicy) {
                        valid = isValidConstraintResourceNames(additionalResource, constraintNames, failures, serviceDef) && valid;
                    } else {
                        valid = isValidResourceNames(additionalResource, failures, serviceDef, policy.getPolicyType()) && valid;
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidResourceNames({}, {}, {}): {}", policy, failures, serviceDef, valid);

        return valid;
    }

    boolean isValidResourceNames(Map<String, RangerPolicyResource> resources, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef, Integer policyType) {
        convertPolicyResourceNamesToLower(resources);

        boolean                      valid           = true;
        Set<String>                  policyResources = resources.keySet();
        RangerServiceDefHelper       defHelper       = new RangerServiceDefHelper(serviceDef);
        Set<List<RangerResourceDef>> hierarchies     = defHelper.getResourceHierarchies(policyType); // this can be empty but not null!

        if (hierarchies.isEmpty()) {
            LOG.debug("RangerPolicyValidator.isValidResourceNames: serviceDef does not have any resource hierarchies, possibly due to invalid service def!!");

            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_RESOURCE_NO_COMPATIBLE_HIERARCHY;

            failures.add(new ValidationFailureDetailsBuilder()
                    .field("service def resource hierarchies")
                    .subField("incompatible")
                    .isSemanticallyIncorrect()
                    .becauseOf(error.getMessage(serviceDef.getName(), " does not have any resource hierarchies"))
                    .errorCode(error.getErrorCode())
                    .build());

            valid = false;
        } else {
            /*
             * A policy is for a single hierarchy however, it doesn't specify which one.  So we have to guess which hierarchy(s) it possibly be for.  First, see if the policy could be for
             * any of the known hierarchies?  A candidate hierarchy is one whose resource levels are a superset of those in the policy.
             * Why?  What we want to catch at this stage is policies that straddles multiple hierarchies, e.g. db, udf and column for a hive policy.
             * This has the side effect of catch spurious levels specified on the policy, e.g. having a level "blah" on a hive policy.
             */
            Set<List<RangerResourceDef>> candidateHierarchies = filterHierarchies_hierarchyHasAllPolicyResources(policyResources, hierarchies, defHelper);

            if (candidateHierarchies.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("No compatible resource hierarchies found: resource[{}], service-def[{}], valid-resource-hierarchies[{}]",
                            policyResources, serviceDef.getName(), toStringHierarchies_all(hierarchies, defHelper));
                }

                ValidationErrorCode error;

                if (hierarchies.size() == 1) { // we can give a simpler message for single hierarchy service-defs which is the majority of cases
                    error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_RESOURCE_NO_COMPATIBLE_HIERARCHY_SINGLE;
                } else {
                    error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_RESOURCE_NO_COMPATIBLE_HIERARCHY;
                }

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("policy resources")
                        .subField("incompatible")
                        .isSemanticallyIncorrect()
                        .becauseOf(error.getMessage(serviceDef.getName(), toStringHierarchies_all(hierarchies, defHelper)))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("isValidResourceNames: Found [{}] compatible hierarchies: {}", candidateHierarchies.size(), toStringHierarchies_all(candidateHierarchies, defHelper));
                }

                /*
                 * Among the candidate hierarchies there should be at least one for which policy specifies all of the mandatory resources.  Note that there could be multiple
                 * hierarchies that meet that criteria, e.g. a hive policy that specified only DB.  It is not clear if it belongs to DB->UDF or DB->TBL->COL hierarchy.
                 * However, if both UDF and TBL were required then we can detect that policy does not specify mandatory levels for any of the candidate hierarchies.
                 */
                Set<List<RangerResourceDef>> validHierarchies = filterHierarchies_mandatoryResourcesSpecifiedInPolicy(policyResources, candidateHierarchies, defHelper);

                if (validHierarchies.isEmpty()) {
                    ValidationErrorCode error;

                    if (candidateHierarchies.size() == 1) { // we can provide better message if there is a single candidate hierarchy
                        error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_RESOURCE_MISSING_MANDATORY_SINGLE;
                    } else {
                        error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_RESOURCE_MISSING_MANDATORY;
                    }

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy resources")
                            .subField("missing mandatory")
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(serviceDef.getName(), toStringHierarchies_mandatory(candidateHierarchies, defHelper)))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("isValidResourceNames: Found hierarchies with all mandatory fields specified: {}", toStringHierarchies_mandatory(validHierarchies, defHelper));
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidResourceNames({}, {}, {}): {}", resources, failures, serviceDef, valid);

        return valid;
    }

    /**
     * Policy resources of a constraint policy must be valid for every constraint used by its items: for each constraint,
     * the resources must be in one of the hierarchies returned by RangerServiceDefHelper.getResourceHierarchiesForConstraint(),
     * with all mandatory resources of that hierarchy specified.
     */
    boolean isValidConstraintResourceNames(Map<String, RangerPolicyResource> resources, Set<String> constraintNames, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValidConstraintResourceNames({}, {}, {}, {})", resources, constraintNames, failures, serviceDef);

        boolean valid = true;

        if (CollectionUtils.isNotEmpty(constraintNames)) { // unknown/missing constraints are reported by isValidConstraintPolicy()
            convertPolicyResourceNamesToLower(resources);

            Set<String>            policyResources = resources.keySet();
            RangerServiceDefHelper defHelper       = new RangerServiceDefHelper(serviceDef);

            for (String constraintName : constraintNames) {
                Set<List<RangerResourceDef>> hierarchies      = defHelper.getResourceHierarchiesForConstraint(constraintName, policyResources);
                Set<List<RangerResourceDef>> validHierarchies = filterHierarchies_mandatoryResourcesSpecifiedInPolicy(policyResources, hierarchies, defHelper);

                if (validHierarchies.isEmpty()) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_RESOURCES_INVALID;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy resources")
                            .subField(constraintName)
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(policyResources, constraintName, toStringHierarchies_all(defHelper.getResourceHierarchiesForConstraint(constraintName, new HashSet<>()), defHelper)))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidConstraintResourceNames({}, {}, {}, {}): {}", resources, constraintNames, failures, serviceDef, valid);

        return valid;
    }

    /**
     * Validates constraint policy items:
     * - constraint policy items are allowed only in constraint policies, and constraint policies can have only constraint policy items
     * - the service-def must have constraintDefs
     * - each item's constraint must be defined in the service-def
     * - each item's access types must be among the accessTypes of its constraintDef (when the constraintDef lists any)
     * - mandatory parameters must be present; parameters must be defined in the constraintDef; values must match the parameter type
     */
    boolean isValidConstraintPolicy(RangerPolicy policy, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValidConstraintPolicy({}, {}, {})", policy, failures, serviceDef);

        boolean valid      = true;
        int     policyType = policy.getPolicyType() == null ? RangerPolicy.POLICY_TYPE_ACCESS : policy.getPolicyType();

        if (policyType != RangerPolicy.POLICY_TYPE_CONSTRAINTS) {
            if (CollectionUtils.isNotEmpty(policy.getConstraintPolicyItems())) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_ITEMS_NOT_ALLOWED;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("constraint policy items")
                        .isSemanticallyIncorrect()
                        .becauseOf(error.getMessage(RangerPolicy.POLICY_TYPE_CONSTRAINTS))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            }
        } else {
            List<String> otherItems = new ArrayList<>();

            if (CollectionUtils.isNotEmpty(policy.getPolicyItems())) {
                otherItems.add("policyItems");
            }

            if (CollectionUtils.isNotEmpty(policy.getDenyPolicyItems())) {
                otherItems.add("denyPolicyItems");
            }

            if (CollectionUtils.isNotEmpty(policy.getAllowExceptions())) {
                otherItems.add("allowExceptions");
            }

            if (CollectionUtils.isNotEmpty(policy.getDenyExceptions())) {
                otherItems.add("denyExceptions");
            }

            if (CollectionUtils.isNotEmpty(policy.getDataMaskPolicyItems())) {
                otherItems.add("dataMaskPolicyItems");
            }

            if (CollectionUtils.isNotEmpty(policy.getRowFilterPolicyItems())) {
                otherItems.add("rowFilterPolicyItems");
            }

            if (!otherItems.isEmpty()) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_POLICY_HAS_OTHER_ITEMS;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("policy items")
                        .isSemanticallyIncorrect()
                        .becauseOf(error.getMessage(otherItems))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            }

            Map<String, RangerConstraintDef> constraintDefs = getConstraintDefs(serviceDef);

            if (constraintDefs.isEmpty()) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINTS_NOT_SUPPORTED;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("policy type")
                        .isSemanticallyIncorrect()
                        .becauseOf(error.getMessage(serviceDef.getName()))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            } else if (CollectionUtils.isNotEmpty(policy.getConstraintPolicyItems())) {
                for (RangerConstraintPolicyItem item : policy.getConstraintPolicyItems()) {
                    if (item != null) { // null items are reported by isValidPolicyItems()
                        valid = isValidConstraintPolicyItem(item, constraintDefs, failures, serviceDef) && valid;
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidConstraintPolicy({}, {}, {}): {}", policy, failures, serviceDef, valid);

        return valid;
    }

    boolean isValidConstraintPolicyItem(RangerConstraintPolicyItem item, Map<String, RangerConstraintDef> constraintDefs, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
        boolean                        valid          = true;
        RangerPolicyItemConstraintInfo constraintInfo = item.getConstraintInfo();
        String                         constraintName = constraintInfo != null ? constraintInfo.getConstraintName() : null;

        if (StringUtils.isBlank(constraintName)) {
            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_MISSING;

            failures.add(new ValidationFailureDetailsBuilder()
                    .field("constraint policy item constraint")
                    .isMissing()
                    .becauseOf(error.getMessage())
                    .errorCode(error.getErrorCode())
                    .build());

            return false;
        }

        RangerConstraintDef constraintDef = constraintDefs.get(constraintName);

        if (constraintDef == null) {
            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_UNKNOWN;

            failures.add(new ValidationFailureDetailsBuilder()
                    .field("constraint policy item constraint")
                    .subField(constraintName)
                    .isSemanticallyIncorrect()
                    .becauseOf(error.getMessage(constraintName, serviceDef.getName()))
                    .errorCode(error.getErrorCode())
                    .build());

            return false;
        }

        // access types
        if (CollectionUtils.isNotEmpty(constraintDef.getAccessTypes()) && CollectionUtils.isNotEmpty(item.getAccesses())) {
            for (RangerPolicyItemAccess access : item.getAccesses()) {
                if (access != null && StringUtils.isNotBlank(access.getType()) && !constraintDef.getAccessTypes().contains(access.getType())) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_ACCESS_TYPE_INVALID;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("constraint policy item access type")
                            .subField(access.getType())
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(access.getType(), constraintName, constraintDef.getAccessTypes()))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }
            }
        }

        // parameters
        Map<String, Object>                   parameters = constraintInfo.getParameters();
        Map<String, RangerConstraintParamDef> paramDefs  = new HashMap<>();

        if (constraintDef.getParameters() != null) {
            for (RangerConstraintParamDef paramDef : constraintDef.getParameters()) {
                if (paramDef != null && paramDef.getName() != null) {
                    paramDefs.put(paramDef.getName(), paramDef);
                }
            }
        }

        for (RangerConstraintParamDef paramDef : paramDefs.values()) {
            Object value = parameters != null ? parameters.get(paramDef.getName()) : null;

            if (Boolean.TRUE.equals(paramDef.getMandatory()) && (value == null || (value instanceof String && StringUtils.isBlank((String) value)))) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_PARAM_MISSING;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("constraint policy item parameter")
                        .subField(paramDef.getName())
                        .isMissing()
                        .becauseOf(error.getMessage(paramDef.getName(), constraintName))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            }
        }

        if (parameters != null) {
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                RangerConstraintParamDef paramDef = paramDefs.get(entry.getKey());

                if (paramDef == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_PARAM_UNKNOWN;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("constraint policy item parameter")
                            .subField(entry.getKey())
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(entry.getKey(), constraintName))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else if (entry.getValue() != null && !isValidConstraintParamValue(entry.getValue(), paramDef, serviceDef)) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_CONSTRAINT_PARAM_INVALID_VALUE;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("constraint policy item parameter")
                            .subField(entry.getKey())
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(entry.getValue(), entry.getKey(), constraintName, paramDef.getType()))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }
            }
        }

        return valid;
    }

    boolean isValidConstraintParamValue(Object value, RangerConstraintParamDef paramDef, RangerServiceDef serviceDef) {
        final boolean ret;
        final String  type = paramDef.getType();

        if ("int".equals(type)) {
            if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte) {
                ret = true;
            } else if (value instanceof Number) {
                double d = ((Number) value).doubleValue();

                ret = d == Math.rint(d) && !Double.isInfinite(d);
            } else if (value instanceof String) {
                ret = isLong((String) value);
            } else {
                ret = false;
            }
        } else if ("bool".equals(type)) {
            ret = value instanceof Boolean || (value instanceof String && ("true".equalsIgnoreCase((String) value) || "false".equalsIgnoreCase((String) value)));
        } else if ("string".equals(type)) {
            ret = value instanceof String;
        } else if ("enum".equals(type)) {
            ret = value instanceof String && getEnumElementNames(serviceDef, paramDef.getSubType()).contains(value);
        } else { // types are validated in the service-def; don't reject values of a type this version doesn't know
            ret = true;
        }

        return ret;
    }

    private static boolean isLong(String value) {
        try {
            Long.parseLong(value.trim());

            return true;
        } catch (NumberFormatException excp) {
            return false;
        }
    }

    private static Set<String> getEnumElementNames(RangerServiceDef serviceDef, String enumName) {
        Set<String> ret = new HashSet<>();

        if (serviceDef.getEnums() != null && enumName != null) {
            for (RangerEnumDef enumDef : serviceDef.getEnums()) {
                if (enumDef != null && enumName.equals(enumDef.getName()) && enumDef.getElements() != null) {
                    for (RangerEnumElementDef element : enumDef.getElements()) {
                        if (element != null && element.getName() != null) {
                            ret.add(element.getName());
                        }
                    }
                }
            }
        }

        return ret;
    }

    private static Map<String, RangerConstraintDef> getConstraintDefs(RangerServiceDef serviceDef) {
        Map<String, RangerConstraintDef> ret = new HashMap<>();

        if (serviceDef != null && serviceDef.getConstraintDefs() != null) {
            for (RangerConstraintDef constraintDef : serviceDef.getConstraintDefs()) {
                if (constraintDef != null && constraintDef.getName() != null) {
                    ret.put(constraintDef.getName(), constraintDef);
                }
            }
        }

        return ret;
    }

    // names of constraints used by the policy's items that are defined in the service-def
    private static Set<String> getDefinedConstraintNames(RangerPolicy policy, RangerServiceDef serviceDef) {
        Set<String>                      ret            = new LinkedHashSet<>();
        Map<String, RangerConstraintDef> constraintDefs = getConstraintDefs(serviceDef);

        if (policy.getConstraintPolicyItems() != null) {
            for (RangerConstraintPolicyItem item : policy.getConstraintPolicyItems()) {
                String constraintName = item != null && item.getConstraintInfo() != null ? item.getConstraintInfo().getConstraintName() : null;

                if (constraintName != null && constraintDefs.containsKey(constraintName)) {
                    ret.add(constraintName);
                }
            }
        }

        return ret;
    }

    /**
     * String representation of mandatory resources of all the hierarchies suitable of showing to user.  Mandatory resources within a hierarchy are not ordered per the hierarchy.
     *
     * @param hierarchies
     * @param defHelper
     * @return
     */
    String toStringHierarchies_mandatory(Set<List<RangerResourceDef>> hierarchies, RangerServiceDefHelper defHelper) {
        // helper function skipping sanity checks of getting null arguments passed
        StringBuilder builder = new StringBuilder();

        for (List<RangerResourceDef> aHierarchy : hierarchies) {
            builder.append(defHelper.getMandatoryResourceNames(aHierarchy));
            builder.append(" ");
        }

        return builder.toString();
    }

    /**
     * String representation of all resources of all hierarchies.  Resources within a hierarchy are ordered per the hierarchy.
     *
     * @param hierarchies
     * @param defHelper
     * @return
     */
    String toStringHierarchies_all(Set<List<RangerResourceDef>> hierarchies, RangerServiceDefHelper defHelper) {
        // helper function skipping sanity checks of getting null arguments passed
        StringBuilder builder = new StringBuilder();

        for (List<RangerResourceDef> aHierarchy : hierarchies) {
            builder.append(defHelper.getAllResourceNamesOrdered(aHierarchy));
            builder.append(" ");
        }

        return builder.toString();
    }

    /**
     * Returns the subset of all hierarchies that are a superset of the policy's resources.
     *
     * @param policyResources
     * @param hierarchies
     * @return
     */
    Set<List<RangerResourceDef>> filterHierarchies_hierarchyHasAllPolicyResources(Set<String> policyResources, Set<List<RangerResourceDef>> hierarchies, RangerServiceDefHelper defHelper) {
        // helper function skipping sanity checks of getting null arguments passed
        Set<List<RangerResourceDef>> result = new HashSet<>(hierarchies.size());

        for (List<RangerResourceDef> aHierarchy : hierarchies) {
            if (defHelper.hierarchyHasAllResources(aHierarchy, policyResources)) {
                result.add(aHierarchy);
            }
        }

        return result;
    }

    /**
     * Returns the subset of hierarchies all of whose mandatory resources were found in policy's resource set.  candidate hierarchies are expected to have passed
     * <code>filterHierarchies_hierarchyHasAllPolicyResources</code> check first.
     *
     * @param policyResources
     * @param hierarchies
     * @param defHelper
     * @return
     */
    Set<List<RangerResourceDef>> filterHierarchies_mandatoryResourcesSpecifiedInPolicy(Set<String> policyResources, Set<List<RangerResourceDef>> hierarchies, RangerServiceDefHelper defHelper) {
        // helper function skipping sanity checks of getting null arguments passed
        Set<List<RangerResourceDef>> result = new HashSet<>(hierarchies.size());

        for (List<RangerResourceDef> aHierarchy : hierarchies) {
            Set<String> mandatoryResources = defHelper.getMandatoryResourceNames(aHierarchy);

            if (policyResources.containsAll(mandatoryResources)) {
                result.add(aHierarchy);
            }
        }

        return result;
    }

    boolean isValidResourceFlags(final Map<String, RangerPolicyResource> inputPolicyResources, final List<ValidationFailureDetails> failures, final List<RangerResourceDef> resourceDefs, final String serviceDefName, final String policyName, boolean isAdmin) {
        LOG.debug("==> RangerPolicyValidator.isValidResourceFlags({}, {}, {}, {}, {}, {})", inputPolicyResources, failures, resourceDefs, serviceDefName, policyName, isAdmin);

        boolean valid = true;

        if (resourceDefs == null) {
            LOG.debug("isValidResourceFlags: service Def is null");
        } else {
            Map<String, RangerPolicyResource> policyResources = getPolicyResourceWithLowerCaseKeys(inputPolicyResources);

            for (RangerResourceDef resourceDef : resourceDefs) {
                if (resourceDef == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_RESOURCE_DEF;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("resource-def")
                            .isAnInternalError()
                            .becauseOf(error.getMessage(serviceDefName))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else if (StringUtils.isBlank(resourceDef.getName())) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_RESOURCE_DEF_NAME;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("resource-def-name")
                            .isAnInternalError()
                            .becauseOf(error.getMessage(serviceDefName))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    String               resourceName   = resourceDef.getName().toLowerCase();
                    RangerPolicyResource policyResource = policyResources.get(resourceName);

                    if (policyResource == null) {
                        LOG.debug("a policy-resource object for resource[{}] on policy [{}] was null", resourceName, policyName);
                    } else {
                        boolean excludesSupported        = Boolean.TRUE.equals(resourceDef.getExcludesSupported()); // could be null
                        boolean policyResourceIsExcludes = Boolean.TRUE.equals(policyResource.getIsExcludes()); // could be null

                        if (policyResourceIsExcludes && !excludesSupported) {
                            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_EXCLUDES_NOT_SUPPORTED;

                            failures.add(new ValidationFailureDetailsBuilder()
                                    .field("isExcludes")
                                    .subField(resourceName)
                                    .isSemanticallyIncorrect()
                                    .becauseOf(error.getMessage(resourceName))
                                    .errorCode(error.getErrorCode())
                                    .build());

                            valid = false;
                        }

                        if (policyResourceIsExcludes && !isAdmin) {
                            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_EXCLUDES_REQUIRES_ADMIN;

                            failures.add(new ValidationFailureDetailsBuilder()
                                    .field("isExcludes")
                                    .subField("isAdmin")
                                    .isSemanticallyIncorrect()
                                    .becauseOf(error.getMessage())
                                    .errorCode(error.getErrorCode())
                                    .build());

                            valid = false;
                        }

                        boolean recursiveSupported = Boolean.TRUE.equals(resourceDef.getRecursiveSupported());
                        boolean policyIsRecursive  = Boolean.TRUE.equals(policyResource.getIsRecursive());

                        if (policyIsRecursive && !recursiveSupported) {
                            ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_RECURSIVE_NOT_SUPPORTED;

                            failures.add(new ValidationFailureDetailsBuilder()
                                    .field("isRecursive")
                                    .subField(resourceName)
                                    .isSemanticallyIncorrect()
                                    .becauseOf(error.getMessage(resourceName))
                                    .errorCode(error.getErrorCode())
                                    .build());

                            valid = false;
                        }
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidResourceFlags({}, {}, {}, {}, {}, {}): {}", inputPolicyResources, failures, resourceDefs, serviceDefName, policyName, isAdmin, valid);

        return valid;
    }

    boolean isValidResourceValues(Map<String, RangerPolicyResource> resourceMap, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValidResourceValues({}, {}, {})", resourceMap, failures, serviceDef);

        boolean             valid              = true;
        Map<String, String> validationRegExMap = getValidationRegExes(serviceDef);

        for (Map.Entry<String, RangerPolicyResource> entry : resourceMap.entrySet()) {
            String               name           = entry.getKey();
            RangerPolicyResource policyResource = entry.getValue();

            if (policyResource != null) {
                policyResource.getValues().removeIf(StringUtils::isBlank);

                if (CollectionUtils.isEmpty(policyResource.getValues())) {
                    LOG.debug("Resource list was empty or contains null: value[{}], resource-name[{}], service-def-name[{}]", policyResource.getValues(), name, serviceDef.getName());

                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_RESOURCE_LIST;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("resource-values")
                            .subField(name)
                            .isMissing()
                            .becauseOf(error.getMessage(name))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    String duplicateValue = getDuplicate(policyResource.getValues());

                    if (!StringUtils.isBlank(duplicateValue)) {
                        ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_DUPLICATE_VALUES_FOR_RESOURCE;

                        LOG.debug("Duplicate values found for the resource name[{}] value[{}] service-def-name[{}]", name, duplicateValue, serviceDef.getName());

                        failures.add(new ValidationFailureDetailsBuilder()
                                .field("resource-values")
                                .subField(name)
                                .isSemanticallyIncorrect()
                                .becauseOf(error.getMessage(name, duplicateValue))
                                .errorCode(error.getErrorCode())
                                .build());

                        valid = false;
                    }

                    if (validationRegExMap.containsKey(name)) {
                        String regEx = validationRegExMap.get(name);

                        for (String aValue : policyResource.getValues()) {
                            if (!aValue.matches(regEx)) {
                                LOG.debug("Resource failed regex check: value[{}], resource-name[{}], regEx[{}], service-def-name[{}]", aValue, name, regEx, serviceDef.getName());

                                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_INVALID_RESOURCE_VALUE_REGEX;

                                failures.add(new ValidationFailureDetailsBuilder()
                                        .field("resource-values")
                                        .subField(name)
                                        .isSemanticallyIncorrect()
                                        .becauseOf(error.getMessage(aValue, name))
                                        .errorCode(error.getErrorCode())
                                        .build());

                                valid = false;
                            }
                        }
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidResourceValues({}, {}, {}): {}", resourceMap, failures, serviceDef, valid);

        return valid;
    }

    boolean isValidPolicyItems(List<RangerPolicyItem> policyItems, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValid({}, {}, {})", policyItems, failures, serviceDef);

        boolean valid = true;

        if (CollectionUtils.isEmpty(policyItems)) {
            LOG.debug("policy items collection was null/empty");
        } else {
            for (RangerPolicyItem policyItem : policyItems) {
                if (policyItem == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy item")
                            .isMissing()
                            .becauseOf(error.getMessage())
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    // we want to go through all elements even though one may be bad so all failures are captured
                    valid = isValidPolicyItem(policyItem, failures, serviceDef) && valid;
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValid({}, {}, {}): {}", policyItems, failures, serviceDef, valid);

        return valid;
    }

    boolean isValidPolicyItem(RangerPolicyItem policyItem, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValid({}, {}, {})", policyItem, failures, serviceDef);

        boolean valid = true;

        if (policyItem == null) {
            LOG.debug("policy item was null!");
        } else {
            if (policyItem instanceof RangerDataMaskPolicyItem) {
                RangerPolicyItemDataMaskInfo dataMaskInfo = ((RangerDataMaskPolicyItem) policyItem).getDataMaskInfo();
                if (StringUtils.isBlank(dataMaskInfo.getDataMaskType())) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM;
                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy item datamask-type")
                            .isMissing()
                            .becauseOf(error.getMessage("policy item datamask-type"))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }
            }
            // access items collection can't be empty (unless delegated admin is true) and should be otherwise valid
            if (CollectionUtils.isEmpty(policyItem.getAccesses())) {
                if (!Boolean.TRUE.equals(policyItem.getDelegateAdmin())) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_FIELD;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy item accesses")
                            .isMissing()
                            .becauseOf(error.getMessage("policy item accesses"))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    LOG.debug("policy item collection was null but delegated admin is true. Ok");
                }
            } else {
                valid = isValidItemAccesses(policyItem.getAccesses(), failures, serviceDef) && valid;
            }

            // both users and user-groups collections can't be empty
            if (CollectionUtils.isEmpty(policyItem.getUsers()) && CollectionUtils.isEmpty(policyItem.getGroups()) && CollectionUtils.isEmpty(policyItem.getRoles())) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_USER_AND_GROUPS;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("policy item users/user-groups/roles")
                        .isMissing()
                        .becauseOf(error.getMessage())
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            } else {
                removeDuplicates(policyItem.getUsers());
                removeDuplicates(policyItem.getGroups());
                removeDuplicates(policyItem.getRoles());

                if (CollectionUtils.isNotEmpty(policyItem.getUsers()) && CollectionUtils.containsAny(policyItem.getUsers(), INVALID_POLICY_ITEM_VALUES)) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM_USER;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy item users")
                            .isMissing()
                            .becauseOf(error.getMessage())
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }

                if (CollectionUtils.isNotEmpty(policyItem.getGroups()) && CollectionUtils.containsAny(policyItem.getGroups(), INVALID_POLICY_ITEM_VALUES)) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM_GROUP;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy item groups")
                            .isMissing()
                            .becauseOf(error.getMessage())
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }

                if (CollectionUtils.isNotEmpty(policyItem.getRoles()) && CollectionUtils.containsAny(policyItem.getRoles(), INVALID_POLICY_ITEM_VALUES)) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM_ROLE;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy item roles")
                            .isMissing()
                            .becauseOf(error.getMessage())
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValid({}, {}, {}): {}", policyItem, failures, serviceDef, valid);

        return valid;
    }

    boolean isValidItemAccesses(List<RangerPolicyItemAccess> accesses, List<ValidationFailureDetails> failures, RangerServiceDef serviceDef) {
        LOG.debug("==> RangerPolicyValidator.isValid({}, {}, {})", accesses, failures, serviceDef);

        boolean valid = true;
        if (CollectionUtils.isEmpty(accesses)) {
            LOG.debug("policy item accesses collection was null/empty!");
        } else {
            Set<String> accessTypes    = getAccessTypes(serviceDef);
            Set<String> uniqueAccesses = new HashSet<>();

            for (Iterator<RangerPolicyItemAccess> accessTypeIterator = accesses.iterator(); accessTypeIterator.hasNext(); ) {
                RangerPolicyItemAccess access = accessTypeIterator.next();

                if (access == null) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_NULL_POLICY_ITEM_ACCESS;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy item access")
                            .isMissing()
                            .becauseOf(error.getMessage())
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    // we want to go through all elements even though one may be bad so all failures are captured
                    if (uniqueAccesses.contains(access.getType())) {
                        accessTypeIterator.remove();
                    } else {
                        valid = isValidPolicyItemAccess(access, failures, accessTypes) && valid;

                        uniqueAccesses.add(access.getType());
                    }
                }
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValid({}, {}, {}): {}", accesses, failures, serviceDef, valid);

        return valid;
    }

    boolean isValidPolicyItemAccess(RangerPolicyItemAccess access, List<ValidationFailureDetails> failures, Set<String> accessTypes) {
        LOG.debug("==> RangerPolicyValidator.isValidPolicyItemAccess({}, {}, {})", access, failures, accessTypes);

        boolean valid = true;
        if (CollectionUtils.isEmpty(accessTypes)) { // caller should firewall this argument!
            LOG.debug("isValidPolicyItemAccess: accessTypes was null!");
        } else if (access == null) {
            LOG.debug("isValidPolicyItemAccess: policy item access was null!");
        } else {
            String accessType = access.getType();

            if (StringUtils.isBlank(accessType)) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_MISSING_FIELD;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("policy item access type")
                        .isMissing()
                        .becauseOf(error.getMessage("policy item access type"))
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            } else {
                String matchedAccessType = getMatchedAccessType(accessType, accessTypes);

                if (StringUtils.isEmpty(matchedAccessType)) {
                    ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_ITEM_ACCESS_TYPE_INVALID;

                    failures.add(new ValidationFailureDetailsBuilder()
                            .field("policy item access type")
                            .isSemanticallyIncorrect()
                            .becauseOf(error.getMessage(accessType, accessTypes))
                            .errorCode(error.getErrorCode())
                            .build());

                    valid = false;
                } else {
                    access.setType(matchedAccessType);
                }
            }

            Boolean isAllowed = access.getIsAllowed();

            // it can be null (which is treated as allowed) but not false
            if (isAllowed != null && !isAllowed) {
                ValidationErrorCode error = ValidationErrorCode.POLICY_VALIDATION_ERR_POLICY_ITEM_ACCESS_TYPE_DENY;

                failures.add(new ValidationFailureDetailsBuilder()
                        .field("policy item access type allowed")
                        .isSemanticallyIncorrect()
                        .becauseOf(error.getMessage())
                        .errorCode(error.getErrorCode())
                        .build());

                valid = false;
            }
        }

        LOG.debug("<== RangerPolicyValidator.isValidPolicyItemAccess({}, {}, {}): {})", access, failures, accessTypes, valid);

        return valid;
    }

    String getMatchedAccessType(String accessType, Set<String> validAccessTypes) {
        String ret = null;

        for (String validType : validAccessTypes) {
            if (StringUtils.equalsIgnoreCase(accessType, validType)) {
                ret = validType;

                break;
            }
        }

        return ret;
    }

    private String getDuplicate(List<String> values) {
        String duplicate = "";

        if (values != null) {
            HashSet<String> set = new HashSet<>();

            for (String val : values) {
                if (set.contains(val)) {
                    duplicate = val;

                    break;
                }

                set.add(val);
            }
        }

        return duplicate;
    }

    private static void removeDuplicates(List<String> values) {
        if (values == null || values.isEmpty()) {
            return;
        }

        HashSet<String> uniqueElements = new HashSet<>();

        values.replaceAll(e -> e == null ? null : e.trim());
        values.removeIf(e -> !uniqueElements.add(e));
    }
}
