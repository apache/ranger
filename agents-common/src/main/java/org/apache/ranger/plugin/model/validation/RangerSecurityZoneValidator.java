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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.errors.ValidationErrorCode;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerPolicyResourceSignature;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerSecurityZone.RangerSecurityZoneService;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerResourceTrie;
import org.apache.ranger.plugin.policyresourcematcher.RangerDefaultPolicyResourceMatcher;
import org.apache.ranger.plugin.policyresourcematcher.RangerPolicyResourceMatcher;
import org.apache.ranger.plugin.store.EmbeddedServiceDefsUtil;
import org.apache.ranger.plugin.store.SecurityZoneStore;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.RangerResourceEvaluatorsRetriever;
import org.apache.ranger.plugin.util.SearchFilter;
import org.apache.ranger.plugin.util.ServiceDefUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.ranger.plugin.model.RangerPolicy.POLICY_TYPES;

public class RangerSecurityZoneValidator extends RangerValidator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerSecurityZoneValidator.class);

    private final SecurityZoneStore securityZoneStore;

    public RangerSecurityZoneValidator(ServiceStore store, SecurityZoneStore securityZoneStore) {
        super(store);

        this.securityZoneStore = securityZoneStore;
    }

    public void validate(RangerSecurityZone securityZone, Action action) throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> RangerSecurityZoneValidator.validate(%s, %s)", securityZone, action));
        }

        List<ValidationFailureDetails> failures = new ArrayList<>();
        boolean                        valid    = isValid(securityZone, action, failures);

        try {
            if (!valid) {
                String message = serializeFailures(failures);

                throw new Exception(message);
            }
        } finally {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("<== RangerSecurityZoneValidator.validate(%s, %s)", securityZone, action));
            }
        }
    }

    @Override
    boolean isValid(String name, Action action, List<ValidationFailureDetails> failures) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> RangerSecurityZoneValidator.isValid(%s, %s, %s)", name, action, failures));
        }

        boolean ret = true;

        if (action != Action.DELETE) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_UNSUPPORTED_ACTION;

            failures.add(new ValidationFailureDetailsBuilder().isAnInternalError().becauseOf(error.getMessage()).errorCode(error.getErrorCode()).build());
            ret = false;
        } else if (StringUtils.isEmpty(name)) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_MISSING_FIELD;

            failures.add(new ValidationFailureDetailsBuilder().becauseOf("security zone name was null/missing").field("name").isMissing().errorCode(error.getErrorCode()).becauseOf(error.getMessage("name")).build());
            ret = false;
        } else if (getSecurityZone(name) == null) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_INVALID_ZONE_ID;

            failures.add(new ValidationFailureDetailsBuilder().becauseOf("security zone does not exist").field("name").errorCode(error.getErrorCode()).becauseOf(error.getMessage(name)).build());
            ret = false;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== RangerSecurityZoneValidator.isValid(%s, %s, %s) : %s", name, action, failures, ret));
        }

        return ret;
    }

    @Override
    boolean isValid(Long id, Action action, List<ValidationFailureDetails> failures) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> RangerSecurityZoneValidator.isValid(%s, %s, %s)", id, action, failures));
        }

        boolean ret = true;

        if (action != Action.DELETE) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_UNSUPPORTED_ACTION;

            failures.add(new ValidationFailureDetailsBuilder().isAnInternalError().becauseOf(error.getMessage()).errorCode(error.getErrorCode()).build());
            ret = false;
        } else if (id == null) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_MISSING_FIELD;

            failures.add(new ValidationFailureDetailsBuilder().becauseOf("security zone id was null/missing").field("id").isMissing().errorCode(error.getErrorCode()).becauseOf(error.getMessage("id")).build());
            ret = false;
        } else if (getSecurityZone(id) == null) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_INVALID_ZONE_ID;

            failures.add(new ValidationFailureDetailsBuilder().becauseOf("security zone id does not exist").field("id").errorCode(error.getErrorCode()).becauseOf(error.getMessage(id)).build());
            ret = false;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== RangerSecurityZoneValidator.isValid(%s, %s, %s) : %s", id, action, failures, ret));
        }

        return ret;
    }

    private boolean isValid(RangerSecurityZone securityZone, Action action, List<ValidationFailureDetails> failures) {
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> RangerSecurityZoneValidator.isValid(%s, %s, %s)", securityZone, action, failures));
        }

        if (!(action == Action.CREATE || action == Action.UPDATE)) {
            throw new IllegalArgumentException("isValid(RangerSecurityZone, ...) is only supported for create/update");
        }

        boolean      ret      = true;
        final String zoneName = securityZone.getName();

        if (StringUtils.isEmpty(StringUtils.trim(zoneName))) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_MISSING_FIELD;

            failures.add(new ValidationFailureDetailsBuilder().becauseOf("security zone name was null/missing").field("name").isMissing().errorCode(error.getErrorCode()).becauseOf(error.getMessage("name")).build());
            ret = false;
        }

        RangerSecurityZone existingZone;

        if (action == Action.CREATE) {
            securityZone.setId(-1L);

            existingZone = getSecurityZone(zoneName);

            if (existingZone != null) {
                ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_ZONE_NAME_CONFLICT;

                failures.add(new ValidationFailureDetailsBuilder().becauseOf("security zone name exists").field("name").errorCode(error.getErrorCode()).becauseOf(error.getMessage(existingZone.getId())).build());
                ret = false;
            }
        } else {
            Long zoneId = securityZone.getId();

            existingZone = getSecurityZone(zoneId);

            if (existingZone == null) {
                ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_INVALID_ZONE_ID;

                failures.add(new ValidationFailureDetailsBuilder().becauseOf("security zone with id does not exist").field("id").errorCode(error.getErrorCode()).becauseOf(error.getMessage(zoneId)).build());
                ret = false;
            } else if (StringUtils.isNotEmpty(StringUtils.trim(zoneName)) && !StringUtils.equals(zoneName, existingZone.getName())) {
                existingZone = getSecurityZone(zoneName);

                if (existingZone != null) {
                    ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_ZONE_NAME_CONFLICT;

                    failures.add(new ValidationFailureDetailsBuilder().becauseOf("security zone name").field("name").errorCode(error.getErrorCode()).becauseOf(error.getMessage(existingZone.getId())).build());
                    ret = false;
                }
            }
        }

        ret = ret && validateWithinSecurityZone(securityZone, action, failures);

        ret = ret && validateAgainstAllSecurityZones(securityZone, action, failures);

        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== RangerSecurityZoneValidator.isValid(%s, %s, %s) : %s", securityZone, action, failures, ret));
        }

        return ret;
    }

    private boolean validateWithinSecurityZone(RangerSecurityZone securityZone, Action action, List<ValidationFailureDetails> failures) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> RangerSecurityZoneValidator.validateWithinSecurityZone(%s, %s, %s)", securityZone, action, failures));
        }

        boolean ret = true;

        // admin users, user-groups and roles collections can't be empty
        if (CollectionUtils.isEmpty(securityZone.getAdminUsers()) && CollectionUtils.isEmpty(securityZone.getAdminUserGroups()) && CollectionUtils.isEmpty(securityZone.getAdminRoles())) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_MISSING_USER_AND_GROUPS_AND_ROLES;

            failures.add(new ValidationFailureDetailsBuilder().field("security zone admin users/user-groups/roles").isMissing().becauseOf(error.getMessage()).errorCode(error.getErrorCode()).build());
            ret = false;
        }
        // audit users, user-groups and roles collections can't be empty
        if (CollectionUtils.isEmpty(securityZone.getAuditUsers()) && CollectionUtils.isEmpty(securityZone.getAuditUserGroups()) && CollectionUtils.isEmpty(securityZone.getAuditRoles())) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_MISSING_USER_AND_GROUPS_AND_ROLES;

            failures.add(new ValidationFailureDetailsBuilder().field("security zone audit users/user-groups/roles").isMissing().becauseOf(error.getMessage()).errorCode(error.getErrorCode()).build());
            ret = false;
        }

        // Validate each service for existence, not being tag-service and each resource-spec for validity
        if (MapUtils.isNotEmpty(securityZone.getServices())) {
            for (Map.Entry<String, RangerSecurityZoneService> entry : securityZone.getServices().entrySet()) {
                String                    serviceName         = entry.getKey();
                RangerSecurityZoneService securityZoneService = entry.getValue();

                ret = validateSecurityZoneService(serviceName, securityZoneService, failures) && ret;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== RangerSecurityZoneValidator.validateWithinSecurityZone(%s, %s, %s) : %s", securityZone, action, failures, ret));
        }

        return ret;
    }

    private boolean validateAgainstAllSecurityZones(RangerSecurityZone securityZone, Action action, List<ValidationFailureDetails> failures) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> RangerSecurityZoneValidator.validateAgainstAllSecurityZones(%s, %s, %s)", securityZone, action, failures));
        }

        boolean      ret = true;
        final String zoneName;

        if (securityZone.getId() != -1L) {
            RangerSecurityZone existingZone = getSecurityZone(securityZone.getId());

            zoneName = existingZone.getName();
        } else {
            zoneName = securityZone.getName();
        }

        for (Map.Entry<String, RangerSecurityZoneService> entry:  securityZone.getServices().entrySet()) {
            String                    serviceName         = entry.getKey();
            RangerSecurityZoneService securityZoneService = entry.getValue();

            if (CollectionUtils.isEmpty(securityZoneService.getResources())) {
                continue;
            }

            SearchFilter             filter = new SearchFilter();
            List<RangerSecurityZone> zones  = null;

            filter.setParam(SearchFilter.SERVICE_NAME, serviceName);
            filter.setParam(SearchFilter.NOT_ZONE_NAME, zoneName);

            try {
                zones = securityZoneStore.getSecurityZones(filter);
            } catch (Exception excp) {
                LOG.error("Failed to get Security-Zones", excp);
                ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_INTERNAL_ERROR;

                failures.add(new ValidationFailureDetailsBuilder().becauseOf(error.getMessage(excp.getMessage())).errorCode(error.getErrorCode()).build());
                ret = false;
            }

            if (CollectionUtils.isEmpty(zones)) {
                continue;
            }

            RangerService    service    = getService(serviceName);
            RangerServiceDef serviceDef = service != null ? getServiceDef(service.getType()) : null;

            if (serviceDef == null) {
                ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_INTERNAL_ERROR;

                failures.add(new ValidationFailureDetailsBuilder().becauseOf(error.getMessage(serviceName)).errorCode(error.getErrorCode()).build());
                ret = false;
            } else {
                zones.add(securityZone);
                ret = ret && validateZoneServiceInAllZones(zones, serviceName, serviceDef, failures);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== RangerSecurityZoneValidator.validateAgainstAllSecurityZones(%s, %s, %s) : %s", securityZone, action, failures, ret));
        }

        return ret;
    }

    private boolean validateZoneServiceInAllZones(List<RangerSecurityZone> zones, String serviceName, RangerServiceDef serviceDef, List<ValidationFailureDetails> failures) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> RangerSecurityZoneValidator.validateZoneServiceInAllZones(%s, %s, %s, %s)", zones, serviceName, serviceDef, failures));
        }

        boolean                         ret              = true;
        RangerServiceDefHelper          serviceDefHelper = new RangerServiceDefHelper(serviceDef);
        List<RangerZoneResourceMatcher> matchers         = new ArrayList<>();
        Set<String>                     resourceNames    = new HashSet<>();

        // For each zone, get list-of-resources corresponding to serviceName.
        //    For each list-of-resources:
        //       get one resource (this is a map of <String, List<String>>); convert it into map of <String, RangerPolicyResource>. excludes is always false, recursive true only for HDFS
        //       build a subclass of RangerPolicyResourceEvaluator with id of zone, zoneName as a member, and RangerDefaultResourceMatcher as matcher.
        //       add this to list-of-evaluators
        for (RangerSecurityZone zone : zones) {
            Map<String, RangerSecurityZoneService> zoneServices = zone.getServices();
            RangerSecurityZoneService              zoneService  = zoneServices != null ? zoneServices.get(serviceName) : null;
            List<HashMap<String, List<String>>>    resources    = zoneService != null ? zoneService.getResources() : null;

            if (CollectionUtils.isEmpty(resources)) {
                continue;
            }

            for (Map<String, List<String>> resource : resources) {
                Map<String, RangerPolicyResource> policyResources = new HashMap<>();

                for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
                    String       resourceDefName = entry.getKey();
                    List<String> resourceValues  = entry.getValue();

                    RangerPolicyResource policyResource = new RangerPolicyResource(resourceValues, false, EmbeddedServiceDefsUtil.isRecursiveEnabled(serviceDef, resourceDefName));

                    policyResources.put(resourceDefName, policyResource);
                }

                RangerZoneResourceMatcher matcher = new RangerZoneResourceMatcher(zone.getName(), policyResources, serviceDefHelper, null);

                matchers.add(matcher);
                resourceNames.addAll(policyResources.keySet());
            }
        }

        // Build a map of trie with list-of-evaluators with one entry corresponds to one resource-def if it exists in the list-of-resources

        Map<String, RangerResourceTrie<RangerZoneResourceMatcher>> trieMap = new HashMap<>();

        for (String resourceName : resourceNames) {
            RangerResourceDef resourceDef = ServiceDefUtil.getResourceDef(serviceDef, resourceName);

            trieMap.put(resourceName, new RangerResourceTrie<>(resourceDef, matchers));
        }

        // For each zone, get list-of-resources corresponding to serviceName
        //    For each list-of-resources:
        //       get one resource; for each level in the resource, run it through map of trie and get possible evaluators.
        //       check each evaluator to see if the resource-match actually happens. If yes then add the zone-evaluator to matching evaluators.
        //       flag error if there are more than one matching evaluators with different zone-ids.
        //

        for (RangerSecurityZone zone : zones) {
            List<HashMap<String, List<String>>> resources = zone.getServices().get(serviceName).getResources();

            for (Map<String, List<String>> resource : resources) {
                Collection<RangerZoneResourceMatcher> smallestList = RangerResourceEvaluatorsRetriever.getEvaluators(trieMap, resource);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Resource:[" + resource +"], matched-zones:[" + smallestList +"]");
                }

                if (CollectionUtils.isEmpty(smallestList) || smallestList.size() == 1) {
                    continue;
                }

                RangerAccessResourceImpl accessResource = new RangerAccessResourceImpl();

                accessResource.setServiceDef(serviceDef);

                for (Map.Entry<String, List<String>> entry : resource.entrySet()) {
                    accessResource.setValue(entry.getKey(), entry.getValue());
                }

                Set<String> matchedZoneNames = new HashSet<>();

                for (RangerZoneResourceMatcher zoneMatcher : smallestList) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Trying to match resource:[" + accessResource +"] using zoneMatcher:[" + zoneMatcher + "]");
                    }
                    // These are potential matches. Try to really match them
                    if (zoneMatcher.getPolicyResourceMatcher().isMatch(accessResource, RangerPolicyResourceMatcher.MatchScope.ANY, null)) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Matched resource:[" + accessResource +"] using zoneMatcher:[" + zoneMatcher + "]");
                        }
                        // Actual match happened
                        matchedZoneNames.add(zoneMatcher.getSecurityZoneName());
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Did not match resource:[" + accessResource +"] using zoneMatcher:[" + zoneMatcher + "]");
                        }
                    }
                }
                LOG.info("The following zone-names matched resource:[" + resource + "]: " + matchedZoneNames);

                if (matchedZoneNames.size() > 1) {
                    ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_ZONE_RESOURCE_CONFLICT;

                    failures.add(new ValidationFailureDetailsBuilder().becauseOf(error.getMessage(matchedZoneNames, resource)).errorCode(error.getErrorCode()).build());
                    ret = false;
                    break;
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== RangerSecurityZoneValidator.validateZoneServiceInAllZones(%s, %s, %s, %s) : %s", zones, serviceName, serviceDef, failures, ret));
        }
        return ret;
    }

    private boolean validateSecurityZoneService(String serviceName, RangerSecurityZoneService securityZoneService, List<ValidationFailureDetails> failures) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> RangerSecurityZoneValidator.validateSecurityZoneService(%s, %s, %s)", serviceName, securityZoneService, failures));
        }

        boolean       ret     = true;
        RangerService service = getService(serviceName); // Verify service with serviceName exists

        if (service == null) {
            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_INVALID_SERVICE_NAME;

            failures.add(new ValidationFailureDetailsBuilder().field("security zone resource service-name").becauseOf(error.getMessage(serviceName)).errorCode(error.getErrorCode()).build());
            ret = false;
        } else {
            RangerServiceDef serviceDef = getServiceDef(service.getType());

            if (serviceDef == null) {
                ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_INVALID_SERVICE_TYPE;

                failures.add(new ValidationFailureDetailsBuilder().field("security zone resource service-type").becauseOf(error.getMessage(service.getType())).errorCode(error.getErrorCode()).build());
                ret = false;
            } else {
                if (CollectionUtils.isNotEmpty(securityZoneService.getResources())) {
                    // For each resource-spec, verify that it forms valid hierarchy for some policy-type
                    Set<String> resourceSignatures = new HashSet<>();

                    for (Map<String, List<String>> resource : securityZoneService.getResources()) {
                        Set<String>            resourceDefNames = resource.keySet();
                        RangerServiceDefHelper serviceDefHelper = new RangerServiceDefHelper(serviceDef);
                        boolean                isValidHierarchy = false;

                        for (int policyType : POLICY_TYPES) {
                            Set<List<RangerResourceDef>> resourceHierarchies = serviceDefHelper.getResourceHierarchies(policyType, resourceDefNames);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Size of resourceHierarchies for resourceDefNames:[" + resourceDefNames + ", policyType=" + policyType + "] = " + resourceHierarchies.size());
                            }

                            for (List<RangerResourceDef> resourceHierarchy : resourceHierarchies) {
                                if (RangerDefaultPolicyResourceMatcher.isHierarchyValidForResources(resourceHierarchy, resource)) {
                                    isValidHierarchy = true;
                                    break;
                                } else {
                                    LOG.info("gaps found in resource, skipping hierarchy:[" + resourceHierarchies + "]");
                                }
                            }
                        }

                        if (!isValidHierarchy) {
                            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_INVALID_RESOURCE_HIERARCHY;

                            failures.add(new ValidationFailureDetailsBuilder().field("security zone resource hierarchy").becauseOf(error.getMessage(serviceName, resourceDefNames)).errorCode(error.getErrorCode()).build());
                            ret = false;
                        }

                        for (Map.Entry<String, List<String>> resourceEntry : resource.entrySet()) {
                            String       resourceName   = resourceEntry.getKey();
                            List<String> resourceValues = resourceEntry.getValue();

                            if (CollectionUtils.isEmpty(resourceValues)) {
                                ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_MISSING_RESOURCES;

                                failures.add(new ValidationFailureDetailsBuilder().field("security zone resources")
                                        .subField("resources").isMissing()
                                        .becauseOf(error.getMessage(resourceName))
                                        .errorCode(error.getErrorCode()).build());
                                ret = false;
                            }
                        }

                        RangerPolicyResourceSignature resourceSignature = RangerPolicyResourceSignature.from(resource);

                        if (!resourceSignatures.add(resourceSignature.getSignature())) {
                            ValidationErrorCode error = ValidationErrorCode.SECURITY_ZONE_VALIDATION_ERR_DUPLICATE_RESOURCE_ENTRY;

                            failures.add(new ValidationFailureDetailsBuilder().field("security zone resources")
                                                 .subField("resources")
                                                 .becauseOf(error.getMessage(resource, serviceName))
                                                 .errorCode(error.getErrorCode()).build());
                            ret = false;
                        }
                    }
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== RangerSecurityZoneValidator.validateSecurityZoneService(%s, %s, %s) : %s", serviceName, securityZoneService, failures, ret));
        }

        return ret;
    }
}
