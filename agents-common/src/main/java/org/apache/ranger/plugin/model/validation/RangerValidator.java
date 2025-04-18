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
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.plugin.errors.ValidationErrorCode;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerPolicy.RangerPolicyResource;
import org.apache.ranger.plugin.model.RangerRole;
import org.apache.ranger.plugin.model.RangerSecurityZone;
import org.apache.ranger.plugin.model.RangerService;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerAccessTypeDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerEnumElementDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerResourceDef;
import org.apache.ranger.plugin.model.RangerServiceDef.RangerServiceConfigDef;
import org.apache.ranger.plugin.store.RoleStore;
import org.apache.ranger.plugin.store.ServiceStore;
import org.apache.ranger.plugin.util.RangerObjectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class RangerValidator {
    private static final Logger LOG = LoggerFactory.getLogger(RangerValidator.class);

    RoleStore           roleStore;
    ServiceStore        store;
    RangerObjectFactory factory = new RangerObjectFactory();

    protected RangerValidator(ServiceStore store) {
        if (store == null) {
            throw new IllegalArgumentException("ServiceValidator(): store is null!");
        }

        this.store = store;
    }

    protected RangerValidator(RoleStore roleStore) {
        if (roleStore == null) {
            throw new IllegalArgumentException("ServiceValidator(): store is null!");
        }

        this.roleStore = roleStore;
    }

    public static String serializeFailures(List<ValidationFailureDetails> failures) {
        LOG.debug("==> RangerValidator.getFailureMessage()");

        String message = null;

        if (CollectionUtils.isEmpty(failures)) {
            LOG.warn("serializeFailures: called while list of failures is null/empty!");
        } else {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < failures.size(); i++) {
                builder.append(String.format("(%d)", i));
                builder.append(failures.get(i));
                builder.append(" ");
            }

            message = builder.toString();
        }

        LOG.debug("<== RangerValidator.serializeFailures(): {}", message);

        return message;
    }

    static Map<Integer, Integer> createMap(int[][] data) {
        Map<Integer, Integer> result = new HashMap<>();

        if (data != null) {
            for (int[] row : data) {
                Integer key   = row[0];
                Integer value = row[1];

                if (result.containsKey(key)) {
                    LOG.warn("createMap: Internal error: duplicate key: multiple rows found for [{}]. Skipped", key);
                } else {
                    result.put(key, value);
                }
            }
        }

        return result;
    }

    static Map<Integer, String> createMap(Object[][] data) {
        Map<Integer, String> result = new HashMap<>();

        if (data != null) {
            for (Object[] row : data) {
                Integer key   = (Integer) row[0];
                String  value = (String) row[1];

                if (key == null) {
                    LOG.warn("createMap: error converting key[{}] to Integer! Sipped!", key);
                } else if (StringUtils.isEmpty(value)) {
                    LOG.warn("createMap: empty/null value.  Skipped!");
                } else if (result.containsKey(key)) {
                    LOG.warn("createMap: Internal error: duplicate key.  Multiple rows found for [{}]. Skipped", key);
                } else {
                    result.put(key, value);
                }
            }
        }

        return result;
    }

    public void validate(Long id, Action action) throws Exception {
        LOG.debug("==> RangerValidator.validate({})", id);

        List<ValidationFailureDetails> failures = new ArrayList<>();

        if (isValid(id, action, failures)) {
            LOG.debug("<== RangerValidator.validate({}): valid", id);
        } else {
            String message = serializeFailures(failures);

            LOG.debug("<== RangerValidator.validate({}): invalid, reason[{}]", id, message);

            throw new Exception(message);
        }
    }

    public void validate(String name, Action action) throws Exception {
        LOG.debug("==> RangerValidator.validate({})", name);

        List<ValidationFailureDetails> failures = new ArrayList<>();

        if (isValid(name, action, failures)) {
            LOG.debug("<== RangerValidator.validate({}): valid", name);
        } else {
            String message = serializeFailures(failures);

            LOG.debug("<== RangerValidator.validate({}): invalid, reason[{}]", name, message);

            throw new Exception(message);
        }
    }

    /**
     * This method is expected to be overridden by sub-classes.  Default implementation provided to not burden implementers from having to implement methods that they know would never be called.
     *
     * @param id
     * @param action
     * @param failures
     * @return
     */
    boolean isValid(Long id, Action action, List<ValidationFailureDetails> failures) {
        failures.add(new ValidationFailureDetailsBuilder()
                .isAnInternalError()
                .becauseOf("unimplemented method called")
                .build());
        return false;
    }

    boolean isValid(String name, Action action, List<ValidationFailureDetails> failures) {
        failures.add(new ValidationFailureDetailsBuilder()
                .isAnInternalError()
                .becauseOf("unimplemented method called")
                .build());
        return false;
    }

    Set<String> getServiceConfigParameters(RangerService service) {
        if (service == null || service.getConfigs() == null) {
            return new HashSet<>();
        } else {
            return service.getConfigs().keySet();
        }
    }

    Set<String> getRequiredParameters(RangerServiceDef serviceDef) {
        LOG.debug("==> RangerValidator.getRequiredParameters({})", serviceDef);

        Set<String> result;

        if (serviceDef == null) {
            result = Collections.emptySet();
        } else {
            List<RangerServiceConfigDef> configs = serviceDef.getConfigs();

            if (CollectionUtils.isEmpty(configs)) {
                result = Collections.emptySet();
            } else {
                result = new HashSet<>(configs.size()); // at worse all of the config items are required!

                for (RangerServiceConfigDef configDef : configs) {
                    if (configDef.getMandatory()) {
                        result.add(configDef.getName());
                    }
                }
            }
        }

        LOG.debug("<== RangerValidator.getRequiredParameters({}): {}", serviceDef, result);

        return result;
    }

    RangerServiceDef getServiceDef(Long id) {
        LOG.debug("==> RangerValidator.getServiceDef({})", id);

        RangerServiceDef result = null;

        try {
            result = store.getServiceDef(id);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving service def from service store!", e);
        }

        LOG.debug("<== RangerValidator.getServiceDef({}): {}", id, result);

        return result;
    }

    RangerServiceDef getServiceDef(String type) {
        LOG.debug("==> RangerValidator.getServiceDef({})", type);

        RangerServiceDef result = null;

        try {
            result = store.getServiceDefByName(type);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving service definition from service store!", e);
        }

        LOG.debug("<== RangerValidator.getServiceDef({}): {}", type, result);

        return result;
    }

    /**
     * @param displayName
     * @return {@link RangerServiceDef} - service using display name if present, <code>null</code> otherwise.
     */
    RangerServiceDef getServiceDefByDisplayName(final String displayName) {
        LOG.debug("==> RangerValidator.getServiceDefByDisplayName({})", displayName);

        RangerServiceDef result = null;

        try {
            result = store.getServiceDefByDisplayName(displayName);
        } catch (Exception e) {
            LOG.debug("Encountered exception while retrieving service definition from service store!", e);
        }

        LOG.debug("<== RangerValidator.getServiceDefByDisplayName({}): {}", displayName, result);

        return result;
    }

    RangerService getService(Long id) {
        LOG.debug("==> RangerValidator.getService({})", id);

        RangerService result = null;

        try {
            result = store.getService(id);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving service from service store!", e);
        }

        LOG.debug("<== RangerValidator.getService({}): {}", id, result);

        return result;
    }

    RangerService getService(String name) {
        LOG.debug("==> RangerValidator.getService({})", name);

        RangerService result = null;

        try {
            result = store.getServiceByName(name);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving service from service store!", e);
        }

        LOG.debug("<== RangerValidator.getService({}): {}", name, result);

        return result;
    }

    RangerService getServiceByDisplayName(final String displayName) {
        LOG.debug("==> RangerValidator.getService({})", displayName);

        RangerService result = null;

        try {
            result = store.getServiceByDisplayName(displayName);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving service from service store!", e);
        }

        LOG.debug("<== RangerValidator.getService({}): {}", displayName, result);

        return result;
    }

    boolean policyExists(Long id) {
        try {
            return store.policyExists(id);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving policy from service store!", e);

            return false;
        }
    }

    RangerPolicy getPolicy(Long id) {
        LOG.debug("==> RangerValidator.getPolicy({})", id);

        RangerPolicy result = null;

        try {
            result = store.getPolicy(id);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving policy from service store!", e);
        }

        LOG.debug("<== RangerValidator.getPolicy({}): {}", id, result);

        return result;
    }

    Long getPolicyId(final Long serviceId, final String policyName, final Long zoneId) {
        LOG.debug("==> RangerValidator.getPolicyId({}, {}, {})", serviceId, policyName, zoneId);

        Long policyId = store.getPolicyId(serviceId, policyName, zoneId);

        LOG.debug("<== RangerValidator.getPolicyId({}, {}, {}): policy-id[{}]", serviceId, policyName, zoneId, policyId);

        return policyId;
    }

    List<RangerPolicy> getPoliciesForResourceSignature(String serviceName, String policySignature) {
        LOG.debug("==> RangerValidator.getPoliciesForResourceSignature({}, {})", serviceName, policySignature);

        List<RangerPolicy> enabledPolicies  = new ArrayList<>();
        List<RangerPolicy> disabledPolicies = new ArrayList<>();

        try {
            enabledPolicies  = store.getPoliciesByResourceSignature(serviceName, policySignature, true);
            disabledPolicies = store.getPoliciesByResourceSignature(serviceName, policySignature, false);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving policies from service store!", e);
        }

        final List<RangerPolicy> ret;

        if (CollectionUtils.isEmpty(enabledPolicies)) {
            ret = disabledPolicies;
        } else if (CollectionUtils.isEmpty(disabledPolicies)) {
            ret = enabledPolicies;
        } else {
            ret = enabledPolicies;

            ret.addAll(disabledPolicies);
        }

        LOG.debug("<== RangerValidator.getPoliciesForResourceSignature({}, {}): count[{}], {}", serviceName, policySignature, (ret == null ? 0 : ret.size()), ret);

        return ret;
    }

    RangerSecurityZone getSecurityZone(Long id) {
        LOG.debug("==> RangerValidator.getSecurityZone({})", id);

        RangerSecurityZone result = null;

        if (id != null) {
            try {
                result = store.getSecurityZone(id);
            } catch (Exception e) {
                LOG.debug("Encountred exception while retrieving security zone from service store!", e);
            }
        }

        LOG.debug("<== RangerValidator.getSecurityZone({}): {}", id, result);

        return result;
    }

    RangerSecurityZone getSecurityZone(String name) {
        LOG.debug("==> RangerValidator.getSecurityZone({})", name);

        RangerSecurityZone result = null;

        if (StringUtils.isNotEmpty(name)) {
            try {
                result = store.getSecurityZone(name);
            } catch (Exception e) {
                LOG.debug("Encountred exception while retrieving security zone from service store!", e);
            }
        }

        LOG.debug("<== RangerValidator.getSecurityZone({}): {}", name, result);

        return result;
    }

    Set<String> getAccessTypes(RangerServiceDef serviceDef) {
        LOG.debug("==> RangerValidator.getAccessTypes({})", serviceDef);

        Set<String> accessTypes = new HashSet<>();

        if (serviceDef == null) {
            LOG.warn("serviceDef passed in was null!");
        } else if (CollectionUtils.isEmpty(serviceDef.getAccessTypes())) {
            LOG.warn("AccessTypeDef collection on serviceDef was null!");
        } else {
            for (RangerAccessTypeDef accessTypeDef : serviceDef.getAccessTypes()) {
                if (accessTypeDef == null) {
                    LOG.warn("Access type def was null!");
                } else {
                    String accessType = accessTypeDef.getName();

                    if (StringUtils.isBlank(accessType)) {
                        LOG.warn("Access type def name was null/empty/blank!");
                    } else {
                        accessTypes.add(accessType);
                    }
                }
            }

            if (serviceDef.getMarkerAccessTypes() != null) {
                for (RangerAccessTypeDef accessTypeDef : serviceDef.getMarkerAccessTypes()) {
                    if (accessTypeDef != null) {
                        accessTypes.add(accessTypeDef.getName());
                    }
                }
            }
        }

        LOG.debug("<== RangerValidator.getAccessTypes({}): {}", serviceDef, accessTypes);

        return accessTypes;
    }

    /**
     * This function exists to encapsulates the current behavior of code which treats and unspecified audit preference to mean audit is enabled.
     *
     * @param policy
     * @return
     */
    boolean getIsAuditEnabled(RangerPolicy policy) {
        LOG.debug("<== RangerValidator.getIsAuditEnabled({})", policy);

        boolean isEnabled = false;

        if (policy == null) {
            LOG.warn("policy was null!");
        } else if (policy.getIsAuditEnabled() == null) {
            isEnabled = true;
        } else {
            isEnabled = policy.getIsAuditEnabled();
        }

        LOG.debug("<== RangerValidator.getIsAuditEnabled({}): {}", policy, isEnabled);

        return isEnabled;
    }

    /**
     * Returns names of resource types set to lower-case to allow for case-insensitive comparison.
     *
     * @param serviceDef
     * @return
     */
    Set<String> getMandatoryResourceNames(RangerServiceDef serviceDef) {
        LOG.debug("==> RangerValidator.getMandatoryResourceNames({})", serviceDef);

        Set<String> resourceNames = new HashSet<>();

        if (serviceDef == null) {
            LOG.warn("serviceDef passed in was null!");
        } else if (CollectionUtils.isEmpty(serviceDef.getResources())) {
            LOG.warn("ResourceDef collection on serviceDef was null!");
        } else {
            for (RangerResourceDef resourceTypeDef : serviceDef.getResources()) {
                if (resourceTypeDef == null) {
                    LOG.warn("resource type def was null!");
                } else {
                    Boolean mandatory = resourceTypeDef.getMandatory();

                    if (mandatory != null && mandatory) {
                        String resourceName = resourceTypeDef.getName();

                        if (StringUtils.isBlank(resourceName)) {
                            LOG.warn("Resource def name was null/empty/blank!");
                        } else {
                            resourceNames.add(resourceName.toLowerCase());
                        }
                    }
                }
            }
        }

        LOG.debug("<== RangerValidator.getMandatoryResourceNames({}): {}", serviceDef, resourceNames);

        return resourceNames;
    }

    Set<String> getAllResourceNames(RangerServiceDef serviceDef) {
        LOG.debug("==> RangerValidator.getAllResourceNames({})", serviceDef);

        Set<String> resourceNames = new HashSet<>();

        if (serviceDef == null) {
            LOG.warn("serviceDef passed in was null!");
        } else if (CollectionUtils.isEmpty(serviceDef.getResources())) {
            LOG.warn("ResourceDef collection on serviceDef was null!");
        } else {
            for (RangerResourceDef resourceTypeDef : serviceDef.getResources()) {
                if (resourceTypeDef == null) {
                    LOG.warn("resource type def was null!");
                } else {
                    String resourceName = resourceTypeDef.getName();

                    if (StringUtils.isBlank(resourceName)) {
                        LOG.warn("Resource def name was null/empty/blank!");
                    } else {
                        resourceNames.add(resourceName.toLowerCase());
                    }
                }
            }
        }

        LOG.debug("<== RangerValidator.getAllResourceNames({}): {}", serviceDef, resourceNames);

        return resourceNames;
    }

    /**
     * Converts, in place, the resources defined in the policy to have lower-case resource-def-names
     *
     * @param resources
     * @return
     */
    void convertPolicyResourceNamesToLower(Map<String, RangerPolicyResource> resources) {
        Map<String, RangerPolicyResource> lowerCasePolicyResources = new HashMap<>();

        if (resources != null) {
            for (Map.Entry<String, RangerPolicyResource> entry : resources.entrySet()) {
                String lowerCasekey = entry.getKey().toLowerCase();

                lowerCasePolicyResources.put(lowerCasekey, entry.getValue());
            }

            resources.clear();
            resources.putAll(lowerCasePolicyResources);
        }
    }

    Map<String, String> getValidationRegExes(RangerServiceDef serviceDef) {
        if (serviceDef == null || CollectionUtils.isEmpty(serviceDef.getResources())) {
            return new HashMap<>();
        } else {
            Map<String, String> result = new HashMap<>();

            for (RangerResourceDef resourceDef : serviceDef.getResources()) {
                if (resourceDef == null) {
                    LOG.warn("A resource def in resource def collection is null");
                } else {
                    String name  = resourceDef.getName();
                    String regEx = resourceDef.getValidationRegEx();

                    if (StringUtils.isBlank(name)) {
                        LOG.warn("resource name is null/empty/blank");
                    } else if (StringUtils.isBlank(regEx)) {
                        LOG.debug("validation regex is null/empty/blank");
                    } else {
                        result.put(name, regEx);
                    }
                }
            }

            return result;
        }
    }

    int getEnumDefaultIndex(RangerEnumDef enumDef) {
        final int index;

        if (enumDef == null) {
            index = -1;
        } else if (enumDef.getDefaultIndex() == null) {
            index = 0;
        } else {
            index = enumDef.getDefaultIndex();
        }

        return index;
    }

    Collection<String> getImpliedGrants(RangerAccessTypeDef def) {
        if (def == null) {
            return null;
        } else if (CollectionUtils.isEmpty(def.getImpliedGrants())) {
            return new ArrayList<>();
        } else {
            List<String> result = new ArrayList<>(def.getImpliedGrants().size());

            for (String name : def.getImpliedGrants()) {
                if (StringUtils.isBlank(name)) {
                    result.add(name); // could be null!
                } else {
                    result.add(name.toLowerCase());
                }
            }

            return result;
        }
    }

    /**
     * Returns a copy of the policy resource map where all keys (resource-names) are lowercase
     *
     * @param input
     * @return
     */
    Map<String, RangerPolicyResource> getPolicyResourceWithLowerCaseKeys(Map<String, RangerPolicyResource> input) {
        if (input == null) {
            return null;
        }

        Map<String, RangerPolicyResource> output = new HashMap<>(input.size());

        for (Map.Entry<String, RangerPolicyResource> entry : input.entrySet()) {
            output.put(entry.getKey().toLowerCase(), entry.getValue());
        }

        return output;
    }

    boolean isUnique(Long value, Set<Long> alreadySeen, String valueName, String collectionName, List<ValidationFailureDetails> failures) {
        return isUnique(value, null, alreadySeen, valueName, collectionName, failures);
    }

    /**
     * NOTE: <code>alreadySeen</code> collection passed in gets updated.
     *
     * @param value
     * @param alreadySeen
     * @param valueName - use-friendly name of the <code>value</code> that would be used when generating failure message
     * @param collectionName - use-friendly name of the <code>value</code> collection that would be used when generating failure message
     * @param failures
     * @return
     */
    boolean isUnique(Long value, String valueContext, Set<Long> alreadySeen, String valueName, String collectionName, List<ValidationFailureDetails> failures) {
        LOG.debug("==> RangerServiceDefValidator.isValueUnique({}, {}, {}, {}, {})", value, alreadySeen, valueName, collectionName, failures);

        boolean valid = true;

        if (value == null) {  // null/empty/blank value is an error
            failures.add(new ValidationFailureDetailsBuilder()
                    .field(valueName)
                    .subField(valueContext)
                    .isMissing()
                    .becauseOf(String.format("%s[%s] is null/empty", valueName, value))
                    .build());

            valid = false;
        } else if (alreadySeen.contains(value)) { // it shouldn't have been seen already
            failures.add(new ValidationFailureDetailsBuilder()
                    .field(valueName)
                    .subField(value.toString())
                    .isSemanticallyIncorrect()
                    .becauseOf(String.format("duplicate %s[%s] in %s", valueName, value, collectionName))
                    .build());

            valid = false;
        } else {
            alreadySeen.add(value); // we have a new unique access type
        }

        LOG.debug("==> RangerServiceDefValidator.isValueUnique({}, {}, {}, {}, {}): {}", value, alreadySeen, valueName, collectionName, failures, valid);

        return valid;
    }

    /**
     * NOTE: <code>alreadySeen</code> collection passed in gets updated.
     *
     * @param value
     * @param alreadySeen
     * @param valueName - use-friendly name of the <code>value</code> that would be used when generating failure message
     * @param collectionName - use-friendly name of the <code>value</code> collection that would be used when generating failure message
     * @param failures
     * @return
     */
    boolean isUnique(Integer value, Set<Integer> alreadySeen, String valueName, String collectionName, List<ValidationFailureDetails> failures) {
        LOG.debug("==> RangerServiceDefValidator.isValueUnique({}, {}, {}, {}, {})", value, alreadySeen, valueName, collectionName, failures);

        boolean valid = true;

        if (value == null) {  // null/empty/blank value is an error
            failures.add(new ValidationFailureDetailsBuilder()
                    .field(valueName)
                    .isMissing()
                    .becauseOf(String.format("%s[%s] is null/empty", valueName, value))
                    .build());

            valid = false;
        } else if (alreadySeen.contains(value)) { // it shouldn't have been seen already
            failures.add(new ValidationFailureDetailsBuilder()
                    .field(valueName)
                    .subField(value.toString())
                    .isSemanticallyIncorrect()
                    .becauseOf(String.format("duplicate %s[%s] in %s", valueName, value, collectionName))
                    .build());

            valid = false;
        } else {
            alreadySeen.add(value); // we have a new unique access type
        }

        LOG.debug("==> RangerServiceDefValidator.isValueUnique({}, {}, {}, {}, {}): {}", value, alreadySeen, valueName, collectionName, failures, valid);

        return valid;
    }

    /*
     * Important: Resource-names are required to be lowercase. This is used in validating policy create/update operations.
     * Ref: RANGER-2272
     */
    boolean isValidResourceName(final String value, final String valueContext, final List<ValidationFailureDetails> failures) {
        boolean ret = true;

        if (value != null && !StringUtils.isEmpty(value)) {
            int sz = value.length();

            for (int i = 0; i < sz; ++i) {
                char c = value.charAt(i);

                if (!(Character.isLowerCase(c) || c == '-' || c == '_')) { // Allow only lowercase, hyphen or underscore characters
                    ret = false;

                    break;
                }
            }
        } else {
            ret = false;
        }

        if (!ret) {
            ValidationErrorCode errorCode = ValidationErrorCode.SERVICE_DEF_VALIDATION_ERR_NOT_LOWERCASE_NAME;

            failures.add(new ValidationFailureDetailsBuilder()
                    .errorCode(errorCode.getErrorCode())
                    .field(value)
                    .becauseOf(errorCode.getMessage(valueContext, value))
                    .build());
        }

        return ret;
    }

    boolean isUnique(final String value, final Set<String> alreadySeen, final String valueName, final String collectionName, final List<ValidationFailureDetails> failures) {
        return isUnique(value, null, alreadySeen, valueName, collectionName, failures);
    }

    /**
     * NOTE: <code>alreadySeen</code> collection passed in gets updated.
     *
     * @param value
     * @param alreadySeen
     * @param valueName - use-friendly name of the <code>value</code> that would be used when generating failure message
     * @param collectionName - use-friendly name of the <code>value</code> collection that would be used when generating failure message
     * @param failures
     * @return
     */
    boolean isUnique(final String value, final String valueContext, final Set<String> alreadySeen, final String valueName, final String collectionName, final List<ValidationFailureDetails> failures) {
        LOG.debug("==> RangerServiceDefValidator.isValueUnique({}, {}, {}, {}, {})", value, alreadySeen, valueName, collectionName, failures);

        boolean valid = true;

        if (StringUtils.isBlank(value)) {  // null/empty/blank value is an error
            failures.add(new ValidationFailureDetailsBuilder()
                    .field(valueName)
                    .subField(valueContext)
                    .isMissing()
                    .becauseOf(String.format("%s[%s] is null/empty", valueName, value))
                    .build());

            valid = false;
        } else if (alreadySeen.contains(value.toLowerCase())) { // it shouldn't have been seen already
            failures.add(new ValidationFailureDetailsBuilder()
                    .field(valueName)
                    .subField(value)
                    .isSemanticallyIncorrect()
                    .becauseOf(String.format("duplicate %s[%s] in %s", valueName, value, collectionName))
                    .build());

            valid = false;
        } else {
            alreadySeen.add(value.toLowerCase()); // we have a new unique access type
        }

        LOG.debug("<== RangerServiceDefValidator.isValueUnique({}, {}, {}, {}, {}): {}", value, alreadySeen, valueName, collectionName, failures, valid);

        return valid;
    }

    Map<String, RangerEnumDef> getEnumDefMap(List<RangerEnumDef> enumDefs) {
        Map<String, RangerEnumDef> result = new HashMap<>();

        if (enumDefs != null) {
            for (RangerEnumDef enumDef : enumDefs) {
                result.put(enumDef.getName(), enumDef);
            }
        }

        return result;
    }

    Set<String> getEnumValues(RangerEnumDef enumDef) {
        Set<String> result = new HashSet<>();

        if (enumDef != null) {
            for (RangerEnumElementDef element : enumDef.getElements()) {
                result.add(element.getName());
            }
        }

        return result;
    }

    RangerRole getRangerRole(Long id) {
        LOG.debug("==> RangerValidator.getRangerRole({})", id);

        RangerRole result = null;

        try {
            result = roleStore.getRole(id);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving RangerRole from RoleStore store!", e);
        }

        LOG.debug("<== RangerValidator.getRangerRole({}): {}", id, result);

        return result;
    }

    boolean roleExists(Long id) {
        try {
            return roleStore.roleExists(id);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving RangerRole from role store!", e);

            return false;
        }
    }

    boolean roleExists(String name) {
        try {
            return roleStore.roleExists(name);
        } catch (Exception e) {
            LOG.debug("Encountred exception while retrieving RangerRole from role store!", e);

            return false;
        }
    }

    public enum Action {
        CREATE, UPDATE, DELETE
    }
}
