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

import { isEmpty, isArray } from "lodash";
import {
  getConstraintNameFromField,
  getConstraintDefByName
} from "Utils/XAUtils";

export const validatePolicyName = (policyName) => {
  if (!policyName || policyName.trim() === "") {
    return "Policy Name is required";
  }
  return undefined;
};

const hasPrincipals = (item) => {
  const usersObj = item.users || {};
  return (
    (usersObj.users || []).length > 0 ||
    (usersObj.groups || []).length > 0 ||
    (usersObj.roles || []).length > 0
  );
};

const hasAccessesInItem = (item) => {
  if (item.accesses?.tableList) {
    return item.accesses.tableList.length > 0;
  } else if (isArray(item.accesses)) {
    return item.accesses.length > 0;
  }
  return false;
};

const hasDataMaskInItem = (item) => {
  return !!(item.dataMaskInfo && item.dataMaskInfo.value);
};

const hasRowFilterInItem = (item) => {
  return !!(item.rowFilterInfo && item.rowFilterInfo.toString().trim() !== "");
};

const hasConditionsInItem = (item) => {
  return !isEmpty(item.conditions);
};

const validateAccessPolicyItem = (item) => {
  const ruleErrors = {};
  const principals = hasPrincipals(item);
  const accesses = hasAccessesInItem(item);
  const delegateAdmin = item.delegateAdmin;
  const conditions = hasConditionsInItem(item);

  if (principals && !accesses && !delegateAdmin) {
    ruleErrors.accesses = "Please select permission for this policy";
  }
  if (accesses && !principals && !delegateAdmin) {
    ruleErrors.users = "Please select users/groups/roles for this policy";
  }
  if (delegateAdmin && !principals) {
    ruleErrors.users = "Please select user/group/role for this policy";
  }
  if (conditions && !principals) {
    ruleErrors.users = "Please select user/group/role for this policy";
  }

  return ruleErrors;
};

const validateDataMaskPolicyItem = (item) => {
  const ruleErrors = {};
  const principals = hasPrincipals(item);
  const accesses = hasAccessesInItem(item);
  const dataMask = hasDataMaskInItem(item);

  if (principals && !accesses) {
    ruleErrors.accesses = "Please select permission for this policy";
  }
  if (principals && !dataMask) {
    ruleErrors.dataMaskInfo = "Please select a masking option for this policy";
  }
  if ((accesses || dataMask) && !principals) {
    ruleErrors.users = "Please select users/groups/roles for this policy";
  }

  return ruleErrors;
};

const validateRowFilterPolicyItem = (item) => {
  const ruleErrors = {};
  const principals = hasPrincipals(item);
  const accesses = hasAccessesInItem(item);
  const rowFilter = hasRowFilterInItem(item);

  if (principals && !accesses) {
    ruleErrors.accesses = "Please select permission for this policy";
  }
  if (principals && !rowFilter) {
    ruleErrors.rowFilterInfo = "Please enter a row level filter expression";
  }
  if ((accesses || rowFilter) && !principals) {
    ruleErrors.users = "Please select users/groups/roles for this policy";
  }

  return ruleErrors;
};

const hasConstraintNameInItem = (item) =>
  Boolean(getConstraintNameFromField(item.constraintInfo?.constraintName));

const hasMissingMandatoryConstraintParameters = (item, serviceDef) => {
  if (!serviceDef) {
    return false;
  }
  const constraintName = getConstraintNameFromField(
    item.constraintInfo?.constraintName
  );
  if (!constraintName) {
    return false;
  }
  const def = getConstraintDefByName(serviceDef, constraintName);
  const parameters = item.constraintInfo?.parameters || {};
  for (const param of def?.parameters || []) {
    if (param.mandatory) {
      const val = parameters[param.name];
      if (val === "" || val == null) {
        return true;
      }
    }
  }
  return false;
};

const validateConstraintPolicyItem = (item, serviceDef) => {
  const ruleErrors = {};
  const principals = hasPrincipals(item);
  const accesses = hasAccessesInItem(item);
  const constraintName = hasConstraintNameInItem(item);
  const missingMandatoryParams = hasMissingMandatoryConstraintParameters(
    item,
    serviceDef
  );

  if (principals && !constraintName) {
    ruleErrors.constraintName =
      "Please select a constraint name for this policy";
  }
  if (principals && constraintName && !accesses) {
    ruleErrors.accesses = "Please select permission for this policy";
  }
  if (principals && constraintName && missingMandatoryParams) {
    ruleErrors.parameters =
      "Please provide all required parameters for this constraint";
  }
  if ((accesses || constraintName) && !principals) {
    ruleErrors.users = "Please select users/groups/roles for this policy";
  }

  return ruleErrors;
};

export const validateSinglePolicyItem = (item, policyType, serviceDef) => {
  if (!item || isEmpty(item)) {
    return {};
  }

  switch (policyType) {
    case 1:
      return validateDataMaskPolicyItem(item);
    case 2:
      return validateRowFilterPolicyItem(item);
    case 4:
      return validateConstraintPolicyItem(item, serviceDef);
    case 0:
    default:
      return validateAccessPolicyItem(item);
  }
};

const getArraysToValidate = (policyType) => {
  if (policyType == 1) {
    return ["dataMaskPolicyItems"];
  } else if (policyType == 2) {
    return ["rowFilterPolicyItems"];
  } else if (policyType == 4) {
    return ["constraintPolicyItems"];
  }
  return [
    "policyItems",
    "allowExceptions",
    "denyPolicyItems",
    "denyExceptions"
  ];
};

export const validatePolicyItemsArray = (values, policyType, serviceDef) => {
  const errors = {};
  const arraysToValidate = getArraysToValidate(policyType);

  arraysToValidate.forEach((key) => {
    (values[key] || []).forEach((item, i) => {
      const ruleErrors = validateSinglePolicyItem(
        item,
        policyType,
        serviceDef
      );

      if (!isEmpty(ruleErrors)) {
        if (!errors[key]) errors[key] = [];
        errors[key][i] = ruleErrors;
      }
    });

    // Clean up arrays that are empty or all-undefined
    if (errors[key] && errors[key].every((e) => e === undefined)) {
      delete errors[key];
    }
  });

  return errors;
};

export const validatePolicyDetails = (values) => {
  const errors = {};
  const policyNameError = validatePolicyName(values.policyName);
  if (policyNameError) {
    errors.policyName = policyNameError;
  }
  return errors;
};

export const validatePolicyItems = (values, serviceDef) => {
  const policyType = values.policyType ?? 0;
  return validatePolicyItemsArray(values, policyType, serviceDef);
};

/**
 * True when a rule has principals but no permissions selected, with delegate admin off
 * (e.g. after resources changed and available permissions were cleared). Use on final
 * submit to block with a user-facing message.
 */
export const hasMissingAccessPermissionOnPolicyItems = (values) => {
  const errors = validatePolicyItems(values);
  if (isEmpty(errors)) {
    return false;
  }
  for (const key of Object.keys(errors)) {
    const arr = errors[key];
    if (!isArray(arr)) {
      continue;
    }
    for (let i = 0; i < arr.length; i++) {
      const itemError = arr[i];
      if (itemError && itemError.accesses) {
        return true;
      }
    }
  }
  return false;
};

/**
 * True when a constraint rule is missing constraint name, permissions, or
 * mandatory parameters (e.g. after resources changed). Use on final submit.
 */
export const hasMissingConstraintFieldsOnPolicyItems = (values, serviceDef) => {
  if ((values.policyType ?? 0) !== 4) {
    return false;
  }
  const errors = validatePolicyItemsArray(values, 4, serviceDef);
  if (isEmpty(errors)) {
    return false;
  }
  const arr = errors.constraintPolicyItems;
  if (!isArray(arr)) {
    return false;
  }
  for (let i = 0; i < arr.length; i++) {
    const itemError = arr[i];
    if (
      itemError &&
      (itemError.accesses ||
        itemError.constraintName ||
        itemError.parameters)
    ) {
      return true;
    }
  }
  return false;
};

export const validatePolicyForm = (values) => ({
  ...validatePolicyDetails(values),
  ...validatePolicyItems(values)
});

export const validateServiceStep = (values) => {
  const errors = {};
  if (!values.service || values.service.trim() === "") {
    errors.service = "Service is required";
  }
  return errors;
};

export const validateResourceStep = (values) => {
  const errors = {};
  // Validate that resources are selected based on policy type
  const policyType = values.policyType ?? 0;

  if (policyType == 1) {
    // Data Masking policy
    if (!values.resources || values.resources.length === 0) {
      errors.resources = "At least one resource is required";
    }
  } else if (policyType == 2) {
    // Row Filter policy
    if (!values.resources || values.resources.length === 0) {
      errors.resources = "At least one resource is required";
    }
  } else {
    // Access policy
    if (!values.resources || values.resources.length === 0) {
      errors.resources = "At least one resource is required";
    }
  }

  return errors;
};

export const validateAccessRulesStep = (values) => {
  const policyType = values.policyType ?? 0;
  return validatePolicyItemsArray(values, policyType);
};

export const validatePolicyDetailsStep = (values) => {
  const errors = {};
  const policyNameError = validatePolicyName(values.policyName);
  if (policyNameError) {
    errors.policyName = policyNameError;
  }
  // Add other policy metadata validations here if needed
  return errors;
};
