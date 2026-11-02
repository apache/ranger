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

import React from "react";
import { Badge, Table } from "react-bootstrap";
import { find, isEmpty } from "lodash";
import {
  getConstraintDefByName,
  getConstraintParamEnumDef
} from "Utils/XAUtils";

/** Audit log attribute name for constraint policy rules (policy type 4). */
export const CONSTRAINT_POLICY_ITEMS_ATTR = "Constraint Policy Items";

const EMPTY_PARAMETERS_TEXT = "<empty>";

const isParameterValuePresent = (parameterValue) =>
  parameterValue !== "" && parameterValue != null;

const getConstraintParameterDef = (constraintDef, parameterName) =>
  find(constraintDef?.parameters, { name: parameterName });

/**
 * Map stored API value to a human-readable string using parameter def + service enums.
 */
export const formatStoredConstraintParameterValue = (
  serviceDef,
  parameterDef,
  storedValue
) => {
  if (!parameterDef) {
    return String(storedValue);
  }
  if (parameterDef.type === "boolean") {
    const isTrue =
      storedValue === true || String(storedValue).toLowerCase() === "true";
    return isTrue ? "Yes" : "No";
  }
  if (parameterDef.type === "enum") {
    const enumDef = getConstraintParamEnumDef(serviceDef, parameterDef);
    const enumElement = find(enumDef?.elements, { name: storedValue });
    return enumElement?.label || enumElement?.name || String(storedValue);
  }
  return String(storedValue);
};

/**
 * Ordered list of { key, label, value } for constraint parameters, using constraintDefs from service def.
 */
export const buildConstraintParameterDisplayItems = (
  serviceDef,
  constraintName,
  parameters
) => {
  if (!parameters || typeof parameters !== "object") {
    return [];
  }

  const constraintDef = getConstraintDefByName(serviceDef, constraintName);
  const parameterDefs = constraintDef?.parameters || [];
  const displayItems = [];

  parameterDefs.forEach((parameterDef) => {
    const storedValue = parameters[parameterDef.name];
    if (!isParameterValuePresent(storedValue)) {
      return;
    }
    displayItems.push({
      key: parameterDef.name,
      label: parameterDef.label || parameterDef.name,
      value: formatStoredConstraintParameterValue(
        serviceDef,
        parameterDef,
        storedValue
      )
    });
  });

  Object.entries(parameters).forEach(([parameterName, storedValue]) => {
    if (!isParameterValuePresent(storedValue)) {
      return;
    }
    if (getConstraintParameterDef(constraintDef, parameterName)) {
      return;
    }
    displayItems.push({
      key: parameterName,
      label: parameterName,
      value: String(storedValue)
    });
  });

  return displayItems;
};

/** Plain-text fallback (audit strings, tooltips). */
export const formatConstraintParameters = (
  parameters,
  serviceDef,
  constraintName
) => {
  const displayItems = buildConstraintParameterDisplayItems(
    serviceDef,
    constraintName,
    parameters
  );
  if (displayItems.length === 0) {
    return EMPTY_PARAMETERS_TEXT;
  }
  return displayItems
    .map(({ label, value }) => `${label}: ${value}`)
    .join(", ");
};

export const ConstraintParameterBadges = ({
  serviceDef,
  constraintName,
  parameters,
  emptyText = EMPTY_PARAMETERS_TEXT
}) => {
  const displayItems = buildConstraintParameterDisplayItems(
    serviceDef,
    constraintName,
    parameters
  );

  if (displayItems.length === 0) {
    return <span>{emptyText}</span>;
  }

  return (
    <span className="d-inline-flex flex-wrap gap-1">
      {displayItems.map(({ key, label, value }) => (
        <Badge bg="info" key={key}>
          {label}: {value}
        </Badge>
      ))}
    </span>
  );
};

const renderConditionsCell = (conditions) => {
  if (isEmpty(conditions)) {
    return null;
  }
  return (
    <tr>
      <td className="table-warning text-nowrap policyitem-field">
        <i>{`Conditions`}</i>
        {`: ${conditions
          .map((type) => `${type.type} : ${(type.values || []).join(", ")}`)
          .join("; ")} `}
      </td>
    </tr>
  );
};

/** Single constraint rule rows (create / delete / import-delete). */
export const renderConstraintPolicyItemBody = (
  policy,
  index,
  itemsLength,
  serviceDef
) => {
  const constraintName = policy?.constraintInfo?.constraintName;
  const parameters = policy?.constraintInfo?.parameters;

  return (
    <tbody key={`constraint-item-${index}`}>
      <tr>
        <td className="table-warning text-nowrap policyitem-field">
          <i>{`Roles`}</i>
          {`: ${!isEmpty(policy.roles) ? policy.roles.join(", ") : "<empty>"} `}
        </td>
      </tr>
      <tr>
        <td className="table-warning text-nowrap policyitem-field">
          <i>{`Groups`}</i>
          {`: ${
            !isEmpty(policy.groups) ? policy.groups.join(", ") : "<empty>"
          } `}
        </td>
      </tr>
      <tr>
        <td className="table-warning text-nowrap policyitem-field">
          <i>{`Users`}</i>
          {`: ${!isEmpty(policy.users) ? policy.users.join(", ") : "<empty>"} `}
        </td>
      </tr>
      <tr>
        <td className="table-warning text-nowrap policyitem-field">
          <i>{`Accesses`}</i>
          {!isEmpty(policy.accesses)
            ? `: ${policy.accesses.map((obj) => obj.type).join(", ")} `
            : "<empty>"}
        </td>
      </tr>
      {renderConditionsCell(policy.conditions)}
      <tr>
        <td className="table-warning text-nowrap policyitem-field">
          <i>{`Constraint Name`}</i>
          {`: ${constraintName || "<empty>"}`}
        </td>
      </tr>
      <tr>
        <td className="table-warning text-nowrap policyitem-field">
          <i>{`Parameters`}</i>
          {": "}
          <ConstraintParameterBadges
            serviceDef={serviceDef}
            constraintName={constraintName}
            parameters={parameters}
          />
        </td>
      </tr>
      <tr>
        <td className="table-warning text-nowrap policyitem-field">
          <i>{`Delegate Admin`}</i>
          {`: ${policy.delegateAdmin === true ? "enabled" : "disabled"}`}
        </td>
      </tr>
      {itemsLength - 1 !== index && (
        <tr>
          <td>
            <br />
          </td>
        </tr>
      )}
    </tbody>
  );
};

/** Read-only table for JSON array strings (newValue or previousValue). */
export const ConstraintPolicyItemsValueTable = ({
  valueStrings,
  columnHeader = "New Value",
  serviceDef
}) => {
  if (isEmpty(valueStrings)) {
    return null;
  }

  return (
    <Table className="table table-bordered w-auto">
      <thead className="thead-light">
        <tr>
          <th>{columnHeader}</th>
        </tr>
      </thead>
      {valueStrings.map((policyitem) => {
        if (isEmpty(policyitem)) {
          return null;
        }
        let parsed = [];
        try {
          parsed = JSON.parse(policyitem);
        } catch {
          return null;
        }
        if (!Array.isArray(parsed)) {
          return null;
        }
        return parsed.map((policy, index) =>
          renderConstraintPolicyItemBody(
            policy,
            index,
            parsed.length,
            serviceDef
          )
        );
      })}
    </Table>
  );
};

export const isConstraintPolicyItemsAttribute = (attributeName) =>
  attributeName === CONSTRAINT_POLICY_ITEMS_ATTR;
