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
import { Field } from "react-final-form";
import { Row, Col, Form as FormB } from "react-bootstrap";
import BootstrapSwitchButton from "bootstrap-switch-button-react";
import Select from "react-select";
import { find } from "lodash";
import { selectInputCustomStyles } from "Components/CommonComponents";

const required = (value) =>
  value !== undefined && value !== null && value !== ""
    ? undefined
    : "Required";

export const getConstraintParamEnumDef = (serviceCompDetails, param) => {
  if (!param?.subType || !serviceCompDetails?.enums) {
    return null;
  }
  return find(serviceCompDetails.enums, { name: param.subType });
};

export const getConstraintParamEnumSelectOptions = (
  serviceCompDetails,
  param
) => {
  const enumDef = getConstraintParamEnumDef(serviceCompDetails, param);
  return (enumDef?.elements || []).map((element) => ({
    label: element.label || element.name,
    value: element.name
  }));
};

export const getDefaultEnumParameterValue = (serviceCompDetails, param) => {
  if (param.defaultValue !== undefined && param.defaultValue !== null) {
    return String(param.defaultValue);
  }
  const enumDef = getConstraintParamEnumDef(serviceCompDetails, param);
  if (
    enumDef?.defaultIndex != null &&
    enumDef.elements?.[enumDef.defaultIndex]
  ) {
    return enumDef.elements[enumDef.defaultIndex].name;
  }
  if (enumDef?.elements?.[0]?.name) {
    return enumDef.elements[0].name;
  }
  return "";
};

export const getInitialParameterValue = (param, serviceCompDetails) => {
  if (param.type === "enum") {
    return getDefaultEnumParameterValue(serviceCompDetails, param);
  }
  if (param.defaultValue === undefined || param.defaultValue === null) {
    if (param.type === "boolean") {
      return false;
    }
    return "";
  }
  if (param.type === "boolean") {
    return (
      param.defaultValue === true ||
      String(param.defaultValue).toLowerCase() === "true"
    );
  }
  if (param.type === "int") {
    return String(param.defaultValue);
  }
  return param.defaultValue;
};

/** Defaults applied only when the user picks a constraint name (not on step remount). */
export const buildDefaultConstraintParameters = (
  parameters = [],
  serviceCompDetails
) => {
  const defaults = {};
  parameters.forEach((param) => {
    if (param.type === "boolean") {
      defaults[param.name] = getInitialParameterValue(param, serviceCompDetails);
      return;
    }
    if (param.type === "enum") {
      const initial = getInitialParameterValue(param, serviceCompDetails);
      if (initial !== "") {
        defaults[param.name] = initial;
      }
      return;
    }
    const initial = getInitialParameterValue(param, serviceCompDetails);
    if (initial !== "" && initial != null) {
      defaults[param.name] = initial;
    }
  });
  return defaults;
};

const validateParameter = (param) => (value) => {
  if (!param.mandatory) {
    return undefined;
  }
  if (param.type === "boolean") {
    return undefined;
  }
  if (param.type === "list") {
    if (Array.isArray(value)) {
      return value.length > 0 ? undefined : "Required";
    }
    return value != null && String(value).trim() !== ""
      ? undefined
      : "Required";
  }
  return required(value);
};

const principalPlaceholder = (type) => {
  if (type === "user") {
    return "Enter user name";
  }
  if (type === "group") {
    return "Enter group name";
  }
  if (type === "role") {
    return "Enter role name";
  }
  return "";
};

const ParameterControl = ({ param, input, meta, enumOptions = [] }) => {
  const inputId = `${param.name}-constraint-param`;
  const showInvalid = meta.touched && meta.error;

  switch (param.type) {
    case "enum": {
      const selectedEnumOption =
        enumOptions.find((opt) => opt.value === input.value) || null;
      return (
        <>
          <Select
            inputId={inputId}
            options={enumOptions}
            value={selectedEnumOption}
            onChange={(option) =>
              input.onChange(option ? option.value : undefined)
            }
            onBlur={input.onBlur}
            placeholder="Select..."
            isClearable={!param.mandatory}
            tabSelectsValue={false}
            styles={selectInputCustomStyles}
          />
          {showInvalid && (
            <div className="text-danger small mt-1">{meta.error}</div>
          )}
        </>
      );
    }

    case "int":
      return (
        <>
          <FormB.Control
            id={inputId}
            type="number"
            {...input}
            value={input.value ?? ""}
            placeholder={param.defaultValue ? String(param.defaultValue) : ""}
            isInvalid={showInvalid}
          />
          {showInvalid && (
            <FormB.Control.Feedback type="invalid">
              {meta.error}
            </FormB.Control.Feedback>
          )}
        </>
      );

    case "boolean":
      return (
        <>
          <BootstrapSwitchButton
            id={inputId}
            checked={
              input.value === true ||
              String(input.value).toLowerCase() === "true"
            }
            onChange={(checked) => input.onChange(checked)}
            onlabel="Yes"
            offlabel="No"
            width={90}
            height={30}
          />
          {showInvalid && (
            <div className="text-danger small mt-1">{meta.error}</div>
          )}
        </>
      );

    case "list":
      return (
        <>
          <FormB.Control
            id={inputId}
            as="textarea"
            rows={2}
            {...input}
            value={input.value ?? ""}
            placeholder="Enter values separated by commas"
            isInvalid={showInvalid}
          />
          {showInvalid && (
            <FormB.Control.Feedback type="invalid">
              {meta.error}
            </FormB.Control.Feedback>
          )}
        </>
      );

    case "user":
    case "group":
    case "role":
      return (
        <>
          <FormB.Control
            id={inputId}
            type="text"
            {...input}
            value={input.value ?? ""}
            placeholder={principalPlaceholder(param.type)}
            isInvalid={showInvalid}
          />
          {showInvalid && (
            <FormB.Control.Feedback type="invalid">
              {meta.error}
            </FormB.Control.Feedback>
          )}
        </>
      );

    case "string":
    default:
      return (
        <>
          <FormB.Control
            id={inputId}
            type="text"
            {...input}
            value={input.value ?? ""}
            placeholder={
              param.defaultValue != null
                ? String(param.defaultValue)
                : `Enter ${param.label || param.name}`
            }
            isInvalid={showInvalid}
          />
          {showInvalid && (
            <FormB.Control.Feedback type="invalid">
              {meta.error}
            </FormB.Control.Feedback>
          )}
        </>
      );
  }
};

/**
 * Renders constraint definition parameters in the same row layout as Constraint Name.
 */
const ConstraintPolicyParameterFields = ({
  parameters = [],
  fieldNamePrefix,
  serviceCompDetails
}) => {
  if (!parameters.length) {
    return (
      <p className="text-muted mb-0">No parameters for this constraint.</p>
    );
  }

  return (
    <>
      {parameters.map((param) => (
        <Row key={param.name} className="mb-4">
          <Col className="column-1-fixed text-end">
            <FormB.Label
              htmlFor={`${param.name}-constraint-param`}
              className="mb-0"
            >
              {param.label || param.name}
              {param.mandatory ? (
                <span className="required-mark">*</span>
              ) : null}
            </FormB.Label>
          </Col>
          <>
            <Col>
              <Field
                name={`${fieldNamePrefix}.${param.name}`}
                validate={validateParameter(param)}
              >
                {({ input, meta }) => (
                  <ParameterControl
                    param={param}
                    input={input}
                    meta={meta}
                    enumOptions={getConstraintParamEnumSelectOptions(
                      serviceCompDetails,
                      param
                    )}
                  />
                )}
              </Field>
            </Col>
            <Col className="column-3-fixed"></Col>
          </>
        </Row>
      ))}
    </>
  );
};

/** Compact parameter fields for legacy PolicyPermissionItem table column. */
export const ConstraintPolicyParameterTableCell = ({
  parameters = [],
  fieldNamePrefix,
  constraintSelected,
  selectFirstMessage = "Please select constraint name first",
  serviceCompDetails
}) => {
  if (!constraintSelected) {
    return (
      <span className="text-muted small editable-add-text">
        {selectFirstMessage}
      </span>
    );
  }
  if (!parameters.length) {
    return (
      <span className="text-muted small editable-add-text">
        No parameters for this constraint
      </span>
    );
  }

  return (
    <div className="table-editable">
      {parameters.map((param) => (
        <div key={param.name} className="mb-2 text-start">
          <FormB.Label className="small mb-1 d-block">
            {param.label || param.name}
            {param.mandatory ? (
              <span className="required-mark">*</span>
            ) : null}
          </FormB.Label>
          <Field
            name={`${fieldNamePrefix}.${param.name}`}
            validate={validateParameter(param)}
          >
            {({ input, meta }) => (
              <ParameterControl
                param={param}
                input={input}
                meta={meta}
                enumOptions={getConstraintParamEnumSelectOptions(
                  serviceCompDetails,
                  param
                )}
              />
            )}
          </Field>
        </div>
      ))}
    </div>
  );
};

export default ConstraintPolicyParameterFields;
