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

import React, { useMemo, useState } from "react";
import { FieldArray } from "react-final-form-arrays";
import {
  Row,
  Col,
  Button,
  Form as FormB,
  Table,
  Container
} from "react-bootstrap";
import { Field, useForm } from "react-final-form";
import { RangerPolicyType } from "Utils/XAEnums";
import { find, isEmpty, isArray, toUpper, map } from "lodash";
import PolicyConditionsComp from "../../PolicyConditionsComp";
import { policyConditionUpdatedJSON } from "Utils/XAUtils";
import UsersGroupsAndRolesSelectionComponent from "./UsersGroupsAndRolesSelectionComponent";
import TagBasePermissionItem from "../../TagBasePermissionItem";
import {
  getAccessTypesByResource,
  getPolicyConditionDisplayLbl
} from "Utils/XAUtils";
import { CustomTooltip } from "Components/CommonComponents";
import {
  getSelectedAccessTypesForRow,
  getSelectedLeafResourceTypes,
  buildActionReqsMapFromConditionDef
} from "Utils/policyConditionUtils";
import { usePruneStaleConditions } from "../../../../hooks/usePruneStaleConditions";

export default function PolicyPermissionItemVertical({
  attrName,
  serviceCompDetails,
  formValues
}) {
  const [showConditionsModalIndex, setShowConditionsModalIndex] =
    useState(null);
  const [showTagPermissionModalIndex, setShowTagPermissionModalIndex] =
    useState(null);
  const form = useForm(); // Get form instance

  const conditionDefVal = useMemo(
    () => policyConditionUpdatedJSON(serviceCompDetails?.policyConditions),
    [serviceCompDetails?.policyConditions]
  );

  const actionReqsMap = useMemo(
    () => buildActionReqsMapFromConditionDef(conditionDefVal),
    [conditionDefVal]
  );

  const leafResourceTypes = useMemo(
    () => getSelectedLeafResourceTypes(serviceCompDetails, formValues),
    [serviceCompDetails, formValues]
  );

  usePruneStaleConditions({
    formValues,
    attrName,
    form,
    leafResourceTypes,
    serviceCompDetails,
    conditionDefVal,
    actionReqsMap
  });

  const accessTypeOptions = getAccessTypesByResource(
    formValues,
    serviceCompDetails
  );

  const getMaskingAccessTypeOptions = (index) => {
    if (serviceCompDetails?.dataMaskDef?.maskTypes?.length > 0) {
      if (
        formValues?.policyType ==
          RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value &&
        serviceCompDetails?.name == "tag"
      ) {
        const accessServices =
          formValues?.dataMaskPolicyItems?.[index]?.accesses?.tableList?.[0]
            ?.serviceName;
        if (accessServices) {
          const filterServiceDetails =
            serviceCompDetails.dataMaskDef.maskTypes.filter((a) => {
              return a.name.includes(accessServices);
            });
          return filterServiceDetails.map(({ label, name: value }) => ({
            label,
            value
          }));
        }
      }
      return serviceCompDetails.dataMaskDef.maskTypes.map(
        ({ label, name: value }) => ({
          label,
          value
        })
      );
    }
    return [];
  };

  const displayTagPermissionItem = (permission) => {
    let srcOp = [];
    if (
      RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value == formValues.policyType
    ) {
      srcOp = serviceCompDetails.dataMaskDef?.accessTypes || [];
    } else if (
      RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value ==
      formValues.policyType
    ) {
      srcOp = serviceCompDetails.rowFilterDef?.accessTypes || [];
    } else {
      srcOp = serviceCompDetails.accessTypes;
    }
    const labels = map(permission, (perm) => {
      return find(srcOp, { name: perm })?.label;
    });
    return labels;
  };

  // Required field validation
  const required = (value) => (value ? undefined : "Required");

  // Helper function to clear all data from a permission item
  const clearPermissionItem = (index) => {
    // Reset the entire permission item to an empty object
    form.change(`${attrName}[${index}]`, {});
  };

  // Handle delegate admin changes with auto-clear logic
  const handleDelegateAdminChange = (input, index) => {
    return (e) => {
      const newValue = e.target.checked;

      if (newValue) {
        // User is checking delegate admin - just set it to true
        input.onChange(true);
      } else {
        // User is unchecking delegate admin - check if this would be the only data
        const currentItem = form.getState().values[attrName]?.[index] || {};

        // Create a copy without delegateAdmin to check other fields
        const itemWithoutDelegate = { ...currentItem };
        delete itemWithoutDelegate.delegateAdmin;

        // If no other meaningful data exists, clear the entire item instead of setting false
        if (Object.keys(itemWithoutDelegate).length === 0) {
          // Keep the item set empty by clearing it completely
          form.change(`${attrName}[${index}]`, {});
        } else {
          // There's other data, so just set delegateAdmin to false
          input.onChange(false);
        }
      }
    };
  };

  return (
    <div className="fnt-14">
      <FieldArray name={attrName}>
        {({ fields }) => (
          <>
            {fields.length === 0 && (
              <Row>
                <Col className="text-center text-muted py-4">
                  <p>
                    No rules added yet. Click &quot;Add more rules&quot; to
                    start.
                  </p>
                </Col>
              </Row>
            )}

            {fields.map((name, index) => (
              <React.Fragment key={name}>
                <Row key={name} className="py-3">
                  {/* Rule Header */}
                  <Col sm={11}>
                    {/* Permissions Section */}
                    <Col xs={12} className="mb-4">
                      <Row>
                        <Col className="column-1-fixed text-end">
                          <FormB.Label>Permissions</FormB.Label>
                        </Col>
                        <Col>
                          {/* Tag-based policy - use modal with table layout */}
                          {serviceCompDetails?.name === "tag" ? (
                            <Field name={`${name}.accesses`}>
                              {({ input, meta }) => (
                                <>
                                  {/* Container with flex layout for table + button side by side */}
                                  {input.value?.tableList?.length > 0 ? (
                                    <Container fluid className="p-0">
                                      <Row>
                                        <Col>
                                          <div style={{ flex: 1 }}>
                                            <Table
                                              bordered
                                              size="sm"
                                              className="condition-group-table"
                                            >
                                              <thead>
                                                <tr>
                                                  <th className="text-center">
                                                    Service
                                                  </th>
                                                  <th className="text-center">
                                                    Permissions
                                                  </th>
                                                </tr>
                                              </thead>
                                              <tbody>
                                                {input.value.tableList.map(
                                                  (tableItem, idx) => (
                                                    <tr
                                                      key={`${tableItem.serviceName}-${idx}`}
                                                    >
                                                      <td className="white-space-nowrap">
                                                        {toUpper(
                                                          tableItem.serviceName
                                                        )}
                                                      </td>
                                                      <td className="text-word-break">
                                                        {displayTagPermissionItem(
                                                          tableItem.permission
                                                        ).join(", ")}
                                                      </td>
                                                    </tr>
                                                  )
                                                )}
                                              </tbody>
                                            </Table>
                                          </div>
                                        </Col>
                                        <Col className="column-3-fixed">
                                          <Button
                                            variant="link"
                                            onClick={() =>
                                              setShowTagPermissionModalIndex(
                                                index
                                              )
                                            }
                                            className="text-primary"
                                            style={{ whiteSpace: "nowrap" }}
                                          >
                                            Edit Permissions
                                          </Button>
                                        </Col>
                                      </Row>
                                    </Container>
                                  ) : (
                                    <Col>
                                      <Button
                                        variant="link"
                                        onClick={() =>
                                          setShowTagPermissionModalIndex(index)
                                        }
                                        className="text-primary"
                                        style={{ whiteSpace: "nowrap" }}
                                      >
                                        Add Permissions
                                      </Button>
                                    </Col>
                                  )}
                                  {/* Tag Permission Modal */}
                                  {showTagPermissionModalIndex === index && (
                                    <TagBasePermissionItem
                                      options={accessTypeOptions}
                                      inputVal={{
                                        ...input,
                                        onChange: (value) => {
                                          input.onChange(value);
                                          // Don't close here, let the modal handle it via onClose
                                        }
                                      }}
                                      formValues={formValues}
                                      dataMaskIndex={index}
                                      serviceCompDetails={serviceCompDetails}
                                      attrName={attrName}
                                      showModal={true}
                                      handleCloseModal={() =>
                                        setShowTagPermissionModalIndex(null)
                                      }
                                    />
                                  )}
                                  {meta.error && (
                                    <div className="text-danger small mt-1">
                                      <i className="fa fa-exclamation-circle me-1" />
                                      {meta.error}
                                    </div>
                                  )}
                                </>
                              )}
                            </Field>
                          ) : (
                            /* Non-tag policy - use checkboxes */
                            <Field name={`${name}.accesses`}>
                              {({ input, meta }) => {
                                const currentValue = input.value || [];
                                const allSelected =
                                  accessTypeOptions.length > 0 &&
                                  accessTypeOptions.every((access) =>
                                    currentValue.some(
                                      (a) => a.type === access.value
                                    )
                                  );

                                const handleSelectAll = () => {
                                  if (allSelected) {
                                    // Deselect all
                                    input.onChange([]);
                                  } else {
                                    // Select all
                                    const allAccesses = accessTypeOptions.map(
                                      (a) => ({
                                        type: a.value,
                                        isAllowed: true,
                                        label: a.label
                                      })
                                    );
                                    input.onChange(allAccesses);
                                  }
                                };

                                return (
                                  <div>
                                    <Container fluid className="p-0">
                                      <div className="gx-3 gy-1 permissions-checkboxes">
                                        {/* Select All Link - Only show if more than 1 option */}
                                        {accessTypeOptions.length > 1 && (
                                          <div className="mb-1">
                                            <FormB.Check
                                              type="checkbox"
                                              id={`${name}-selectAll`}
                                              label="Select All"
                                              checked={allSelected}
                                              onChange={handleSelectAll}
                                              className="text-nowrap text-primary"
                                            />
                                          </div>
                                        )}
                                        {accessTypeOptions.map((access) => {
                                          const isChecked =
                                            Array.isArray(input.value) &&
                                            input.value.some(
                                              (a) => a.type === access.value
                                            );

                                          return (
                                            <div
                                              className="mb-1"
                                              key={access.value}
                                            >
                                              <FormB.Check
                                                type="checkbox"
                                                id={`${name}-${access.value}`}
                                                label={access.label}
                                                checked={isChecked}
                                                onChange={(e) => {
                                                  const currentValue =
                                                    input.value || [];
                                                  if (e.target.checked) {
                                                    input.onChange([
                                                      ...currentValue,
                                                      {
                                                        type: access.value,
                                                        isAllowed: true,
                                                        label: access.label
                                                      }
                                                    ]);
                                                  } else {
                                                    input.onChange(
                                                      currentValue.filter(
                                                        (a) =>
                                                          a.type !==
                                                          access.value
                                                      )
                                                    );
                                                  }
                                                }}
                                                className="text-nowrap"
                                              />
                                            </div>
                                          );
                                        })}
                                      </div>
                                    </Container>
                                    {meta.error && (
                                      <div className="text-danger small mt-1">
                                        <i className="fa fa-exclamation-circle me-1" />
                                        {meta.error}
                                      </div>
                                    )}
                                  </div>
                                );
                              }}
                            </Field>
                          )}
                        </Col>
                      </Row>
                    </Col>

                    {/* Users, Groups, Roles Selection */}
                    <Col xl={12} className="mb-4">
                      <Row>
                        <Col className="column-1-fixed text-end">
                          <FormB.Label>Users, Groups, Roles</FormB.Label>
                        </Col>
                        <Col>
                          <Field name={`${name}.users`}>
                            {({ input, meta }) => (
                              <>
                                <UsersGroupsAndRolesSelectionComponent
                                  value={input.value}
                                  onChange={input.onChange}
                                  name={`${name}.users`}
                                  placeholder="Select users, groups, roles..."
                                />
                                {meta.error && (
                                  <div className="text-danger small mt-1">
                                    <i className="fa fa-exclamation-circle me-1" />
                                    {meta.error}
                                  </div>
                                )}
                              </>
                            )}
                          </Field>
                        </Col>
                      </Row>
                    </Col>

                    {/* Rule Conditions */}
                    {serviceCompDetails?.policyConditions?.length > 0 && (
                      <Col xs={12} className="mb-4">
                        <Row>
                          <Col className="column-1-fixed text-end">
                            <FormB.Label>Rule Conditions</FormB.Label>
                          </Col>
                          {fields.value[index]?.conditions &&
                          !isEmpty(fields.value[index].conditions) ? (
                            <>
                              <Col>
                                <div style={{ flex: 1 }}>
                                  <Table
                                    bordered
                                    size="sm"
                                    className="condition-group-table"
                                  >
                                    <thead>
                                      <tr>
                                        <th className="text-center">
                                          Condition
                                        </th>
                                        <th className="text-center">Value</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {Object.keys(
                                        fields.value[index].conditions
                                      ).map((keyName) => {
                                        const conditionValue =
                                          fields.value[index].conditions[
                                            keyName
                                          ];

                                        if (
                                          conditionValue != "" &&
                                          conditionValue != null
                                        ) {
                                          let conditionObj = find(
                                            serviceCompDetails?.policyConditions,
                                            function (m) {
                                              return m.name == keyName;
                                            }
                                          );

                                          return (
                                            <tr key={keyName}>
                                              <td className="white-space-nowrap">
                                                {getPolicyConditionDisplayLbl(
                                                  conditionObj?.label
                                                )}
                                              </td>
                                              <td className="text-word-break">
                                                {isArray(conditionValue)
                                                  ? conditionValue
                                                      .map((m) =>
                                                        typeof m === "object" &&
                                                        m?.value != null
                                                          ? m.value
                                                          : m
                                                      )
                                                      .join(", ")
                                                  : conditionValue}
                                              </td>
                                            </tr>
                                          );
                                        }
                                        return null;
                                      })}
                                    </tbody>
                                  </Table>
                                </div>
                              </Col>
                              <Col className="column-3-fixed">
                                {/* Edit/Add Button - Always visible on the right */}
                                <Button
                                  variant="link"
                                  onClick={() =>
                                    setShowConditionsModalIndex(index)
                                  }
                                  className="text-primary"
                                  style={{ whiteSpace: "nowrap" }}
                                >
                                  Edit Conditions
                                </Button>
                              </Col>
                            </>
                          ) : (
                            <Col>
                              {/* Edit/Add Button - Always visible on the right */}
                              <Button
                                variant="link"
                                onClick={() =>
                                  setShowConditionsModalIndex(index)
                                }
                                className="text-primary"
                                style={{ whiteSpace: "nowrap" }}
                              >
                                Add Conditions
                              </Button>
                            </Col>
                          )}
                          {showConditionsModalIndex === index && (
                            <PolicyConditionsComp
                              policyConditionDetails={conditionDefVal}
                              inputVal={{
                                value: fields.value[index]?.conditions || {},
                                onChange: (newValue) => {
                                  form.change(`${name}.conditions`, newValue);
                                }
                              }}
                              showModal={true}
                              handleCloseModal={() =>
                                setShowConditionsModalIndex(null)
                              }
                              modalHeader="Rule Conditions"
                              scope="policyItem"
                              servicedefName={serviceCompDetails?.name}
                              actionReqsMap={actionReqsMap}
                              actionFilterContext={{
                                selectedAccessTypes:
                                  getSelectedAccessTypesForRow(
                                    formValues,
                                    attrName,
                                    index
                                  ),
                                leafResourceTypes,
                                accessTypeDefs: serviceCompDetails?.accessTypes
                              }}
                            />
                          )}
                        </Row>
                      </Col>
                    )}

                    {/* Data Masking Option (for Masking policies) */}
                    {RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value ==
                      formValues?.policyType && (
                      <Col xs={12} className="mb-4">
                        <Row>
                          <Col className="column-1-fixed text-end">
                            <FormB.Label>Select Masking Option</FormB.Label>
                          </Col>
                          <Col>
                            {serviceCompDetails?.name !== "tag" ? (
                              <>
                                {" "}
                                <Field name={`${name}.dataMaskInfo`}>
                                  {({ input, meta }) => {
                                    const maskingOptions =
                                      getMaskingAccessTypeOptions(index);
                                    return (
                                      <>
                                        <Container fluid className="p-0">
                                          <Row>
                                            {/* Masking Type Radio Buttons */}
                                            {maskingOptions.map((maskType) => (
                                              <Col
                                                xxl={3}
                                                xl={4}
                                                md={6}
                                                sm={12}
                                                className="mb-3"
                                                key={maskType.value}
                                              >
                                                <FormB.Check
                                                  type="radio"
                                                  id={`${name}-mask-${maskType.value}`}
                                                  name={`${name}.dataMaskInfo`}
                                                  label={maskType.label}
                                                  checked={
                                                    input.value?.value ===
                                                    maskType.value
                                                  }
                                                  onChange={() =>
                                                    input.onChange({
                                                      label: maskType.label,
                                                      value: maskType.value
                                                    })
                                                  }
                                                  className="mb-2"
                                                />
                                              </Col>
                                            ))}
                                          </Row>
                                        </Container>
                                        {/* Custom Value Expression (shown when "Custom" is selected) */}
                                        {input.value?.label === "Custom" && (
                                          <div className="mt-2">
                                            <Field
                                              name={`${name}.dataMaskInfo.valueExpr`}
                                              validate={required}
                                            >
                                              {({
                                                input: exprInput,
                                                meta: exprMeta
                                              }) => (
                                                <div>
                                                  <FormB.Control
                                                    {...exprInput}
                                                    type="text"
                                                    placeholder="Enter masked value or expression..."
                                                    isInvalid={
                                                      exprMeta.touched &&
                                                      exprMeta.error
                                                    }
                                                  />
                                                  {exprMeta.touched &&
                                                    exprMeta.error && (
                                                      <FormB.Control.Feedback type="invalid">
                                                        {exprMeta.error}
                                                      </FormB.Control.Feedback>
                                                    )}
                                                </div>
                                              )}
                                            </Field>
                                          </div>
                                        )}
                                        {/* Form-level validation error - only render if string (nested field errors bubble up as objects) */}
                                        {typeof meta.error === "string" && (
                                          <div className="text-danger small mt-1">
                                            <i className="fa fa-exclamation-circle me-1" />
                                            {meta.error}
                                          </div>
                                        )}
                                      </>
                                    );
                                  }}
                                </Field>
                              </>
                            ) : formValues?.dataMaskPolicyItems?.[index]
                                ?.accesses?.tableList?.length > 0 ? (
                              <>
                                {" "}
                                <Field name={`${name}.dataMaskInfo`}>
                                  {({ input, meta }) => {
                                    const maskingOptions =
                                      getMaskingAccessTypeOptions(index);
                                    return (
                                      <>
                                        <Container fluid className="p-0">
                                          <Row>
                                            {/* Masking Type Radio Buttons */}
                                            {maskingOptions.map((maskType) => (
                                              <Col
                                                xxl={3}
                                                xl={4}
                                                md={6}
                                                sm={12}
                                                className="mb-3"
                                                key={maskType.value}
                                              >
                                                <FormB.Check
                                                  type="radio"
                                                  id={`${name}-mask-${maskType.value}`}
                                                  name={`${name}.dataMaskInfo`}
                                                  label={maskType.label}
                                                  checked={
                                                    input.value?.value ===
                                                    maskType.value
                                                  }
                                                  onChange={() =>
                                                    input.onChange({
                                                      label: maskType.label,
                                                      value: maskType.value
                                                    })
                                                  }
                                                  className="mb-2"
                                                />
                                              </Col>
                                            ))}
                                          </Row>
                                        </Container>
                                        {/* Custom Value Expression (shown when "Custom" is selected) */}
                                        {input.value?.label === "Custom" && (
                                          <div className="mt-2">
                                            <Field
                                              name={`${name}.dataMaskInfo.valueExpr`}
                                              validate={required}
                                            >
                                              {({
                                                input: exprInput,
                                                meta: exprMeta
                                              }) => (
                                                <div>
                                                  <FormB.Control
                                                    {...exprInput}
                                                    type="text"
                                                    placeholder="Enter masked value or expression..."
                                                    isInvalid={
                                                      exprMeta.touched &&
                                                      exprMeta.error
                                                    }
                                                  />
                                                  {exprMeta.touched &&
                                                    exprMeta.error && (
                                                      <FormB.Control.Feedback type="invalid">
                                                        {exprMeta.error}
                                                      </FormB.Control.Feedback>
                                                    )}
                                                </div>
                                              )}
                                            </Field>
                                          </div>
                                        )}
                                        {/* Form-level validation error - only render if string (nested field errors bubble up as objects) */}
                                        {typeof meta.error === "string" && (
                                          <div className="text-danger small mt-1">
                                            <i className="fa fa-exclamation-circle me-1" />
                                            {meta.error}
                                          </div>
                                        )}
                                      </>
                                    );
                                  }}
                                </Field>
                              </>
                            ) : (
                              <Col>
                                <div className="text-muted fst-italic">
                                  Please add permissions to select masking
                                  option.
                                </div>
                              </Col>
                            )}
                          </Col>
                        </Row>
                      </Col>
                    )}

                    {/* Row Level Filter (for Row Filter policies) */}
                    {RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value ==
                      formValues?.policyType && (
                      <Col xs={12} className="mb-4">
                        <Row>
                          <Col className="column-1-fixed text-end">
                            <FormB.Label>Row Level Filter</FormB.Label>
                          </Col>
                          <Col>
                            <Field name={`${name}.rowFilterInfo`}>
                              {({ input, meta }) => (
                                <div>
                                  <FormB.Control
                                    {...input}
                                    as="textarea"
                                    rows={3}
                                    placeholder="Enter filter expression (e.g., country = 'US')"
                                    isInvalid={!!meta.error}
                                  />
                                  {meta.error && (
                                    <FormB.Control.Feedback type="invalid">
                                      {meta.error}
                                    </FormB.Control.Feedback>
                                  )}
                                </div>
                              )}
                            </Field>
                          </Col>
                        </Row>
                      </Col>
                    )}

                    {/* Delegate Admin (for Access policies only, non-tag) */}
                    {RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value ==
                      formValues?.policyType &&
                      serviceCompDetails?.name !== "tag" && (
                        <Col xs={12} className="mb-0">
                          <Row>
                            <Col className="column-1-fixed text-end">
                              <FormB.Label htmlFor={`${name}.delegateAdmin`}>
                                Delegate Admin
                                <span className="ps-2">
                                  <CustomTooltip
                                    placement="right"
                                    content={
                                      <p
                                        className="pd-5"
                                        style={{ fontSize: "small" }}
                                      >
                                        Delegate Admin grants users or groups or
                                        roles the authority to create and manage
                                        policies, but strictly within the
                                        specific resource boundaries defined by
                                        the original policy.
                                      </p>
                                    }
                                    icon="fa-fw fa fa-info-circle"
                                  />
                                </span>
                              </FormB.Label>
                            </Col>
                            <Col>
                              <Field
                                className="form-control"
                                name={`${name}.delegateAdmin`}
                                data-js="delegatedAdmin"
                                data-cy="delegatedAdmin"
                                type="checkbox"
                              >
                                {({ input }) => (
                                  <div
                                    className="d-flex align-items-center"
                                    style={{ height: "21px" }}
                                  >
                                    <input
                                      {...input}
                                      type="checkbox"
                                      onChange={handleDelegateAdminChange(
                                        input,
                                        index
                                      )}
                                    />{" "}
                                  </div>
                                )}
                              </Field>
                            </Col>
                          </Row>
                        </Col>
                      )}
                  </Col>
                  <Col
                    sm={1}
                    className="mb-4 justify-content-end d-flex align-items-start"
                  >
                    {fields.length > 1 ? (
                      <Button
                        variant="danger"
                        size="sm"
                        title="Remove"
                        onClick={() => fields.remove(index)}
                        data-action="delete"
                        data-cy="delete"
                      >
                        <i className="fa-fw fa fa-remove"></i>
                      </Button>
                    ) : (
                      // Show clear button only when there's one item and it has data
                      <Button
                        variant="danger"
                        size="sm"
                        title="Clear item data"
                        onClick={() => clearPermissionItem(index)}
                        data-action="clear"
                        data-cy="clear"
                      >
                        <i className="fa-fw fa fa-remove"></i>
                      </Button>
                    )}
                  </Col>
                </Row>
                <hr />
              </React.Fragment>
            ))}

            {/* Add More Rules Button */}
            <Row className="my-3">
              <Col>
                <Button
                  variant="outline-primary"
                  onClick={() => fields.push({})}
                >
                  <i className="fa-fw fa fa-plus me-1" />
                  {fields.length === 0 ? "Add Rule" : "Add more rules"}
                </Button>
              </Col>
            </Row>
          </>
        )}
      </FieldArray>
    </div>
  );
}
