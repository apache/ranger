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

import React, { useState, useCallback } from "react";
import {
  Form as FormB,
  Row,
  Col,
  Container,
  Button,
  Table,
  Collapse
} from "react-bootstrap";
import { Field } from "react-final-form";
import { isEmpty, trim, find, isArray } from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import {
  selectInputCustomStyles,
  trimInputValue
} from "Components/CommonComponents";
import AsyncCreatableSelect from "react-select/async-creatable";
import BootstrapSwitchButton from "bootstrap-switch-button-react";
import PolicyValidityPeriodModal from "./PolicyValidityPeriodModal";
import PolicyConditionsComp from "../../PolicyConditionsComp";
import {
  policyConditionUpdatedJSON,
  getPolicyConditionDisplayLbl
} from "Utils/XAUtils";
import moment from "moment";

const PolicyDetailsStep = ({ values, form, selectedServiceComponentDef }) => {
  const [defaultPolicyLabelOptions, setDefaultPolicyLabelOptions] = useState(
    []
  );
  const [showConditionsModal, setShowConditionsModal] = useState(false);
  const [showValidityPeriodModal, setShowValidityPeriodModal] = useState(false);
  const [open, setOpen] = useState(false);

  const required = (value) => (value ? undefined : "Required");

  const handleToggleSetting = (e) => {
    e.preventDefault(); // Prevents the "#" from jumping the page to the top
    setOpen(!open);
  };

  // Fetch policy labels with lodash trim
  const fetchPolicyLabel = useCallback(async (inputValue) => {
    const params = {};
    if (!isEmpty(inputValue)) {
      params.policyLabel = trim(inputValue);
    }

    try {
      const policyLabelResp = await fetchApi({
        url: "plugins/policyLabels",
        params
      });

      return policyLabelResp.data.map((name) => ({
        label: trim(name),
        value: trim(name)
      }));
    } catch (error) {
      console.error("Error fetching policy labels:", error);
      return [];
    }
  }, []);

  // Load default options on focus
  const onFocusPolicyLabel = useCallback(() => {
    fetchPolicyLabel().then((opts) => {
      setDefaultPolicyLabelOptions(opts);
    });
  }, [fetchPolicyLabel]);

  return (
    <Container fluid className="p-0">
      <Row>
        <Col lg={11}>
          <p className="text-muted">Add policy details</p>

          {/* Policy Name */}
          <FormB.Group as={Row} className="mb-4">
            <FormB.Label
              column
              className="fnt-14 column-1-fixed text-end"
              htmlFor="policyName"
            >
              Policy Name
              <span className="required-mark">*</span>
            </FormB.Label>
            <Col className="mw-50">
              <Field name="policyName" validate={required}>
                {({ input, meta }) => (
                  <>
                    <FormB.Control
                      {...input}
                      type="text"
                      placeholder="Enter policy name"
                      onBlur={(e) => trimInputValue(e, input)}
                    />
                    {meta.touched && meta.error && (
                      <span className="text-danger">{meta.error}</span>
                    )}
                  </>
                )}
              </Field>
            </Col>
          </FormB.Group>

          {/* Policy Labels */}
          <FormB.Group as={Row} className="mb-4">
            <FormB.Label
              column
              className="fnt-14 column-1-fixed text-end"
              htmlFor="policyLabels"
            >
              Policy Labels
            </FormB.Label>
            <Col className="mw-50 labels-select-input">
              <Field name="policyLabels" className="">
                {({ input }) => (
                  <AsyncCreatableSelect
                    {...input}
                    isMulti
                    loadOptions={fetchPolicyLabel}
                    onFocus={onFocusPolicyLabel}
                    defaultOptions={defaultPolicyLabelOptions}
                    styles={selectInputCustomStyles}
                    placeholder="Add Policy Labels"
                    tabSelectsValue={false}
                    formatCreateLabel={(inputValue) =>
                      `Create "${trim(inputValue)}"`
                    }
                    onCreateOption={(inputValue) => {
                      const policyLabelVal = trim(inputValue);
                      if (!isEmpty(policyLabelVal)) {
                        input.onChange([
                          ...(input.value || []),
                          {
                            label: policyLabelVal,
                            value: policyLabelVal
                          }
                        ]);
                      }
                    }}
                  />
                )}
              </Field>
            </Col>
          </FormB.Group>

          {/* Policy Description */}
          <FormB.Group as={Row} className="mb-4">
            <FormB.Label
              column
              className="fnt-14 column-1-fixed text-end"
              htmlFor="description"
            >
              Policy Description
            </FormB.Label>
            <Col className="mw-50">
              <Field name="description">
                {({ input }) => (
                  <FormB.Control
                    {...input}
                    as="textarea"
                    rows={3}
                    placeholder="Enter policy description"
                    onBlur={(e) => trimInputValue(e, input)}
                  />
                )}
              </Field>
            </Col>
          </FormB.Group>

          {/* Policy Status */}
          <FormB.Group as={Row} className="mb-4 align-items-center">
            <FormB.Label
              column
              className="fnt-14 column-1-fixed text-end"
              htmlFor="isEnabled"
            >
              Policy Status
            </FormB.Label>
            <Col className="mw-50">
              <Field name="isEnabled">
                {({ input }) => (
                  <BootstrapSwitchButton
                    {...input}
                    checked={!(input.value === false)}
                    onlabel="Enabled"
                    onstyle="primary"
                    offlabel="Disabled"
                    offstyle="outline-secondary"
                    width={100}
                    size="xs"
                    key="isEnabled"
                  />
                )}
              </Field>
            </Col>
          </FormB.Group>

          {/* Policy Priority */}
          <FormB.Group as={Row} className="mb-4">
            <FormB.Label
              column
              className="fnt-14 column-1-fixed text-end"
              htmlFor="policyPriority"
            >
              Policy Priority
            </FormB.Label>
            <Col className="mw-50">
              <Field name="policyPriority">
                {({ input }) => (
                  <div className="d-flex gap-5">
                    <FormB.Check
                      type="radio"
                      label="Normal"
                      name="policyPriority"
                      value={0}
                      checked={input.value === 0 || input.value === undefined}
                      onChange={(e) => input.onChange(Number(e.target.value))}
                      id="policyPriorityNormal"
                    />
                    <FormB.Check
                      type="radio"
                      label="Override"
                      name="policyPriority"
                      value={1}
                      checked={input.value === 1}
                      onChange={(e) => input.onChange(Number(e.target.value))}
                      id="policyPriorityOverride"
                    />
                  </div>
                )}
              </Field>
            </Col>
          </FormB.Group>

          {/* {Hide show advance setting} */}
          <div className="mt-4">
            <FormB.Group as={Row} className="mb-4">
              <Col sm={12}>
                <div className="d-flex align-items-center">
                  <p className="mb-0">
                    <a
                      href="#"
                      className="link-underline-light text-primary fw-bold d-flex align-items-center"
                      onClick={handleToggleSetting}
                      style={{ textDecoration: "none" }}
                    >
                      <span className="me-2">
                        {open
                          ? "Hide Advanced Settings"
                          : "Show Advanced Settings"}
                      </span>
                      <i
                        className={`fa-fw fa ${open ? "fa-angle-up" : "fa-angle-down"}`}
                      />
                    </a>
                  </p>
                </div>
              </Col>
            </FormB.Group>

            {/* The Collapse Component replaces Accordion.Collapse */}
            <Collapse in={open}>
              <div>
                {/* Policy Validity Period */}
                <Col xl={12} className="mb-4">
                  <Row>
                    <Col className="fnt-14 column-1-fixed text-end">
                      <FormB.Label>Policy Validity Period</FormB.Label>
                    </Col>
                    <Col>
                      {values?.validitySchedules &&
                      !isEmpty(values.validitySchedules) ? (
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
                                    <th className="text-center">Start Date</th>
                                    <th className="text-center">End Date</th>
                                    <th className="text-center">Time Zone</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {values.validitySchedules
                                    .filter((schedule) => schedule != null)
                                    .map((schedule, idx) => (
                                      <tr key={idx}>
                                        <td className="text-center">
                                          {schedule?.startTime
                                            ? moment(schedule.startTime).format(
                                                "MM-DD-YYYY HH:mm:ss"
                                              )
                                            : "-"}
                                        </td>
                                        <td className="text-center">
                                          {schedule?.endTime
                                            ? moment(schedule.endTime).format(
                                                "MM-DD-YYYY HH:mm:ss"
                                              )
                                            : "-"}
                                        </td>
                                        <td className="text-center">
                                          {schedule?.timeZone?.text || "-"}
                                        </td>
                                      </tr>
                                    ))}
                                </tbody>
                              </Table>
                            </div>
                          </Col>
                          <Col className="column-3-fixed">
                            <Button
                              variant="link"
                              onClick={() => setShowValidityPeriodModal(true)}
                              className="text-primary"
                              style={{ whiteSpace: "nowrap" }}
                            >
                              Edit Validity Period
                            </Button>
                          </Col>
                        </Row>
                      ) : (
                        <Button
                          variant="link"
                          onClick={() => setShowValidityPeriodModal(true)}
                          className="text-primary"
                          style={{ whiteSpace: "nowrap" }}
                        >
                          Add Validity Period
                        </Button>
                      )}

                      {/* Policy Validity Period Modal */}
                      {showValidityPeriodModal && (
                        <PolicyValidityPeriodModal
                          showModal={showValidityPeriodModal}
                          handleCloseModal={() =>
                            setShowValidityPeriodModal(false)
                          }
                          currentValues={values?.validitySchedules || []}
                          onSave={(newValues) => {
                            form.change("validitySchedules", newValues);
                          }}
                        />
                      )}
                    </Col>
                  </Row>
                </Col>

                {/* Policy Conditions */}
                {selectedServiceComponentDef?.policyConditions?.length > 0 && (
                  <Col xl={12} className="mb-4">
                    <Row>
                      <Col className="fnt-14 column-1-fixed text-end">
                        <FormB.Label>Policy Conditions</FormB.Label>
                      </Col>
                      <Col>
                        {values?.conditions && !isEmpty(values.conditions) ? (
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
                                      <th className="text-center">Condition</th>
                                      <th className="text-center">Value</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {Object.keys(values.conditions).map(
                                      (keyName) => {
                                        const conditionValue =
                                          values.conditions[keyName];
                                        if (
                                          conditionValue != "" &&
                                          conditionValue != null
                                        ) {
                                          let conditionObj = find(
                                            selectedServiceComponentDef?.policyConditions,
                                            (m) => m.name === keyName
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
                                                  ? conditionValue.join(", ")
                                                  : conditionValue}
                                              </td>
                                            </tr>
                                          );
                                        }
                                        return null;
                                      }
                                    )}
                                  </tbody>
                                </Table>
                              </div>
                            </Col>
                            <Col className="column-3-fixed">
                              <Button
                                variant="link"
                                onClick={() => setShowConditionsModal(true)}
                                className="text-primary"
                                style={{ whiteSpace: "nowrap" }}
                              >
                                Edit Conditions
                              </Button>
                            </Col>
                          </Row>
                        ) : (
                          <Button
                            variant="link"
                            onClick={() => setShowConditionsModal(true)}
                            className="text-primary"
                            style={{ whiteSpace: "nowrap" }}
                          >
                            Add Conditions
                          </Button>
                        )}

                        {/* Policy Conditions Modal */}
                        {showConditionsModal && (
                          <PolicyConditionsComp
                            policyConditionDetails={policyConditionUpdatedJSON(
                              selectedServiceComponentDef.policyConditions
                            )}
                            inputVal={{
                              value: values?.conditions || {},
                              onChange: (newValue) => {
                                form.change("conditions", newValue);
                              }
                            }}
                            showModal={showConditionsModal}
                            handleCloseModal={() =>
                              setShowConditionsModal(false)
                            }
                          />
                        )}
                      </Col>
                    </Row>
                  </Col>
                )}

                {/* Audit Logging */}
                <FormB.Group as={Row} className="mb-4 align-items-center">
                  <FormB.Label
                    column
                    className="fnt-14 column-1-fixed text-end"
                    htmlFor="isAuditEnabled"
                  >
                    Audit Logging
                  </FormB.Label>
                  <Col sm={2}>
                    <Field name="isAuditEnabled">
                      {({ input }) => (
                        <BootstrapSwitchButton
                          {...input}
                          checked={!!input.value}
                          onlabel="Yes"
                          onstyle="primary"
                          offlabel="No"
                          offstyle="outline-secondary"
                          width={100}
                          size="xs"
                          key="isAuditEnabled"
                        />
                      )}
                    </Field>
                  </Col>
                </FormB.Group>
              </div>
            </Collapse>
          </div>
        </Col>
      </Row>
    </Container>
  );
};

export default PolicyDetailsStep;
