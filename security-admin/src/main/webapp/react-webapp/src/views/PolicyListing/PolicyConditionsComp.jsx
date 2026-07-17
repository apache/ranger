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

import React, { useMemo } from "react";
import { Col, Form as FormB, Row, Modal, Button } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import Select from "react-select";
import CreatableSelect from "react-select/creatable";
import { find, isArray, isEmpty } from "lodash";
import { InfoIcon } from "Utils/XAUtils";
import { RegexMessage } from "Utils/XAMessages";
import {
  selectInputCustomStyles,
  trimInputValue
} from "Components/CommonComponents";
import {
  isPerRowCondition,
  getCleanConditions,
  parseConditionUiHint,
  sortPolicyConditions,
  buildActionReqsMapFromConditionDef,
  getAllowedActionMatchesForCondition
} from "Utils/policyConditionUtils";
const esprima = require("esprima");
const POLICY_SCOPE = "policy";
const POLICY_ITEM_SCOPE = "policyItem";

const toSelectValue = (val) => {
  if (!isArray(val) || isEmpty(val)) {
    return [];
  }
  return val
    .map((m) => {
      if (typeof m === "object" && m?.value) {
        const value = String(m.value).trim();
        return value ? { label: m.label || value, value } : null;
      }
      if (typeof m === "string") {
        const trimmed = m.trim();
        return trimmed ? { label: trimmed, value: trimmed } : null;
      }
      return null;
    })
    .filter(Boolean);
};

const pruneActionMatchesOnSubmit = ({
  conditions,
  conditionDefVal,
  actionFilterContext,
  actionReqsMap,
  servicedefName
}) => {
  const pruned = { ...conditions };

  for (const conditionName in pruned) {
    if (!isPerRowCondition(conditionName)) {
      continue;
    }

    const conditionDef = find(conditionDefVal, { name: conditionName });
    const uiHintAttb = parseConditionUiHint(conditionDef?.uiHint);
    const current = pruned[conditionName];

    if (
      !uiHintAttb?.isMultiValue ||
      !Array.isArray(uiHintAttb?.options) ||
      !Array.isArray(current) ||
      current.length === 0 ||
      !actionFilterContext?.selectedAccessTypes?.length
    ) {
      continue;
    }

    const { prunedSelection } = getAllowedActionMatchesForCondition({
      conditionName,
      actionFilterContext,
      actionReqsMap,
      servicedefName,
      uiHintAttb,
      currentSelection: toSelectValue(current)
    });

    if (prunedSelection?.length > 0) {
      pruned[conditionName] = prunedSelection.map((o) => o.value);
    } else {
      delete pruned[conditionName];
    }
  }

  return pruned;
};

export default function PolicyConditionsComp(props) {
  const {
    policyConditionDetails,
    inputVal,
    showModal,
    handleCloseModal,
    modalHeader,
    scope = POLICY_SCOPE,
    servicedefName,
    actionFilterContext,
    actionReqsMap: actionReqsMapProp
  } = props;

  const isPolicyItemScope = scope === POLICY_ITEM_SCOPE;

  const conditionDefVal = useMemo(
    () =>
      Array.isArray(policyConditionDetails) ? policyConditionDetails : [],
    [policyConditionDetails]
  );

  const actionReqsMap = useMemo(
    () =>
      actionReqsMapProp ?? buildActionReqsMapFromConditionDef(conditionDefVal),
    [actionReqsMapProp, conditionDefVal]
  );

  const filteredPolicyConditionDetails = useMemo(() => {
    if (!Array.isArray(policyConditionDetails)) {
      return policyConditionDetails;
    }
    const filtered = isPolicyItemScope
      ? policyConditionDetails
      : policyConditionDetails.filter((c) => !isPerRowCondition(c?.name));
    return sortPolicyConditions(filtered);
  }, [policyConditionDetails, isPolicyItemScope]);

  const accessedOpt = [
    { value: "yes", label: "Yes" },
    { value: "no", label: "No" }
  ];

  const handleSubmit = (values) => {
    if (values?.conditions) {
      let newConditions = getCleanConditions(values.conditions);

      if (isPolicyItemScope) {
        newConditions = pruneActionMatchesOnSubmit({
          conditions: newConditions,
          conditionDefVal,
          actionFilterContext,
          actionReqsMap,
          servicedefName
        });
      } else {
        for (const val in newConditions) {
          if (isPerRowCondition(val)) {
            delete newConditions[val];
          }
        }
      }

      inputVal.onChange(newConditions);
    } else {
      inputVal.onChange(values?.conditions);
    }
    handleClose();
  };

  const handleClose = () => {
    handleCloseModal(false);
  };

  const formInitialData = () => {
    const conditions = {};
    if (inputVal?.value) {
      for (const val in inputVal.value) {
        conditions[val] = inputVal.value[val];
      }
    }
    return { conditions };
  };

  const accessedVal = (val) => {
    let value = null;
    if (val) {
      const opObj = find(accessedOpt, { value: val });
      if (opObj) {
        value = opObj;
      }
    }
    return value;
  };

  const accessedOnChange = (val, input) => {
    let value = null;
    val && val.value && (value = val.value);
    input.onChange(value);
  };

  const handleChange = (val, input) => {
    if (!val || val.length === 0) {
      input.onChange(undefined);
      return;
    }
    input.onChange(val.map((m) => m.value));
  };

  const ipRangeVal = (val) => toSelectValue(val);

  const validater = (values) => {
    let errors = "";
    if (values) {
      try {
        esprima.parseScript(values);
      } catch (e) {
        errors = e.message;
      }
    }
    return errors;
  };

  const renderMultiValueField = (m, uiHintAttb) => {
    const isActionMatches =
      isPolicyItemScope &&
      isPerRowCondition(m.name) &&
      Array.isArray(uiHintAttb?.options);
    if (isActionMatches) {
      return (
        <Field
          className="form-control"
          name={`conditions.${m.name}`}
          render={({ input }) => {
            const { dropdownOptions, prunedSelection } =
              getAllowedActionMatchesForCondition({
                conditionName: m.name,
                actionFilterContext,
                actionReqsMap,
                servicedefName,
                uiHintAttb,
                currentSelection: toSelectValue(input.value)
              });
            const displayedValue =
              prunedSelection?.length > 0 ? prunedSelection : null;
            return (
              <Select
                isMulti
                isClearable
                placeholder=""
                options={dropdownOptions || []}
                value={displayedValue}
                onChange={(e) => handleChange(e, input)}
                styles={selectInputCustomStyles}
              />
            );
          }}
        />
      );
    }

    return (
      <Field
        className="form-control"
        name={`conditions.${m.name}`}
        render={({ input }) => (
          <CreatableSelect
            {...input}
            isMulti
            isClearable
            placeholder=""
            width="500px"
            value={ipRangeVal(input.value)}
            onChange={(e) => handleChange(e, input)}
            styles={selectInputCustomStyles}
            formatCreateLabel={(inputValue) =>
              `Create "${inputValue.trim()}"`
            }
            onCreateOption={(inputValue) => {
              const trimmedValue = inputValue.trim();
              if (trimmedValue) {
                const currentValues = input.value || [];
                const newValues = [...currentValues, trimmedValue];
                input.onChange(newValues);
              }
            }}
          />
        )}
      />
    );
  };

  return (
    <>
      <Modal
        show={showModal}
        onHide={handleClose}
        size="xl"
        aria-labelledby="contained-modal-title-vcenter"
        centered
      >
        <Form
          onSubmit={handleSubmit}
          initialValues={formInitialData()}
          render={({ handleSubmit }) => (
            <form onSubmit={handleSubmit}>
              <Modal.Header closeButton>
                <Modal.Title>{modalHeader}</Modal.Title>
              </Modal.Header>
              <Modal.Body>
                {filteredPolicyConditionDetails?.length > 0 &&
                  filteredPolicyConditionDetails.map((m) => {
                    const uiHintAttb = parseConditionUiHint(m.uiHint);
                    if (uiHintAttb) {
                      if (uiHintAttb?.singleValue) {
                        return (
                          <div key={m.name}>
                            <FormB.Group className="mb-3">
                              <b>{m.label}:</b>
                              <Field
                                className="form-control"
                                name={`conditions.${m.name}`}
                                render={({ input }) => (
                                  <Select
                                    {...input}
                                    options={accessedOpt}
                                    isClearable
                                    value={accessedVal(input.value)}
                                    onChange={(val) =>
                                      accessedOnChange(val, input)
                                    }
                                  />
                                )}
                              />
                            </FormB.Group>
                          </div>
                        );
                      }
                      if (uiHintAttb?.isMultiline) {
                        return (
                          <div key={m.name}>
                            <FormB.Group className="mb-3">
                              <Row>
                                <Col>
                                  <b>{m.label}:</b>
                                  <InfoIcon
                                    position="right"
                                    message={
                                      <p className="pd-10">
                                        {
                                          RegexMessage.MESSAGE
                                            .policyConditionInfoIcon
                                        }
                                      </p>
                                    }
                                  />
                                </Col>
                              </Row>
                              <Row>
                                <Col>
                                  <Field
                                    name={`conditions.${m.name}`}
                                    validate={validater}
                                    render={({ input, meta }) => (
                                      <>
                                        <FormB.Control
                                          {...input}
                                          className={
                                            meta.error
                                              ? "form-control border border-danger"
                                              : "form-control"
                                          }
                                          as="textarea"
                                          rows={3}
                                          onBlur={(e) =>
                                            trimInputValue(e, input)
                                          }
                                        />
                                        {meta.error && (
                                          <span className="invalid-field">
                                            {meta.error}
                                          </span>
                                        )}
                                      </>
                                    )}
                                  />
                                </Col>
                              </Row>
                            </FormB.Group>
                          </div>
                        );
                      }
                      if (uiHintAttb?.isMultiValue) {
                        return (
                          <div key={m.name}>
                            <FormB.Group className="mb-3">
                              <b>{m.label}:</b>
                              {renderMultiValueField(m, uiHintAttb)}
                            </FormB.Group>
                          </div>
                        );
                      }
                    }
                    return null;
                  })}
              </Modal.Body>
              <Modal.Footer>
                <Button variant="secondary" size="sm" onClick={handleClose}>
                  Close
                </Button>
                <Button title="Save" size="sm" type="submit">
                  Save
                </Button>
              </Modal.Footer>
            </form>
          )}
        />
      </Modal>
    </>
  );
}
