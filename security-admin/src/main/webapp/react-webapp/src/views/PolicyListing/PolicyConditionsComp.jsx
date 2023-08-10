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

import React, { useState } from "react";
import { Col, Form as FormB, Row, Modal, Button } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import Select from "react-select";
import CreatableSelect from "react-select/creatable";
import { find, omit } from "lodash";
import { InfoIcon } from "../../utils/XAUtils";
import { RegexMessage } from "../../utils/XAMessages";
const esprima = require("esprima");

export default function PolicyConditionsComp(props) {
  const { policyConditionDetails, inputVal, showModal, handleCloseModal } =
    props;

  const accessedOpt = [
    { value: "yes", label: "Yes" },
    { value: "no", label: "No" }
  ];

  const handleSubmit = (values) => {
    for (let val in values.conditions) {
      if (values.conditions[val] == null || values.conditions[val] == "") {
        omit(values.conditions, val);
      }
    }
    inputVal.onChange(values.conditions);
    handleClose();
  };

  const handleClose = () => {
    handleCloseModal(false);
  };

  const formInitialData = () => {
    var conditions = {};
    if (inputVal && inputVal.value) {
      for (let val in inputVal.value) {
        conditions[val] = inputVal.value[val];
      }
    }
    let formData = { conditions };
    return formData;
  };

  const accessedVal = (val) => {
    let value = null;
    if (val) {
      let opObj = find(accessedOpt, { value: val });
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
    let value = null;
    if (val) {
      value = val.map((m) => m.value).join(",");
    }
    input.onChange(value);
  };

  const ipRangeVal = (val) => {
    let value = null;
    if (val) {
      value = val.split(",").map((m) => ({ label: m, value: m }));
    }
    return value;
  };

  const validater = (values) => {
    let errors = "";
    if (values) {
      try {
        let t = esprima.parseScript(values);
      } catch (e) {
        errors = e.message;
      }
    }
    return errors;
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
          initialValues={formInitialData}
          render={({ handleSubmit, form, submitting, pristine, values }) => (
            <form onSubmit={handleSubmit}>
              <Modal.Header closeButton>
                <Modal.Title>Policy Condition</Modal.Title>
              </Modal.Header>
              <Modal.Body>
                {policyConditionDetails?.length > 0 &&
                  policyConditionDetails.map((m, index) => {
                    if (m.name == "accessed-after-expiry") {
                      return (
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
                                onChange={(val) => accessedOnChange(val, input)}
                              />
                            )}
                          />
                        </FormB.Group>
                      );
                    }
                    if (m.name == "expression") {
                      return (
                        <>
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
                                          .policyconditioninfoicon
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
                        </>
                      );
                    }
                    if (m.name == "ip-range") {
                      return (
                        <FormB.Group className="mb-3">
                          <b>{m.label}:</b>

                          <Field
                            className="form-control"
                            name={`conditions.${m.name}`}
                            render={({ input }) => (
                              <CreatableSelect
                                {...input}
                                isMulti
                                isClearable
                                placeholder="enter expression"
                                width="500px"
                                value={ipRangeVal(input.value)}
                                onChange={(e) => handleChange(e, input)}
                              />
                            )}
                          />
                        </FormB.Group>
                      );
                    }
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
