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

import React, { useState, useRef } from "react";
import { Field } from "react-final-form";
import { Form as FormB, Row, Col, Modal, Button } from "react-bootstrap";
import Select from "react-select";
import AsyncSelect from "react-select/async";
import {
  getServiceDefType,
  getServiceNameByServiceType,
  getZoneNameByServiceID,
  isRenderMasking,
  isRenderRowFilter,
  getSelectBoxErrorStyles,
  isKeyAdmin,
  isKMSAuditor
} from "Utils/XAUtils";

const required = (value) => (value ? undefined : "Required");

const ServiceStep = ({
  values,
  form,
  selectedServiceComponentDef,
  onServiceTypeChange,
  hasProgressedPastService,
  onResetForm
}) => {
  const [showResetModal, setShowResetModal] = useState(false);
  const pendingServiceSnapshotRef = useRef(null);
  const pendingAfterResetRef = useRef(null);

  const needsConfirmReset = Boolean(hasProgressedPastService);
  const isKeyAdminOrKMSAuditor = isKeyAdmin() || isKMSAuditor();

  /**
   * After the user has moved past the Service step, changing service details
   * invalidates later wizard data. We ask for confirmation, then reinitialize
   * the form from defaults plus the new service-field snapshot.
   */
  const requestChangeOrConfirmReset = (
    applyChange,
    getServiceSnapshot,
    afterReset
  ) => {
    if (!needsConfirmReset) {
      applyChange();
      return;
    }
    pendingServiceSnapshotRef.current = getServiceSnapshot();
    pendingAfterResetRef.current = afterReset || null;
    setShowResetModal(true);
  };

  const handleServiceTypeChange = (input, newValue) => {
    if (values?.serviceType?.value === newValue?.value) {
      return;
    }

    requestChangeOrConfirmReset(
      () => {
        input.onChange(newValue);
        form.change("serviceName", null);
        if (onServiceTypeChange) {
          onServiceTypeChange(newValue);
        }
      },
      () => ({
        serviceType: newValue,
        serviceName: null,
        zoneName: null,
        policyType: 0
      }),
      () => onServiceTypeChange?.(newValue)
    );
  };

  const handleServiceNameChange = (input, newValue) => {
    if (values?.serviceName?.value === newValue?.value) {
      return;
    }

    requestChangeOrConfirmReset(
      () => {
        input.onChange(newValue);
        form.change("zoneName", null);
      },
      () => ({
        serviceType: values.serviceType,
        serviceName: newValue,
        zoneName: null,
        policyType: values.policyType !== undefined ? values.policyType : 0
      })
    );
  };

  const handleZoneNameChange = (input, newValue) => {
    const prev = values?.zoneName;
    const same =
      prev === newValue ||
      (prev?.value != null &&
        newValue?.value != null &&
        prev.value === newValue.value);
    if (same) {
      return;
    }

    requestChangeOrConfirmReset(
      () => {
        input.onChange(newValue);
      },
      () => ({
        serviceType: values.serviceType,
        serviceName: values.serviceName,
        zoneName: newValue,
        policyType: values.policyType !== undefined ? values.policyType : 0
      })
    );
  };

  const handlePolicyTypeChange = (input, newValue) => {
    const cur = input.value === undefined ? 0 : input.value;
    const next = newValue === undefined ? 0 : newValue;
    if (cur === next) {
      return;
    }

    requestChangeOrConfirmReset(
      () => {
        input.onChange(newValue);
      },
      () => ({
        serviceType: values.serviceType,
        serviceName: values.serviceName,
        zoneName: values.zoneName,
        policyType: newValue
      })
    );
  };

  const handleCancelReset = () => {
    pendingServiceSnapshotRef.current = null;
    pendingAfterResetRef.current = null;
    setShowResetModal(false);
  };

  const handleConfirmReset = () => {
    const snapshot = pendingServiceSnapshotRef.current;
    const afterReset = pendingAfterResetRef.current;
    pendingServiceSnapshotRef.current = null;
    pendingAfterResetRef.current = null;
    setShowResetModal(false);
    afterReset?.();
    if (onResetForm) {
      onResetForm(snapshot || {});
    }
  };

  const loadServiceNames = async () => {
    if (!values?.serviceType) {
      return [];
    }
    const serviceTypeValue = values.serviceType.value;
    return await getServiceNameByServiceType(serviceTypeValue);
  };

  const loadZoneNames = async () => {
    if (!values?.serviceName || !values?.serviceType) {
      return [];
    }
    const serviceID = values.serviceName.serviceId;
    return await getZoneNameByServiceID(serviceID, selectedServiceComponentDef);
  };

  return (
    <Row>
      <Col lg={11}>
        <p className="stepper-instruction-text">
          Pick the service to manage access rules for its resources
        </p>
        {/* Service Type */}
        <FormB.Group as={Row} className="mb-4">
          <FormB.Label column className="fnt-14 column-1-fixed text-end">
            Service Type
            <span className="required-mark">*</span>
          </FormB.Label>
          <Col className="mw-50">
            <Field name="serviceType" validate={required}>
              {({ input, meta }) => (
                <>
                  <Select
                    {...input}
                    isClearable={true}
                    options={getServiceDefType()}
                    menuPlacement="auto"
                    placeholder="Select Service Type"
                    tabSelectsValue={false}
                    onChange={(value) => handleServiceTypeChange(input, value)}
                    styles={getSelectBoxErrorStyles(meta)}
                  />
                  {meta.touched && meta.error && (
                    <span className="text-danger">{meta.error}</span>
                  )}
                </>
              )}
            </Field>
          </Col>
        </FormB.Group>

        {/* Service Name */}
        <FormB.Group as={Row} className="mb-4">
          <FormB.Label column className="fnt-14 column-1-fixed text-end">
            Service Name
            <span className="required-mark">*</span>
          </FormB.Label>
          <Col className="mw-50">
            <Field name="serviceName" validate={required}>
              {({ input, meta }) => (
                <>
                  <AsyncSelect
                    {...input}
                    key={values?.serviceType?.value}
                    isClearable={true}
                    loadOptions={loadServiceNames}
                    defaultOptions
                    menuPlacement="auto"
                    placeholder="Select Service Name"
                    tabSelectsValue={false}
                    isDisabled={!values.serviceType}
                    onChange={(value) => handleServiceNameChange(input, value)}
                    styles={getSelectBoxErrorStyles(meta)}
                  />
                  {meta.touched && meta.error && (
                    <span className="text-danger">{meta.error}</span>
                  )}
                </>
              )}
            </Field>
          </Col>
        </FormB.Group>

        {/* Show Zone Name and Policy Type only if Service Name is selected */}
        {values.serviceName && !isKeyAdminOrKMSAuditor && (
          <>
            {/* Zone Name */}
            <FormB.Group as={Row} className="mb-4">
              <FormB.Label column className="fnt-14 column-1-fixed text-end">
                Zone Name
              </FormB.Label>
              <Col className="mw-50">
                <Field name="zoneName">
                  {({ input }) => (
                    <AsyncSelect
                      {...input}
                      key={values?.serviceName?.value}
                      isClearable={true}
                      P
                      loadOptions={loadZoneNames}
                      defaultOptions
                      menuPlacement="auto"
                      placeholder="Select Security Zone"
                      tabSelectsValue={false}
                      onChange={(value) => handleZoneNameChange(input, value)}
                    />
                  )}
                </Field>
              </Col>
            </FormB.Group>

            {/* Policy Type */}
            {selectedServiceComponentDef &&
              (isRenderMasking(selectedServiceComponentDef.dataMaskDef) ||
                isRenderRowFilter(
                  selectedServiceComponentDef.rowFilterDef
                )) && (
                <FormB.Group as={Row} className="mb-4">
                  <FormB.Label column className="column-1-fixed text-end">
                    Policy Type
                  </FormB.Label>
                  <Col className="mw-50">
                    <Field name="policyType">
                      {({ input }) => (
                        <div className="d-flex gap-3">
                          {/* Access Policy - Value: 0 */}
                          <div
                            className="card p-3 w-33"
                            style={{
                              cursor: "pointer",
                              border:
                                input.value === 0 || input.value === undefined
                                  ? "2px solid #0b7fad"
                                  : "1px solid #dee2e6"
                            }}
                            onClick={() => handlePolicyTypeChange(input, 0)}
                          >
                            <div className="form-check">
                              <input
                                className="form-check-input"
                                type="radio"
                                name="policyType"
                                id="access"
                                value={0}
                                checked={
                                  input.value === 0 || input.value === undefined
                                }
                                onChange={() =>
                                  handlePolicyTypeChange(input, 0)
                                }
                              />
                              <label
                                className="form-check-label"
                                htmlFor="access"
                              >
                                <strong>Access</strong>
                              </label>
                            </div>
                            <small className="text-muted mt-2">
                              Controls who can access the resource.
                            </small>
                          </div>

                          {/* Masking Policy - Value: 1 */}
                          {isRenderMasking(
                            selectedServiceComponentDef.dataMaskDef
                          ) && (
                            <div
                              className="card p-3 w-33"
                              style={{
                                cursor: "pointer",
                                border:
                                  input.value === 1
                                    ? "2px solid #0b7fad"
                                    : "1px solid #dee2e6"
                              }}
                              onClick={() => handlePolicyTypeChange(input, 1)}
                            >
                              <div className="form-check">
                                <input
                                  className="form-check-input"
                                  type="radio"
                                  name="policyType"
                                  id="masking"
                                  value={1}
                                  checked={input.value === 1}
                                  onChange={() =>
                                    handlePolicyTypeChange(input, 1)
                                  }
                                />
                                <label
                                  className="form-check-label"
                                  htmlFor="masking"
                                >
                                  <strong>Masking</strong>
                                </label>
                              </div>
                              <small className="text-muted mt-2">
                                Masks sensitive data fields
                              </small>
                            </div>
                          )}

                          {/* Row Filtering Policy - Value: 2 */}
                          {isRenderRowFilter(
                            selectedServiceComponentDef.rowFilterDef
                          ) && (
                            <div
                              className="card p-3 w-33"
                              style={{
                                cursor: "pointer",
                                border:
                                  input.value === 2
                                    ? "2px solid #0b7fad"
                                    : "1px solid #dee2e6"
                              }}
                              onClick={() => handlePolicyTypeChange(input, 2)}
                            >
                              <div className="form-check">
                                <input
                                  className="form-check-input"
                                  type="radio"
                                  name="policyType"
                                  id="rowFiltering"
                                  value={2}
                                  checked={input.value === 2}
                                  onChange={() =>
                                    handlePolicyTypeChange(input, 2)
                                  }
                                />
                                <label
                                  className="form-check-label"
                                  htmlFor="rowFiltering"
                                >
                                  <strong>Row Filtering</strong>
                                </label>
                              </div>
                              <small className="text-muted mt-2">
                                Shows only allowed rows
                              </small>
                            </div>
                          )}
                        </div>
                      )}
                    </Field>
                  </Col>
                </FormB.Group>
              )}
          </>
        )}

        <Modal show={showResetModal} onHide={handleCancelReset} centered>
          <Modal.Header closeButton>
            <Modal.Title>Reset policy form?</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <p className="mt-2 mb-0 text-muted">
              All policy form fields will be cleared. Are you sure you want to
              reset and start over ?
            </p>
          </Modal.Body>
          <Modal.Footer>
            <Button variant="secondary" size="sm" onClick={handleCancelReset}>
              Cancel
            </Button>
            <Button variant="danger" size="sm" onClick={handleConfirmReset}>
              Reset form
            </Button>
          </Modal.Footer>
        </Modal>
      </Col>
    </Row>
  );
};

export default ServiceStep;
