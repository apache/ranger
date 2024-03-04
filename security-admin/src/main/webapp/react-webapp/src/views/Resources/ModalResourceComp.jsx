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
import Modal from "react-bootstrap/Modal";
import { Form, Field } from "react-final-form";
import { Button } from "react-bootstrap";
import ResourceComp from "./ResourceComp";

export default function ModalResourceComp(props) {
  const {
    serviceDetails,
    serviceCompDetails,
    modelState,
    handleClose,
    handleSave
  } = props;

  const saveResourceVal = (values) => {
    modelState.data = values;
    handleSave();
  };

  if (!modelState.data) {
    return null;
  }

  return (
    <>
      <Modal
        show={modelState.showModalResource}
        onHide={handleClose}
        size="xl"
        aria-labelledby="contained-modal-title-vcenter"
        centered
      >
        <Form
          onSubmit={saveResourceVal}
          initialValues={modelState.data}
          render={({ handleSubmit, values }) => (
            <form onSubmit={handleSubmit}>
              <Modal.Header closeButton>
                <Modal.Title>Resource Details</Modal.Title>
              </Modal.Header>
              <Modal.Body>
                <Field name="resources">
                  {(input) => (
                    <div className="wrap">
                      <ResourceComp
                        {...input}
                        serviceDetails={serviceDetails}
                        serviceCompDetails={serviceCompDetails}
                        formValues={values}
                        policyType={0}
                        isMultiResources={false}
                      />
                    </div>
                  )}
                </Field>
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
