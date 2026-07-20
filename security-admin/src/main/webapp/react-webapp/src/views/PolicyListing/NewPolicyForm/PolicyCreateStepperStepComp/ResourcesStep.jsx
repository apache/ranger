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
import { Row, Col, Button } from "react-bootstrap";
import { FieldArray } from "react-final-form-arrays";
import ResourceComp from "../../../Resources/ResourceComp";

const ResourcesStep = ({ values, form, selectedServiceComponentDef }) => {
  // Get policyType from form values (defaults to 0 for Access policy)
  const policyType = values?.policyType !== undefined ? values.policyType : 0;

  // Ensure additionalResources exists
  React.useEffect(() => {
    if (
      !values.additionalResources ||
      values.additionalResources.length === 0
    ) {
      form.change("additionalResources", [{}]);
    }
  }, []);

  // Add a new resource to the array
  const addResource = () => {
    const currentResources = values.additionalResources || [];
    form.change("additionalResources", [...currentResources, {}]);
  };

  // serviceName can be a react-select option {value, label} (stepper) or a plain string (edit form)
  const serviceDetails = {
    name:
      typeof values.serviceName === "object"
        ? (values.serviceName?.value ?? "")
        : (values.serviceName ?? "")
  };

  return (
    <Row>
      <Col xxl={12}>
        <p className="stepper-instruction-text mb-4">
          Define the resources for which this policy will apply
        </p>

        {/* Multiple Resources with FieldArray */}
        <FieldArray name="additionalResources">
          {({ fields }) => (
            <>
              {fields.map((name, index) => (
                <Row className="resource-block mb-3" key={`${name}-${index}`}>
                  <Col md={11}>
                    {selectedServiceComponentDef && (
                      <ResourceComp
                        serviceDetails={serviceDetails}
                        serviceCompDetails={selectedServiceComponentDef}
                        formValues={values.additionalResources?.[index]}
                        policyType={policyType}
                        name={name}
                        isMultiResources={true}
                      />
                    )}
                  </Col>
                  <Col
                    md={1}
                    className="d-flex align-items-start justify-content-start"
                  >
                    {fields.length > 1 && (
                      <Button
                        variant="danger"
                        size="sm"
                        title="Remove Resource"
                        onClick={() => fields.remove(index)}
                        data-action="delete"
                        data-cy="delete"
                      >
                        <i className="fa-fw fa fa-remove"></i>
                      </Button>
                    )}
                  </Col>
                  {fields.length > 1 && index < fields.length - 1 && <hr />}
                </Row>
              ))}
            </>
          )}
        </FieldArray>

        {/* Add Resource Button */}
        <Row className="mt-3">
          <Col>
            <Button
              type="button"
              variant="outline-primary"
              onClick={addResource}
              data-action="addResource"
              data-cy="addResource"
            >
              <i className="fa-fw fa fa-plus"></i> Add Resource
            </Button>
          </Col>
        </Row>
      </Col>
    </Row>
  );
};

export default ResourcesStep;
