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

import React, { useEffect, useState, useRef } from "react";
import { Form as FormB, Row, Col } from "react-bootstrap";
import { Field } from "react-final-form";
import Select from "react-select";
import BootstrapSwitchButton from "bootstrap-switch-button-react";
import { filter, groupBy, some } from "lodash";
import { toast } from "react-toastify";
import { udfResourceWarning } from "../../utils/XAMessages";
import ResourceSelectComp from "./ResourceSelectComp";
import { getResourcesDefVal } from "Utils/XAUtils";
import { selectInputCustomStyles } from "Components/CommonComponents";

const noneOptions = {
  label: "None",
  value: "none"
};

export default function ResourceComp(props) {
  const {
    serviceCompDetails,
    formValues,
    serviceDetails,
    policyType,
    policyId,
    name,
    isMultiResources
  } = props;
  const [rsrcState, setLoader] = useState({ loader: false, resourceKey: -1 });
  const toastId = useRef(null);

  let resources = getResourcesDefVal(serviceCompDetails, policyType);

  useEffect(() => {
    if (rsrcState.loader) {
      setLoader({
        loader: false,
        resourceKey: -1
      });
    }
  }, [rsrcState.loader]);

  const grpResources = groupBy(resources, "level");
  let grpResourcesKeys = [];
  for (const resourceKey in grpResources) {
    grpResourcesKeys.push(+resourceKey);
  }
  grpResourcesKeys = grpResourcesKeys.sort();

  const getResourceLabelOp = (levelKey, index) => {
    let op = grpResources[levelKey];
    if (index !== 0) {
      let previousKey = grpResourcesKeys[index - 1];
      const parentResourceKey = `resourceName-${previousKey}`;
      if (formValues && formValues[parentResourceKey]) {
        op = filter(grpResources[levelKey], {
          parent: formValues[parentResourceKey].name
        });
        if (formValues[parentResourceKey].isValidLeaf) {
          op.push(noneOptions);
        }
      }
    }
    return op;
  };

  const RenderValidateField = ({ name }) =>
    (formValues && formValues[name]?.mandatory && <span>*</span>) || (
      <span>&nbsp;</span>
    );

  const renderResourceSelect = (levelKey, index) => {
    let renderLabel = false;
    const resourceKey = `resourceName-${levelKey}`;
    let levelOp = grpResources[levelKey];
    if (index !== 0) {
      levelOp = getResourceLabelOp(levelKey, index);
    }
    if (levelOp.length === 1 && !formValues[resourceKey]?.parent?.length > 0) {
      renderLabel = true;
    } else {
      if (index !== 0) {
        let previousKey = grpResourcesKeys[index - 1];
        const parentResourceKey = `resourceName-${previousKey}`;
        let op = filter(levelOp, {
          parent: formValues[parentResourceKey].name
        });
        if (op.length === 1 && !formValues[parentResourceKey].isValidLeaf) {
          renderLabel = true;
        }
      }
    }
    return renderLabel;
  };

  const validateResourceForm = (levelKey, index) => {
    let previousKey = grpResourcesKeys[index - 1];
    const resourceKey = `resourceName-${previousKey}`;
    let hasValid = false;
    if (formValues && formValues[resourceKey]) {
      hasValid = some(grpResources[levelKey], {
        parent: formValues[resourceKey].name
      });
    }
    return hasValid;
  };

  const handleResourceChange = (selectedVal, input, index) => {
    for (let i = index; i < grpResourcesKeys.length; i++) {
      let levelKey = grpResourcesKeys[i];

      delete formValues[`resourceName-${levelKey}`];
      delete formValues[`value-${levelKey}`];
      delete formValues[`isExcludesSupport-${levelKey}`];
      delete formValues[`isRecursiveSupport-${levelKey}`];
    }
    delete formValues[`value-${grpResourcesKeys[index]}`];
    let CurrentSelectedResourcs = selectedVal.name;
    for (let j = index + 1; j < grpResourcesKeys.length; j++) {
      let level = grpResourcesKeys[j];
      let nextResource = resources.find((m) => {
        if (m?.parent) {
          return m.parent === CurrentSelectedResourcs;
        }
      });
      if (nextResource) {
        formValues[`resourceName-${level}`] = nextResource;
        CurrentSelectedResourcs = nextResource.name;
      }
    }

    if (selectedVal?.name === "udf" && selectedVal?.parent === "database") {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning(udfResourceWarning());
    }

    input.onChange(selectedVal);
    setLoader({
      loader: true,
      resourceKey: grpResourcesKeys[index]
    });
  };

  return grpResourcesKeys.map((levelKey, index) => {
    const resourceKey = `resourceName-${levelKey}`;
    if (index !== 0) {
      let hasValid = validateResourceForm(levelKey, index);
      if (!hasValid) {
        return null;
      }
    }
    if (rsrcState.loader && rsrcState.resourceKey < levelKey) {
      return null;
    }

    return (
      <FormB.Group
        as={Row}
        className="mb-3"
        controlId={`Resource-${levelKey}`}
        key={`Resource-${levelKey}`}
      >
        <Col sm={3}>
          <Field
            defaultValue={!policyId && getResourceLabelOp(levelKey, index)[0]}
            name={
              isMultiResources
                ? `${name}.resourceName-${levelKey}`
                : `resourceName-${levelKey}`
            }
            render={({ input }) =>
              formValues[resourceKey] ? (
                renderResourceSelect(levelKey, index) ? (
                  <span className="float-end fnt-14">
                    <FormB.Label className="position-relative pe-2">
                      {getResourceLabelOp(levelKey, index)[0]["label"]}
                      <RenderValidateField name={`resourceName-${levelKey}`} />
                    </FormB.Label>
                  </span>
                ) : (
                  <div className="resource-drop-down">
                    <span className="w-75">
                      <Select
                        {...input}
                        options={getResourceLabelOp(levelKey, index)}
                        getOptionLabel={(obj) => obj.label}
                        getOptionValue={(obj) => obj.name}
                        onChange={(value) =>
                          handleResourceChange(value, input, index)
                        }
                        isSearchable={false}
                        styles={selectInputCustomStyles}
                      />
                    </span>
                    <RenderValidateField name={`resourceName-${levelKey}`} />
                  </div>
                )
              ) : null
            }
          />
        </Col>

        {formValues[`resourceName-${levelKey}`] && (
          <>
            <Col sm={5}>
              <ResourceSelectComp
                levelKey={levelKey}
                formValues={formValues}
                grpResourcesKeys={grpResourcesKeys}
                serviceDetails={serviceDetails}
                name={name}
                isMultiResources={isMultiResources}
              />
            </Col>
          </>
        )}
        {formValues[`resourceName-${levelKey}`] && (
          <Col sm={4}>
            <Row>
              {formValues[`resourceName-${levelKey}`]["excludesSupported"] && (
                <Col sm={5}>
                  <Field
                    className="form-control"
                    name={
                      isMultiResources
                        ? `${name}.isExcludesSupport-${levelKey}`
                        : `isExcludesSupport-${levelKey}`
                    }
                    render={({ input }) => (
                      <BootstrapSwitchButton
                        {...input}
                        checked={!(input.value === false)}
                        onlabel="Include"
                        onstyle="primary"
                        offlabel="Exclude"
                        offstyle="outline-secondary"
                        style="w-100"
                        size="xs"
                        key={`isExcludesSupport-${levelKey}`}
                      />
                    )}
                  />
                </Col>
              )}
              {formValues[`resourceName-${levelKey}`]["recursiveSupported"] && (
                <Col sm={5} className="toggle-switch">
                  <Field
                    className="form-control"
                    name={
                      isMultiResources
                        ? `${name}.isRecursiveSupport-${levelKey}`
                        : `isRecursiveSupport-${levelKey}`
                    }
                    render={({ input }) => (
                      <BootstrapSwitchButton
                        {...input}
                        checked={!(input.value === false)}
                        onlabel="Recursive"
                        onstyle="primary"
                        width={130}
                        offlabel="Non-recursive"
                        offstyle="outline-secondary"
                        size="xs"
                        key={`isRecursiveSupport-${levelKey}`}
                      />
                    )}
                  />
                </Col>
              )}
            </Row>
          </Col>
        )}
      </FormB.Group>
    );
  });
}
