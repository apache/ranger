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

import React, { useEffect, useReducer, useState } from "react";
import { Form as FormB, Row, Col } from "react-bootstrap";
import { Field } from "react-final-form";
import Select from "react-select";
import BootstrapSwitchButton from "bootstrap-switch-button-react";
import AsyncCreatableSelect from "react-select/async-creatable";
import { debounce, filter, groupBy, some, sortBy } from "lodash";

import { fetchApi } from "Utils/fetchAPI";
import { RangerPolicyType } from "Utils/XAEnums";

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
    policyItem,
    policyId
  } = props;
  const [rsrcState, setLoader] = useState({ loader: false, resourceKey: -1 });
  let resources = sortBy(serviceCompDetails.resources, "itemId");
  if (RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value == policyType) {
    resources = sortBy(serviceCompDetails.dataMaskDef.resources, "itemId");
  } else if (
    RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value == policyType
  ) {
    resources = sortBy(serviceCompDetails.rowFilterDef.resources, "itemId");
  }

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

  const fetchResourceLookup = async (
    inputValue,
    resourceObj,
    selectedValues,
    callback
  ) => {
    let resourceName = resourceObj.name;
    let data = {
      resourceName,
      resources: {
        [resourceName]: selectedValues?.map?.(({ value }) => value) || []
      }
    };
    if (inputValue) {
      data["userInput"] = inputValue || "";
    }

    let op = [];
    try {
      if (resourceObj.lookupSupported) {
        const resourceResp = await fetchApi({
          url: `plugins/services/lookupResource/${serviceDetails.name}`,
          method: "POST",
          data
        });
        op =
          resourceResp.data?.map?.((name) => ({
            label: name,
            value: name
          })) || [];
      }
    } catch (error) {}

    callback(op);
  };

  const _fetchResourceLookup = debounce(fetchResourceLookup, 1000);

  const getResourceLabelOp = (levelKey, index) => {
    let op = grpResources[levelKey];
    if (index !== 0) {
      let previousKey = grpResourcesKeys[index - 1];
      const parentResourceKey = `resourceName-${previousKey}`;
      const resourceKey = `resourceName-${levelKey}`;
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
    (formValues && formValues[name]?.mandatory && (
      <span className="compulsory-resource">*</span>
    )) ||
    null;

  const renderResourceSelect = (levelKey, index) => {
    let renderLabel = false;
    const resourceKey = `resourceName-${levelKey}`;
    let levelOp = grpResources[levelKey];
    if (index !== 0) {
      levelOp = getResourceLabelOp(levelKey, index);
    }
    if (
      levelOp.length === 1 &&
      !formValues[resourceKey].hasOwnProperty("parent")
    ) {
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
    for (let i = index + 1; i < grpResourcesKeys.length; i++) {
      let levelKey = grpResourcesKeys[i];

      delete formValues[`resourceName-${levelKey}`];
      delete formValues[`value-${levelKey}`];
      delete formValues[`isExcludesSupport-${levelKey}`];
      delete formValues[`isRecursiveSupport-${levelKey}`];
    }
    if (policyItem) {
      removedSeletedAccess();
    }
    setLoader({
      loader: true,
      resourceKey: grpResourcesKeys[index]
    });
    delete formValues[`value-${grpResourcesKeys[index]}`];
    input.onChange(selectedVal);
  };

  const removedSeletedAccess = () => {
    for (const name of [
      "policyItems",
      "allowExceptions",
      "denyPolicyItems",
      "denyExceptions"
    ]) {
      for (const policyObj of formValues[name]) {
        if (policyObj?.accesses) {
          policyObj.accesses = [];
        }
      }
    }
  };

  const required = (value) => {
    if (!value || value.length == 0) {
      return "Required";
    }
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

    const customStyles = {
      container: () => ({
        width: "75%",
        display: "inline-block",
        float: "right"
      })
    };

    return (
      <FormB.Group
        as={Row}
        className="mb-3"
        controlId="policyName"
        key={`Resource-${levelKey}`}
      >
        <Col sm={3}>
          <Field
            defaultValue={getResourceLabelOp(levelKey, index)[0]}
            className="form-control"
            name={`resourceName-${levelKey}`}
            render={({ input, meta }) =>
              formValues[resourceKey] ? (
                renderResourceSelect(levelKey, index) ? (
                  <span className="pull-right fnt-14">
                    <FormB.Label>
                      {getResourceLabelOp(levelKey, index)[0]["label"]}
                    </FormB.Label>
                    <RenderValidateField name={`resourceName-${levelKey}`} />
                  </span>
                ) : (
                  <>
                    <Select
                      {...input}
                      options={getResourceLabelOp(levelKey, index)}
                      getOptionLabel={(obj) => obj.label}
                      getOptionValue={(obj) => obj.name}
                      onChange={(value) =>
                        handleResourceChange(value, input, index)
                      }
                      styles={customStyles}
                    />
                    <RenderValidateField name={`resourceName-${levelKey}`} />
                  </>
                )
              ) : null
            }
          />
        </Col>
        {formValues[`resourceName-${levelKey}`] && (
          <Col sm={5}>
            <Field
              key={formValues[`resourceName-${levelKey}`].name}
              className="form-control"
              name={`value-${levelKey}`}
              validate={
                formValues &&
                formValues[`resourceName-${levelKey}`]?.mandatory &&
                required
              }
              render={({ input, meta }) => (
                <>
                  <AsyncCreatableSelect
                    {...input}
                    id={
                      formValues &&
                      formValues[`resourceName-${levelKey}`]?.mandatory &&
                      meta.error &&
                      meta.touched
                        ? "isError"
                        : `value-${levelKey}`
                    }
                    defaultOptions
                    isMulti
                    isDisabled={
                      formValues[`resourceName-${levelKey}`].value ===
                      noneOptions.value
                    }
                    loadOptions={(inputValue) =>
                      new Promise((resolve, reject) => {
                        _fetchResourceLookup(
                          inputValue,
                          formValues[`resourceName-${levelKey}`],
                          input.value,
                          resolve
                        );
                      })
                    }
                  />
                  {formValues &&
                    formValues[`resourceName-${levelKey}`]?.mandatory &&
                    meta.touched &&
                    meta.error && (
                      <span className="invalid-field">{meta.error}</span>
                    )}
                </>
              )}
            />
          </Col>
        )}
        {formValues[`resourceName-${levelKey}`] && (
          <Col sm={4}>
            <Row>
              {formValues[`resourceName-${levelKey}`]["excludesSupported"] && (
                <Col sm={5}>
                  <Field
                    className="form-control"
                    name={`isExcludesSupport-${levelKey}`}
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
                    name={`isRecursiveSupport-${levelKey}`}
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
