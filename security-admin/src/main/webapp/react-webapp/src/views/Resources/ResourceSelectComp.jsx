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
import CreatableSelect from "react-select/creatable";
import { debounce, isArray } from "lodash";
import { toast } from "react-toastify";
import { fetchApi } from "Utils/fetchAPI";
import { selectInputCustomStyles } from "Components/CommonComponents";

const noneOptions = {
  label: "None",
  value: "none"
};

export default function ResourceSelectComp(props) {
  const {
    formValues,
    grpResourcesKeys,
    levelKey,
    serviceDetails,
    name,
    isMultiResources
  } = props;
  const [options, setOptions] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const toastId = useRef(null);

  const fetchResourceLookup = async (
    inputValue,
    resourceObj,
    selectedFormValues
  ) => {
    setIsLoading(true);
    let resourceName = resourceObj.name;
    let resourceDataLevel = grpResourcesKeys.slice(
      0,
      grpResourcesKeys.indexOf(resourceObj.level) + 1
    );
    let resourceData = {};
    if (resourceDataLevel.length > 0) {
      resourceDataLevel.map(function (m) {
        resourceData[selectedFormValues[`resourceName-${m}`].name] = isArray(
          selectedFormValues[`value-${m}`]
        )
          ? selectedFormValues[`value-${m}`]?.map?.(({ value }) => value) || []
          : selectedFormValues[`value-${m}`] != undefined
          ? [selectedFormValues[`value-${m}`].value]
          : [];
      });
    }
    let data = {
      resourceName,
      resources: resourceData,
      userInput: inputValue || ""
    };

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
    } catch (error) {
      setIsLoading(false);
      toast.dismiss(toastId.current);
      if (error?.response?.data?.msgDesc) {
        toastId.current = toast.error(error.response.data.msgDesc);
      } else {
        toastId.current = toast.error(
          "Resouce lookup failed for current resource"
        );
      }
    }
    setOptions(op);
    setIsLoading(false);
  };

  const fetchDelayResourceLookup = useRef(debounce(fetchResourceLookup, 1000));

  const required = (val, formVal) => {
    if (!val || val.length == 0) {
      return "Required";
    }
    if (formVal?.validationRegEx && val?.length > 0) {
      var regex = new RegExp(formVal.validationRegEx);
      if (formVal.validationRegEx == "^\\*$") {
        if (regex.test(val[val.length - 1].value) == false)
          return 'Only "*" value is allowed';
      }
      if (formVal.validationRegEx == "^[/*]$|^/.*?[^/]$") {
        if (regex.test(val[val.length - 1].value) == false)
          return "Relative Path start with slash and must not end with a slash";
      }
    }
  };

  const onLookupChange = (
    object,
    resourceObj,
    selectedFormValues,
    { action }
  ) => {
    switch (action) {
      case "input-change":
        if (object != undefined) {
          setIsLoading(true);
          setOptions([]);
          fetchDelayResourceLookup.current(
            object,
            resourceObj,
            selectedFormValues
          );
        }
        return;
      default:
        return;
    }
  };

  const rcsValidation = (m) => {
    return required(m, formValues[`resourceName-${levelKey}`]);
  };

  const supportMultipleVal = (rscVal) => {
    if (rscVal?.uiHint) {
      let singleVal = JSON.parse(rscVal.uiHint);
      return singleVal.singleValue ? false : true;
    } else {
      return true;
    }
  };

  const customFilterOptions = (option, rawInput) => {
    const inputValue = rawInput.trim().toLowerCase();

    if (!inputValue || inputValue == "*") {
      return true; // Show all options when input is empty
    }
    return true;
  };

  return (
    <Field
      key={formValues[`resourceName-${levelKey}`].name}
      className="form-control"
      name={
        isMultiResources ? `${name}.value-${levelKey}` : `value-${levelKey}`
      }
      validate={
        formValues &&
        formValues[`resourceName-${levelKey}`]?.mandatory &&
        rcsValidation
      }
      render={({ input, meta }) => (
        <>
          <CreatableSelect
            {...input}
            id={
              formValues &&
              formValues[`resourceName-${levelKey}`]?.mandatory &&
              meta.error
                ? "isError"
                : isMultiResources
                ? `${name}.value-${levelKey}`
                : `value-${levelKey}`
            }
            isMulti={supportMultipleVal(formValues[`resourceName-${levelKey}`])}
            isClearable
            isDisabled={
              formValues[`resourceName-${levelKey}`].value === noneOptions.value
            }
            options={options}
            onFocus={(e) => {
              e.stopPropagation();
              setOptions([]);
              fetchResourceLookup(
                "",
                formValues[`resourceName-${levelKey}`],
                formValues
              );
            }}
            onInputChange={(inputVal, action) => {
              onLookupChange(
                inputVal,
                formValues[`resourceName-${levelKey}`],
                formValues,
                action
              );
            }}
            filterOption={customFilterOptions}
            isLoading={isLoading}
            styles={selectInputCustomStyles}
          />
          {formValues &&
            formValues[`resourceName-${levelKey}`]?.mandatory &&
            meta.touched &&
            meta.error && <span className="invalid-field">{meta.error}</span>}
        </>
      )}
    />
  );
}
