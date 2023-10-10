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

import React, { useState, useReducer, useEffect, useMemo } from "react";
import {
  serverError,
  policyConditionUpdatedJSON
} from "../../../utils/XAUtils";
import Editable from "Components/Editable";
import { useNavigate } from "react-router-dom";
import { Button, Form as FormB, Card } from "react-bootstrap";
import Select from "react-select";
import { filter, groupBy, some, sortBy, forIn, has, isArray } from "lodash";
import AsyncSelect from "react-select/async";
import { Form, Field } from "react-final-form";
import { fetchApi } from "Utils/fetchAPI";
import PrinciplePermissionComp from "../Dataset/PrinciplePermissionComp";

import AddSharedResourceComp from "./AddSharedResourceComp";
import { toast } from "react-toastify";

const initialState = {
  loader: false,
  preventUnBlock: false,
  blockUI: false,
  formData: {}
};

function reducer(state, action) {
  switch (action.type) {
    case "SET_DATA":
      return {
        ...state,
        loader: false,
        serviceDetails: action.serviceDetails,
        serviceCompDetails: action.serviceCompDetails,
        policyData: action?.policyData,
        formData: action.formData
      };
    default:
      throw new Error();
  }
}

const datashareFormReducer = (state, action) => {
  switch (action.type) {
    case "SET_SELECTED_SERVICE":
      return {
        ...state,
        selectedService: action.selectedService
      };
    case "SET_SELECTED_ZONE":
      return {
        ...state,
        selectedZone: action.selectedZone
      };
    case "SET_PREVENT_ALERT":
      return {
        ...state,
        preventUnBlock: action.preventUnBlock
      };
    case "SET_BLOCK_UI":
      return {
        ...state,
        blockUI: action.blockUI
      };
    default:
      throw new Error();
  }
};

const AddDatashareView = () => {
  const [step, setStep] = useState(1);
  const [saveButtonText, setSaveButtonText] = useState("Continue");
  const [datashareName, setName] = useState();
  const [datashareDescription, setDatashareDescription] = useState();
  const [datashareDetails, dispatch] = useReducer(
    datashareFormReducer,
    initialState
  );
  const { loader, selectedService, selectedZone, preventUnBlock } =
    datashareDetails;
  const [serviceDef, setServiceDef] = useState({});
  const [serviceDetails, setService] = useState({});
  const [policyState, policyDispatch] = useReducer(reducer, initialState);
  const { loaders, serviceDetailss, serviceCompDetails, policyData, formData } =
    policyState;
  const [blockUI, setBlockUI] = useState(false);
  const [tagName, setTagName] = useState();
  const [showModal, policyConditionState] = useState(false);
  const [sharedResourceList, setSharedResourceList] = useState([]);
  const [userList, setUserList] = useState([]);
  const [groupList, setGroupList] = useState([]);
  const [roleList, setRoleList] = useState([]);
  const [maskDef, setMaskDef] = useState(false);
  const [rowFilter, setRowFilterDef] = useState(false);
  const [accessTypeOptions, setAccessTypeOptions] = useState([]);
  const [accessType, setAccessType] = useState([]);
  const [maskTypeOptions, setMaskTypeOptions] = useState([]);
  const [selectedShareMask, setSelectedShareMask] = useState({});
  const [datashareTermsAndConditions, setDatashareTermsAndConditions] =
    useState();
  const [datashareConditionExpr, setDatashareConditionExpr] = useState();
  const navigate = useNavigate();

  const cancelDatashareDetails = () => {
    if (step == 1) {
      navigate("/gds/mydatasharelisting");
    } else {
      setStep(step - 1);
    }
    setSaveButtonText("Continue");
  };

  const datashareTermsAndConditionsChange = (event) => {
    setDatashareTermsAndConditions(event.target.value);
  };

  const noneOptions = {
    label: "None",
    value: "none"
  };

  const toggleMoreOptionsModalClose = () => {
    setShowMoreOptions(false);
  };

  const toggleClose = () => {
    setShowMoreOptions(false);
  };

  const subhmitDatashareDetails = async (values) => {
    if (step == 5) {
      let dataShareInfo = {
        name: datashareName,
        acl: {
          users: {},
          groups: {},
          roles: {}
        },
        zone: selectedZone != undefined ? selectedZone.label : "",
        service: selectedService.label,
        description: datashareDescription,
        termsOfUse: datashareTermsAndConditions,
        conditionExpr: datashareConditionExpr,
        defaultAccessTypes:
          values.permission != undefined
            ? Object.entries(values.permission).map(([key, obj]) => {
                return obj.value;
              })
            : [],
        defaultMasks: {}
      };

      if (values.shareDataMaskInfo != undefined) {
        let data = {};
        if (values.shareDataMaskInfo.valueExpr != undefined) {
          data = {
            [tagName]: {
              dataMaskType: values.shareDataMaskInfo.value,
              valueExpr: values.shareDataMaskInfo.valueExpr
            }
          };
        } else {
          data = {
            [tagName]: { dataMaskType: values.shareDataMaskInfo.value }
          };
        }
        dataShareInfo.defaultMasks = data;
      }

      userList.forEach((user) => {
        dataShareInfo.acl.users[user.name] = user.perm;
      });

      groupList.forEach((group) => {
        dataShareInfo.acl.groups[group.name] = group.perm;
      });

      roleList.forEach((role) => {
        dataShareInfo.acl.roles[role.name] = role.perm;
      });

      dispatch({
        type: "SET_PREVENT_ALERT",
        preventUnBlock: true
      });
      try {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: true
        });
        const createDatashareResp = await fetchApi({
          url: `gds/datashare`,
          method: "post",
          data: dataShareInfo
        });
        toast.success("Datashare created successfully!!");
        self.location.hash = "#/gds/mydatasharelisting";
      } catch (error) {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });
        serverError(error);
        console.error(`Error occurred while creating datashare  ${error}`);
      }
    } else if (step == 4) {
      setSaveButtonText("Create Datashare");
      setStep(step + 1);
    } else if (step == 1) {
      if (datashareName == undefined) {
        toast.error("Please add Datashare Name");
        return;
      } else if (selectedService == undefined) {
        toast.error("Please add Service Name");
        return;
      } else {
        fetchServiceDef(selectedService.def);
        fetchServiceByName(selectedService.label);
        setStep(step + 1);
      }
      setStep(step + 1);
    } else {
      setSaveButtonText("Continue");
      setStep(step + 1);
    }
  };

  const fetchServiceDef = async (serviceDefName) => {
    let serviceDefsResp = [];
    try {
      serviceDefsResp = await fetchApi({
        url: `plugins/definitions/name/${serviceDefName}`
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Service Definition or CSRF headers! ${error}`
      );
    }
    let modifiedServiceDef = serviceDefsResp.data;
    for (const obj of modifiedServiceDef.resources) {
      obj.recursiveSupported = false;
      obj.excludesSupported = false;
    }
    if (Object.keys(modifiedServiceDef.rowFilterDef).length !== 0) {
      setMaskDef(true);
    }
    if (Object.keys(modifiedServiceDef.dataMaskDef).length !== 0) {
      setRowFilterDef(true);
    }
    setAccessTypeOptions(
      modifiedServiceDef.accessTypes.map(({ label, name: value }) => ({
        label,
        value
      }))
    );
    setMaskTypeOptions(
      modifiedServiceDef.dataMaskDef.maskTypes.map(
        ({ label, name: value }) => ({
          label,
          value
        })
      )
    );
    setServiceDef(modifiedServiceDef);
  };

  const fetchServiceByName = async (serviceName) => {
    let serviceResp = [];
    try {
      serviceResp = await fetchApi({
        url: "plugins/services/name/hase_service_1"
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Service or CSRF headers! ${error}`
      );
    }
    setService(serviceResp.data);
  };

  const datashareNameChange = (event) => {
    setName(event.target.value);
    console.log("DatashareName is:", event.target.value);
  };

  const tagNameChange = (event) => {
    setTagName(event.target.value);
  };

  const datashareDescriptionChange = (event) => {
    setDatashareDescription(event.target.value);
    console.log("Datashare Description is:", event.target.value);
  };

  const onSubmit = () => {
    console.log("Submitting");
  };

  const fetchService = async (inputValue) => {
    let params = {};
    if (inputValue) {
      params["serviceName"] = inputValue || "";
    }
    //params["serviceName"] = "hdfs1";
    const serviceResp = await fetchApi({
      url: "plugins/services",
      params: params
    });
    return serviceResp.data.services.map(({ name, id, type }) => ({
      label: name,
      value: id,
      def: type
    }));
  };

  const fetchSecurityZone = async (inputValue) => {
    let params = {};
    if (inputValue) {
      params["zoneName"] = inputValue || "";
    }
    //params["zoneName"] = "zone1";
    const zoneResp = await fetchApi({
      url: "zones/zones",
      params: params
    });
    return zoneResp.data.securityZones.map(({ name, id }) => ({
      label: name,
      value: id
    }));
  };

  const onServiceChange = (e, input) => {
    dispatch({
      type: "SET_SELECTED_SERVICE",
      selectedService: e
    });
    input.onChange(e);
    console.log("Adding to selectedService");
    console.log(selectedService);
  };

  const setZone = (e, input) => {
    dispatch({
      type: "SET_SELECTED_ZONE",
      selectedZone: e
    });
    input.onChange(e);
    console.log("Adding to selectedZone");
    console.log(selectedZone);
  };

  const resourceErrorCheck = (errors, values) => {
    let serviceCompResourcesDetails = serviceDef.resources;

    const grpResources = groupBy(serviceCompResourcesDetails || [], "level");
    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    for (const key of grpResourcesKeys) {
      if (errors[`value-${key}`] !== undefined) {
        return true;
      }
    }
  };

  const showMoreOptionsComp = () => {
    setShowMoreOptions(true);
  };

  const handleSubmit = async (values) => {
    let data = {};
    let serviceCompRes = serviceDef.resources;
    const grpResources = groupBy(serviceCompRes || [], "level");
    let grpResourcesKeys = [];
    data.dataShareId = dataShareId;
    //data.name =
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    data.resources = {};
    for (const level of grpResourcesKeys) {
      if (
        values[`resourceName-${level}`] &&
        values[`resourceName-${level}`].value !== noneOptions.value
      ) {
        let defObj = serviceCompRes.find(function (m) {
          if (m.name == values[`resourceName-${level}`].name) {
            return m;
          }
        });
        data.resources[values[`resourceName-${level}`].name] = {
          values: isArray(values[`value-${level}`])
            ? values[`value-${level}`]?.map(({ value }) => value)
            : [values[`value-${level}`].value]
        };
      }
    }
    try {
      setBlockUI(true);
      const resp = await fetchApi({
        url: "plugins/policies",
        method: "POST",
        data
      });
      let tblpageData = {};
      if (state && state != null) {
        tblpageData = state.tblpageData;
        if (state.tblpageData.pageRecords % state.tblpageData.pageSize == 0) {
          tblpageData["totalPage"] = state.tblpageData.totalPage + 1;
        } else {
          if (tblpageData !== undefined) {
            tblpageData["totalPage"] = state.tblpageData.totalPage;
          }
        }
      }
      setBlockUI(false);
      toast.dismiss(toastId.current);
      toastId.current = toast.success("Policy save successfully!!");
      navigate(`/service/${serviceId}/policies/${policyType}`, {
        state: {
          showLastPage: true,
          addPageData: tblpageData
        }
      });
    } catch (error) {
      setBlockUI(false);
      let errorMsg = `Failed to save policy form`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
      console.error(`Error while saving policy form! ${error}`);
    }
  };

  const addResources = (resourceList) => {
    setSharedResourceList(resourceList);
  };

  const onAccessTypeChange = (event, input) => {
    setAccessType(event);
    input.onChange(event);
  };

  const getAccessTypeOptions = () => {
    let srcOp = serviceDef.accessTypes;
    return srcOp.map(({ label, name: value }) => ({
      label,
      value
    }));
  };

  const getMaskingAccessTypeOptions = () => {
    return serviceDef.dataMaskDef.maskTypes.map(({ label, name: value }) => ({
      label,
      value
    }));
  };

  const onShareMaskChange = (event, input) => {
    setSelectedShareMask(event);
    input.onChange(event);
  };

  const handleDataChange = (userList, groupList, roleList) => {
    setUserList(userList);
    setGroupList(groupList);
    setRoleList(roleList);
  };

  const datashareBooleanExpression = (event) => {
    setDatashareConditionExpr(event.target.value);
  };

  const fetchMaskOptions = () => {
    return serviceDef.dataMaskDef.maskTypes.map(({ label, name: value }) => ({
      label,
      value
    }));
  };

  return (
    <>
      <Form
        id="myform2"
        name="myform2"
        onSubmit={onSubmit}
        render={({ handleSubmit, submitting, required, values }) => (
          <div>
            <div className="gds-form-header-wrapper">
              <h3 className="gds-header bold">Create Datashare</h3>

              <div className="gds-header-btn-grp">
                <Button
                  variant="secondary"
                  type="button"
                  size="sm"
                  onClick={cancelDatashareDetails}
                  data-id="cancel"
                  data-cy="cancel"
                >
                  Back
                </Button>
                <Button
                  variant="primary"
                  onClick={() => subhmitDatashareDetails(values)}
                  size="sm"
                  data-id="save"
                  data-cy="save"
                >
                  {saveButtonText}
                </Button>
              </div>
            </div>

            {step == 1 && (
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 1</h6>
                  <h2 className="gds-form-step-name">Basic details</h2>
                </div>
                <div className="gds-form-content">
                  <div className="gds-form-input">
                    <input
                      type="text"
                      name="datashareName"
                      placeholder="Datashare Name"
                      className="form-control"
                      data-cy="datashareName"
                      onChange={datashareNameChange}
                      value={datashareName}
                    />
                  </div>
                  <div className="gds-form-content">
                    <Field
                      className="form-control"
                      name="selectService"
                      render={({ input }) => (
                        <div className="gds-form-input">
                          <AsyncSelect
                            {...input}
                            defaultOptions
                            value={selectedService}
                            loadOptions={fetchService}
                            onChange={(e) => onServiceChange(e, input)}
                            isClearable={false}
                            placeholder="Select Service"
                            width="500px"
                          />
                        </div>
                      )}
                    />
                    <div className="gds-form-input">
                      <Field
                        className="form-control"
                        name="selectSecurityZone"
                        render={({ input }) => (
                          <div className="gds-form-input">
                            <AsyncSelect
                              {...input}
                              defaultOptions
                              value={selectedZone}
                              loadOptions={fetchSecurityZone}
                              onChange={(e) => setZone(e, input)}
                              isClearable={false}
                              placeholder="Select Security Zone (Optional)"
                              width="500px"
                            />
                          </div>
                        )}
                      />
                    </div>
                  </div>
                  <div className="gds-form-input">
                    <textarea
                      placeholder="Datashare Description"
                      className="form-control"
                      id="description"
                      data-cy="description"
                      onChange={datashareDescriptionChange}
                      value={datashareDescription}
                      rows={4}
                    />
                  </div>
                </div>
              </div>
            )}

            {step == 2 && (
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 2</h6>
                  <h2 className="gds-form-step-name">Specify Conditions</h2>
                </div>
                <div>
                  <Card className="gds-section-card gds-bg-white">
                    <div className="gds-section-title">
                      <p className="gds-card-heading">Conditions</p>
                    </div>
                    <div className="gds-flex mg-b-10">
                      <textarea
                        placeholder="Boolean Expression"
                        className="form-control"
                        id="dsBooleanExpression"
                        onChange={datashareBooleanExpression}
                        data-cy="dsBooleanExpression"
                        rows={4}
                      />
                    </div>
                  </Card>
                </div>
              </div>
            )}

            {step == 3 && (
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 3</h6>
                  <h2 className="gds-form-step-name">
                    Select default access type and masks
                  </h2>
                </div>
                <div>
                  <div className="gds-section-title">
                    <p className="gds-card-heading">Access Configuration</p>
                  </div>
                  <div className="gds-flex mg-b-10">
                    <div className="gds-flex">
                      <div>
                        <Field
                          name={`permission`}
                          render={({ input, meta }) => (
                            <div className="flex-1">
                              <span> Default Access Types : </span>
                              <Select
                                {...input}
                                options={accessTypeOptions}
                                onChange={(e) => onAccessTypeChange(e, input)}
                                menuPortalTarget={document.body}
                                value={accessType}
                                menuPlacement="auto"
                                placeholder="All Permissions"
                                isClearable
                                isMulti
                              />
                            </div>
                          )}
                        />
                      </div>
                    </div>
                  </div>
                </div>
                {maskDef ? (
                  <div className="gds-section-title">
                    <p className="gds-card-heading">Masking Configuration</p>
                    <div>
                      <span>Tag Name : </span>
                      <div className="gds-form-input">
                        <input
                          type="text"
                          name="tagName"
                          className="form-control"
                          data-cy="tagName"
                          onChange={tagNameChange}
                          value={tagName}
                        />
                      </div>
                    </div>
                    <div>
                      <span>Masking Type : </span>
                      <Field
                        className="form-control"
                        name={`shareDataMaskInfo`}
                        render={({ input, meta }) => (
                          <div>
                            <Select
                              {...input}
                              defaultOptions
                              value={selectedShareMask}
                              options={maskTypeOptions}
                              onChange={(e) => onShareMaskChange(e, input)}
                              isClearable={false}
                              width="500px"
                            />
                            {selectedShareMask != undefined &&
                              selectedShareMask.label == "Custom" && (
                                <>
                                  <Field
                                    className="form-control"
                                    name={`shareDataMaskInfo.valueExpr`}
                                    validate={required}
                                    render={({ input, meta }) => (
                                      <>
                                        <FormB.Control
                                          type="text"
                                          {...input}
                                          placeholder="Enter masked value or expression..."
                                        />
                                        {meta.error && (
                                          <span className="invalid-field">
                                            {meta.error}
                                          </span>
                                        )}
                                      </>
                                    )}
                                  />
                                </>
                              )}
                          </div>
                        )}
                      />
                    </div>
                  </div>
                ) : (
                  <div></div>
                )}
              </div>
            )}

            {step == 4 && (
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 4</h6>
                  <h2 className="gds-form-step-name">Specify Permissions</h2>
                </div>
                <PrinciplePermissionComp
                  userList={userList}
                  groupList={groupList}
                  roleList={roleList}
                  onDataChange={handleDataChange}
                />
              </div>
            )}

            {step == 5 && (
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 5</h6>
                  <h2 className="gds-form-step-name">
                    Specify terms and conditions
                  </h2>
                </div>
                <table className="gds-table">
                  <tr>
                    <td>
                      <textarea
                        placeholder="Terms & Conditions"
                        className="form-control"
                        id="termsAndConditions"
                        data-cy="termsAndConditions"
                        onChange={datashareTermsAndConditionsChange}
                        value={datashareTermsAndConditions}
                        rows={16}
                      />
                    </td>
                  </tr>
                </table>
              </div>
            )}
          </div>
        )}
      />
    </>
  );
};

export default AddDatashareView;
