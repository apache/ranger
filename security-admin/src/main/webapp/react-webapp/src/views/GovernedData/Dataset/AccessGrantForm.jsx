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

import React, { useState, useEffect, useReducer, useRef } from "react";
import { Form as FormB, Button, Card } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import arrayMutators from "final-form-arrays";
import moment from "moment-timezone";
import {
  dragStart,
  dragEnter,
  drop,
  dragOver,
  getAllTimeZoneList,
  policyConditionUpdatedJSON,
  isSystemAdmin
} from "../../../utils/XAUtils";
import { fetchApi } from "Utils/fetchAPI";
import { maxBy, find, isEmpty, isArray, isEqual, isObject } from "lodash";
import userGreyIcon from "../../../images/user-grey.svg";
import groupGreyIcon from "../../../images/group-grey.svg";
import roleGreyIcon from "../../../images/role-grey.svg";
import AsyncCreatableSelect from "react-select/async-creatable";
import PolicyValidityPeriodComp from "../../PolicyListing/PolicyValidityPeriodComp";
import PolicyConditionsComp from "../../PolicyListing/PolicyConditionsComp";
import { Loader } from "Components/CommonComponents";
import dateFormat from "dateformat";
import Select from "react-select";
import AsyncSelect from "react-select/async";
import { FieldArray } from "react-final-form-arrays";
import { CustomTooltip } from "../../../components/CommonComponents";
import Editable from "Components/Editable";

const initialState = {
  loader: false,
  policyData: null,
  formData: {}
};

function reducer(state, action) {
  switch (action.type) {
    case "SET_DATA":
      return {
        ...state,
        loader: false,
        policyData: action?.policyData,
        formData: action.formData
      };
    default:
      throw new Error();
  }
}

function AccessGrantForm({
  dataset,
  onDataChange,
  serviceCompDetails,
  isAdmin
}) {
  const [policyState, dispatch] = useReducer(reducer, initialState);
  const { loader, policyData, formData } = policyState;
  const [showModal, policyConditionState] = useState(false);
  const [validityPeriod, setValidityPeriod] = useState([]);
  const dragOverItem = useRef();
  const dragItem = useRef();
  const addPolicyItemClickRef = useRef();

  useEffect(() => {
    fetchInitalData();
  }, []);

  const fetchInitalData = async () => {
    let policyData = null;
    if (dataset.name) {
      policyData = await fetchAccessGrantData();
    }
    if (policyData != null) {
      setValidityPeriod(policyData.validitySchedules);
      dispatch({
        type: "SET_DATA",
        policyData: policyData || null,
        formData: generateFormData(policyData, serviceCompDetails)
      });
    }
    if (policyData == null) {
      addPolicyItemClickRef.current.click();
    }
    return policyData;
  };

  const fetchAccessGrantData = async () => {
    let data = null;
    try {
      policyState.loader = true;
      const resp = await fetchApi({
        url: `/gds/dataset/${dataset.id}/policy`
      });
      policyState.loader = false;
      if (resp.data.length > 0) {
        data = resp.data[0];
      }
    } catch (error) {
      console.error(
        `Error occurred while fetching dataset policy details ! ${error}`
      );
    }
    return data;
  };

  const generateFormData = (policyData, serviceCompData) => {
    let data = {};
    data.policyType = 0;
    if (policyData != undefined && policyData != null) {
      data.policyItems =
        policyData.id && policyData?.policyItems?.length > 0
          ? setPolicyItemVal(
              policyData?.policyItems,
              serviceCompData?.accessTypes
            )
          : [{}];
      if (policyData.id) {
        data.policyName = policyData?.name;
        data.isEnabled = policyData?.isEnabled;
        data.policyPriority = policyData?.policyPriority == 0 ? false : true;
        data.description = policyData?.description;
        data.isAuditEnabled = policyData?.isAuditEnabled;
        data.policyLabel =
          policyData &&
          policyData?.policyLabels?.map((val) => {
            return { label: val, value: val };
          });
        let serviceCompResourcesDetails = serviceCompData?.resources;
        if (policyData?.resources) {
          let lastResourceLevel = [];
          Object.entries(policyData?.resources).map(([key, value]) => {
            let setResources = find(serviceCompResourcesDetails, ["name", key]);
            data[`resourceName-${setResources?.level}`] = setResources;
            data[`value-${setResources?.level}`] = value.values.map((m) => {
              return { label: m, value: m };
            });
            lastResourceLevel.push({
              level: setResources.level,
              name: setResources.name
            });
          });
          lastResourceLevel = maxBy(lastResourceLevel, "level");
          let setLastResources = find(serviceCompResourcesDetails, [
            "parent",
            lastResourceLevel.name
          ]);
          if (setLastResources && setLastResources?.isValidLeaf) {
            data[`resourceName-${setLastResources.level}`] = {
              label: "None",
              value: "none"
            };
          }
        }
        if (policyData?.validitySchedules) {
          data["validitySchedules"] = [];
          policyData?.validitySchedules.filter((val) => {
            let obj = {};
            if (val.endTime) {
              obj["endTime"] = moment(val.endTime, "YYYY/MM/DD HH:mm:ss");
            }
            if (val.startTime) {
              obj["startTime"] = moment(val.startTime, "YYYY/MM/DD HH:mm:ss");
            }
            if (val.timeZone) {
              obj["timeZone"] = getAllTimeZoneList().find((tZoneVal) => {
                return tZoneVal.id == val.timeZone;
              });
            }
            data["validitySchedules"].push(obj);
          });
        }

        if (policyData?.conditions?.length > 0) {
          data.conditions = {};
          for (let val of policyData.conditions) {
            data.conditions[val?.type] = val?.values?.join(",");
          }
        }
      }
    }
    return data;
  };

  const setPolicyItemVal = (formData, accessTypes) => {
    return formData.map((val) => {
      let obj = {},
        accessTypesObj = [];

      if (val.hasOwnProperty("delegateAdmin")) {
        obj.delegateAdmin = val.delegateAdmin;
      }

      for (let i = 0; val?.accesses?.length > i; i++) {
        accessTypes.map((opt) => {
          if (val.accesses[i].type == opt.name) {
            accessTypesObj.push({ label: opt.label, value: opt.name });
          }
        });
      }
      obj["accesses"] = accessTypesObj;
      let principle = [];
      if (val?.groups?.length > 0) {
        val.groups.map((opt) => {
          principle.push({
            label: (
              <div>
                <img src={groupGreyIcon} height="20px" width="20px" />
                {opt}
              </div>
            ),
            value: opt,
            type: "GROUP"
          });
        });
      }
      if (val?.users?.length > 0) {
        val.users.map((opt) => {
          principle.push({
            label: (
              <div>
                <img src={userGreyIcon} height="20px" width="20px" />
                {opt}
              </div>
            ),
            value: opt,
            type: "USER"
          });
        });
      }
      if (val?.roles?.length > 0) {
        val.roles.map((opt) => {
          principle.push({
            label: (
              <div>
                <img src={roleGreyIcon} height="20px" width="20px" />
                {opt}
              </div>
            ),
            value: opt,
            type: "ROLE"
          });
        });
      }
      obj.principle = principle;
      if (val?.conditions?.length > 0) {
        obj.conditions = {};

        for (let data of val.conditions) {
          let conditionObj = find(
            policyConditionUpdatedJSON(serviceCompDetails?.policyConditions),
            function (m) {
              if (m.name == data.type) {
                return m;
              }
            }
          );

          if (!isEmpty(conditionObj.uiHint)) {
            obj.conditions[data?.type] = JSON.parse(conditionObj.uiHint)
              .isMultiValue
              ? data?.values.map((m) => {
                  return { value: m, label: m };
                })
              : data?.values.toString();
          }
        }
      }
      return obj;
    });
  };

  const handleSubmit = async (values) => {
    saveAccessGrant(values);
  };

  const fetchPrincipleData = async (inputValue) => {
    let params = { name: inputValue || "", isVisible: 1 };
    let op = [];
    const principleResp = await fetchApi({
      url: "xusers/lookup/principals",
      params: params
    });
    op = principleResp.data;

    return op.map((obj) => ({
      label: (
        <div>
          <img
            src={
              obj.type == "USER"
                ? userGreyIcon
                : obj.type == "GROUP"
                  ? groupGreyIcon
                  : roleGreyIcon
            }
            height="20px"
            width="20px"
          />{" "}
          {obj.name}{" "}
        </div>
      ),
      value: obj.name,
      type: obj.type
    }));
  };

  const FormChange = (props) => {
    const { isDirtyField, formValues } = props;
    if (isDirtyField) {
      onDataChange(formValues, policyData);
    }
    return null;
  };

  const onConditionChange = () => {
    onDataChange(formValues, policyData);
  };

  const onRemovingPolicyItem = (index) => {
    onDataChange(undefined, undefined);
  };

  const isDirtyFieldCheck = (dirtyFields, modified, values, initialValues) => {
    let modifiedVal = false;
    if (!isEmpty(dirtyFields)) {
      for (let dirtyFieldVal in dirtyFields) {
        modifiedVal = modified?.[dirtyFieldVal];
        if (
          values?.validitySchedules ||
          modified?.validitySchedules ||
          values?.conditions ||
          modified?.conditions ||
          modifiedVal == true
        ) {
          modifiedVal = true;
          break;
        }
        if (
          dirtyFieldVal == "policyItems" &&
          values.policyItems.length == 1 &&
          values.policyItems[0] == undefined
        ) {
        } else if (!isEqual(values.policyItems, initialValues.policyItems)) {
          modifiedVal = true;
          break;
        }
      }
    }
    if (
      !isEqual(values?.validitySchedules, initialValues?.validitySchedules) ||
      !isEqual(values?.conditions, initialValues?.conditions)
    ) {
      modifiedVal = true;
    }
    return modifiedVal;
  };

  const requiredForPolicyItem = (fieldVals, index) => {
    if (fieldVals && !isEmpty(fieldVals[index])) {
      let error, accTypes;
      let users = (fieldVals[index]?.users || []).length > 0;
      let grps = (fieldVals[index]?.groups || []).length > 0;
      let roles = (fieldVals[index]?.roles || []).length > 0;
      let delegateAdmin = fieldVals[index]?.delegateAdmin;
      let policyConditionVal = fieldVals[index]?.conditions;
      if (fieldVals[index]?.accesses && !isArray(fieldVals[index]?.accesses)) {
        accTypes =
          JSON.stringify(fieldVals[index]?.accesses || {}) !==
          JSON.stringify({});
      } else {
        accTypes = (fieldVals[index]?.accesses || []).length > 0;
      }
      if ((users || grps || roles) && !accTypes) {
        if (delegateAdmin !== undefined && delegateAdmin === false) {
          error =
            "Please select permision item for selected users/groups/roles";
        } else if (delegateAdmin == undefined) {
          error =
            "Please select permision item for selected users/groups/roles";
        }
      }
      if (accTypes && !users && !grps && !roles) {
        if (delegateAdmin !== undefined && delegateAdmin === false) {
          error =
            "Please select users/groups/roles for selected permission item";
        } else if (delegateAdmin == undefined) {
          error =
            "Please select users/groups/roles for selected permission item";
        }
      }
      if (delegateAdmin && !users && !grps && !roles) {
        error = "Please select user/group/role for the selected delegate Admin";
      }
      if (policyConditionVal) {
        for (const key in policyConditionVal) {
          if (
            policyConditionVal[key] == null ||
            policyConditionVal[key] == ""
          ) {
            delete policyConditionVal[key];
          }
        }
        if (
          Object.keys(policyConditionVal).length != 0 &&
          !users &&
          !grps &&
          !roles
        ) {
          error =
            "Please select user/group/role for the entered policy condition";
        }
      }
      return error;
    }
  };

  const getAccessTypeOptions = () => {
    if (serviceCompDetails != undefined) {
      let srcOp = serviceCompDetails.accessTypes;
      return srcOp.map(({ label, name: value }) => ({
        label,
        value
      }));
    }
  };

  return (
    <>
      {policyState.loader ? (
        <Loader />
      ) : (
        <div className="wrap-gds">
          <Form
            onSubmit={handleSubmit}
            mutators={{
              ...arrayMutators
            }}
            initialValues={formData}
            render={({
              form: {
                mutators: { push: addPolicyItem, pop }
              },
              values,
              dirtyFields,
              dirty,
              modified,
              initialValues
            }) => {
              return (
                <div className="gds-access-content gds-content-border">
                  <FormChange
                    isDirtyField={
                      dirty == true || !isEqual(initialValues, values)
                        ? isDirtyFieldCheck(
                            dirtyFields,
                            modified,
                            values,
                            initialValues
                          )
                        : false
                    }
                    formValues={values}
                  />
                  <div className="datasetPolicyItem">
                    <div className="mb-5 gds-content-border gds-action-card pb-4 mt-3">
                      <div className="gds-section-title">
                        <p
                          className="formHeader border-0 m-0"
                          style={{ fontSize: "1.125rem", fontWeight: "400" }}
                        >
                          Grants
                        </p>
                        {isAdmin ? (
                          <div className="d-flex gap-half">
                            {(values.conditions == undefined ||
                              isEmpty(values.conditions)) && (
                              <Button
                                className="btn btn-sm"
                                onClick={() => {
                                  policyConditionState(true);
                                }}
                                data-js="customPolicyConditions"
                                data-cy="customPolicyConditions"
                                variant="secondary"
                              >
                                Add Conditions
                              </Button>
                            )}
                            {(validityPeriod == undefined ||
                              validityPeriod.length == 0) && (
                              <PolicyValidityPeriodComp
                                addPolicyItem={addPolicyItem}
                                isGDS={true}
                              />
                            )}
                          </div>
                        ) : (
                          <></>
                        )}
                      </div>
                      <div className="drag-drop-wrap pt-3">
                        <FieldArray name="policyItems">
                          {({ fields }) =>
                            fields.map((name, index) => (
                              <table className="w-100 mg-b-10">
                                <tr
                                  key={name}
                                  onDragStart={(e) =>
                                    dragStart(e, index, dragItem)
                                  }
                                  onDragEnter={(e) =>
                                    dragEnter(e, index, dragOverItem)
                                  }
                                  onDragEnd={(e) =>
                                    drop(e, fields, dragItem, dragOverItem)
                                  }
                                  onDragOver={(e) => dragOver(e)}
                                  draggable
                                  id={index}
                                  className="drag-drop-wrap"
                                >
                                  <div className="gds-grant-row">
                                    <i className="fa-fw fa fa-bars mt-2"></i>
                                    <div className="d-flex gap-half">
                                      <div className="flex-1 mg-b-10 gds-grant-principle">
                                        <Field
                                          name={`${name}.principle`}
                                          render={({ input, meta }) => (
                                            <div>
                                              <AsyncSelect
                                                {...input}
                                                placeholder="Select users, groups, roles"
                                                isMulti
                                                isDisabled={!isAdmin}
                                                loadOptions={fetchPrincipleData}
                                                data-name="usersSeusersPrinciplelect"
                                                data-cy="usersPrinciple"
                                              />
                                            </div>
                                          )}
                                        />
                                      </div>

                                      <div className="d-flex gap-1 mg-b-10 gds-grant-permission">
                                        <Field
                                          name={`${name}.accesses`}
                                          render={({ input, meta }) => (
                                            <div className="flex-1">
                                              <Select
                                                {...input}
                                                options={getAccessTypeOptions()}
                                                menuPlacement="auto"
                                                placeholder="Permissions"
                                                isClearable
                                                isMulti
                                                isDisabled={!isAdmin}
                                              />
                                            </div>
                                          )}
                                        />
                                      </div>
                                      <div className="d-flex gap-1 mg-b-10 gds-grant-condition">
                                        {serviceCompDetails?.policyConditions
                                          ?.length > 0 && (
                                          <div
                                            key="Policy Conditions"
                                            className="align-middle w-100"
                                          >
                                            <Field
                                              className="form-control "
                                              name={`${name}.conditions`}
                                              validate={(value, formValues) =>
                                                requiredForPolicyItem(
                                                  formValues["policyItems"],
                                                  index
                                                )
                                              }
                                              render={({ input }) => (
                                                <div className="table-editable">
                                                  <Editable
                                                    {...input}
                                                    placement="auto"
                                                    type="custom"
                                                    conditionDefVal={policyConditionUpdatedJSON(
                                                      serviceCompDetails.policyConditions
                                                    )}
                                                    selectProps={{
                                                      isMulti: true
                                                    }}
                                                    isGDS={true}
                                                  />
                                                </div>
                                              )}
                                            />
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                    {isAdmin ? (
                                      <div>
                                        <Button
                                          variant="danger"
                                          size="sm"
                                          title="Remove"
                                          onClick={() => {
                                            fields.remove(index);
                                            onRemovingPolicyItem();
                                          }}
                                          data-action="delete"
                                          data-cy="delete"
                                        >
                                          <i className="fa-fw fa fa-remove"></i>
                                        </Button>
                                      </div>
                                    ) : (
                                      <></>
                                    )}
                                  </div>
                                </tr>
                              </table>
                            ))
                          }
                        </FieldArray>
                      </div>
                      {isAdmin ? (
                        <Button
                          className="btn btn-sm mt-2 mg-l-32 mb-5"
                          type="button"
                          onClick={() =>
                            addPolicyItem("policyItems", undefined)
                          }
                          data-action="addGroup"
                          data-cy="addGroup"
                          title="Add"
                          ref={addPolicyItemClickRef}
                        >
                          Add More
                        </Button>
                      ) : (
                        <></>
                      )}

                      {values?.conditions && !isEmpty(values.conditions) && (
                        <div className="gds-action-card mb-5 pl-0 pr-0">
                          <div className="gds-section-title">
                            <p className="gds-card-heading">Conditions</p>
                            <Button
                              className="btn btn-sm"
                              onClick={() => {
                                policyConditionState(true);
                              }}
                              data-js="customPolicyConditions"
                              data-cy="customPolicyConditions"
                              variant="secondary"
                            >
                              Edit Conditions
                            </Button>
                          </div>
                          {Object.keys(values.conditions).map((keyName) => {
                            if (
                              values.conditions[keyName] != "" &&
                              values.conditions[keyName] != null
                            ) {
                              let conditionObj = find(
                                serviceCompDetails?.policyConditions,
                                function (m) {
                                  if (m.name == keyName) {
                                    return m;
                                  }
                                }
                              );
                              return (
                                <div className="pt-3">
                                  {isObject(values.conditions[keyName]) ? (
                                    <div>
                                      <span className="fnt-14">
                                        {values.conditions[keyName].length > 1
                                          ? values.conditions[keyName].map(
                                              (m) => {
                                                return ` ${m.label} `;
                                              }
                                            )
                                          : values.conditions[keyName].label}
                                      </span>
                                    </div>
                                  ) : (
                                    <div>
                                      <span className="fnt-14">
                                        {values.conditions[keyName]}
                                      </span>
                                    </div>
                                  )}
                                </div>
                              );
                            }
                          })}
                        </div>
                      )}
                      {isAdmin && showModal && (
                        <Field
                          className="form-control"
                          name="conditions"
                          render={({ input }) => (
                            <PolicyConditionsComp
                              policyConditionDetails={policyConditionUpdatedJSON(
                                serviceCompDetails.policyConditions
                              )}
                              inputVal={input}
                              showModal={showModal}
                              handleCloseModal={policyConditionState}
                            />
                          )}
                        />
                      )}

                      {validityPeriod != undefined &&
                        validityPeriod.length > 0 && (
                          <div className="gds-action-card pl-0 pr-0">
                            <div className="gds-section-title">
                              <p className="gds-card-heading">
                                Validity Period
                              </p>
                              {isAdmin ? (
                                <PolicyValidityPeriodComp
                                  addPolicyItem={addPolicyItem}
                                  editValidityPeriod={true}
                                  isGDS={true}
                                />
                              ) : (
                                <></>
                              )}
                            </div>
                            {validityPeriod.map((obj, index) => {
                              return (
                                <div className="gds-inline-field-grp gds-inline-listing w-100">
                                  <div className="wrapper pt-3">
                                    <div className="gds-left-inline-field">
                                      <span className="gds-label-color fnt-14">
                                        Start Date
                                      </span>
                                    </div>
                                    {obj?.startTime != undefined ? (
                                      <span className="fnt-14">
                                        {dateFormat(
                                          obj.startTime,
                                          "mm/dd/yyyy hh:MM:ss TT"
                                        )}
                                      </span>
                                    ) : (
                                      <p className="mb-0">--</p>
                                    )}
                                    <span className="gds-label-color pl-5 fnt-14">
                                      {obj?.timeZone}
                                    </span>
                                  </div>
                                  <div className="wrapper ">
                                    <div className="gds-left-inline-field">
                                      <span className="gds-label-color fnt-14">
                                        End Date
                                      </span>
                                    </div>
                                    {obj?.endTime != undefined ? (
                                      <span className="fnt-14">
                                        {dateFormat(
                                          obj.endTime,
                                          "mm/dd/yyyy hh:MM:ss TT"
                                        )}
                                      </span>
                                    ) : (
                                      <p className="mb-0">--</p>
                                    )}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        )}
                    </div>
                  </div>
                </div>
              );
            }}
          />
        </div>
      )}
    </>
  );
}

export default AccessGrantForm;
