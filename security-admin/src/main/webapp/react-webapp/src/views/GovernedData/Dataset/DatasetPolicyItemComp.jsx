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

import React, { useRef, useState } from "react";
import { FieldArray } from "react-final-form-arrays";
import {
  dragStart,
  dragEnter,
  drop,
  dragOver,
  policyConditionUpdatedJSON,
} from "../../../utils/XAUtils";
import AsyncSelect from "react-select/async";
import { Field } from "react-final-form";
import { Form as FormB, Button } from "react-bootstrap";
import { isEmpty, isArray } from "lodash";
import Select from "react-select";
import Editable from "Components/Editable";

const DatasetPolicyItemComp = (props) => {
  const {
    addPolicyItem,
    attrName,
    serviceCompDetails,
    fetchPrincipleData,
    onRemovingPolicyItem,
  } = props;
  const dragOverItem = useRef();
  const dragItem = useRef();

  const getAccessTypeOptions = () => {
    let srcOp = serviceCompDetails.accessTypes;
    return srcOp.map(({ label, name: value }) => ({
      label,
      value,
    }));
  };

  const accessTypes = [
    { value: "_CREATE", label: "_CREATE" },
    { value: "_MANAGE", label: "_MANAGE" },
    { value: "_DELETE", label: "_DELETE" },
  ];

  const requiredForPolicyItem = (fieldVals, index) => {
    if (fieldVals && !isEmpty(fieldVals[index])) {
      let error, accTypes;
      let users = (fieldVals[index]?.users || []).length > 0;
      let grps = (fieldVals[index]?.groups || []).length > 0;
      let roles = (fieldVals[index]?.roles || []).length > 0;
      let delegateAdmin = fieldVals[index]?.delegateAdmin;
      let policyConditionVal = fieldVals[index]?.conditions;
      if (fieldVals[index]?.accesses && !isArray(fieldVals[index]?.accesses)) {
        if (serviceCompDetails?.name == "tag") {
          accTypes =
            isEmpty(fieldVals[index]?.accesses?.tableList) !== isEmpty({});
        } else {
          accTypes =
            JSON.stringify(fieldVals[index]?.accesses || {}) !==
            JSON.stringify({});
        }
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

  return (
    <>
      <div className="gds-grant-det-cond gds-content-border">
        <div>
          <p className="formHeader">Grants</p>
        </div>
        <div className="drag-drop-wrap">
          <FieldArray name={attrName}>
            {({ fields }) =>
              fields.map((name, index) => (
                <table className="w-100">
                  <tr
                    key={name}
                    onDragStart={(e) => dragStart(e, index, dragItem)}
                    onDragEnter={(e) => dragEnter(e, index, dragOverItem)}
                    onDragEnd={(e) => drop(e, fields, dragItem, dragOverItem)}
                    onDragOver={(e) => dragOver(e)}
                    draggable
                    id={index}
                    className="drag-drop-wrap"
                  >
                    <div className="gds-grant-row">
                      <i className="fa-fw fa fa-bars"></i>
                      <div>
                        <div className="flex-1 mg-b-10">
                          <Field
                            name={`${name}.principle`}
                            render={({ input, meta }) => (
                              <div>
                                <AsyncSelect
                                  {...input}
                                  placeholder="Select users, groups, roles"
                                  isMulti
                                  loadOptions={fetchPrincipleData}
                                  data-name="usersSeusersPrinciplelect"
                                  data-cy="usersPrinciple"
                                />
                              </div>
                            )}
                          />
                        </div>

                        <div className="d-flex gap-1 mg-b-10">
                          {serviceCompDetails?.policyConditions?.length > 0 && (
                            <td
                              key="Policy Conditions"
                              className="align-middle"
                            >
                              <Field
                                className="form-control"
                                name={`${name}.conditions`}
                                validate={(value, formValues) =>
                                  requiredForPolicyItem(
                                    formValues[attrName],
                                    index
                                  )
                                }
                                render={({ input, meta }) => (
                                  <div className="table-editable">
                                    <Editable
                                      {...input}
                                      placement="auto"
                                      type="custom"
                                      conditionDefVal={policyConditionUpdatedJSON(
                                        serviceCompDetails.policyConditions
                                      )}
                                      selectProps={{ isMulti: true }}
                                    />
                                  </div>
                                )}
                              />
                            </td>
                          )}
                          <Field
                            name={`${name}.accesses`}
                            render={({ input, meta }) => (
                              <div className="flex-1">
                                <Select
                                  {...input}
                                  options={getAccessTypeOptions()}
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
                    </div>
                  </tr>
                </table>
              ))
            }
          </FieldArray>
        </div>
        <Button
          className="btn btn-mini mt-2"
          type="button"
          onClick={() => addPolicyItem(attrName, undefined)}
          data-action="addGroup"
          data-cy="addGroup"
          title="Add"
        >
          Add More
        </Button>
      </div>
    </>
  );
};
export default DatasetPolicyItemComp;
