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

import React, { useMemo, useRef, useState } from "react";
import { Table, Button, Badge, Form } from "react-bootstrap";
import { FieldArray } from "react-final-form-arrays";
import { Col } from "react-bootstrap";
import { Field, useFormState } from "react-final-form";
import AsyncSelect from "react-select/async";
import { find, groupBy, isEmpty, isArray, has } from "lodash";
import { toast } from "react-toastify";
import Editable from "Components/Editable";
import { RangerPolicyType } from "Utils/XAEnums";
import TagBasePermissionItem from "./TagBasePermissionItem";
import { dragStart, dragEnter, drop, dragOver } from "../../utils/XAUtils";

const noneOptions = {
  label: "None",
  value: "none"
};

export default function PolicyPermissionItem(props) {
  const {
    addPolicyItem,
    attrName,
    serviceCompDetails,
    fetchUsersData,
    fetchGroupsData,
    fetchRolesData,
    formValues
  } = props;
  const dragItem = useRef();
  const dragOverItem = useRef();
  const toastId = React.useRef(null);
  const [defaultRoleOptions, setDefaultRoleOptions] = useState([]);
  const [defaultGroupOptions, setDefaultGroupOptions] = useState([]);
  const [defaultUserOptions, setDefaultUserOptions] = useState([]);
  const [roleLoading, setRoleLoading] = useState(false);
  const [groupLoading, setGroupLoading] = useState(false);
  const [userLoading, setUserLoading] = useState(false);

  let { values, errors, change, error, ...args } = useFormState();

  const permList = ["Select Roles", "Select Groups", "Select Users"];

  if (serviceCompDetails?.policyConditions?.length > 0) {
    permList.push("Policy Conditions");
  }
  permList.push("Permissions");
  if (
    RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value ==
      formValues?.policyType &&
    serviceCompDetails.name !== "tag"
  ) {
    permList.push("Deligate Admin");
  }
  if (
    RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value == formValues?.policyType
  ) {
    permList.push("Select Masking Option");
  }
  if (
    RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value ==
    formValues?.policyType
  ) {
    permList.push("Row Level Filter");
  }

  const tableHeader = () => {
    return permList.map((data) => {
      return <th key={data}>{data}</th>;
    });
  };

  const grpResourcesKeys = useMemo(() => {
    const { resources = [] } = serviceCompDetails;
    const grpResources = groupBy(resources, "level");
    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    return grpResourcesKeys;
  }, []);

  const getAccessTypeOptions = () => {
    let srcOp = [];
    for (let i = grpResourcesKeys.length - 1; i >= 0; i--) {
      let selectedResource = `resourceName-${grpResourcesKeys[i]}`;
      if (
        formValues[selectedResource] &&
        formValues[selectedResource].value !== noneOptions.value
      ) {
        if (
          RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value ==
          formValues.policyType
        ) {
          srcOp = serviceCompDetails.dataMaskDef.accessTypes;
        } else if (
          RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value ==
          formValues.policyType
        ) {
          srcOp = serviceCompDetails.rowFilterDef.accessTypes;
        } else {
          srcOp = serviceCompDetails.accessTypes;
        }
        if (formValues[selectedResource].accessTypeRestrictions?.length > 0) {
          let op = [];
          for (const name of formValues[selectedResource]
            .accessTypeRestrictions) {
            let typeOp = find(srcOp, { name });
            if (typeOp) {
              op.push(typeOp);
            }
          }
          srcOp = op;
        }
        break;
      }
    }
    return srcOp.map(({ label, name: value }) => ({
      label,
      value
    }));
  };

  const getMaskingAccessTypeOptions = (index) => {
    if (serviceCompDetails?.dataMaskDef?.maskTypes?.length > 0) {
      if (
        formValues?.policyType ==
          RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value &&
        serviceCompDetails?.name == "tag"
      ) {
        let accessServices =
          formValues?.dataMaskPolicyItems[index]?.accesses?.tableList[0]
            ?.serviceName;
        let filterServiceDetails =
          serviceCompDetails.dataMaskDef.maskTypes.filter((a) => {
            return a.name.includes(accessServices);
          });
        return filterServiceDetails.map(({ label, name: value }) => ({
          label,
          value
        }));
      } else {
        return serviceCompDetails.dataMaskDef.maskTypes.map(
          ({ label, name: value }) => ({
            label,
            value
          })
        );
      }
    }
  };

  const required = (value) => (value ? undefined : "Required");

  const requiredForPermission = (fieldVals, index) => {
    if (fieldVals && !isEmpty(fieldVals[index])) {
      let error, accTypes;
      let users = (fieldVals[index]?.users || []).length > 0;
      let grps = (fieldVals[index]?.groups || []).length > 0;
      let roles = (fieldVals[index]?.roles || []).length > 0;
      let delegateAdmin = fieldVals[index]?.delegateAdmin;
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
      return error;
    }
  };

  const requiredForDeleGateAdmin = (fieldVals, index) => {
    if (
      !isEmpty(fieldVals?.[index]) &&
      has(fieldVals?.[index], "delegateAdmin")
    ) {
      let delError;
      let users = (fieldVals[index]?.users || []).length > 0;
      let grps = (fieldVals[index]?.groups || []).length > 0;
      let roles = (fieldVals[index]?.roles || []).length > 0;
      let delegateAdmin = fieldVals[index]?.delegateAdmin;

      if (delegateAdmin && !users && !grps && !roles) {
        delError =
          "Please select user/group/role for the selected delegate Admin";
      }
      return delError;
    }
  };

  const customStyles = {
    control: (base) => ({
      ...base,
      width: 200,
      whiteSpace: "nowrap"
    })
  };

  const onFocusRoleSelect = () => {
    setRoleLoading(true);
    fetchRolesData().then((opts) => {
      setDefaultRoleOptions(opts);
      setRoleLoading(false);
    });
  };

  const onFocusGroupSelect = () => {
    setGroupLoading(true);
    fetchGroupsData().then((opts) => {
      setDefaultGroupOptions(opts);
      setGroupLoading(false);
    });
  };

  const onFocusUserSelect = () => {
    setUserLoading(true);
    fetchUsersData().then((opts) => {
      setDefaultUserOptions(opts);
      setUserLoading(false);
    });
  };

  return (
    <div>
      <Col sm="12">
        <div className="table-responsive">
          <Table
            bordered
            className="policy-permission-table"
            id={`${attrName}-table`}
          >
            <thead className="thead-light">
              <tr>
                {tableHeader()}
                <th></th>
              </tr>
            </thead>
            <tbody className="drag-drop-wrap">
              <FieldArray name={attrName}>
                {({ fields }) =>
                  fields.map((name, index) => (
                    <tr
                      key={name}
                      onDragStart={(e) => dragStart(e, index, dragItem)}
                      onDragEnter={(e) => dragEnter(e, index, dragOverItem)}
                      onDragEnd={(e) => drop(e, fields, dragItem, dragOverItem)}
                      onDragOver={(e) => dragOver(e)}
                      draggable
                      id={index}
                    >
                      {permList.map((colName) => {
                        if (colName == "Select Roles") {
                          return (
                            <td key={colName} className="align-middle">
                              <Field
                                className="form-control"
                                name={`${name}.roles`}
                                render={({ input, meta }) => (
                                  <div className="d-flex">
                                    <AsyncSelect
                                      {...input}
                                      menuPortalTarget={document.body}
                                      loadOptions={fetchRolesData}
                                      onFocus={() => {
                                        onFocusRoleSelect();
                                      }}
                                      defaultOptions={defaultRoleOptions}
                                      noOptionsMessage={() =>
                                        roleLoading
                                          ? "Loading..."
                                          : "No options"
                                      }
                                      styles={customStyles}
                                      cacheOptions
                                      isMulti
                                    />
                                  </div>
                                )}
                              />
                            </td>
                          );
                        }
                        if (colName == "Select Groups") {
                          return (
                            <td key={colName} className="align-middle">
                              <Field
                                className="form-control"
                                name={`${name}.groups`}
                                render={({ input, meta }) => (
                                  <div>
                                    <AsyncSelect
                                      {...input}
                                      menuPortalTarget={document.body}
                                      loadOptions={fetchGroupsData}
                                      onFocus={() => {
                                        onFocusGroupSelect();
                                      }}
                                      defaultOptions={defaultGroupOptions}
                                      noOptionsMessage={() =>
                                        groupLoading
                                          ? "Loading..."
                                          : "No options"
                                      }
                                      styles={customStyles}
                                      cacheOptions
                                      isMulti
                                    />
                                  </div>
                                )}
                              />
                            </td>
                          );
                        }
                        if (colName == "Select Users") {
                          return (
                            <td key={colName} className="align-middle">
                              <Field
                                className="form-control"
                                name={`${name}.users`}
                                render={({ input, meta }) => (
                                  <div>
                                    <AsyncSelect
                                      {...input}
                                      menuPortalTarget={document.body}
                                      loadOptions={fetchUsersData}
                                      onFocus={() => {
                                        onFocusUserSelect();
                                      }}
                                      defaultOptions={defaultUserOptions}
                                      noOptionsMessage={() =>
                                        userLoading
                                          ? "Loading..."
                                          : "No options"
                                      }
                                      styles={customStyles}
                                      cacheOptions
                                      isMulti
                                    />
                                  </div>
                                )}
                              />
                            </td>
                          );
                        }
                        if (colName == "Policy Conditions") {
                          return serviceCompDetails?.policyConditions?.length ==
                            1 ? (
                            <td key={colName} className="align-middle">
                              <Field
                                className="form-control"
                                name={`${name}.conditions`}
                                render={({ input, meta }) => (
                                  <div className="table-editable">
                                    <Editable
                                      {...input}
                                      placement="auto"
                                      type="select"
                                      conditionDefVal={
                                        serviceCompDetails.policyConditions[0]
                                      }
                                      servicedefName={serviceCompDetails.name}
                                      selectProps={{ isMulti: true }}
                                    />
                                  </div>
                                )}
                              />
                            </td>
                          ) : (
                            <td key={colName} className="align-middle">
                              <Field
                                className="form-control"
                                name={`${name}.conditions`}
                                render={({ input, meta }) => (
                                  <div className="table-editable">
                                    <Editable
                                      {...input}
                                      placement="auto"
                                      type="custom"
                                      conditionDefVal={
                                        serviceCompDetails.policyConditions
                                      }
                                    />
                                  </div>
                                )}
                              />
                            </td>
                          );
                        }
                        if (colName == "Permissions") {
                          if (serviceCompDetails?.name == "tag") {
                            return (
                              <td key={colName} className="align-middle">
                                <Field
                                  className="form-control"
                                  name={`${name}.accesses`}
                                  validate={(value, formValues) =>
                                    requiredForPermission(
                                      formValues[attrName],
                                      index
                                    )
                                  }
                                  render={({ input, meta }) => (
                                    <div className="table-editable">
                                      <TagBasePermissionItem
                                        options={getAccessTypeOptions()}
                                        inputVal={input}
                                        formValues={formValues}
                                        dataMaskIndex={index}
                                        serviceCompDetails={serviceCompDetails}
                                      />
                                    </div>
                                  )}
                                />
                              </td>
                            );
                          } else {
                            return (
                              <td key={colName} className="align-middle">
                                <Field
                                  className="form-control"
                                  name={`${name}.accesses`}
                                  validate={(value, formValues) =>
                                    requiredForPermission(
                                      formValues[attrName],
                                      index
                                    )
                                  }
                                  render={({ input, meta }) => (
                                    <div className="table-editable">
                                      <Editable
                                        {...input}
                                        placement="auto"
                                        type="checkbox"
                                        options={getAccessTypeOptions()}
                                        showSelectAll={true}
                                        selectAllLabel="Select All"
                                      />
                                    </div>
                                  )}
                                />
                              </td>
                            );
                          }
                        }
                        if (colName == "Select Masking Option") {
                          if (serviceCompDetails?.name == "tag") {
                            return (
                              <td key={colName} className="align-middle">
                                <Field
                                  className="form-control"
                                  name={`${name}.dataMaskInfo`}
                                  render={({ input, meta }) =>
                                    fields?.value[index]?.accesses?.tableList
                                      ?.length > 0 ? (
                                      <div className="table-editable">
                                        <Editable
                                          {...input}
                                          placement="auto"
                                          type="radio"
                                          options={getMaskingAccessTypeOptions(
                                            index,
                                            input
                                          )}
                                          showSelectAll={false}
                                          selectAllLabel="Select All"
                                          formValues={formValues}
                                          dataMaskIndex={
                                            fields?.value[index]?.accesses
                                              ?.tableList
                                          }
                                        />
                                        {fields?.value[index]?.dataMaskInfo
                                          ?.label == "Custom" && (
                                          <>
                                            <Field
                                              className="form-control"
                                              name={`${name}.dataMaskInfo.valueExpr`}
                                              validate={required}
                                              render={({ input, meta }) => (
                                                <>
                                                  <Form.Control
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
                                    ) : (
                                      <div>
                                        <span className="editable-add-text">
                                          Select Masking Option
                                        </span>
                                        <Button
                                          className="mg-10 btn-mini text-secondary"
                                          variant="outline-dark"
                                          size="sm"
                                          type="button"
                                          onClick={() => {
                                            toast.dismiss(toastId.current);
                                            return (toast.current =
                                              toast.warning(
                                                "Please select access type first to enable add masking options."
                                              ));
                                          }}
                                        >
                                          <i className="fa-fw fa fa-plus"></i>
                                        </Button>
                                      </div>
                                    )
                                  }
                                />
                              </td>
                            );
                          } else {
                            return (
                              <td key={colName} className="align-middle">
                                <Field
                                  className="form-control"
                                  name={`${name}.dataMaskInfo`}
                                  render={({ input, meta }) => (
                                    <div className="table-editable">
                                      <Editable
                                        {...input}
                                        placement="auto"
                                        type="radio"
                                        options={getMaskingAccessTypeOptions()}
                                        showSelectAll={false}
                                        selectAllLabel="Select All"
                                      />
                                      {fields?.value[index]?.dataMaskInfo
                                        ?.label == "Custom" && (
                                        <>
                                          <Field
                                            className="form-control"
                                            name={`${name}.dataMaskInfo.valueExpr`}
                                            validate={required}
                                            render={({ input, meta }) => (
                                              <>
                                                <Form.Control
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
                              </td>
                            );
                          }
                        }
                        if (colName == "Row Level Filter") {
                          return (
                            <td key={colName} className="align-middle">
                              <Field
                                className="form-control"
                                name={`${name}.rowFilterInfo`}
                                render={({ input, meta }) => (
                                  <div className="table-editable">
                                    <Editable
                                      {...input}
                                      placement="auto"
                                      type="input"
                                    />
                                    {meta.touched && meta.error && (
                                      <span>{meta.error}</span>
                                    )}
                                  </div>
                                )}
                              />
                            </td>
                          );
                        }
                        if (
                          colName == "Deligate Admin" &&
                          serviceCompDetails?.name !== "tag"
                        ) {
                          return (
                            <td
                              key={`${name}-${index}`}
                              className="text-center align-middle"
                            >
                              <div key={`${name}-${index}`}>
                                <Field
                                  className="form-control"
                                  name={`${name}.delegateAdmin`}
                                  validate={(value, formValues) =>
                                    requiredForDeleGateAdmin(
                                      formValues[attrName],
                                      index
                                    )
                                  }
                                  data-js="delegatedAdmin"
                                  data-cy="delegatedAdmin"
                                  type="checkbox"
                                >
                                  {({ input, meta }) => (
                                    <div>
                                      <input {...input} type="checkbox" />
                                    </div>
                                  )}
                                </Field>
                              </div>
                            </td>
                          );
                        }
                        return <td key={colName}>{colName}</td>;
                      })}
                      <td className="align-middle">
                        <Button
                          variant="danger"
                          size="sm"
                          title="Remove"
                          onClick={() => fields.remove(index)}
                          data-action="delete"
                          data-cy="delete"
                        >
                          <i className="fa-fw fa fa-remove"></i>
                        </Button>
                      </td>
                    </tr>
                  ))
                }
              </FieldArray>
            </tbody>
          </Table>
        </div>
      </Col>
      <Button
        className="btn btn-mini mt-2"
        type="button"
        onClick={() => addPolicyItem(attrName, undefined)}
        data-action="addGroup"
        data-cy="addGroup"
        title="Add"
      >
        <i className="fa-fw fa fa-plus"></i>{" "}
      </Button>
    </div>
  );
}
