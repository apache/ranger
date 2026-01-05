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
import { Button, Table } from "react-bootstrap";
import { Field } from "react-final-form";
import { FieldArray } from "react-final-form-arrays";
import Select from "react-select";
import AsyncSelect from "react-select/async";
import Editable from "Components/Editable";
import CreatableField from "Components/CreatableField";
import ModalResourceComp from "Views/Resources/ModalResourceComp";
import { uniq, map, join, isEmpty, isArray } from "lodash";
import TagBasePermissionItem from "Views/PolicyListing/TagBasePermissionItem";
import { dragStart, dragEnter, drop, dragOver } from "Utils/XAUtils";
import { selectInputCustomStyles } from "Components/CommonComponents";

export default function ServiceAuditFilter(props) {
  const {
    serviceDetails,
    serviceDefDetails,
    fetchUsersData,
    fetchGroupsData,
    fetchRolesData,
    addAuditFilter,
    formValues
  } = props;
  const dragItem = useRef();
  const dragOverItem = useRef();
  const [defaultUserOptions, setDefaultUserOptions] = useState([]);
  const [defaultGroupOptions, setDefaultGroupOptions] = useState([]);
  const [defaultRoleOptions, setDefaultRoleOptions] = useState([]);
  const [roleLoading, setRoleLoading] = useState(false);
  const [groupLoading, setGroupLoading] = useState(false);
  const [userLoading, setUserLoading] = useState(false);
  const [modelState, setModalstate] = useState({
    showModalResource: false,
    resourceInput: null,
    data: {}
  });

  const handleClose = () => {
    setModalstate({
      showModalResource: false,
      resourceInput: null,
      data: {}
    });
  };

  const renderResourcesModal = (input) => {
    setModalstate({
      showModalResource: true,
      resourceInput: input,
      data: isEmpty(input.value) ? {} : input.value
    });
  };

  const handleSave = () => {
    let inputData;
    inputData = modelState.data;
    modelState.resourceInput.onChange(inputData);
    handleClose();
  };

  const handleRemove = (input) => {
    for (const obj in input.value) {
      delete input.value[obj];
    }
    setModalstate({
      showModalResource: false,
      resourceInput: null,
      data: {}
    });
  };

  const getResourceData = (resourceData) => {
    let dataStructure = [];

    let levels = uniq(map(serviceDefDetails?.resources, "level"));

    dataStructure = levels.map((level, index) => {
      if (
        resourceData[`resourceName-${level}`] !== undefined &&
        resourceData[`value-${level}`] !== undefined
      ) {
        let excludesSupported =
          resourceData[`resourceName-${level}`].excludesSupported;
        let recursiveSupported =
          resourceData[`resourceName-${level}`].recursiveSupported;
        return (
          <div className="resource-filter" key={index}>
            <div>
              <span className="bold me-1">
                {resourceData[`resourceName-${level}`].name}
              </span>
              :
              <span className="ms-1">
                {isArray(resourceData[`value-${level}`])
                  ? join(map(resourceData[`value-${level}`], "value"), ", ")
                  : [resourceData[`value-${level}`].value]}
              </span>
            </div>
            <div>
              {excludesSupported && (
                <h6 className="text-center">
                  {resourceData[`isExcludesSupport-${level}`] == false ? (
                    <span className="badge bg-dark">Exclude</span>
                  ) : (
                    <span className="badge bg-dark">Include</span>
                  )}
                </h6>
              )}
              {recursiveSupported && (
                <h6 className="text-center">
                  {resourceData[`isRecursiveSupport-${level}`] == false ? (
                    <span className="badge bg-dark">Non Recursive</span>
                  ) : (
                    <span className="badge bg-dark">Recursive</span>
                  )}
                </h6>
              )}
            </div>
          </div>
        );
      }
    });

    return dataStructure;
  };

  const getResourceIcon = (resourceData) => {
    let iconClass = !isEmpty(resourceData)
      ? "fa fa-fw fa-pencil"
      : "fa fa-fw fa-plus";

    return iconClass;
  };

  const getAccessTypeOptions = () => {
    let srcOp = [];
    srcOp = serviceDefDetails?.accessTypes;
    return srcOp?.map(({ label, name: value }) => ({
      label,
      value
    }));
  };

  const permList = [
    "Is Audited",
    "Access Result",
    "Resources",
    "Operations",
    "Permissions",
    "Users",
    "Groups",
    "Roles",
    " "
  ];

  const tableHeader = () => {
    return permList.map((data) => {
      return <th key={data}>{data}</th>;
    });
  };

  const handleSelectChange = (value, input) => {
    input.onChange(value);
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
    <div className="table-responsive">
      <Table
        bordered
        size="sm"
        className="mt-3 table-audit-filter text-center d-table"
      >
        <thead>
          <tr>{tableHeader()}</tr>
        </thead>
        <tbody className="drag-drop-wrap">
          {formValues.auditFilters !== undefined &&
            formValues.auditFilters.length === 0 && (
              <tr className="text-center">
                <td colSpan={permList.length}>
                  Click on below <i className="fa-fw fa fa-plus"></i>
                  button to add audit filter !!
                </td>
              </tr>
            )}

          <FieldArray name="auditFilters">
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
                    if (colName == "Is Audited") {
                      return (
                        <td key={`${name}.isAudited`} className="align-middle">
                          <div className="d-flex">
                            <Field
                              className="form-control audit-filter-select"
                              name={`${name}.isAudited`}
                              component="select"
                              style={{ minWidth: "75px" }}
                            >
                              <option value="true">Yes</option>
                              <option value="false">No</option>
                            </Field>
                          </div>
                        </td>
                      );
                    }
                    if (colName == "Access Result") {
                      return (
                        <td key={colName} className="align-middle">
                          <Field
                            className="form-control"
                            name={`${name}.accessResult`}
                          >
                            {({ input }) => (
                              <div style={{ minWidth: "220px" }}>
                                <Select
                                  {...input}
                                  menuPortalTarget={document.body}
                                  isClearable={true}
                                  isSearchable={false}
                                  options={[
                                    { value: "DENIED", label: "DENIED" },
                                    { value: "ALLOWED", label: "ALLOWED" },
                                    {
                                      value: "NOT_DETERMINED",
                                      label: "NOT_DETERMINED"
                                    }
                                  ]}
                                  menuPlacement="auto"
                                  placeholder="Select Access Result"
                                />
                              </div>
                            )}
                          </Field>
                        </td>
                      );
                    }
                    if (colName == "Resources") {
                      return (
                        <td key={`${name}.resources`} className="align-middle">
                          <Field
                            name={`${name}.resources`}
                            render={({ input }) => (
                              <React.Fragment>
                                <div style={{ minWidth: "195px" }}>
                                  <div className="resource-group">
                                    {getResourceData(input.value)}
                                  </div>

                                  <Button
                                    className="me-1 btn-mini"
                                    variant="primary"
                                    size="sm"
                                    onClick={() => renderResourcesModal(input)}
                                    data-js="serviceResource"
                                    data-cy="serviceResource"
                                  >
                                    <i
                                      className={getResourceIcon(input.value)}
                                    ></i>
                                  </Button>
                                  <Button
                                    className="me-1 btn-mini"
                                    variant="danger"
                                    size="sm"
                                    onClick={() => handleRemove(input)}
                                  >
                                    <i className="fa-fw fa fa-remove"></i>
                                  </Button>
                                </div>
                              </React.Fragment>
                            )}
                          />
                        </td>
                      );
                    }
                    if (colName == "Operations") {
                      return (
                        <td
                          className="mx-w-210 align-middle"
                          key={`${name}.actions`}
                          width="210px"
                        >
                          <Field
                            className="form-control"
                            name={`${name}.actions`}
                          >
                            {({ input }) => (
                              <CreatableField
                                actionValues={input.value}
                                creatableOnChange={(value) =>
                                  handleSelectChange(value, input)
                                }
                              />
                            )}
                          </Field>
                        </td>
                      );
                    }
                    if (colName == "Permissions") {
                      if (serviceDefDetails?.name == "tag") {
                        return (
                          <td
                            key={`${name}.accessTypes`}
                            className="align-middle"
                          >
                            <Field
                              className="form-control"
                              name={`${name}.accessTypes`}
                              render={({ input }) => (
                                <TagBasePermissionItem
                                  options={getAccessTypeOptions()}
                                  inputVal={input}
                                />
                              )}
                            />
                          </td>
                        );
                      } else {
                        return (
                          <td
                            key={`${name}.accessTypes`}
                            className="align-middle"
                          >
                            <Field
                              className="form-control"
                              name={`${name}.accessTypes`}
                              render={({ input }) => (
                                <div style={{ minWidth: "100px" }}>
                                  <Editable
                                    {...input}
                                    placement="right"
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
                    if (colName == "Roles") {
                      return (
                        <td key={`${name}.roles`} className="align-middle">
                          <Field
                            className="form-control"
                            name={`${name}.roles`}
                            render={({ input }) => (
                              <div
                                style={{
                                  minWidth: "150px",
                                  maxWidth: "350px"
                                }}
                              >
                                <AsyncSelect
                                  {...input}
                                  menuPortalTarget={document.body}
                                  components={{
                                    IndicatorSeparator: () => null
                                  }}
                                  loadOptions={fetchRolesData}
                                  onFocus={() => {
                                    onFocusRoleSelect();
                                  }}
                                  defaultOptions={defaultRoleOptions}
                                  isClearable={false}
                                  noOptionsMessage={() =>
                                    roleLoading ? "Loading..." : "No options"
                                  }
                                  menuPlacement="auto"
                                  cacheOptions
                                  isMulti
                                  styles={selectInputCustomStyles}
                                  placeholder="Select Roles"
                                />
                              </div>
                            )}
                          />
                        </td>
                      );
                    }
                    if (colName == "Groups") {
                      return (
                        <td key={`${name}.groups`} className="align-middle">
                          <Field
                            className="form-control"
                            name={`${name}.groups`}
                            render={({ input }) => (
                              <div
                                style={{
                                  minWidth: "150px",
                                  maxWidth: "350px"
                                }}
                              >
                                <AsyncSelect
                                  {...input}
                                  menuPortalTarget={document.body}
                                  components={{
                                    IndicatorSeparator: () => null
                                  }}
                                  loadOptions={fetchGroupsData}
                                  onFocus={() => {
                                    onFocusGroupSelect();
                                  }}
                                  defaultOptions={defaultGroupOptions}
                                  isClearable={false}
                                  noOptionsMessage={() =>
                                    groupLoading ? "Loading..." : "No options"
                                  }
                                  menuPlacement="auto"
                                  cacheOptions
                                  isMulti
                                  styles={selectInputCustomStyles}
                                  placeholder="Select Groups"
                                />
                              </div>
                            )}
                          />
                        </td>
                      );
                    }
                    if (colName == "Users") {
                      return (
                        <td key={`${name}.users`} className="align-middle">
                          <Field
                            className="form-control"
                            name={`${name}.users`}
                            render={({ input }) => (
                              <div
                                style={{
                                  minWidth: "150px",
                                  maxWidth: "350px"
                                }}
                              >
                                <AsyncSelect
                                  {...input}
                                  menuPortalTarget={document.body}
                                  components={{
                                    IndicatorSeparator: () => null
                                  }}
                                  loadOptions={fetchUsersData}
                                  onFocus={() => {
                                    onFocusUserSelect();
                                  }}
                                  defaultOptions={defaultUserOptions}
                                  isClearable={false}
                                  noOptionsMessage={() =>
                                    userLoading ? "Loading..." : "No options"
                                  }
                                  menuPlacement="auto"
                                  cacheOptions
                                  isMulti
                                  styles={selectInputCustomStyles}
                                  placeholder="Select Users"
                                />
                              </div>
                            )}
                          />
                        </td>
                      );
                    }
                  })}
                  <td key={`${index}.remove`} className="align-middle">
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

      <Button
        variant="outline-dark"
        size="sm"
        className="btn-sm"
        type="button"
        onClick={() => addAuditFilter("auditFilters", undefined)}
        data-action="addGroup"
        data-cy="addGroup"
        title="Add"
      >
        <i className="fa-fw fa fa-plus"></i>
      </Button>

      <ModalResourceComp
        serviceDetails={serviceDetails}
        serviceCompDetails={serviceDefDetails}
        cancelButtonText="Cancel"
        actionButtonText="Submit"
        handleSave={handleSave}
        modelState={modelState}
        handleClose={handleClose}
      />
    </div>
  );
}
