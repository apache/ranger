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

import { Form, Field } from "react-final-form";
import {
  Button,
  Col,
  Form as FormB,
  Row,
  Table,
  Spinner
} from "react-bootstrap";
import React, { useEffect, useReducer, useRef } from "react";
import { useParams, useNavigate } from "react-router-dom";
import {
  Loader,
  BlockUi,
  selectInputCustomStyles
} from "Components/CommonComponents";
import { fetchApi } from "Utils/fetchAPI";
import AsyncSelect from "react-select/async";
import { toast } from "react-toastify";
import { cloneDeep, find, findIndex, isEmpty, map, reverse } from "lodash";
import { AccessResult } from "Utils/XAEnums";
import {
  CustomInfinteScroll,
  commonBreadcrumb,
  serverError
} from "Utils/XAUtils";

const initialState = {
  loader: true,
  permissionData: null,
  selectedGrp: [],
  selectedUsr: [],
  usrloading: false,
  grploading: false,
  blockUI: false
};

function reducer(state, action) {
  switch (action.type) {
    case "SET_DATA":
      return {
        ...state,
        loader: false,
        permissionData: action.data,
        selectedGrp: action.grpData,
        selectedUsr: action.usrData
      };
    case "SET_SELECTED_GRP":
      return {
        ...state,
        selectedGrp: action.grpData,
        grploading: false
      };
    case "SET_SELECTED_USR":
      return {
        ...state,
        selectedUsr: action.usrData,
        usrloading: false
      };
    case "USR_LOADING": {
      return { ...state, usrloading: true };
    }
    case "GRP_LOADING": {
      return { ...state, grploading: true };
    }
    case "SET_BLOCK_UI":
      return {
        ...state,
        blockUI: action.blockUI
      };
    default:
      throw new Error();
  }
}
const EditPermission = () => {
  let { permissionId } = useParams();
  const navigate = useNavigate();
  const toastId = useRef(null);
  const [permissionState, dispatch] = useReducer(reducer, initialState);
  const {
    loader,
    permissionData,
    usrloading,
    grploading,
    selectedGrp,
    selectedUsr,
    blockUI
  } = permissionState;

  useEffect(() => {
    fetchPermissions();
  }, []);

  const onSubmit = async (values) => {
    const formData = cloneDeep(values);
    if (
      (values.selectGroups && values.selectGroups.length > 0) ||
      (values.selectuser && values.selectuser.length > 0)
    ) {
      toast.dismiss(toastId.current);
      toastId.current = toast.error(
        "Please add selected user/group to permissions else user/group will not be added."
      );
      return false;
    }
    for (const grpObj of formData.groupPermList) {
      let index = findIndex(selectedGrp, { value: grpObj.groupId });
      if (index === -1) {
        grpObj.isAllowed = AccessResult.ACCESS_RESULT_DENIED.value;
      }
    }
    for (const grpObj of selectedGrp) {
      let obj = find(formData.groupPermList, { groupId: grpObj.value });
      if (!obj) {
        formData.groupPermList.push({
          groupId: grpObj.value,
          isAllowed: AccessResult.ACCESS_RESULT_ALLOWED.value,
          moduleId: formData.id
        });
      }
    }
    for (const userObj of formData.userPermList) {
      let index = findIndex(selectedUsr, { value: userObj.userId });
      if (index === -1) {
        userObj.isAllowed = AccessResult.ACCESS_RESULT_DENIED.value;
      }
    }
    for (const userObj of selectedUsr) {
      let obj = find(formData.userPermList, { userId: userObj.value });
      if (!obj) {
        formData.userPermList.push({
          userId: userObj.value,
          isAllowed: AccessResult.ACCESS_RESULT_ALLOWED.value,
          moduleId: formData.id
        });
      }
    }

    try {
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: true
      });
      await fetchApi({
        url: `xusers/permission/${permissionId}`,
        method: "PUT",
        data: formData
      });
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: false
      });
      navigate("/permissions/models");
      toast.dismiss(toastId.current);
      toastId.current = toast.success(
        "Success! Module Permissions updated successfully"
      );
    } catch (error) {
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: false
      });
      console.error(`Error occurred while fetching Policies ! ${error}`);
      serverError(error);
    }
  };

  const fetchPermissions = async () => {
    let data = null;
    let groups = [];
    let users = [];
    try {
      const permissionResp = await fetchApi({
        url: `xusers/permission/${permissionId}`,
        params: {}
      });
      data = permissionResp?.data;
    } catch (error) {
      console.error(`Error occurred while fetching Permissions ! ${error}`);
    }
    groups = reverse(data?.groupPermList);
    users = reverse(data?.userPermList);
    dispatch({
      type: "SET_DATA",
      data,
      grpData: groups?.map((obj) => ({
        label: obj.groupName,
        value: obj.groupId
      })),
      usrData: users?.map((obj) => ({
        label: obj.userName,
        value: obj.userId
      }))
    });
  };

  const fetchGroups = async (inputValue) => {
    let params = { isVisible: 1 };
    let groupsOp = [];

    if (inputValue) {
      params["name"] = inputValue || "";
    }

    try {
      const groupResp = await fetchApi({
        url: "xusers/groups",
        params: params
      });
      groupsOp = groupResp.data?.vXGroups;
    } catch (error) {
      console.error(`Error occurred while fetching Groups ! ${error}`);
      serverError(error);
    }

    return map(groupsOp, function (group) {
      return { label: group.name, value: group.id };
    });
  };

  const filterGrpOp = ({ data }) => {
    return findIndex(selectedGrp, data) === -1;
  };

  const addInSelectedGrp = (formData, input) => {
    dispatch({ type: "GRP_LOADING" });
    setTimeout(() => {
      dispatch({
        type: "SET_SELECTED_GRP",
        grpData: [...formData.selectGroups, ...selectedGrp]
      });
      input.onChange([]);
    }, 100);
  };

  const handleRemoveGrp = (obj) => {
    dispatch({ type: "GRP_LOADING" });
    setTimeout(() => {
      let index = findIndex(selectedGrp, obj);
      if (index !== -1) {
        selectedGrp.splice(index, 1);
        dispatch({
          type: "SET_SELECTED_GRP",
          grpData: selectedGrp
        });
      }
    }, 100);
  };

  const fetchUsers = async (inputValue) => {
    let params = { isVisible: 1 };
    let usersOp = [];
    let notAllowedRoles = ["ROLE_KEY_ADMIN", "ROLE_KEY_ADMIN_AUDITOR"];
    if (inputValue) {
      params["name"] = inputValue || "";
    }

    try {
      const userResp = await fetchApi({
        url: "xusers/users",
        params: params
      });
      usersOp =
        permissionData.module == "Tag Based Policies"
          ? userResp.data?.vXUsers.filter(
              (users) => !notAllowedRoles.includes(users.userRoleList[0])
            )
          : userResp.data?.vXUsers;
    } catch (error) {
      console.error(`Error occurred while fetching Users ! ${error}`);
      serverError(error);
    }

    return map(usersOp, function (user) {
      return { label: user.name, value: user.id };
    });
  };

  const filterUsrOp = ({ data }) => {
    return findIndex(selectedUsr, data) === -1;
  };

  const addInSelectedUsr = (formData, input) => {
    dispatch({ type: "USR_LOADING" });
    setTimeout(() => {
      let data = [...formData.selectuser, ...selectedUsr];
      dispatch({
        type: "SET_SELECTED_USR",
        usrData: data
      });
      input.onChange([]);
    }, 100);
  };

  const handleRemoveUsr = (obj) => {
    dispatch({ type: "USR_LOADING" });
    setTimeout(() => {
      let index = findIndex(selectedUsr, obj);
      if (index !== -1) {
        selectedUsr.splice(index, 1);
        dispatch({
          type: "SET_SELECTED_USR",
          usrData: selectedUsr
        });
      }
    }, 100);
  };
  return loader ? (
    <Loader />
  ) : (
    <div>
      <div className="header-wraper">
        <h3 className="wrap-header bold">Edit Permission</h3>
        {commonBreadcrumb(
          ["ModulePermissions", "ModulePermissionEdit"],
          permissionData
        )}
      </div>
      <div className="wrap non-collapsible">
        <Form
          id="myform2"
          name="myform2"
          onSubmit={onSubmit}
          initialValues={permissionData}
          render={({ handleSubmit, submitting, values }) => (
            <form onSubmit={handleSubmit}>
              <div className="form-horizontal">
                <p className="form-header">Module Details:</p>

                <div>
                  <Field
                    className="form-control"
                    name="module"
                    render={({ input }) => (
                      <FormB.Group
                        as={Row}
                        className="mb-3"
                        controlId="moduleName"
                      >
                        <FormB.Label column sm="3" className="text-end">
                          <strong>Module Name *</strong>
                        </FormB.Label>
                        <Col sm="4">
                          <FormB.Control {...input} disabled />
                        </Col>
                      </FormB.Group>
                    )}
                  />
                </div>
              </div>
              <br />
              <br />
              <div className="form-horizontal">
                <p className="form-header">User and Group Permissions:</p>

                <Field
                  className="form-control"
                  name="zoneservices"
                  render={() => (
                    <FormB.Group
                      as={Row}
                      className="mb-3"
                      controlId="formPlaintextEmail"
                    >
                      <FormB.Label className="text-end" column sm="2">
                        Permissions
                      </FormB.Label>
                      <Col sm="10">
                        <Table bordered>
                          <thead className="table-edit-permission">
                            <tr>
                              <th width="49%">Select and Add Group</th>
                              <th width="49%">Select and Add User </th>
                            </tr>
                          </thead>
                          <tbody>
                            <tr>
                              <td>
                                <Field
                                  className="form-control"
                                  name="selectGroups"
                                  render={({ input }) => (
                                    <div>
                                      <AsyncSelect
                                        {...input}
                                        className="edit-perm-select"
                                        defaultOptions
                                        filterOption={filterGrpOp}
                                        loadOptions={fetchGroups}
                                        components={{
                                          DropdownIndicator: () => null,
                                          IndicatorSeparator: () => null
                                        }}
                                        isClearable={false}
                                        placeholder="Select Groups"
                                        isMulti
                                        styles={selectInputCustomStyles}
                                      />
                                      <Button
                                        size="sm"
                                        className="ms-2  m-r-sm"
                                        variant="outline-secondary"
                                        onClick={() => {
                                          if (
                                            !values.selectGroups ||
                                            values.selectGroups.length === 0
                                          ) {
                                            toast.dismiss(toastId.current);
                                            toastId.current = toast.error(
                                              "Please select group!!"
                                            );
                                            return false;
                                          }
                                          addInSelectedGrp(values, input);
                                        }}
                                        data-id="addGroupBtn"
                                        data-cy="addGroupBtn"
                                      >
                                        <i className="fa fa-plus"></i>
                                      </Button>
                                    </div>
                                  )}
                                />
                              </td>
                              <td>
                                <Field
                                  className="form-control"
                                  name="selectuser"
                                  render={({ input }) => (
                                    <div>
                                      <AsyncSelect
                                        {...input}
                                        className="edit-perm-select"
                                        defaultOptions
                                        filterOption={filterUsrOp}
                                        loadOptions={fetchUsers}
                                        components={{
                                          DropdownIndicator: () => null,
                                          IndicatorSeparator: () => null
                                        }}
                                        isClearable={false}
                                        placeholder="Select Users"
                                        isMulti
                                        styles={selectInputCustomStyles}
                                      />

                                      <Button
                                        size="sm"
                                        className="ms-2  m-r-sm"
                                        variant="outline-secondary"
                                        onClick={() => {
                                          if (
                                            !values.selectuser ||
                                            values.selectuser.length === 0
                                          ) {
                                            toast.dismiss(toastId.current);
                                            toastId.current = toast.error(
                                              "Please select user!!"
                                            );
                                            return false;
                                          }
                                          addInSelectedUsr(values, input);
                                        }}
                                        data-id="addUserBtn"
                                        data-cy="addUserBtn"
                                      >
                                        <i className="fa fa-plus"></i>
                                      </Button>
                                    </div>
                                  )}
                                />
                              </td>
                            </tr>
                            <tr>
                              {!isEmpty(selectedGrp) ? (
                                <td>
                                  {grploading ? (
                                    <div className="permission-infinite-scroll text-center">
                                      <Spinner
                                        animation="border"
                                        size="sm"
                                        role="status"
                                      ></Spinner>
                                    </div>
                                  ) : (
                                    <CustomInfinteScroll
                                      data={selectedGrp}
                                      removeUsrGrp={handleRemoveGrp}
                                      scrollableDiv="scrollableGrpDiv"
                                    />
                                  )}
                                </td>
                              ) : (
                                <td className="align-middle text-center">
                                  <strong className="text-danger font-italic">
                                    No Selected Groups
                                  </strong>
                                </td>
                              )}

                              {!isEmpty(selectedUsr) ? (
                                <td>
                                  {usrloading ? (
                                    <div className="permission-infinite-scroll text-center">
                                      <Spinner
                                        animation="border"
                                        size="sm"
                                        role="status"
                                      ></Spinner>
                                    </div>
                                  ) : (
                                    <CustomInfinteScroll
                                      data={selectedUsr}
                                      removeUsrGrp={handleRemoveUsr}
                                      scrollableDiv="scrollableUsrDiv"
                                    />
                                  )}
                                </td>
                              ) : (
                                <td className="align-middle text-center">
                                  <strong className="text-danger font-italic">
                                    No Selected Users
                                  </strong>
                                </td>
                              )}
                            </tr>
                          </tbody>
                        </Table>
                      </Col>
                    </FormB.Group>
                  )}
                />
              </div>

              <div className="text-center form-actions">
                <Button
                  type="submit"
                  variant="primary"
                  size="sm"
                  disabled={submitting}
                  data-id="save"
                  data-cy="save"
                >
                  Save
                </Button>
                <Button
                  variant="secondary"
                  size="sm"
                  type="button"
                  onClick={() => {
                    navigate(`/permissions/models`);
                  }}
                  data-id="cancel"
                  data-cy="cancel"
                >
                  Cancel
                </Button>
              </div>
            </form>
          )}
        />
        <BlockUi isUiBlock={blockUI} />
      </div>
    </div>
  );
};
export default EditPermission;
