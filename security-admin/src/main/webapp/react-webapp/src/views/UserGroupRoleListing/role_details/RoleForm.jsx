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

import React, { useEffect, useReducer } from "react";
import { Button, Form as BForm, Col, Row, Table } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import {
  scrollToError,
  BlockUi,
  Loader,
  CustomTooltip,
  selectInputCustomStyles
} from "Components/CommonComponents";
import { FieldArray } from "react-final-form-arrays";
import arrayMutators from "final-form-arrays";
import AsyncSelect from "react-select/async";
import { toast } from "react-toastify";
import { findIndex, isEmpty, filter } from "lodash";
import { commonBreadcrumb, serverError } from "Utils/XAUtils";
import { useLocation, useParams, useNavigate } from "react-router-dom";
import { fetchApi } from "Utils/fetchAPI";
import usePrompt from "Hooks/usePrompt";
import { RegexValidation } from "Utils/XAEnums";

const initialState = {
  loader: true,
  roleInfo: {},
  selectedUser: [],
  selectedGroup: [],
  selectedRole: [],
  preventUnBlock: false,
  blockUI: false
};

const PromtDialog = (props) => {
  const { isDirtyField, isUnblock } = props;
  usePrompt("Are you sure you want to leave", isDirtyField && !isUnblock);
  return null;
};

function reducer(state, action) {
  switch (action.type) {
    case "SET_LOADER":
      return {
        ...state,
        loader: action.loader
      };
    case "SET_ROLE_DATA":
      return {
        ...state,
        roleInfo: action.roleInfo,
        loader: action.loader
      };
    case "SET_SELECTED_USER":
      return {
        ...state,
        selectedUser: action.selectedUser
      };
    case "SET_SELECTED_ROLE":
      return {
        ...state,
        selectedRole: action.selectedRole
      };
    case "SET_SELECTED_GROUP":
      return {
        ...state,
        selectedGroup: action.selectedGroup
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
}

function RoleForm() {
  const params = useParams();
  const { state } = useLocation();
  const navigate = useNavigate();
  const [roleFormState, dispatch] = useReducer(reducer, initialState);
  const {
    loader,
    roleInfo,
    selectedUser,
    selectedRole,
    selectedGroup,
    preventUnBlock,
    blockUI
  } = roleFormState;
  const toastId = React.useRef(null);
  useEffect(() => {
    if (params?.roleID) {
      fetchRoleData(params.roleID);
    } else {
      dispatch({
        type: "SET_LOADER",
        loader: false
      });
    }
  }, []);

  const filterUsrOp = (data, formVal) => {
    if (formVal && formVal.users) {
      let userSelectedData = formVal.users.map((m) => {
        return { label: m.name, value: m.name };
      });
      return findIndex(userSelectedData, data) === -1;
    } else {
      return findIndex(selectedUser, data) === -1;
    }
  };
  const filterGroupOp = (data, formVal) => {
    if (formVal && formVal.groups) {
      let groupSelectedData = formVal.groups.map((m) => {
        return { label: m.name, value: m.name };
      });
      return findIndex(groupSelectedData, data) === -1;
    } else {
      return findIndex(selectedGroup, data) === -1;
    }
  };
  const filterRoleOp = (data, formVal) => {
    if (formVal && formVal.roles) {
      let roleSelectedData = formVal.roles.map((m) => {
        return { label: m.name, value: m.name };
      });
      return findIndex(roleSelectedData, data) === -1;
    } else {
      return findIndex(selectedRole, data) === -1;
    }
  };

  const fetchRoleData = async (roleID) => {
    let roleRespData;
    try {
      const { fetchApi } = await import("Utils/fetchAPI");
      roleRespData = await fetchApi({
        url: "roles/roles/" + roleID
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Role or CSRF headers! ${error}`
      );
    }
    dispatch({
      type: "SET_ROLE_DATA",
      roleInfo: roleRespData?.data,
      loader: false
    });
  };

  const handleSubmit = async (formData) => {
    let roleFormData = {
      ...roleInfo,
      ...formData
    };

    if (
      !isEmpty(selectedUser) ||
      !isEmpty(selectedGroup) ||
      !isEmpty(selectedRole)
    ) {
      toast.dismiss(toastId.current);
      return (toastId.current = toast.warning(
        `Please add selected user/group/roles to there respective table else user/group/roles will not be added.`
      ));
    }

    dispatch({
      type: "SET_PREVENT_ALERT",
      preventUnBlock: true
    });

    if (params?.roleID) {
      try {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: true
        });
        await fetchApi({
          url: `roles/roles/${params.roleID}`,
          method: "put",
          data: roleFormData
        });
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });
        toast.success("Role updated successfully!!");
        navigate("/users/roletab");
      } catch (error) {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });
        serverError(error);
        console.error(`Error occurred while creating Role! ${error}`);
      }
    } else {
      try {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: true
        });
        await fetchApi({
          url: "roles/roles",
          method: "post",
          data: formData
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
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });
        toast.success("Role created successfully!!");
        navigate("/users/roletab", {
          state: {
            showLastPage: true,
            addPageData: tblpageData
          }
        });
      } catch (error) {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });
        serverError(error);
        console.error(`Error occurred while updating Role! ${error}`);
      }
    }
    if (toastId.current !== null) {
      toast.dismiss(toastId.current);
    }
  };

  const fetchUserOp = async (inputValue) => {
    let params = { name: inputValue || "", isVisible: 1 };
    let op = [];
    const userResp = await fetchApi({
      url: "xusers/lookup/users",
      params: params
    });
    op = userResp.data.vXStrings;
    return op.map((obj) => ({
      label: obj.value,
      value: obj.value
    }));
  };

  const handleUserChange = (value) => {
    dispatch({
      type: "SET_SELECTED_USER",
      selectedUser: value
    });
  };

  const handleUserAdd = (push) => {
    if (selectedUser.length == 0) {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please select atleast one user!!");
    } else {
      let usr = selectedUser.map(({ value }) => ({
        name: value,
        isAdmin: false
      }));
      usr.map((val) => {
        push("users", val);
      });

      dispatch({
        type: "SET_SELECTED_USER",
        selectedUser: []
      });
    }
  };

  const handleGroupAdd = (push) => {
    if (selectedGroup.length == 0) {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please select atleast one group!!");
    } else {
      let grp = selectedGroup.map(({ value }) => ({
        name: value,
        isAdmin: false
      }));
      grp.map((val) => {
        push("groups", val);
      });
      dispatch({
        type: "SET_SELECTED_GROUP",
        selectedGroup: []
      });
    }
  };

  const fetchGroupOp = async (inputValue) => {
    let params = { name: inputValue || "", isVisible: 1 };
    let op = [];
    const userResp = await fetchApi({
      url: "xusers/lookup/groups",
      params: params
    });
    op = userResp.data.vXStrings;
    return op.map((obj) => ({
      label: obj.value,
      value: obj.value
    }));
  };

  const handleGroupChange = (value) => {
    dispatch({
      type: "SET_SELECTED_GROUP",
      selectedGroup: value
    });
  };

  const handleRoleAdd = (push) => {
    if (selectedRole.length == 0) {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please select atleast one role!!");
    } else {
      let rol = selectedRole.map(({ value }) => ({
        name: value,
        isAdmin: false
      }));
      rol.map((val) => {
        push("roles", val);
      });
      dispatch({
        type: "SET_SELECTED_ROLE",
        selectedRole: []
      });
    }
  };

  const fetchRoleOp = async (inputValue) => {
    let params = { roleNamePartial: inputValue || "" };
    let op = [];

    const userResp = await fetchApi({
      url: "roles/roles",
      params: params
    });
    if (roleInfo.id !== undefined) {
      op = filter(userResp.data.roles, function (role) {
        return role.id !== roleInfo.id;
      });
    } else {
      op = userResp.data.roles;
    }

    return op.map((obj) => ({
      label: obj.name,
      value: obj.name
    }));
  };

  const handleRoleChange = (value) => {
    dispatch({
      type: "SET_SELECTED_ROLE",
      selectedRole: value
    });
  };
  const closeForm = () => {
    navigate("/users/roletab");
  };

  const setRoleFormData = () => {
    let formValueObj = {};
    if (params?.roleID) {
      if (Object.keys(roleInfo).length > 0) {
        formValueObj.name = roleInfo.name;
        formValueObj.description = roleInfo.description;
        formValueObj.users = roleInfo.users;
        formValueObj.groups = roleInfo.groups;
        formValueObj.roles = roleInfo.roles;
      }
    }
    return formValueObj;
  };
  const validateForm = (values) => {
    const errors = {};
    if (!values.name) {
      errors.name = "Required";
    } else {
      if (
        !RegexValidation.NAME_VALIDATION.regexExpressionForName.test(
          values.name
        )
      ) {
        errors.name = RegexValidation.NAME_VALIDATION.nameValidationMessage;
      }
    }
    return errors;
  };

  return (
    <>
      <div className="header-wraper">
        <h3 className="wrap-header bold">Role Detail</h3>
        {commonBreadcrumb(
          ["Roles", params.roleID ? "RoleEdit" : "RoleCreate"],
          params.roleID
        )}
      </div>

      {loader ? (
        <Loader />
      ) : (
        <Form
          onSubmit={handleSubmit}
          initialValues={setRoleFormData()}
          validate={validateForm}
          mutators={{
            ...arrayMutators
          }}
          render={({
            handleSubmit,
            form: {
              mutators: { push }
            },
            form,
            submitting,
            invalid,
            errors,
            values,
            dirty
          }) => (
            <div className="wrap user-role-grp-form">
              <PromtDialog isDirtyField={dirty} isUnblock={preventUnBlock} />

              <form
                onSubmit={(event) => {
                  handleSubmit(event);
                }}
              >
                <Field name="name">
                  {({ input, meta }) => (
                    <Row className="form-group">
                      <Col xs={3}>
                        <label className="form-label float-end">
                          Role Name *
                        </label>
                      </Col>
                      <Col xs={4} className={"position-relative"}>
                        <input
                          {...input}
                          type="text"
                          name="name"
                          placeholder="Role Name"
                          id={meta.error && meta.touched ? "isError" : "name"}
                          className={
                            meta.error && meta.touched
                              ? "form-control border-danger"
                              : "form-control"
                          }
                          disabled={params.roleID ? true : false}
                          data-cy="name"
                        />
                        <span className="input-box-info-icon">
                          <CustomTooltip
                            placement="right"
                            content={
                              <p
                                className="pd-10"
                                style={{ fontSize: "small" }}
                              >
                                1. User name should be start with alphabet /
                                numeric / underscore / non-us characters.
                                <br />
                                2. Allowed special character ,._-+/@= and space.
                                <br />
                                3. Name length should be greater than one.
                              </p>
                            }
                            icon="fa-fw fa fa-info-circle"
                          />
                        </span>
                        {meta.error && meta.touched && (
                          <span className="invalid-field">{meta.error}</span>
                        )}
                      </Col>
                    </Row>
                  )}
                </Field>
                <Field name="description">
                  {({ input }) => (
                    <Row className="form-group">
                      <Col xs={3}>
                        <label className="form-label float-end">
                          Description
                        </label>
                      </Col>
                      <Col xs={4}>
                        <textarea
                          {...input}
                          placeholder="Description"
                          className="form-control"
                          data-cy="description"
                        />
                      </Col>
                    </Row>
                  )}
                </Field>
                <div>
                  <fieldset>
                    <p className="formHeader">Users:</p>
                  </fieldset>
                  <div className="wrap">
                    <Col sm="8">
                      <FieldArray name="users">
                        {({ fields }) => (
                          <Table className="table table-bordered fixed-headertable">
                            <thead className="thead-light">
                              <tr>
                                <th className="text-center">User Name</th>
                                <th className="text-center">Is Role Admin</th>
                                <th className="text-center">Action</th>
                              </tr>
                            </thead>
                            <tbody>
                              {fields.value == undefined ? (
                                <tr>
                                  <td
                                    className="text-center text-muted"
                                    colSpan="3"
                                  >
                                    No users found
                                  </td>
                                </tr>
                              ) : (
                                fields.map((name, index) => (
                                  <tr key={index}>
                                    <td className="text-center more-less-width text-truncate">
                                      <span title={fields.value[index].name}>
                                        {fields.value[index].name}
                                      </span>
                                    </td>
                                    <td className="text-center">
                                      <Field
                                        className="form-control"
                                        name={`${name}.isAdmin`}
                                        render={({ input }) => (
                                          <div>
                                            <BForm.Group>
                                              <BForm.Check
                                                {...input}
                                                checked={input.value}
                                                type="checkbox"
                                                data-js="isRoleAdmin"
                                                data-cy="isRoleAdmin"
                                              />
                                            </BForm.Group>
                                          </div>
                                        )}
                                      />
                                    </td>
                                    <td className="text-center">
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
                              )}
                            </tbody>
                          </Table>
                        )}
                      </FieldArray>
                      <div className="form-group row">
                        <div className="col-sm-9">
                          <AsyncSelect
                            value={selectedUser}
                            filterOption={({ data }) =>
                              filterUsrOp(data, values)
                            }
                            onChange={handleUserChange}
                            loadOptions={fetchUserOp}
                            defaultOptions
                            isMulti
                            data-name="usersSelect"
                            data-cy="usersSelect"
                            styles={selectInputCustomStyles}
                          />
                        </div>
                        <div className="col-sm-3">
                          <Button
                            type="button"
                            className="btn btn-primary"
                            onClick={() => handleUserAdd(push)}
                            size="sm"
                            data-name="usersAddBtn"
                            data-cy="usersAddBtn"
                          >
                            Add Users
                          </Button>
                        </div>
                      </div>
                    </Col>
                  </div>
                </div>
                <div>
                  <fieldset>
                    <p className="formHeader">Groups:</p>
                  </fieldset>
                  <div className="wrap">
                    <Col sm="8">
                      <FieldArray name="groups">
                        {({ fields }) => (
                          <Table
                            bordered
                            className="table table-bordered fixed-headertable"
                          >
                            <thead className="thead-light">
                              <tr>
                                <th className="text-center">Group Name</th>
                                <th className="text-center">Is Role Admin</th>
                                <th className="etxt-center">Action</th>
                              </tr>
                            </thead>
                            <tbody>
                              {fields.value == undefined ? (
                                <tr>
                                  <td
                                    className="text-center text-muted"
                                    colSpan="3"
                                  >
                                    No groups found
                                  </td>
                                </tr>
                              ) : (
                                fields.map((name, index) => (
                                  <tr key={index}>
                                    <td className="text-center more-less-width text-truncate">
                                      <span title={fields.value[index].name}>
                                        {fields.value[index].name}
                                      </span>
                                    </td>
                                    <td className="text-center">
                                      <Field
                                        className="form-control"
                                        name={`${name}.isAdmin`}
                                        render={({ input }) => (
                                          <div>
                                            <BForm.Group>
                                              <BForm.Check
                                                {...input}
                                                checked={input.value}
                                                type="checkbox"
                                                data-js="isRoleAdmin"
                                                data-cy="isRoleAdmin"
                                              />
                                            </BForm.Group>
                                          </div>
                                        )}
                                      />
                                    </td>
                                    <td className="text-center">
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
                              )}
                            </tbody>
                          </Table>
                        )}
                      </FieldArray>
                      <div className="form-group row">
                        <div className="col-sm-9">
                          <AsyncSelect
                            value={selectedGroup}
                            filterOption={({ data }) =>
                              filterGroupOp(data, values)
                            }
                            onChange={handleGroupChange}
                            loadOptions={fetchGroupOp}
                            defaultOptions
                            isMulti
                            data-name="groupsSelect"
                            data-cy="groupsSelect"
                            styles={selectInputCustomStyles}
                          />
                        </div>
                        <div className="col-sm-3">
                          <Button
                            type="button"
                            className="btn btn-primary"
                            onClick={() => handleGroupAdd(push)}
                            size="sm"
                            data-name="groupsAddBtn"
                            data-cy="groupsAddBtn"
                          >
                            Add Groups
                          </Button>
                        </div>
                      </div>
                    </Col>
                  </div>
                </div>
                <div>
                  <fieldset>
                    <p className="formHeader">Roles:</p>
                  </fieldset>
                  <div className="wrap">
                    <Col sm="8">
                      <FieldArray name="roles">
                        {({ fields }) => (
                          <Table
                            bordered
                            className="table table-bordered fixed-headertable"
                          >
                            <thead className="thead-light">
                              <tr>
                                <th className="text-center">Role Name</th>
                                <th className="text-center">Is Role Admin</th>
                                <th className="text-center">Action</th>
                              </tr>
                            </thead>
                            <tbody>
                              {fields.value == undefined ? (
                                <tr>
                                  <td
                                    className="text-center text-muted"
                                    colSpan="3"
                                  >
                                    No roles found
                                  </td>
                                </tr>
                              ) : (
                                fields.map((name, index) => (
                                  <tr key={index}>
                                    <td className="text-center more-less-width text-truncate">
                                      <span title={fields.value[index].name}>
                                        {fields.value[index].name}
                                      </span>
                                    </td>
                                    <td className="text-center">
                                      <Field
                                        className="form-control"
                                        name={`${name}.isAdmin`}
                                        render={({ input }) => (
                                          <div>
                                            <BForm.Group>
                                              <BForm.Check
                                                {...input}
                                                checked={input.value}
                                                type="checkbox"
                                                data-js="isRoleAdmin"
                                                data-cy="isRoleAdmin"
                                              />
                                            </BForm.Group>
                                          </div>
                                        )}
                                      />
                                    </td>
                                    <td className="text-center">
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
                              )}
                            </tbody>
                          </Table>
                        )}
                      </FieldArray>
                      <div className="form-group row">
                        <div className="col-sm-9">
                          <AsyncSelect
                            value={selectedRole}
                            filterOption={({ data }) =>
                              filterRoleOp(data, values)
                            }
                            onChange={handleRoleChange}
                            loadOptions={fetchRoleOp}
                            defaultOptions
                            isMulti
                            data-name="rolesSelect"
                            data-cy="rolesSelect"
                            styles={selectInputCustomStyles}
                          />
                        </div>
                        <div className="col-sm-3">
                          <Button
                            type="button"
                            className="btn btn-primary"
                            onClick={() => handleRoleAdd(push)}
                            size="sm"
                            data-name="rolesAddBtn"
                            data-cy="rolesAddBtn"
                            s
                          >
                            Add Roles
                          </Button>
                        </div>
                      </div>
                    </Col>
                  </div>
                </div>

                <div className="row form-actions">
                  <div className="col-md-9 offset-md-3">
                    <Button
                      variant="primary"
                      onClick={() => {
                        if (invalid) {
                          let selector =
                            document.getElementById("isError") ||
                            document.getElementById(Object.keys(errors)[0]) ||
                            document.querySelector(
                              `input[name=${Object.keys(errors)[0]}]`
                            ) ||
                            document.querySelector(
                              `input[id=${Object.keys(errors)[0]}]`
                            ) ||
                            document.querySelector(
                              `span[className="invalid-field"]`
                            );
                          scrollToError(selector);
                        }
                        handleSubmit(values);
                      }}
                      size="sm"
                      disabled={submitting}
                      data-id="save"
                      data-cy="save"
                    >
                      Save
                    </Button>
                    <Button
                      variant="secondary"
                      type="button"
                      size="sm"
                      onClick={() => {
                        form.reset;
                        dispatch({
                          type: "SET_PREVENT_ALERT",
                          preventUnBlock: true
                        });
                        closeForm();
                      }}
                      data-id="cancel"
                      data-cy="cancel"
                    >
                      Cancel
                    </Button>
                  </div>
                </div>
              </form>
            </div>
          )}
        />
      )}
      <BlockUi isUiBlock={blockUI} />
    </>
  );
}

export default RoleForm;
