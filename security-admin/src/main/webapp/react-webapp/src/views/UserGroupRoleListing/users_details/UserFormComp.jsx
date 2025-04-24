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

import React, { useReducer, useState } from "react";
import { Button, Row, Col } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import {
  BlockUi,
  scrollToError,
  selectInputCustomStyles
} from "Components/CommonComponents";
import AsyncSelect from "react-select/async";
import Select from "react-select";
import { fetchApi } from "Utils/fetchAPI";
import {
  ActivationStatus,
  RegexValidation,
  UserRoles,
  UserTypes
} from "Utils/XAEnums";
import { toast } from "react-toastify";
import { InfoIcon, getUserAccessRoleList, serverError } from "Utils/XAUtils";
import { getUserProfile, setUserProfile } from "Utils/appState";
import { cloneDeep, has, isEmpty, isUndefined } from "lodash";
import { SyncSourceDetails } from "Views/UserGroupRoleListing/SyncSourceDetails";
import { RegexMessage, roleChngWarning } from "Utils/XAMessages";
import { useLocation, useNavigate } from "react-router-dom";
import usePrompt from "Hooks/usePrompt";

const initialState = {
  blockUI: false
};

const PromtDialog = (props) => {
  const { isDirtyField, isUnblock } = props;
  usePrompt("Are you sure you want to leave", isDirtyField && !isUnblock);
  return null;
};

function reducer(state, action) {
  switch (action.type) {
    case "SET_BLOCK_UI":
      return {
        ...state,
        blockUI: action.blockUI
      };
    default:
      throw new Error();
  }
}

function UserFormComp(props) {
  const { state } = useLocation();
  const navigate = useNavigate();
  const [userFormState, dispatch] = useReducer(reducer, initialState);
  const { blockUI } = userFormState;
  const { isEditView, userInfo } = props;
  const [preventUnBlock, setPreventUnblock] = useState(false);
  const toastId = React.useRef(null);
  const isExternalOrFederatedUser =
    userInfo?.userSource == UserTypes.USER_EXTERNAL.value ||
    userInfo?.userSource == UserTypes.USER_FEDERATED.value;

  const handleSubmit = async (formData) => {
    let userFormData = {};

    userFormData["name"] = formData.name;
    userFormData["password"] = formData.password;
    userFormData["firstName"] = formData.firstName;
    userFormData["lastName"] = formData.lastName;
    userFormData["emailAddress"] = formData.emailAddress;
    userFormData["userRoleList"] = formData.userRoleList
      ? [formData.userRoleList.value]
      : [];

    if (formData.groupIdList) {
      userFormData["groupIdList"] = formData.groupIdList.map(
        (obj) => obj.value + ""
      );
    }

    userFormData["status"] = ActivationStatus.ACT_STATUS_ACTIVE.value;

    setPreventUnblock(true);
    if (isEditView) {
      let userEditData = { ...userInfo, ...userFormData };
      delete userEditData.password;

      try {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: true
        });
        await fetchApi({
          url: `xusers/secure/users/${userInfo.id}`,
          method: "put",
          data: userEditData
        });

        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });

        const userProps = cloneDeep(getUserProfile());
        if (userProps.loginId == userInfo.name) {
          userProps.firstName = userEditData.firstName;
          userProps.emailAddress = userEditData.emailAddress;
          userProps.lastName = userEditData.lastName;

          setUserProfile(userProps);
        }
        toast.success("User updated successfully!!");
        navigate("/users/usertab");
      } catch (error) {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });
        serverError(error);
        console.error("Error occurred while updating user");
      }
    } else {
      try {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: true
        });
        await fetchApi({
          url: "xusers/secure/users",
          method: "post",
          data: userFormData
        });

        let tblpageData = {};
        if (state && state !== null) {
          tblpageData = state.tblpageData;
          if (state.tblpageData.pageRecords % state.tblpageData.pageSize == 0) {
            tblpageData["totalPage"] = state.tblpageData.totalPage + 1;
          } else {
            if (state !== undefined) {
              tblpageData["totalPage"] = state.tblpageData.totalPage;
            }
          }
        }

        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });
        toast.success("User created successfully!!");
        navigate("/users/usertab", {
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
        console.error(`Error occurred while creating user`);
      }
    }
    if (toastId.current !== null) {
      toast.dismiss(toastId.current);
    }
  };

  const closeForm = () => {
    navigate("/users/usertab");
  };

  const groupNameList = ({ input }) => {
    const loadOptions = async (inputValue) => {
      let params = {},
        op = [];
      if (inputValue) {
        params["name"] = inputValue || "";
      }
      const opResp = await fetchApi({
        url: "xusers/groups",
        params: params
      });
      if (opResp.data && opResp.data.vXGroups) {
        op = opResp.data.vXGroups.map((obj) => {
          return {
            label: obj.name,
            value: obj.id
          };
        });
      }
      return op;
    };
    return (
      <AsyncSelect
        {...input}
        id="groupIdList"
        data-cy="groupIdList"
        cacheOptions
        loadOptions={loadOptions}
        defaultOptions
        isMulti
        isDisabled={
          isEditView && userInfo && isExternalOrFederatedUser ? true : false
        }
        styles={selectInputCustomStyles}
      />
    );
  };

  const disabledUserRoleField = () => {
    const userProps = getUserProfile();
    let disabledUserRolefield;
    if (isEditView && userInfo) {
      if (userInfo.userSource == UserTypes.USER_EXTERNAL.value) {
        disabledUserRolefield = true;
      }
      if (userInfo.userSource == UserTypes.USER_FEDERATED.value) {
        return (disabledUserRolefield = true);
      }
      if (userProps.loginId != "admin") {
        if (userInfo.name != "admin") {
          if (
            userProps.userRoleList[0] == "ROLE_SYS_ADMIN" ||
            userProps.userRoleList[0] == "ROLE_KEY_ADMIN"
          ) {
            disabledUserRolefield = false;
          } else {
            disabledUserRolefield = true;
          }
        } else {
          disabledUserRolefield = true;
        }
      } else {
        disabledUserRolefield = false;
      }
      if (userInfo.name == userProps.loginId) {
        disabledUserRolefield = true;
      }
    }
    return disabledUserRolefield;
  };

  const userRoleListData = () => {
    return getUserAccessRoleList();
  };

  const userData = () => {
    if (userInfo) {
      return userInfo;
    } else {
      return "";
    }
  };

  const setUserFormData = () => {
    let formValueObj = {};
    if (isEditView && userInfo) {
      formValueObj.name = userInfo.name;
      formValueObj.firstName = userInfo.firstName;
      formValueObj.lastName = userInfo.lastName;
      formValueObj.emailAddress = userInfo.emailAddress;
      formValueObj.firstName = userInfo.firstName;
    }
    if (userInfo && userInfo.userRoleList) {
      formValueObj.userRoleList = {
        label: UserRoles[userInfo.userRoleList[0]].label,
        value: userInfo.userRoleList[0]
      };
    } else {
      formValueObj.userRoleList = userRoleListData()[0];
    }
    if (userInfo && userInfo.groupIdList && userInfo.groupNameList) {
      formValueObj.groupIdList = userInfo.groupNameList.map((val, index) => {
        return { label: val, value: userInfo.groupIdList[index] };
      });
    }

    return formValueObj;
  };

  const getUserRole = (e, input) => {
    if (
      isEditView &&
      userInfo &&
      userInfo.userSource == UserTypes.USER_EXTERNAL.value &&
      e.label != input.value.label
    ) {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning(roleChngWarning(userInfo?.name));
    }

    input.onChange(e);
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
    if (!values.password && !isEditView) {
      errors.password = "Required";
    }
    if (!values.passwordConfirm && !isEditView) {
      errors.passwordConfirm = "Required";
    }
    if (isEditView) {
      if (
        !values.firstName &&
        userInfo.userSource !== UserTypes.USER_EXTERNAL.value &&
        userInfo.userSource !== UserTypes.USER_FEDERATED.value
      ) {
        errors.firstName = "Required";
      }
    } else {
      if (!values.firstName) {
        errors.firstName = "Required";
      } else {
        if (
          !RegexValidation.NAME_VALIDATION.regexExpressionForFirstAndLastName.test(
            values.firstName
          )
        ) {
          errors.firstName =
            RegexValidation.NAME_VALIDATION.secondaryNameValidationMessage;
        }
      }
    }

    if (
      (!isEmpty(values.lastName) || !isUndefined(values.lastName)) &&
      values.lastName.length > 0 &&
      !RegexValidation.NAME_VALIDATION.regexExpressionForFirstAndLastName.test(
        values.lastName
      )
    ) {
      errors.lastName =
        RegexValidation.NAME_VALIDATION.secondaryNameValidationMessage;
    }

    if (
      values &&
      has(values, "password") &&
      !RegexValidation.PASSWORD.regexExpression.test(values.password)
    ) {
      errors.password = RegexValidation.PASSWORD.message;
    }

    if (
      values &&
      has(values, "password") &&
      has(values, "passwordConfirm") &&
      values.password !== values.passwordConfirm
    ) {
      errors.passwordConfirm = "Password must be match with new password";
    }
    if (
      (!isEmpty(values.emailAddress) || !isUndefined(values.emailAddress)) &&
      !RegexValidation.EMAIL_VALIDATION.regexExpressionForEmail.test(
        values.emailAddress
      )
    ) {
      errors.emailAddress = RegexValidation.EMAIL_VALIDATION.message;
    }
    return errors;
  };

  return (
    <>
      <Form
        onSubmit={handleSubmit}
        keepDirtyOnReinitialize={true}
        validate={validateForm}
        initialValues={(userData(), setUserFormData())}
        render={({
          handleSubmit,
          form,
          submitting,
          values,
          invalid,
          errors,
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
                        User Name *
                      </label>
                    </Col>
                    <Col xs={4} className={"position-relative"}>
                      <input
                        {...input}
                        type="text"
                        name="name"
                        placeholder="User Name"
                        id={meta.error && meta.touched ? "isError" : "name"}
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                        disabled={isEditView ? true : false}
                        data-cy="name"
                      />
                      <InfoIcon
                        css="input-box-info-icon"
                        position="right"
                        message={RegexMessage.MESSAGE.userNameValidationMsg}
                      />

                      {meta.error && meta.touched && (
                        <span className="invalid-field">{meta.error}</span>
                      )}
                    </Col>
                  </Row>
                )}
              </Field>
              {!isEditView && (
                <Field name="password">
                  {({ input, meta }) => (
                    <Row className="form-group">
                      <Col xs={3}>
                        <label className="form-label float-end">
                          New Password *
                        </label>
                      </Col>
                      <Col xs={4} className={"position-relative"}>
                        <input
                          {...input}
                          type="password"
                          autoComplete="off"
                          name="password"
                          placeholder="Enter New Password"
                          id={
                            meta.error && meta.touched ? "isError" : "password"
                          }
                          className={
                            meta.error && meta.touched
                              ? "form-control border-danger"
                              : "form-control"
                          }
                          data-cy="password"
                        />
                        <InfoIcon
                          css="input-box-info-icon"
                          position="right"
                          message={
                            <p
                              className="pd-10 mb-0"
                              style={{ fontSize: "small" }}
                            >
                              {
                                RegexMessage.MESSAGE
                                  .passwordvalidationinfomessage
                              }
                            </p>
                          }
                        />

                        {meta.error && meta.touched && (
                          <span className="invalid-field">{meta.error}</span>
                        )}
                      </Col>
                    </Row>
                  )}
                </Field>
              )}
              {!isEditView && (
                <Field name="passwordConfirm">
                  {({ input, meta }) => (
                    <Row className="form-group">
                      <Col xs={3}>
                        <label className="form-label float-end">
                          Password Confirm *
                        </label>
                      </Col>
                      <Col xs={4} className={"position-relative"}>
                        <input
                          {...input}
                          name="passwordConfirm"
                          type="password"
                          autoComplete="off"
                          placeholder="Confirm New Password"
                          id={
                            meta.error && meta.touched
                              ? "isError"
                              : "passwordConfirm"
                          }
                          className={
                            meta.error && meta.touched
                              ? "form-control border-danger"
                              : "form-control"
                          }
                          data-cy="passwordConfirm"
                        />
                        <InfoIcon
                          css="input-box-info-icon"
                          position="right"
                          message={
                            <p
                              className="pd-10 mb-0"
                              style={{ fontSize: "small" }}
                            >
                              {
                                RegexMessage.MESSAGE
                                  .passwordvalidationinfomessage
                              }
                            </p>
                          }
                        />
                        {meta.error && meta.touched && (
                          <span className="invalid-field">{meta.error}</span>
                        )}
                      </Col>
                    </Row>
                  )}
                </Field>
              )}
              <Field name="firstName">
                {({ input, meta }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label float-end">
                        First Name *
                      </label>
                    </Col>
                    <Col xs={4} className={"position-relative"}>
                      <input
                        {...input}
                        name="firstName"
                        type="text"
                        placeholder="First Name"
                        id={
                          meta.error && meta.touched ? "isError" : "firstName"
                        }
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                        disabled={
                          isEditView && userInfo && isExternalOrFederatedUser
                            ? true
                            : false
                        }
                        data-cy="firstName"
                      />
                      <InfoIcon
                        css="input-box-info-icon"
                        position="right"
                        message={RegexMessage.MESSAGE.firstNameValidationMsg}
                      />
                      {meta.error && meta.touched && (
                        <span className="invalid-field">{meta.error}</span>
                      )}
                    </Col>
                  </Row>
                )}
              </Field>
              <Field name="lastName">
                {({ input, meta }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label float-end">Last Name</label>
                    </Col>
                    <Col xs={4} className={"position-relative"}>
                      <input
                        {...input}
                        name="lastName"
                        type="text"
                        placeholder="Last Name"
                        id={meta.error && meta.touched ? "isError" : "lastName"}
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                        disabled={
                          isEditView && userInfo && isExternalOrFederatedUser
                            ? true
                            : false
                        }
                        data-cy="lastName"
                      />
                      <InfoIcon
                        css="input-box-info-icon"
                        position="right"
                        message={RegexMessage.MESSAGE.lastNameValidationMsg}
                      />
                      {meta.error && meta.touched && (
                        <span className="invalid-field">{meta.error}</span>
                      )}
                    </Col>
                  </Row>
                )}
              </Field>
              <Field name="emailAddress">
                {({ input, meta }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label float-end">
                        Email Address
                      </label>
                    </Col>
                    <Col xs={4} className={"position-relative"}>
                      <input
                        {...input}
                        name="emailAddress"
                        type="email"
                        placeholder="Email Address"
                        id={
                          meta.error && meta.touched
                            ? "isError"
                            : "emailAddress"
                        }
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                        disabled={
                          isEditView && userInfo && isExternalOrFederatedUser
                            ? true
                            : false
                        }
                        data-cy="emailAddress"
                      />
                      <InfoIcon
                        css="input-box-info-icon"
                        position="right"
                        message={
                          RegexMessage.MESSAGE.emailvalidationinfomessage
                        }
                      />

                      {meta.error && meta.touched && (
                        <span className="invalid-field">{meta.error}</span>
                      )}
                    </Col>
                  </Row>
                )}
              </Field>
              <Row className="form-group">
                <Col xs={3}>
                  <label className="form-label float-end">Select Role *</label>
                </Col>
                <Col xs={4}>
                  <Field
                    name="userRoleList"
                    className="form-control"
                    render={({ input }) => (
                      <Select
                        {...input}
                        id="userRoleList"
                        data-cy="userRoleList"
                        options={userRoleListData()}
                        onChange={(e) => getUserRole(e, input)}
                        isDisabled={disabledUserRoleField()}
                      ></Select>
                    )}
                  ></Field>
                </Col>
              </Row>

              <Row className="form-group">
                <Col xs={3}>
                  <label className="form-label float-end">Group</label>
                </Col>
                <Col xs={4}>
                  <Field
                    name="groupIdList"
                    component={groupNameList}
                    className="form-control"
                  ></Field>
                </Col>
              </Row>
              <div className="row">
                <div className="col-sm-12 ">
                  <p className="form-header mg-0">Sync Details :</p>
                  <div className="wrap">
                    <SyncSourceDetails
                      syncDetails={
                        userInfo && userInfo.otherAttributes
                          ? JSON.parse(userInfo.otherAttributes)
                          : {}
                      }
                    ></SyncSourceDetails>
                  </div>
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
                      setPreventUnblock(true);
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
      <BlockUi isUiBlock={blockUI} />
    </>
  );
}
export default UserFormComp;
