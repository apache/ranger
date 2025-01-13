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
import { Tab, Button, Nav, Row, Col } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import UserFormComp from "Views/UserGroupRoleListing/users_details/UserFormComp";
import { Loader, scrollToError } from "Components/CommonComponents";
import { fetchApi } from "Utils/fetchAPI";
import { UserTypes, RegexValidation } from "Utils/XAEnums";
import { toast } from "react-toastify";
import { useParams, useNavigate } from "react-router-dom";
import { has } from "lodash";
import { commonBreadcrumb, InfoIcon } from "../../../utils/XAUtils";

const initialState = {
  userInfo: {},
  loader: true
};
const userFormReducer = (state, action) => {
  switch (action.type) {
    case "SET_LOADER":
      return {
        ...state,
        loader: action.loader
      };
    case "SET_USER_DATA":
      return {
        ...state,
        userInfo: action.userInfo,
        loader: action.loader
      };
    default:
      throw new Error();
  }
};

function AddUserView() {
  const params = useParams();
  const [userDetails, dispatch] = useReducer(userFormReducer, initialState);
  const { userInfo, loader } = userDetails;
  const navigate = useNavigate();

  useEffect(() => {
    if (params?.userID) {
      fetchUserData(params.userID);
    } else {
      dispatch({
        type: "SET_LOADER",
        loader: false
      });
    }
  }, []);

  const fetchUserData = async (userID) => {
    let userRespData;
    try {
      userRespData = await fetchApi({
        url: "xusers/secure/users/" + userID
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Users or CSRF headers! ${error}`
      );
    }
    dispatch({
      type: "SET_USER_DATA",
      userInfo: userRespData?.data,
      loader: false
    });
  };

  const validateForm = (values) => {
    const errors = {};
    if (!values.newPassword) {
      errors.newPassword = "Required";
    } else {
      if (!RegexValidation.PASSWORD.regexExpression.test(values.newPassword)) {
        errors.newPassword = RegexValidation.PASSWORD.message;
      }
    }
    if (!values.reEnterPassword) {
      errors.reEnterPassword = "Required";
    } else if (
      has(values, "newPassword") &&
      values.newPassword !== values.reEnterPassword
    ) {
      errors.reEnterPassword = "Password must be match with new password";
    }
    return errors;
  };

  const handleSubmit = async (values) => {
    let userDetails = {};
    userDetails.password = values.newPassword;
    userDetails = {
      ...userInfo,
      ...userDetails
    };
    try {
      await fetchApi({
        url: `xusers/secure/users/${params.userID}`,
        method: "PUT",
        data: userDetails
      });
      toast.success("User password change successfully!!");
      navigate("/users/usertab");
    } catch (error) {
      toast.error("Error occured while updating user details!");
      console.error(`Error occurred while updating user details! ${error}`);
    }
  };

  const closeForm = () => {
    navigate("/users/usertab");
  };

  return loader ? (
    <Loader />
  ) : userInfo.userSource == UserTypes.USER_EXTERNAL.value ||
    userInfo.userSource == UserTypes.USER_FEDERATED.value ? (
    <>
      <div className="header-wraper">
        <h3 className="wrap-header bold">User Detail</h3>
        {commonBreadcrumb(["Users", "UserEdit"], params.userID)}
      </div>
      <UserFormComp
        isEditView={true}
        userID={params.userID}
        userInfo={userInfo}
      />
    </>
  ) : (
    <>
      <div className="header-wraper">
        <h3 className="wrap-header bold">User Detail</h3>
        {commonBreadcrumb(["Users", "UserEdit"], params.userID)}
      </div>
      <div className="wrap">
        <Tab.Container transition={false} defaultActiveKey="edit-basic-info">
          <Nav variant="tabs">
            <Nav.Item>
              <Nav.Link eventKey="edit-basic-info">
                <i className="fa-fw fa fa-edit bigger-125"></i>Basic Info
              </Nav.Link>
            </Nav.Item>
            <Nav.Item>
              <Nav.Link eventKey="edit-password">
                <i className="fa-fw fa fa-key bigger-125"></i>Change Password
              </Nav.Link>
            </Nav.Item>
          </Nav>
          <div className="user-details">
            <Tab.Content>
              <Tab.Pane eventKey="edit-basic-info">
                <UserFormComp
                  isEditView={true}
                  userID={params.userID}
                  userInfo={userInfo}
                />
              </Tab.Pane>
            </Tab.Content>
            <Tab.Content>
              <Tab.Pane eventKey="edit-password">
                <>
                  <Form
                    onSubmit={handleSubmit}
                    validate={validateForm}
                    render={({
                      handleSubmit,
                      form,
                      submitting,
                      invalid,
                      errors
                    }) => (
                      <div className="wrap">
                        <form
                          onSubmit={(event) => {
                            if (invalid) {
                              let selector =
                                document.getElementById("isError") ||
                                document.querySelector(
                                  `input[name=${Object.keys(errors)[0]}]`
                                );
                              scrollToError(selector);
                            }
                            handleSubmit(event);
                          }}
                        >
                          <Field name="newPassword">
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
                                    name="newPassword"
                                    placeholder="New Password"
                                    id={
                                      meta.error && meta.touched
                                        ? "isError"
                                        : "newPassword"
                                    }
                                    className={
                                      meta.error && meta.touched
                                        ? "form-control border-danger"
                                        : "form-control"
                                    }
                                    data-cy="newPassword"
                                  />
                                  <InfoIcon
                                    css="input-box-info-icon"
                                    position="right"
                                    message={
                                      <p
                                        className="pd-10 mb-0"
                                        style={{ fontSize: "small" }}
                                      >
                                        {RegexValidation.PASSWORD.message}
                                      </p>
                                    }
                                  />
                                  {meta.error && meta.touched && (
                                    <span className="invalid-field">
                                      {meta.error}
                                    </span>
                                  )}
                                </Col>
                              </Row>
                            )}
                          </Field>
                          <Field name="reEnterPassword">
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
                                    name="reEnterPassword"
                                    autoComplete="off"
                                    type="password"
                                    placeholder="Re-enter New Password"
                                    id={
                                      meta.error && meta.touched
                                        ? "isError"
                                        : "reEnterPassword"
                                    }
                                    className={
                                      meta.error && meta.touched
                                        ? "form-control border-danger"
                                        : "form-control"
                                    }
                                    data-cy="reEnterPassword"
                                  />
                                  <InfoIcon
                                    css="input-box-info-icon"
                                    position="right"
                                    message={
                                      <p
                                        className="pd-10 mb-0"
                                        style={{ fontSize: "small" }}
                                      >
                                        {RegexValidation.PASSWORD.message}
                                      </p>
                                    }
                                  />
                                  {meta.error && meta.touched && (
                                    <span className="invalid-field">
                                      {meta.error}
                                    </span>
                                  )}
                                </Col>
                              </Row>
                            )}
                          </Field>
                          <div className="row form-actions">
                            <div className="col-md-9 offset-md-3">
                              <Button
                                variant="primary"
                                type="submit"
                                disabled={submitting}
                                size="sm"
                                data-id="save"
                                data-cy="save"
                              >
                                Save
                              </Button>
                              <Button
                                variant="secondary"
                                type="button"
                                onClick={() => {
                                  form.reset;
                                  closeForm();
                                }}
                                size="sm"
                                data-id="save"
                                data-cy="save"
                              >
                                Cancel
                              </Button>
                            </div>
                          </div>
                        </form>
                      </div>
                    )}
                  />
                </>
              </Tab.Pane>
            </Tab.Content>
          </div>
        </Tab.Container>
      </div>
    </>
  );
}
export default AddUserView;
