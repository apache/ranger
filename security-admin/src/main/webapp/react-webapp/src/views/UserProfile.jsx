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

import React, { Component } from "react";
import { Button, Nav, Tab, Row, Col } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import { toast } from "react-toastify";
import { getUserProfile, setUserProfile } from "Utils/appState";
import { InfoIcon } from "../utils/XAUtils";
import { BlockUi, scrollToError } from "../components/CommonComponents";
import withRouter from "Hooks/withRouter";
import { UserTypes, RegexValidation } from "Utils/XAEnums";
import { has, isEmpty, isUndefined } from "lodash";
import { RegexMessage } from "../utils/XAMessages";
import { fetchApi } from "Utils/fetchAPI";
import CustomBreadcrumb from "./CustomBreadcrumb";
class UserProfile extends Component {
  constructor(props) {
    super(props);
    this.state = {
      blockUI: false
    };
  }
  updateUserInfo = async (values) => {
    const userProps = getUserProfile();

    userProps.firstName = values.firstName;
    userProps.emailAddress = values.emailAddress;
    userProps.lastName = values.lastName;

    try {
      this.setState({ blockUI: true });
      const profResp = await fetchApi({
        url: "users",
        method: "put",
        data: userProps
      });
      this.setState({ blockUI: false });
      setUserProfile(profResp.data);
      this.props.navigate("/");
      toast.success("Successfully updated user profile");
    } catch (error) {
      this.setState({ blockUI: false });
      if (
        error?.response !== undefined &&
        has(error?.response, "data.msgDesc")
      ) {
        toast.error(
          `Error occurred while updating user profile! ${error?.response?.data?.msgDesc}`
        );
      }
      console.error(`Error occurred while updating user profile! ${error}`);
    }
  };

  updatePassword = async (values) => {
    const userProps = getUserProfile();

    let jsonData = {};
    jsonData["emailAddress"] = "";
    jsonData["loginId"] = userProps.loginId;
    jsonData["oldPassword"] = values.oldPassword;
    jsonData["updPassword"] = values.newPassword;

    try {
      this.setState({ blockUI: true });
      const passwdResp = await fetchApi({
        url: "users/" + userProps.id + "/passwordchange",
        method: "post",
        data: jsonData
      });
      this.setState({ blockUI: false });
      toast.success("Successfully updated user password");
      this.props.navigate("/");
    } catch (error) {
      this.setState({ blockUI: false });
      if (
        (has(error?.response, "data.msgDesc") &&
          error?.response?.data?.msgDesc == "serverMsg.userMgrOldPassword") ||
        "serverMsg.userMgrNewPassword"
      ) {
        toast.error("You can not use old password.");
      }
      console.error(`Error occurred while updating user password! ${error}`);
    }
  };

  validatePasswordForm = (values) => {
    const errors = {};
    if (!values.oldPassword) {
      errors.oldPassword = "Required";
    }
    if (!values.newPassword) {
      errors.newPassword = "Required";
    }
    if (
      values &&
      has(values, "newPassword") &&
      !RegexValidation.PASSWORD.regexExpression.test(values.newPassword)
    ) {
      errors.newPassword = RegexValidation.PASSWORD.message;
    }
    if (!values.reEnterPassword) {
      errors.reEnterPassword = "Required";
    } else if (values.newPassword !== values.reEnterPassword) {
      errors.reEnterPassword = "Must match";
    }
    return errors;
  };

  validateUserForm = (values) => {
    const errors = {};
    if (!values.firstName) {
      errors.firstName = "Required";
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

  render() {
    const userProps = getUserProfile();
    return (
      <div>
        <div className="header-wraper">
          <h3 className="wrap-header bold">User Profile</h3>
          <CustomBreadcrumb />
        </div>
        <div className="wrap">
          <BlockUi isUiBlock={this.state.blockUI} />
          <Tab.Container transition={false} defaultActiveKey="edit-basic-info">
            {userProps.userSource == UserTypes.USER_INTERNAL.value && (
              <Nav variant="tabs">
                <Nav.Item>
                  <Nav.Link eventKey="edit-basic-info">
                    <i className="fa-fw fa fa-edit bigger-125"></i>Basic Info
                  </Nav.Link>
                </Nav.Item>
                <Nav.Item>
                  <Nav.Link eventKey="edit-password">
                    <i className="fa-fw fa fa-key bigger-125"></i>Change
                    Password
                  </Nav.Link>
                </Nav.Item>
              </Nav>
            )}
            <div className="user-profile">
              <Tab.Content>
                <Tab.Pane eventKey="edit-basic-info">
                  <Form
                    onSubmit={this.updateUserInfo}
                    validate={this.validateUserForm}
                    initialValues={{
                      firstName: userProps.firstName,
                      lastName: userProps.lastName,
                      emailAddress: userProps.emailAddress,
                      userRoleList: userProps.userRoleList[0]
                    }}
                    render={({
                      handleSubmit,
                      form,
                      submitting,
                      values,
                      invalid,
                      errors
                    }) => (
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
                        <Field name="firstName">
                          {({ input, meta }) => (
                            <Row className="form-group">
                              <Col xs={3}>
                                <label className="form-label pull-right">
                                  First Name *
                                </label>
                              </Col>
                              <Col xs={4}>
                                <input
                                  {...input}
                                  type="text"
                                  name="firstName"
                                  placeholder="First Name"
                                  id={
                                    meta.error && meta.touched
                                      ? "isError"
                                      : "firstName"
                                  }
                                  className={
                                    meta.error && meta.touched
                                      ? "form-control border-danger"
                                      : "form-control"
                                  }
                                  disabled={
                                    userProps.userSource ==
                                    UserTypes.USER_INTERNAL.value
                                      ? false
                                      : true
                                  }
                                  data-cy="firstName"
                                />
                                <InfoIcon
                                  css="info-user-role-grp-icon"
                                  position="right"
                                  message={
                                    RegexMessage.MESSAGE.firstNameValidationMsg
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
                        <Field name="lastName">
                          {({ input, meta }) => (
                            <Row className="form-group">
                              <Col xs={3}>
                                <label className="form-label pull-right">
                                  Last Name
                                </label>
                              </Col>
                              <Col xs={4}>
                                <input
                                  {...input}
                                  type="text"
                                  name="lastName"
                                  placeholder="Last Name"
                                  id={
                                    meta.error && meta.touched
                                      ? "isError"
                                      : "lastName"
                                  }
                                  className="form-control"
                                  disabled={
                                    userProps.userSource ==
                                    UserTypes.USER_INTERNAL.value
                                      ? false
                                      : true
                                  }
                                  data-cy="lastName"
                                />
                                <InfoIcon
                                  css="info-user-role-grp-icon"
                                  position="right"
                                  message={
                                    RegexMessage.MESSAGE.lastNameValidationMsg
                                  }
                                />
                              </Col>
                            </Row>
                          )}
                        </Field>
                        <Field name="emailAddress">
                          {({ input, meta }) => (
                            <Row className="form-group">
                              <Col xs={3}>
                                <label className="form-label pull-right">
                                  Email Address
                                </label>
                              </Col>
                              <Col xs={4}>
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
                                    userProps.userSource ==
                                    UserTypes.USER_INTERNAL.value
                                      ? false
                                      : true
                                  }
                                  data-cy="emailAddress"
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

                        <Row className="form-group">
                          <Col xs={3}>
                            {" "}
                            <label className="form-label pull-right">
                              Select Role *
                            </label>
                          </Col>
                          <Col xs={4}>
                            <Field
                              name="userRoleList"
                              component="select"
                              className="form-control"
                              disabled
                            >
                              <option value="ROLE_SYS_ADMIN">Admin</option>
                              <option value="ROLE_KEY_ADMIN">KeyAdmin</option>
                              <option value="ROLE_USER">User</option>
                              <option value="ROLE_ADMIN_AUDITOR">
                                Auditor
                              </option>
                              <option value="ROLE_KEY_ADMIN_AUDITOR">
                                KeyAuditor
                              </option>
                            </Field>
                          </Col>
                        </Row>

                        <div className="row form-actions">
                          <div className="col-md-9 offset-md-3">
                            <Button
                              variant="primary"
                              type="submit"
                              size="sm"
                              disabled={
                                userProps.userSource ==
                                UserTypes.USER_INTERNAL.value
                                  ? false
                                  : true
                              }
                              data-id="save"
                              data-cy="save"
                            >
                              Save
                            </Button>

                            <Button
                              variant="secondary"
                              type="button"
                              size="sm"
                              onClick={() => this.props.navigate("/")}
                              data-id="cancel"
                              data-cy="cancel"
                            >
                              Cancel
                            </Button>
                          </div>
                        </div>
                      </form>
                    )}
                  />
                </Tab.Pane>
              </Tab.Content>
              <Tab.Content>
                <Tab.Pane eventKey="edit-password">
                  <Form
                    onSubmit={this.updatePassword}
                    validate={this.validatePasswordForm}
                    render={({
                      handleSubmit,
                      form,
                      submitting,
                      values,
                      invalid,
                      errors
                    }) => (
                      <form
                        onSubmit={(event) => {
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
                          handleSubmit(event);
                        }}
                      >
                        <Field name="oldPassword">
                          {({ input, meta }) => (
                            <Row className="form-group">
                              <Col xs={3}>
                                <label className="form-label pull-right">
                                  Old Password *
                                </label>
                              </Col>
                              <Col xs={4}>
                                <input
                                  {...input}
                                  type="password"
                                  autoComplete="off"
                                  name="oldPassword"
                                  placeholder="Old Password"
                                  id={
                                    meta.error && meta.touched
                                      ? "isError"
                                      : "oldPassword"
                                  }
                                  className={
                                    meta.error && meta.touched
                                      ? "form-control border-danger"
                                      : "form-control"
                                  }
                                  data-cy="oldPassword"
                                />
                                <InfoIcon
                                  css="info-user-role-grp-icon"
                                  position="right"
                                  message={
                                    <p
                                      className="pd-10"
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
                        <Field name="newPassword">
                          {({ input, meta }) => (
                            <Row className="form-group">
                              <Col xs={3}>
                                <label className="form-label pull-right">
                                  New Password *
                                </label>
                              </Col>
                              <Col xs={4}>
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
                                  css="info-user-role-grp-icon"
                                  position="right"
                                  message={
                                    <p
                                      className="pd-10"
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
                                <label className="form-label pull-right">
                                  Re-enter New Password *
                                </label>
                              </Col>
                              <Col xs={4}>
                                <input
                                  {...input}
                                  type="password"
                                  autoComplete="off"
                                  name="reEnterPassword"
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
                                  css="info-user-role-grp-icon"
                                  position="right"
                                  message={
                                    <p
                                      className="pd-10"
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
                              onClick={() => this.props.navigate("/")}
                              data-id="cancel"
                              data-cy="cancel"
                            >
                              Cancel
                            </Button>
                          </div>
                        </div>
                      </form>
                    )}
                  />
                </Tab.Pane>
              </Tab.Content>
            </div>
          </Tab.Container>
        </div>
      </div>
    );
  }
}

export default withRouter(UserProfile);
