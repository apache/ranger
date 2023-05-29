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

import rangerLogo from "Images/ranger_logo.png";
import React, { Component } from "react";
import { Nav, Navbar, NavDropdown } from "react-bootstrap";
import { Link, NavLink } from "react-router-dom";
import { getUserProfile, setUserProfile } from "Utils/appState";
import {
  hasAccessToTab,
  isAuditor,
  isKeyAdmin,
  isSystemAdmin,
  getBaseUrl,
  isKMSAuditor,
  checkKnoxSSO
} from "Utils/XAUtils";
import withRouter from "Hooks/withRouter";

class Header extends Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  render() {
    const userProps = getUserProfile();
    const apiUrl = getBaseUrl() + "apidocs/index.html";
    const loginId = (
      <span className="login-id">
        <i className="fa fa-user-circle fa-lg"></i>
        {userProps?.loginId}
      </span>
    );

    const accessManager = (
      <span>
        <i className="fa fa-fw fa-shield"></i> Access Manager
      </span>
    );

    const encryption = (
      <span>
        <i className="fa fa-fw fa-lock"></i> Encryption
      </span>
    );

    const settings = (
      <span>
        <i className="fa-fw fa fa-gear"></i> Settings
      </span>
    );

    return (
      <React.Fragment>
        <Navbar
          fixed="top"
          expand="lg"
          id="top-navbar"
          className="ranger-navbar"
          collapseOnSelect
        >
          <Navbar.Brand
            to="/policymanager/resource"
            className="navbar-brand logo"
            as={Link}
          >
            <img src={rangerLogo} alt="Ranger logo" />
          </Navbar.Brand>
          <Navbar.Toggle aria-controls="responsive-navbar-nav" />
          <Navbar.Collapse id="responsive-navbar-nav">
            <Nav className="mr-auto">
              <NavDropdown
                title={accessManager}
                className="header-dropdown"
                renderMenuOnMount={true}
              >
                {hasAccessToTab("Resource Based Policies") && (
                  <NavDropdown.Item to="/policymanager/resource" as={NavLink}>
                    <i className="fa fa-fw fa-file m-r-xs"></i> Resource Based
                    Policies
                  </NavDropdown.Item>
                )}
                {hasAccessToTab("Tag Based Policies") && (
                  <NavDropdown.Item to="/policymanager/tag" as={NavLink}>
                    <i className="fa fa-fw fa-tags m-r-xs"></i> Tag Based
                    Policies
                  </NavDropdown.Item>
                )}
                {hasAccessToTab("Reports") && (
                  <NavDropdown.Item
                    to="/reports/userAccess?policyType=0"
                    as={NavLink}
                  >
                    <i className="fa fa-fw fa-file-text m-r-xs"></i> Reports
                  </NavDropdown.Item>
                )}
              </NavDropdown>
              {hasAccessToTab("Audit") && (
                <Nav.Link to="/reports/audit/bigData" as={NavLink}>
                  <i className="fa fa-fw fa-file-o"></i>
                  Audit
                </Nav.Link>
              )}

              {hasAccessToTab("Security Zone") && (
                <React.Fragment>
                  {!isKeyAdmin() && (
                    <Nav.Link to="/zones/zone/list" as={NavLink}>
                      <span className="zone-icon fa-stack fa-lg">
                        <i className="fa fa-square-o fa-stack-2x"></i>
                        <i className="fa fa-bolt fa-stack-1x"></i>
                      </span>
                      {` Security Zone `}
                    </Nav.Link>
                  )}
                </React.Fragment>
              )}
              {hasAccessToTab("Key Manager") && (
                <React.Fragment>
                  {(isKeyAdmin() || isKMSAuditor()) && (
                    <NavDropdown
                      title={encryption}
                      className="header-dropdown"
                      renderMenuOnMount={true}
                    >
                      <NavDropdown.Item
                        to="/kms/keys/new/manage/service"
                        as={NavLink}
                      >
                        <i className="fa fa-fw fa-key m-r-xs"></i> Key Manager
                      </NavDropdown.Item>
                    </NavDropdown>
                  )}
                </React.Fragment>
              )}
              <>
                {(hasAccessToTab("Users/Groups") ||
                  isAuditor() ||
                  isSystemAdmin()) && (
                  <NavDropdown
                    title={settings}
                    className="header-dropdown"
                    renderMenuOnMount={true}
                  >
                    {hasAccessToTab("Users/Groups") && (
                      <NavDropdown.Item to="/users/usertab" as={NavLink}>
                        <i className="fa-fw fa fa-group m-r-xs"></i>
                        Users/Groups/Roles
                      </NavDropdown.Item>
                    )}
                    {(isAuditor() || isSystemAdmin()) && (
                      <NavDropdown.Item to="/permissions/models" as={NavLink}>
                        <i className="fa-fw fa fa-file-o m-r-xs"></i>{" "}
                        Permissions
                      </NavDropdown.Item>
                    )}
                  </NavDropdown>
                )}
              </>
            </Nav>
            <Nav>
              <NavDropdown title={loginId} id="user-dropdown" alignRight>
                <NavDropdown.Item
                  className="dropdown-item"
                  to="/userprofile"
                  as={NavLink}
                >
                  <i className="fa fa-user"></i> Profile
                </NavDropdown.Item>
                <a className="dropdown-item" href={apiUrl} target="_blank">
                  <i className="fa fa-user"></i> API Documentation
                </a>
                <NavDropdown.Item
                  onClick={() => checkKnoxSSO(this.props.navigate)}
                  data-id="logout"
                  data-cy="logout"
                >
                  <i className="fa fa-power-off"></i> Logout
                </NavDropdown.Item>
              </NavDropdown>
            </Nav>
          </Navbar.Collapse>
        </Navbar>
      </React.Fragment>
    );
  }
}

export default withRouter(Header);
