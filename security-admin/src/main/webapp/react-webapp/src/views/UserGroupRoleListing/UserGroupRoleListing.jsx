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
import { Tab, Tabs } from "react-bootstrap";
import withRouter from "Hooks/withRouter";
import { Outlet } from "react-router-dom";
import CustomBreadcrumb from "../CustomBreadcrumb";

class UserGroupRoleListing extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeKey: this.activeTab()
    };
  }
  tabChange = (tabName) => {
    this.setState({ activeKey: tabName });
    this.props.navigate(`/users/${tabName}`);
  };
  componentDidUpdate(nextProps, prevState) {
    let activeTabVal = this.activeTab();

    if (prevState.activeKey !== activeTabVal) {
      this.setState({ activeKey: this.activeTab() });
    }
  }
  activeTab = () => {
    let activeTabVal;
    if (this.props.location.pathname) {
      if (this.props.location.pathname == "/users/usertab") {
        activeTabVal = "usertab";
      } else if (this.props.location.pathname == "/users/grouptab") {
        activeTabVal = "grouptab";
      } else {
        activeTabVal = "roletab";
      }
    }
    return activeTabVal;
  };
  render() {
    return (
      <React.Fragment>
        <div className="header-wraper">
          <h3 className="wrap-header bold">Users/Groups/Roles</h3>
          <CustomBreadcrumb />
        </div>
        <div className="usrGrpRoleListing">
          <Tabs
            id="userGroupRoleListing"
            activeKey={this.state.activeKey}
            onSelect={(tabKey) => this.tabChange(tabKey)}
          >
            <Tab eventKey="usertab" title="Users" />
            <Tab eventKey="grouptab" title="Groups" />
            <Tab eventKey="roletab" title="Roles" />
          </Tabs>
        </div>
        <Outlet />
      </React.Fragment>
    );
  }
}

export default withRouter(UserGroupRoleListing);
