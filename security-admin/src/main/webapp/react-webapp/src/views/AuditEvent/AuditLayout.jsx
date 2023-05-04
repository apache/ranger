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

class AuditLayout extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeKey: this.activeTab()
    };
  }

  tabChange = (tabName) => {
    this.setState({ activeKey: tabName });
    this.props.navigate(`/reports/audit/${tabName}`, { replace: true });
  };

  componentDidUpdate(nextProps, prevState) {
    let activeTabVal = this.activeTab();

    if (prevState.activeKey !== activeTabVal) {
      this.setState({ activeKey: this.activeTab() });
    }
  }

  activeTab = () => {
    let activeTabVal;
    if (this.props?.location?.pathname) {
      if (this.props.location.pathname == "/reports/audit/bigData") {
        activeTabVal = "bigData";
      } else if (this.props.location.pathname == "/reports/audit/admin") {
        activeTabVal = "admin";
      } else if (
        this.props.location.pathname == "/reports/audit/loginSession"
      ) {
        activeTabVal = "loginSession";
      } else if (this.props.location.pathname == "/reports/audit/agent") {
        activeTabVal = "agent";
      } else if (
        this.props.location.pathname == "/reports/audit/pluginStatus"
      ) {
        activeTabVal = "pluginStatus";
      } else {
        activeTabVal = "userSync";
      }
    }
    return activeTabVal;
  };

  render() {
    return (
      <>
        <Tabs
          id="AuditLayout"
          activeKey={this.state.activeKey}
          onSelect={(tabKey) => this.tabChange(tabKey)}
          className="mt-5"
        >
          <Tab eventKey="bigData" title="Access" />
          <Tab eventKey="admin" title="Admin" />
          <Tab eventKey="loginSession" title="Login Sessions" />
          <Tab eventKey="agent" title="Plugins" />
          <Tab eventKey="pluginStatus" title="Plugin Status" />
          <Tab eventKey="userSync" title="User Sync" />
        </Tabs>
        <Outlet />
      </>
    );
  }
}

export default withRouter(AuditLayout);
