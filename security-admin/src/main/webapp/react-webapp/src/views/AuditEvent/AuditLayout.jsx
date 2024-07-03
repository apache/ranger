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
import { fetchApi } from "Utils/fetchAPI";

class AuditLayout extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeKey: this.activeTab(),
      services: null,
      servicesAvailable: null
    };
  }

  tabChange = (tabName) => {
    this.setState({ activeKey: tabName });
    this.props.navigate(`/reports/audit/${tabName}`);
  };

  componentDidUpdate(nextProps, prevState) {
    let activeTabVal = this.activeTab();

    if (prevState.activeKey !== activeTabVal) {
      this.setState({ activeKey: this.activeTab() });
    }
  }

  componentDidMount() {
    this.fetchServices();
  }

  fetchServices = async () => {
    let servicesResp = [];
    try {
      const response = await fetchApi({
        url: "plugins/services"
      });
      servicesResp = response?.data?.services || [];
    } catch (error) {
      console.error(
        `Error occurred while fetching Services or CSRF headers! ${error}`
      );
    }

    this.setState({
      services: servicesResp,
      servicesAvailable:
        servicesResp.length === 0
          ? "SERVICES_NOT_AVAILABLE"
          : "SERVICES_AVAILABLE"
    });
  };

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
        <div className="header-wraper">
          <h3 className="wrap-header bold">Audits</h3>
        </div>
        <Tabs
          id="AuditLayout"
          activeKey={this.state.activeKey}
          onSelect={(tabKey) => this.tabChange(tabKey)}
        >
          <Tab eventKey="bigData" title="Access" />
          <Tab eventKey="admin" title="Admin" />
          <Tab eventKey="loginSession" title="Login Sessions" />
          <Tab eventKey="agent" title="Plugins" />
          <Tab eventKey="pluginStatus" title="Plugin Status" />
          <Tab eventKey="userSync" title="User Sync" />
        </Tabs>
        <Outlet
          context={{
            services: this.state.services,
            servicesAvailable: this.state.servicesAvailable
          }}
        />
      </>
    );
  }
}

export default withRouter(AuditLayout);
