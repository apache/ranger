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
import ServiceDefinitions from "./ServiceManager/ServiceDefinitions";
import { Tab, Tabs } from "react-bootstrap";
import withRouter from "Hooks/withRouter";
import { hasAccessToTab } from "../utils/XAUtils";
import CustomBreadcrumb from "./CustomBreadcrumb";
import { isEmpty } from "lodash";

class Home extends Component {
  constructor(props) {
    super(props);
    this.state = {
      isTagView: this.props.isTagView,
      activeKey: this.activeTab(),
      loader: false
    };
  }
  tabChange = (tabName) => {
    this.props.navigate(`/policymanager/${tabName}`, { replace: true });
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
      if (this.props.location.pathname == "/policymanager/resource") {
        activeTabVal = "resource";
      } else if (this.props.location.pathname == "/policymanager/tag") {
        activeTabVal = "tag";
      }
    }
    return activeTabVal;
  };

  disableTabs = (loader) => {
    loader == true &&
      document
        .getElementById("resourceSelectedZone")
        ?.classList?.add("disabledEvents");
    loader == true &&
      document
        .getElementById("tagSelectedZone")
        ?.classList?.add("disabledEvents");
    loader == true &&
      document
        .getElementsByClassName("sidebar-header")?.[0]
        ?.classList?.add("disabledEvents");
    loader == true &&
      document.getElementById("rangerIcon")?.classList?.add("disabledCursor");

    this.setState({ loader: loader });
    loader == false &&
      document
        .getElementsByClassName("sidebar-header")?.[0]
        ?.classList.remove("disabledEvents");
    loader == false &&
      document
        .getElementById("rangerIcon")
        ?.classList?.remove("disabledCursor");

    loader == false &&
      document
        .getElementById("resourceSelectedZone")
        ?.classList?.remove("disabledEvents");
    loader == false &&
      document
        .getElementById("tagSelectedZone")
        ?.classList?.remove("disabledEvents");
  };
  render() {
    return (
      <>
        <div className="header-wraper">
          <h3 className="wrap-header bold">Service Manager</h3>
          <CustomBreadcrumb />
        </div>
        <Tabs
          id="ServiceManager"
          activeKey={this.state.activeKey}
          onSelect={(tabKey) => this.tabChange(tabKey)}
          className={`${this.state.loader ? "not-allowed" : ""}`}
        >
          {hasAccessToTab("Resource Based Policies") && (
            <Tab
              eventKey="resource"
              title="Resource"
              disabled={this.state.loader ? true : false}
            >
              {!this.state.isTagView && (
                <ServiceDefinitions
                  isTagView={this.state.isTagView}
                  disableTabs={this.disableTabs}
                  key={
                    !isEmpty(localStorage.getItem("zoneDetails")) &&
                    JSON.parse(localStorage.getItem("zoneDetails"))?.value
                  }
                ></ServiceDefinitions>
              )}
            </Tab>
          )}
          {hasAccessToTab("Tag Based Policies") && (
            <Tab
              eventKey="tag"
              title="Tag"
              disabled={this.state.loader ? true : false}
            >
              {this.state.isTagView && (
                <ServiceDefinitions
                  isTagView={this.state.isTagView}
                  disableTabs={this.disableTabs}
                  key={
                    !isEmpty(localStorage.getItem("zoneDetails")) &&
                    JSON.parse(localStorage.getItem("zoneDetails"))?.value
                  }
                ></ServiceDefinitions>
              )}
            </Tab>
          )}
        </Tabs>
      </>
    );
  }
}

export default withRouter(Home);
