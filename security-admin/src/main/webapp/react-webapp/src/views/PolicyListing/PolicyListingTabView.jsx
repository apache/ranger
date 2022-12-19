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
import PolicyListing from "./PolicyListing";
import { fetchApi } from "Utils/fetchAPI";
import { isRenderMasking, isRenderRowFilter } from "Utils/XAUtils";
import { Loader } from "Components/CommonComponents";
import { commonBreadcrumb } from "../../utils/XAUtils";
import { pick } from "lodash";
import withRouter from "Hooks/withRouter";

class PolicyListingTabView extends Component {
  state = {
    serviceData: {},
    serviceDefData: {},
    loader: true,
    show: true
  };

  componentDidMount() {
    this.fetchServiceDetails();
  }

  fetchServiceDetails = async () => {
    let getServiceData;
    let getServiceDefData;
    try {
      getServiceData = await fetchApi({
        url: `plugins/services/${this.props.params.serviceId}`
      });
      getServiceDefData = await fetchApi({
        url: `plugins/definitions/name/${getServiceData.data.type}`
      });
    } catch (error) {
      console.error(`Error occurred while fetching service details ! ${error}`);
    }
    this.setState({
      serviceData: getServiceData.data,
      serviceDefData: getServiceDefData.data,
      loader: false
    });
  };

  tabChange = (tabName) => {
    this.props.navigate(
      `/service/${this.props.params.serviceId}/policies/${tabName}`,
      { replace: true }
    );
  };

  policyBreadcrumb = () => {
    let policyDetails = {};
    policyDetails["serviceId"] = this.props.params.serviceId;
    policyDetails["policyType"] = this.props.params.policyType;
    policyDetails["serviceName"] = this.state.serviceData.displayName;
    policyDetails["selectedZone"] = JSON.parse(
      localStorage.getItem("zoneDetails")
    );
    if (this.state.serviceDefData.name === "tag") {
      if (policyDetails.selectedZone != null) {
        return commonBreadcrumb(
          ["TagBasedServiceManager", "ManagePolicies"],
          policyDetails
        );
      } else {
        return commonBreadcrumb(
          ["TagBasedServiceManager", "ManagePolicies"],
          pick(policyDetails, ["serviceId", "policyType", "serviceName"])
        );
      }
    } else {
      if (policyDetails.selectedZone != null) {
        return commonBreadcrumb(
          ["ServiceManager", "ManagePolicies"],
          policyDetails
        );
      } else {
        return commonBreadcrumb(
          ["ServiceManager", "ManagePolicies"],
          pick(policyDetails, ["serviceId", "policyType", "serviceName"])
        );
      }
    }
  };

  render() {
    const { serviceDefData, serviceData } = this.state;
    return this.state.loader ? (
      <Loader />
    ) : (
      <React.Fragment>
        {this.policyBreadcrumb()}
        <h4 className="wrap-header bold">
          {`List of Policies : ${this.state.serviceData.displayName}`}
        </h4>

        {isRenderMasking(serviceDefData.dataMaskDef) ||
        isRenderRowFilter(serviceDefData.rowFilterDef) ? (
          <Tabs
            id="PolicyListing"
            activeKey={this.props.params.policyType}
            onSelect={(k) => this.tabChange(k)}
          >
            <Tab eventKey="0" title="Access">
              {this.props.params.policyType == "0" && (
                <PolicyListing
                  serviceDef={serviceDefData}
                  serviceData={serviceData}
                />
              )}
            </Tab>
            {isRenderMasking(serviceDefData.dataMaskDef) && (
              <Tab eventKey="1" title="Masking">
                {this.props.params.policyType == "1" && (
                  <PolicyListing
                    serviceDef={serviceDefData}
                    serviceData={serviceData}
                  />
                )}
              </Tab>
            )}
            {isRenderRowFilter(serviceDefData.rowFilterDef) && (
              <Tab eventKey="2" title="Row Level Filter">
                {this.props.params.policyType == "2" && (
                  <PolicyListing
                    serviceDef={serviceDefData}
                    serviceData={serviceData}
                  />
                )}
              </Tab>
            )}
          </Tabs>
        ) : (
          <PolicyListing
            serviceDef={serviceDefData}
            serviceData={serviceData}
          />
        )}
      </React.Fragment>
    );
  }
}

export default withRouter(PolicyListingTabView);
