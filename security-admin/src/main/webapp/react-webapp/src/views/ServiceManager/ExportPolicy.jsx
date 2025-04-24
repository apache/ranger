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
import { Modal, Button } from "react-bootstrap";
import Select from "react-select";
import { map, toString, isEmpty } from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import { toast } from "react-toastify";
import { getBaseUrl, serverError } from "Utils/XAUtils";
import { selectInputCustomStyles } from "Components/CommonComponents";

class ExportPolicy extends Component {
  constructor(props) {
    super(props);
    this.serviceDefOptions = this.props.serviceDef.map((serviceDef) => {
      return {
        value: serviceDef.name,
        label: serviceDef.displayName
      };
    });
    this.serviceOptions = this.props.services.map((service) => {
      return {
        value: service.name,
        label: service.displayName
      };
    });
    this.state = {
      isParentExport: this.props.isParentExport,
      selectedServices: this.serviceOptions,
      serviceOptions: this.serviceOptions
    };
  }

  Theme = (theme) => {
    return {
      ...theme,
      colors: {
        ...theme.colors,
        text: "#444444",
        primary25: "#0b7fad;",
        primary: "#0b7fad;"
      }
    };
  };

  downloadFile = ({ responseData }) => {
    let serviceNameList = responseData.config.params.serviceName;
    let zoneName = responseData.config.params.zoneName;

    let downloadUrl =
      getBaseUrl() +
      "service/plugins/policies/exportJson?serviceName=" +
      serviceNameList +
      "&checkPoliciesExists=false" +
      (zoneName !== undefined ? "&zoneName=" + zoneName : "");

    const link = document.createElement("a");

    link.href = downloadUrl;

    const clickEvt = new MouseEvent("click", {
      view: window,
      bubbles: true,
      cancelable: true
    });
    document.body.appendChild(link);
    link.dispatchEvent(clickEvt);
    link.remove();
  };

  handleServiceChange = (value) => {
    this.setState({ selectedServices: value });
  };

  handleServiceDefChange = (value) => {
    let filterServices = value.map((serviceDef) => {
      return this.props.services.filter(
        (service) => service.type === serviceDef.value
      );
    });

    let filterServiceOptions = filterServices.flat().map((service) => {
      return {
        value: service.name,
        label: service.displayName
      };
    });

    this.setState({
      selectedServices: filterServiceOptions,
      serviceOptions: filterServiceOptions
    });
  };

  export = async (e) => {
    e.preventDefault();

    let exportResp;
    let serviceNameList = toString(map(this.state.selectedServices, "value"));
    let exportParams = {};

    exportParams["serviceName"] = serviceNameList;
    exportParams["checkPoliciesExists"] = true;

    if (this.props.zone) {
      exportParams["zoneName"] = this.props.zone;
    }

    try {
      this.props.onHide();
      this.props.showBlockUI(true, exportResp);
      exportResp = await fetchApi({
        url: "/plugins/policies/exportJson",
        params: { ...exportParams }
      });

      if (exportResp.status === 200) {
        this.downloadFile({
          responseData: exportResp
        });
      } else {
        toast.warning("No policies found to export");
      }
      this.props.showBlockUI(false, exportResp);
    } catch (error) {
      this.props.showBlockUI(false, error?.response);
      serverError(error);
      console.error(`Error occurred while exporting policies ${error}`);
    }
  };

  render() {
    const { isParentExport, serviceOptions, selectedServices } = this.state;
    return (
      <React.Fragment>
        <Modal
          show={this.props.show}
          onHide={this.props.onHide}
          size={!isEmpty(this.props.services) ? "lg" : "md"}
        >
          <Modal.Header closeButton>
            <Modal.Title>
              {!isEmpty(this.props.services) ? (
                "Export Policy"
              ) : (
                <h6>No service found to export policies.</h6>
              )}
            </Modal.Title>
          </Modal.Header>
          {!isEmpty(this.props.services) && (
            <Modal.Body>
              {isParentExport && (
                <div className="mb-3">
                  <h6 className="bold">Service Type *</h6>
                  <Select
                    isMulti
                    onChange={this.handleServiceDefChange}
                    components={{
                      DropdownIndicator: () => null,
                      IndicatorSeparator: () => null
                    }}
                    isClearable={false}
                    theme={this.Theme}
                    options={this.serviceDefOptions}
                    defaultValue={this.serviceDefOptions}
                    menuPlacement="auto"
                    placeholder="Select Service Type"
                    styles={selectInputCustomStyles}
                  />
                </div>
              )}

              <div className="mt-2">
                <h6 className="bold">Select Service Name *</h6>
                <Select
                  isMulti
                  onChange={this.handleServiceChange}
                  components={{
                    DropdownIndicator: () => null,
                    IndicatorSeparator: () => null
                  }}
                  isClearable={false}
                  theme={this.Theme}
                  options={serviceOptions}
                  defaultValue={this.serviceOptions}
                  value={selectedServices}
                  menuPlacement="auto"
                  placeholder="Select Service Name"
                  styles={selectInputCustomStyles}
                />
              </div>
            </Modal.Body>
          )}
          <Modal.Footer>
            {!isEmpty(this.props.services) ? (
              <>
                <Button
                  variant="secondary"
                  className="btn-sm"
                  onClick={this.props.onHide}
                >
                  Cancel
                </Button>
                <Button
                  variant="primary"
                  className="btn-sm"
                  onClick={this.export}
                  disabled={isEmpty(this.state.selectedServices)}
                >
                  Export
                </Button>
              </>
            ) : (
              <Button
                variant="primary"
                className="btn-sm"
                onClick={this.props.onHide}
              >
                OK
              </Button>
            )}
          </Modal.Footer>
        </Modal>
      </React.Fragment>
    );
  }
}

export default ExportPolicy;
