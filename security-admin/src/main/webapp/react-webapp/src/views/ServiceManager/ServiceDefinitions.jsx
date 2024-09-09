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
import { Button, Col, Row } from "react-bootstrap";
import Select from "react-select";
import { toast } from "react-toastify";
import { filter, map, sortBy, uniq, isEmpty, cloneDeep } from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import {
  isSystemAdmin,
  isKeyAdmin,
  isAuditor,
  isKMSAuditor,
  isUser
} from "Utils/XAUtils";
import withRouter from "Hooks/withRouter";
import ServiceDefinition from "./ServiceDefinition";
import ExportPolicy from "./ExportPolicy";
import ImportPolicy from "./ImportPolicy";
import { serverError } from "../../utils/XAUtils";
import { BlockUi, Loader } from "../../components/CommonComponents";
import { getServiceDef } from "../../utils/appState";
import noServiceImage from "Images/no-service.svg";

class ServiceDefinitions extends Component {
  constructor(props) {
    super(props);
    this.serviceDefData = cloneDeep(getServiceDef());
    this.state = {
      serviceDefs: this.props.isTagView
        ? this.serviceDefData.tagServiceDefs
        : this.serviceDefData.serviceDefs,
      filterServiceDefs: this.props.isTagView
        ? this.serviceDefData.tagServiceDefs
        : this.serviceDefData.serviceDefs,
      services: [],
      filterServices: [],
      allServices: [],
      selectedZone: JSON.parse(localStorage.getItem("zoneDetails")) || "",
      zones: [],
      isTagView: this.props.isTagView,
      showExportModal: false,
      showImportModal: false,
      isAdminRole: isSystemAdmin() || isKeyAdmin(),
      isAuditorRole: isAuditor() || isKMSAuditor(),
      isUserRole: isUser(),
      isKMSRole: isKeyAdmin() || isKMSAuditor(),
      loader: true,
      blockUI: false
    };
  }

  componentDidMount() {
    this.initialFetchResp();
  }

  initialFetchResp = async () => {
    await this.fetchServices();
    if (!this.state.isKMSRole) {
      await this.fetchZones();
    }
  };

  showExportModal = () => {
    this.setState({ showExportModal: true });
  };

  hideExportModal = () => {
    this.setState({ showExportModal: false });
  };

  showImportModal = () => {
    this.setState({ showImportModal: true });
  };

  hideImportModal = () => {
    this.setState({ showImportModal: false });
  };

  showBlockUI = (blockingUI, respData) => {
    if (blockingUI == true) {
      this.setState({ blockUI: blockingUI });
    }
    if (respData?.status !== undefined && respData?.status !== null) {
      this.setState({ blockUI: false });
    }
  };

  fetchZones = async () => {
    this.props.disableTabs(true);
    this.setState({
      loader: true
    });
    let zoneList = [];
    try {
      const zonesResp = await fetchApi({
        url: "public/v2/api/zone-headers"
      });
      zoneList = zonesResp.data;
    } catch (error) {
      console.error(`Error occurred while fetching Zones! ${error}`);
    }
    this.setState({
      zones: sortBy(zoneList, ["name"]),
      loader: false
    });
    this.props.disableTabs(false);
    this.getSelectedZone(this.state.selectedZone);
  };

  fetchServices = async () => {
    this.props.disableTabs(true);
    this.setState({
      loader: true
    });
    let servicesResp = [];
    let resourceServices = [];
    let tagServices = [];

    try {
      servicesResp = await fetchApi({
        url: "plugins/services"
      });
      if (this.state.isTagView) {
        tagServices = filter(servicesResp.data.services, ["type", "tag"]);
      } else {
        if (this.state.isKMSRole) {
          resourceServices = filter(
            servicesResp.data.services,
            (service) => service.type == "kms"
          );
        } else {
          resourceServices = filter(
            servicesResp.data.services,
            (service) => service.type !== "tag" && service.type !== "kms"
          );
        }
      }
    } catch (error) {
      console.error(
        `Error occurred while fetching Services or CSRF headers! ${error}`
      );
    }

    this.setState({
      allServices: servicesResp.data.services,
      services: this.state.isTagView ? tagServices : resourceServices,
      filterServices: this.state.isTagView ? tagServices : resourceServices,
      loader: false
    });
    this.props.disableTabs(false);
  };

  getSelectedZone = async (e) => {
    this.props.disableTabs(true);
    this.setState({
      loader: true
    });
    try {
      let zonesResp = [];
      if (e && e !== undefined) {
        const response = await fetchApi({
          url: `public/v2/api/zones/${e && e.value}/service-headers`
        });
        zonesResp = response?.data || [];
        zonesResp &&
          this.props.navigate(
            `${this.props.location.pathname}?securityZone=${e.label}`
          );

        let zoneServiceNames = map(zonesResp, "name");

        let zoneServices = zoneServiceNames.map((zoneService) => {
          return this.state.services.filter((service) => {
            return service.name === zoneService;
          });
        });

        zoneServices = zoneServices.flat();

        if (this.state.isTagView) {
          zoneServices = filter(zoneServices, function (zoneService) {
            return zoneService.type === "tag";
          });
        } else {
          zoneServices = filter(zoneServices, function (zoneService) {
            return zoneService.type !== "tag";
          });
        }

        let zoneServiceDefTypes = uniq(map(zoneServices, "type"));
        let filterZoneServiceDef = [];
        filterZoneServiceDef = sortBy(
          zoneServiceDefTypes.map((obj) => {
            return this.state.serviceDefs.find((serviceDef) => {
              return serviceDef.name == obj;
            });
          }),
          "id"
        );

        let zoneDetails = {};
        zoneDetails["label"] = e.label;
        zoneDetails["value"] = e.value;
        localStorage.setItem("zoneDetails", JSON.stringify(zoneDetails));
        this.setState({
          filterServiceDefs: filterZoneServiceDef,
          filterServices: zoneServices,
          selectedZone: { label: e.label, value: e.value },
          loader: false
        });
        this.props.disableTabs(false);
      } else {
        localStorage.removeItem("zoneDetails");
        this.props.navigate(this.props.location.pathname);
        this.setState({
          filterServiceDefs: this.state.serviceDefs,
          filterServices: this.state.services,
          selectedZone: "",
          loader: false
        });
        this.props.disableTabs(false);
      }
    } catch (error) {
      console.error(`Error occurred while fetching Zone Services ! ${error}`);
    }
  };

  deleteService = async (sid) => {
    let localStorageZoneDetails = localStorage.getItem("zoneDetails");
    let zonesResp = [];
    try {
      this.setState({ blockUI: true });

      await fetchApi({
        url: `plugins/services/${sid}`,
        method: "delete"
      });

      if (
        localStorageZoneDetails !== undefined &&
        localStorageZoneDetails !== null
      ) {
        let filterZoneServiceDef = [];

        const response = await fetchApi({
          url: `public/v2/api/zones/${
            JSON.parse(localStorageZoneDetails)?.value
          }/service-headers`
        });

        zonesResp = response?.data || [];

        if (!isEmpty(zonesResp)) {
          let zoneServiceNames = filter(zonesResp, ["isTagService", false]);

          zoneServiceNames = map(zoneServiceNames, "name");

          let zoneServices = zoneServiceNames?.map((zoneService) => {
            return this.state.services?.filter((service) => {
              return service.name === zoneService;
            });
          });

          let zoneServiceDefTypes = uniq(map(zoneServices?.flat(), "type"));

          filterZoneServiceDef = sortBy(
            zoneServiceDefTypes?.map((obj) => {
              return this.state.serviceDefs?.find((serviceDef) => {
                return serviceDef.name == obj;
              });
            }),
            "id"
          );
        }
        this.setState({
          filterServiceDefs: filterZoneServiceDef
        });
      }

      this.setState({
        services: this.state.services.filter((s) => s.id !== sid),
        filterServices: this.state.filterServices.filter((s) => s.id !== sid),
        blockUI: false
      });

      toast.success("Successfully deleted the service");
      this.props.navigate(this.props.location.pathname);
    } catch (error) {
      this.setState({ blockUI: false });
      serverError(error);
      console.error(
        `Error occurred while deleting Service id - ${sid}!  ${error}`
      );
    }
  };

  Theme = (theme) => {
    return {
      ...theme,
      colors: {
        ...theme.colors,
        primary: "#0081ab"
      }
    };
  };

  formatOptionLabel = ({ label }) => (
    <div title={label} className="text-truncate">
      {label}
    </div>
  );

  render() {
    const {
      filterServiceDefs,
      filterServices,
      zones,
      selectedZone,
      showExportModal,
      showImportModal,
      isAdminRole,
      isUserRole,
      isKMSRole
    } = this.state;

    const customStyles = {
      control: (provided) => ({
        ...provided,
        minHeight: "33px",
        height: "33px"
      }),
      indicatorsContainer: (provided) => ({
        ...provided,
        height: "33px"
      }),
      valueContainer: (provided) => ({
        ...provided,
        paddingTop: "0px"
      })
    };

    return (
      <React.Fragment>
        <div>
          <div className="text-end px-3 pt-3">
            {!isKMSRole && (
              <div
                className="body bold  pd-b-10"
                style={{ display: "inline-block" }}
              >
                Security Zone :
              </div>
            )}
            {!isKMSRole && (
              <div
                style={{
                  display: "inline-block",
                  width: "220px",
                  textAlign: "left",
                  verticalAlign: "middle",
                  cursor: "not-allowed"
                }}
                title={`${isEmpty(zones) ? "Create zone first" : ""} `}
                className="mg-l-5"
              >
                <Select
                  className={isEmpty(zones) ? "not-allowed" : "pe-auto"}
                  styles={customStyles}
                  value={
                    isEmpty(this.state.selectedZone)
                      ? ""
                      : {
                          label:
                            this.state.selectedZone &&
                            this.state.selectedZone.label,
                          value:
                            this.state.selectedZone &&
                            this.state.selectedZone.value
                        }
                  }
                  formatOptionLabel={this.formatOptionLabel}
                  isDisabled={isEmpty(zones) ? true : false}
                  onChange={this.getSelectedZone}
                  isClearable
                  components={{
                    IndicatorSeparator: () => null
                  }}
                  theme={this.Theme}
                  options={zones.map((zone) => {
                    return {
                      value: zone.id,
                      label: zone.name
                    };
                  })}
                  menuPlacement="auto"
                  placeholder="Select Zone Name"
                />
              </div>
            )}
            {isAdminRole && (
              <Button
                variant="outline-secondary"
                size="sm"
                className={`${
                  isEmpty(filterServiceDefs) ? "not-allowed" : "pe-auto"
                } ms-2`}
                onClick={this.showImportModal}
                data-id="importBtn"
                data-cy="importBtn"
                disabled={isEmpty(filterServiceDefs) ? true : false}
              >
                <i className="fa fa-fw fa-rotate-180 fa-external-link-square" />
                Import
              </Button>
            )}
            {filterServiceDefs?.length > 0 && showImportModal && (
              <ImportPolicy
                serviceDef={filterServiceDefs}
                services={filterServices}
                zones={zones}
                zone={selectedZone.label}
                isParentImport={true}
                show={showImportModal}
                onHide={this.hideImportModal}
                showBlockUI={this.showBlockUI}
                allServices={this.state.allServices}
              />
            )}
            {isAdminRole && (
              <Button
                variant="outline-secondary"
                size="sm"
                className={`${
                  isEmpty(filterServiceDefs) ? "not-allowed" : "pe-auto"
                } ms-2`}
                onClick={this.showExportModal}
                data-id="exportBtn"
                data-cy="exportBtn"
                disabled={isEmpty(filterServiceDefs) ? true : false}
              >
                <i className="fa fa-fw fa-external-link-square" />
                Export
              </Button>
            )}
            {filterServiceDefs?.length > 0 && showExportModal && (
              <ExportPolicy
                serviceDef={filterServiceDefs}
                services={filterServices}
                zone={selectedZone.label}
                isParentExport={true}
                show={showExportModal}
                onHide={this.hideExportModal}
                showBlockUI={this.showBlockUI}
              />
            )}
          </div>
        </div>
        {this.state.loader ? (
          <Loader />
        ) : (
          <div className="wrap policy-manager">
            {!isEmpty(filterServiceDefs) ? (
              <Row>
                {filterServiceDefs?.map((serviceDef) => (
                  <ServiceDefinition
                    key={serviceDef && serviceDef.id}
                    serviceDefData={serviceDef}
                    servicesData={sortBy(
                      filterServices.filter(
                        (service) => service.type === serviceDef.name
                      ),
                      "name"
                    )}
                    deleteService={this.deleteService}
                    selectedZone={selectedZone}
                    zones={zones}
                    isAdminRole={isAdminRole}
                    isUserRole={isUserRole}
                    showBlockUI={this.showBlockUI}
                    allServices={this.state.allServices}
                  ></ServiceDefinition>
                ))}
              </Row>
            ) : (
              <Row className="justify-content-md-center">
                <Col md="auto">
                  <div className="pt-5 pr-5">
                    <img
                      alt="No Services"
                      className="w-50 p-3 d-block mx-auto"
                      src={noServiceImage}
                    />
                  </div>
                </Col>
              </Row>
            )}
            <BlockUi isUiBlock={this.state.blockUI} />
          </div>
        )}
      </React.Fragment>
    );
  }
}

export default withRouter(ServiceDefinitions);
