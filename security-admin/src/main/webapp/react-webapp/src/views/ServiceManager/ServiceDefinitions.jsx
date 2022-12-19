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

import React, { Component, useCallback } from "react";
import { Button, Col, Row } from "react-bootstrap";
import Select from "react-select";
import { toast } from "react-toastify";
import { filter, map, sortBy, uniq, some, isEmpty } from "lodash";
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
import { commonBreadcrumb } from "../../utils/XAUtils";
import { ContentLoader } from "../../components/CommonComponents";

class ServiceDefinitions extends Component {
  constructor(props) {
    super(props);
    this.state = {
      serviceDefs: [],
      filterServiceDefs: [],
      services: [],
      filterServices: [],
      selectedZone: JSON.parse(localStorage.getItem("zoneDetails")) || "",
      zones: [],
      isTagView: this.props.isTagView,
      showExportModal: false,
      showImportModal: false,
      isAdminRole: isSystemAdmin() || isKeyAdmin(),
      isAuditorRole: isAuditor() || isKMSAuditor(),
      isUserRole: isUser(),
      isKMSRole: isKeyAdmin() || isKMSAuditor(),
      loader: true
    };
  }

  componentDidMount() {
    this.initialFetchResp();
  }

  initialFetchResp = async () => {
    await this.fetchServiceDefs();
    await this.fetchServices();
    await this.fetchZones();
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

  fetchServiceDefs = async () => {
    this.setState({
      loader: true
    });
    let serviceDefsResp;
    let resourceServiceDef;
    let tagServiceDef;

    try {
      serviceDefsResp = await fetchApi({
        url: "plugins/definitions"
      });

      if (this.state.isTagView) {
        tagServiceDef = filter(serviceDefsResp.data.serviceDefs, [
          "name",
          "tag"
        ]);
      } else {
        resourceServiceDef = filter(
          serviceDefsResp.data.serviceDefs,
          (serviceDef) => serviceDef.name !== "tag"
        );
      }
    } catch (error) {
      console.error(
        `Error occurred while fetching Service Definitions or CSRF headers! ${error}`
      );
    }
    this.setState({
      serviceDefs: this.state.isTagView ? tagServiceDef : resourceServiceDef,
      filterServiceDefs: this.state.isTagView
        ? tagServiceDef
        : resourceServiceDef,
      loader: false
    });
  };

  fetchZones = async () => {
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
    this.getSelectedZone(this.state.selectedZone);
  };

  fetchServices = async () => {
    this.setState({
      loader: true
    });
    let servicesResp;
    let resourceServices;
    let tagServices;

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
      services: this.state.isTagView ? tagServices : resourceServices,
      filterServices: this.state.isTagView ? tagServices : resourceServices,
      loader: false
    });
  };

  getSelectedZone = async (e) => {
    this.setState({
      loader: true
    });
    try {
      let zonesResp = [];
      if (e && e !== undefined) {
        zonesResp = await fetchApi({
          url: `public/v2/api/zones/${e && e.value}/service-headers`
        });

        zonesResp &&
          this.props.navigate(
            `${this.props.location.pathname}?securityZone=${e.label}`,
            { replace: true }
          );

        let zoneServiceNames = map(zonesResp.data, "name");

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
        let filterZoneServiceDef;
        if (!this.state.isTagView) {
          filterZoneServiceDef = zoneServiceDefTypes.map((obj) => {
            return this.state.serviceDefs.find((serviceDef) => {
              return serviceDef.name == obj;
            });
          });
        } else {
          filterZoneServiceDef = this.state.serviceDefs;
        }
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
      } else {
        localStorage.removeItem("zoneDetails");
        this.props.navigate(this.props.location.pathname);
        this.setState({
          filterServiceDefs: this.state.serviceDefs,
          filterServices: this.state.services,
          selectedZone: "",
          loader: false
        });
      }
    } catch (error) {
      console.error(`Error occurred while fetching Zone Services ! ${error}`);
    }
  };

  deleteService = async (sid) => {
    console.log("Service Id to delete is ", sid);
    try {
      await fetchApi({
        url: `plugins/services/${sid}`,
        method: "delete"
      });
      this.setState({
        services: this.state.filterServices.filter((s) => s.id !== sid),
        filterServices: this.state.filterServices.filter((s) => s.id !== sid)
      });
      toast.success("Successfully deleted the service");
    } catch (error) {
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
        text: "#444444",
        primary25: "#0b7fad;",
        primary: "#0b7fad;"
      }
    };
  };
  serviceBreadcrumb = () => {
    let serviceDetails = {};
    serviceDetails["selectedZone"] = JSON.parse(
      localStorage.getItem("zoneDetails")
    );
    if (some(this.state.serviceDefs, { name: "tag" })) {
      if (serviceDetails.selectedZone) {
        return commonBreadcrumb(["TagBasedServiceManager"], serviceDetails);
      } else {
        return commonBreadcrumb(["TagBasedServiceManager"]);
      }
    } else {
      if (serviceDetails.selectedZone) {
        return commonBreadcrumb(["ServiceManager"], serviceDetails);
      } else {
        return commonBreadcrumb(["ServiceManager"]);
      }
    }
  };

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
        minHeight: "30px",
        height: "25px"
      }),
      indicatorsContainer: (provided) => ({
        ...provided,
        height: "30px"
      }),
      valueContainer: (provided) => ({
        ...provided,
        paddingTop: "0px"
      })
    };
    return (
      <React.Fragment>
        {this.serviceBreadcrumb()}
        <Row>
          <Col sm={5}>
            <h5 className="wrap-header bold  pd-b-10">Service Manager</h5>
          </Col>
          <Col sm={7} className="text-right">
            {!isKMSRole && (
              <div
                className="body bold  pd-b-10"
                style={{ display: "inline-block" }}
              >
                Security Zone:
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
                className="mg-l-5"
              >
                <Select
                  className={isEmpty(zones) ? "not-allowed" : ""}
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
                className="ml-2 btn-mini "
                onClick={this.showImportModal}
                data-id="importBtn"
                data-cy="importBtn"
              >
                <i className="fa fa-fw fa-rotate-180 fa-external-link-square" />
                Import
              </Button>
            )}
            {filterServiceDefs.length > 0 && showImportModal && (
              <ImportPolicy
                serviceDef={filterServiceDefs}
                services={filterServices}
                zones={zones}
                zone={selectedZone.label}
                isParentImport={true}
                show={showImportModal}
                onHide={this.hideImportModal}
              />
            )}
            {isAdminRole && (
              <Button
                variant="outline-secondary"
                size="sm"
                className="ml-2 btn-mini "
                onClick={this.showExportModal}
                data-id="exportBtn"
                data-cy="exportBtn"
              >
                <i className="fa fa-fw fa-external-link-square" />
                Export
              </Button>
            )}
            {filterServiceDefs.length > 0 && showExportModal && (
              <ExportPolicy
                serviceDef={filterServiceDefs}
                services={filterServices}
                zone={selectedZone.label}
                isParentExport={true}
                show={showExportModal}
                onHide={this.hideExportModal}
              />
            )}
          </Col>
        </Row>
        <div className="wrap policy-manager mt-2">
          {this.state.loader ? (
            <ContentLoader size="50px" />
          ) : (
            <Row>
              {filterServiceDefs.map((serviceDef) => (
                <ServiceDefinition
                  key={serviceDef && serviceDef.id}
                  serviceDefData={serviceDef}
                  servicesData={filterServices.filter(
                    (service) => service.type === serviceDef.name
                  )}
                  deleteService={this.deleteService}
                  selectedZone={selectedZone}
                  zones={zones}
                  isAdminRole={isAdminRole}
                  isUserRole={isUserRole}
                ></ServiceDefinition>
              ))}
            </Row>
          )}
        </div>
      </React.Fragment>
    );
  }
}

export default withRouter(ServiceDefinitions);
