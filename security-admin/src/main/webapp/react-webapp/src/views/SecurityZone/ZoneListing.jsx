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
import noZoneImage from "Images/defult_zone.png";
import { Link } from "react-router-dom";
import { toast } from "react-toastify";
import { fetchApi } from "Utils/fetchAPI";
import { isSystemAdmin, isKeyAdmin } from "Utils/XAUtils";
import ZoneDisplay from "./ZoneDisplay";
import { Row, Col, Collapse } from "react-bootstrap";
import { sortBy } from "lodash";
import withRouter from "Hooks/withRouter";
import { BlockUi, Loader } from "../../components/CommonComponents";
import CustomBreadcrumb from "../CustomBreadcrumb";

class ZoneListing extends Component {
  constructor(props) {
    super(props);
    this.state = {
      services: [],
      zones: [],
      selectedZone: null,
      isCollapse: true,
      loader: true,
      filterZone: [],
      isAdminRole: isSystemAdmin() || isKeyAdmin(),
      blockUI: false
    };
    this.onChangeSearch = this.onChangeSearch.bind(this);
  }

  componentDidMount() {
    this.fetchData();
  }

  componentDidUpdate(prevProps) {
    if (
      prevProps?.params?.zoneId !== undefined &&
      this.props.params.zoneId != prevProps.params.zoneId
    ) {
      this.fetchData();
    }
  }

  fetchData = async () => {
    let servicesResp;
    let zoneList = [],
      selectedZone = null,
      zoneId = this.props.params.zoneId;
    try {
      const zonesResp = await fetchApi({
        url: "zones/zones"
      });
      zoneList = zonesResp.data.securityZones || [];
    } catch (error) {
      console.error(`Error occurred while fetching Zones! ${error}`);
    }

    try {
      servicesResp = await fetchApi({
        url: "plugins/services",
        params: {
          page: 0,
          pageSize: 200,
          total_pages: 0,
          startIndex: 0
        }
      });
    } catch (error) {
      console.error(`Error occurred while fetching Services! ${error}`);
    }

    zoneList = sortBy(zoneList, ["name"]);
    if (zoneId !== undefined) {
      selectedZone =
        zoneList.find((obj) => obj.id === +zoneId) ||
        this.props.navigate("/dataNotFound");
    } else {
      if (zoneList.length > 0) {
        selectedZone = zoneList[0];
        this.props.navigate(`/zones/zone/${zoneList[0].id}`, { replace: true });
      }
    }

    this.setState({
      loader: false,
      selectedZone: selectedZone,
      zones: zoneList,
      filterZone: zoneList,
      services: servicesResp.data.services
    });
  };

  clickBtn = (zoneid) => {
    let selectedZone = this.state.zones.find((obj) => zoneid === obj.id);
    if (selectedZone) {
      this.setState({ selectedZone: selectedZone });
      this.props.navigate(`/zones/zone/${zoneid}`);
    }
  };

  onChangeSearch = (e) => {
    let filterZone = this.state.zones.filter((obj) =>
      obj.name.toLowerCase().includes(e.target.value.toLowerCase())
    );
    this.setState({ filterZone: filterZone });
  };

  deleteZone = async (zoneId) => {
    let getSelectedZone = [];

    try {
      this.setState({ blockUI: true });
      await fetchApi({
        url: `zones/zones/${zoneId}`,
        method: "delete"
      });
      let availableZone = this.state.filterZone.filter(
        (obj) => obj.id !== zoneId
      );
      let localStorageZoneDetails = localStorage.getItem("zoneDetails");
      if (
        localStorageZoneDetails !== undefined &&
        localStorageZoneDetails !== null
      ) {
        JSON.parse(localStorageZoneDetails).value == zoneId &&
          localStorage.removeItem("zoneDetails");
      }
      getSelectedZone = availableZone.length > 0 ? availableZone[0] : null;

      this.setState({
        selectedZone: getSelectedZone,
        filterZone: availableZone,
        zones: availableZone,
        blockUI: false
      });

      if (getSelectedZone && getSelectedZone !== undefined) {
        this.props.navigate(`/zones/zone/${getSelectedZone.id}`);
      } else {
        this.props.navigate(`/zones/zone/list`);
      }
      toast.success("Successfully deleted the zone");
    } catch (error) {
      this.setState({ blockUI: false });
      let errorMsg = `Error occurred while deleting Zone id - ${zoneId}!  ${error}`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
      console.error(errorMsg);
    }
  };

  expandBtn = (open) => {
    this.setState({
      isCollapse: !open
    });
  };

  render() {
    return (
      <React.Fragment>
        <div className="header-wraper">
          <h3 className="wrap-header bold">Security Zone</h3>
          <CustomBreadcrumb />
        </div>
        {this.state.loader ? (
          <Loader />
        ) : (
          <React.Fragment>
            <div className="wrap">
              <Row>
                <BlockUi isUiBlock={this.state.blockUI} />
                <Collapse in={this.state.isCollapse} data-id="panel">
                  <Col sm={3} className="border-end border-grey">
                    <Row>
                      {this.state.isAdminRole && (
                        <Col>
                          <Link
                            to={{
                              pathname: "/zones/create",
                              state: {
                                detail: this.state.filterZone[0]
                              }
                            }}
                            className="btn btn-outline-secondary btn-sm"
                            title="Create zone"
                          >
                            <i className="fa-fw fa fa-plus"></i>Create Zone
                          </Link>
                          <hr />
                        </Col>
                      )}
                    </Row>
                    <Row>
                      <Col>
                        <input
                          className="form-control"
                          type="text"
                          onChange={this.onChangeSearch}
                          placeholder="Search"
                          data-id="zoneSearch"
                          data-cy="zoneSearch"
                        ></input>
                      </Col>
                    </Row>
                    <Row>
                      <Col>
                        {this.state.filterZone.length !== 0 ? (
                          <ul className="zone-listing mt-2">
                            {this.state.filterZone.map((zone) => (
                              <li
                                className="trim-containt"
                                key={zone.id}
                                onClick={() => {
                                  this.clickBtn(zone.id);
                                }}
                                data-id={zone.name}
                                data-cy={zone.name}
                                title={zone.name}
                              >
                                <a
                                  className={
                                    this.state.selectedZone != null &&
                                    this.state.selectedZone.id === zone.id
                                      ? `selected`
                                      : ``
                                  }
                                >
                                  {zone.name}
                                </a>
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <h6 className="text-muted large mt-3 bold">
                            No Zone Found !
                          </h6>
                        )}
                      </Col>
                    </Row>
                  </Col>
                </Collapse>
                <Col>
                  {this.state.selectedZone === null ? (
                    <Row className="justify-content-md-center">
                      <Col md="auto">
                        <div className="pt-5 pr-5">
                          <img
                            alt="No Zones"
                            className="w-50 p-3 d-block mx-auto"
                            src={noZoneImage}
                          />
                          {this.state.isAdminRole && (
                            <Link
                              to={{
                                pathname: "/zones/create",
                                state: {
                                  detail: this.state.filterZone[0]
                                }
                              }}
                              className="btn-add-security2 btn-lg text-decoration-none"
                            >
                              <i className="fa-fw fa fa-plus"></i>Click here to
                              Create new Zone
                            </Link>
                          )}
                        </div>
                      </Col>
                    </Row>
                  ) : (
                    <ZoneDisplay
                      history={this.props.navigate}
                      zone={this.state.selectedZone}
                      services={this.state.services}
                      deleteZone={this.deleteZone}
                      expandBtn={this.expandBtn}
                      isCollapse={this.state.isCollapse}
                    />
                  )}
                </Col>
              </Row>
            </div>
          </React.Fragment>
        )}
      </React.Fragment>
    );
  }
}

export default withRouter(ZoneListing);
