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
import { Link } from "react-router-dom";
import { Button, Col, Modal, Table } from "react-bootstrap";
import { isEmpty } from "lodash";
import { RangerPolicyType } from "Utils/XAEnums";
import ExportPolicy from "./ExportPolicy";
import ImportPolicy from "./ImportPolicy";
import { getServiceDefIcon } from "Utils/XAUtils";
import { ServiceViewDetails } from "Views/ServiceManager/ServiceViewDetails";

class ServiceDefinition extends Component {
  constructor(props) {
    super(props);
    this.state = {
      serviceDef: this.props.serviceDefData,
      showExportModal: false,
      showImportModal: false,
      showDelete: null,
      showView: null
    };
  }

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

  showDeleteModal = (id) => {
    this.setState({ showDelete: id });
  };

  hideDeleteModal = () => {
    this.setState({ showDelete: null });
  };

  showViewModal = (id) => {
    this.setState({ showView: id });
  };

  hideViewModal = () => {
    this.setState({ showView: null });
  };

  showBlockUI = (blockUI, respData) => {
    this.props.showBlockUI(blockUI, respData);
  };

  deleteService = (id) => {
    this.setState({ showDelete: null });
    this.props.deleteService(id);
  };

  render() {
    const { serviceDef, showImportModal, showExportModal } = this.state;
    return (
      <Col sm={4}>
        <div className="position-relative">
          <Table bordered hover size="sm">
            <thead>
              <tr>
                <th>
                  <div className="policy-title clearfix">
                    <span className="float-start">
                      {getServiceDefIcon(serviceDef.name)}
                      {serviceDef.displayName}
                    </span>

                    {this.props.isAdminRole && (
                      <span className="float-end">
                        {isEmpty(this.props.selectedZone) ? (
                          <Link
                            to={`/service/${serviceDef.id}/create`}
                            title="Add New Service"
                          >
                            <i className="fa-fw fa fa-plus"></i>
                          </Link>
                        ) : (
                          <a
                            className="not-allowed"
                            title="Services cannot be added while filtering Security Zones"
                          >
                            <i className="fa-fw fa fa-plus"></i>
                          </a>
                        )}

                        <a
                          className="text-decoration cursor-pointer"
                          onClick={this.showImportModal}
                          title="Import"
                          data-id="uploadBtnOnServices"
                          data-cy="uploadBtnOnServices"
                        >
                          <i className="fa-fw fa fa-rotate-180 fa-external-link-square"></i>
                        </a>
                        <a
                          className="text-decoration cursor-pointer"
                          onClick={this.showExportModal}
                          title="Export"
                          data-id="downloadBtnOnService"
                          data-cy="downloadBtnOnService"
                        >
                          <i className="fa-fw fa fa-external-link-square"></i>
                        </a>
                        {[serviceDef].length > 0 && showImportModal && (
                          <ImportPolicy
                            serviceDef={serviceDef}
                            services={this.props.servicesData}
                            zones={this.props.zones}
                            isParentImport={false}
                            selectedZone={this.props.selectedZone}
                            show={showImportModal}
                            onHide={this.hideImportModal}
                            showBlockUI={this.showBlockUI}
                            allServices={this.props.allServices}
                          />
                        )}
                        {[serviceDef].length > 0 && showExportModal && (
                          <ExportPolicy
                            serviceDef={[serviceDef]}
                            services={this.props.servicesData}
                            zone={this.props.selectedZone.label}
                            isParentExport={false}
                            show={showExportModal}
                            onHide={this.hideExportModal}
                            showBlockUI={this.showBlockUI}
                          />
                        )}
                      </span>
                    )}
                  </div>
                </th>
              </tr>
            </thead>
            <tbody className="table-service-scroll">
              {this.props.servicesData.map((s) => (
                <tr key={s.id} className="d-table w-100">
                  <td>
                    <div className="clearfix">
                      <span className="float-start">
                        {!s.isEnabled && (
                          <i
                            className="fa-fw fa fa-ban text-color-red fa-lg"
                            title="Disable"
                          ></i>
                        )}
                        <Link
                          to={{
                            pathname: `/service/${s.id}/policies/${RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value}`,
                            detail: this.props.selectedZone
                          }}
                          className="service-name text-info"
                        >
                          {s.displayName !== undefined ? s.displayName : s.name}
                        </Link>
                      </span>
                      <span className="float-end">
                        {!this.props.isUserRole && (
                          <Button
                            variant="outline-dark"
                            className="m-r-5 btn btn-mini"
                            title="View"
                            onClick={() => {
                              this.showViewModal(s.id);
                            }}
                            data-name="viewService"
                            data-id={s.id}
                            data-cy={s.id}
                          >
                            <i className="fa-fw fa fa-eye"></i>
                          </Button>
                        )}

                        <ServiceViewDetails
                          serviceDefData={serviceDef}
                          serviceData={s}
                          showViewModal={this.state.showView === s.id}
                          hideViewModal={this.hideViewModal}
                          dashboardServiceView={true}
                        />

                        {this.props.isAdminRole && (
                          <React.Fragment>
                            <Link
                              className="btn btn-mini m-r-5"
                              title="Edit"
                              to={`/service/${this.state.serviceDef.id}/edit/${s.id}`}
                              state={"services"}
                              data-id={s.id}
                              data-cy={s.id}
                            >
                              <i className="fa-fw fa fa-edit"></i>
                            </Link>
                            <Button
                              title="Delete"
                              className="btn btn-mini btn-danger"
                              onClick={() => {
                                this.showDeleteModal(s.id);
                              }}
                              data-id={s.id}
                              data-cy={s.id}
                            >
                              <i className="fa-fw fa fa-trash"></i>
                            </Button>
                            <Modal
                              show={this.state.showDelete === s.id}
                              onHide={this.hideDeleteModal}
                            >
                              <Modal.Header closeButton>
                                <span className="text-word-break">
                                  Are you sure want to delete
                                  service&nbsp;&quot;
                                  <b>{`${s?.displayName}`}</b>&quot; ?
                                </span>
                              </Modal.Header>
                              <Modal.Footer>
                                <Button
                                  variant="secondary"
                                  size="sm"
                                  title="Cancel"
                                  onClick={this.hideDeleteModal}
                                >
                                  Cancel
                                </Button>
                                <Button
                                  variant="primary"
                                  size="sm"
                                  title="Yes"
                                  onClick={() => this.deleteService(s.id)}
                                >
                                  Yes
                                </Button>
                              </Modal.Footer>
                            </Modal>
                          </React.Fragment>
                        )}
                      </span>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </Table>
        </div>
      </Col>
    );
  }
}

export default ServiceDefinition;
