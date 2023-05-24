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
import {
  Accordion,
  Card,
  Form,
  Row,
  Col,
  Table,
  Badge,
  Button,
  Modal
} from "react-bootstrap";
import { Link } from "react-router-dom";
import { find } from "lodash";
import { isSystemAdmin, isKeyAdmin } from "Utils/XAUtils";

class ZoneDisplay extends Component {
  constructor(props) {
    super(props);
    this.state = {
      expand: true,
      eventKey0: true,
      eventKey1: true,
      eventKey2: true,
      showDeleteModal: null,
      isAdminRole: isSystemAdmin() || isKeyAdmin()
    };
    this.expandbtn = this.expandbtn.bind(this);
    this.firstAccordian = this.firstAccordian.bind(this);
    this.secondAccordian = this.secondAccordian.bind(this);
    this.thirdAccordian = this.thirdAccordian.bind(this);
    this.closeZoneModal = this.closeZoneModal.bind(this);
  }

  deleteZoneModal = (zoneId) => {
    this.setState({ showDeleteModal: zoneId });
  };

  closeZoneModal = () => {
    this.setState({ showDeleteModal: null });
  };

  deleteZone = (id) => {
    this.setState({ showDeleteModal: null });
    this.props.deleteZone(id);
  };

  expandbtn = () => {
    this.setState({ expand: true });
  };

  firstAccordian = () => {
    this.setState({ eventKey0: !this.state.eventKey0 });
  };

  secondAccordian = () => {
    this.setState({ eventKey1: !this.state.eventKey1 });
  };

  thirdAccordian = () => {
    this.setState({ eventKey2: !this.state.eventKey2 });
  };
  render() {
    return (
      <div className="row">
        <div className="col-sm-12">
          <div className="d-flex justify-content-between">
            <div className="float-left d-flex align-items-center">
              <Button
                variant="outline-secondary"
                size="sm"
                className="btn-slide-toggle m-r-sm pull-left"
                aria-controls="example-collapse-text"
                aria-expanded={this.props.isCollapse}
                onClick={() => this.props.expandBtn(this.props.isCollapse)}
                data-id="sideBarBtn"
                data-cy="sideBarBtn"
              >
                <i className="fa-fw fa fa-reorder"></i>
              </Button>
              <h5 className="text-info d-inline zone-name mb-0">
                {this.props.zone.name}
              </h5>
            </div>
            {this.state.isAdminRole && (
              <div className="float-right d-flex align-items-start">
                <Link
                  className="btn btn-sm btn-outline-primary m-r-5 "
                  title="Edit"
                  to={`/zones/edit/${this.props.zone.id}`}
                  data-id="editZone"
                  data-cy="editZone"
                  style={{ whiteSpace: "nowrap" }}
                >
                  <i className="fa-fw fa fa-edit"></i> Edit
                </Link>
                <Button
                  variant="danger"
                  size="sm"
                  title="Delete"
                  onClick={() => this.deleteZoneModal(this.props.zone.id)}
                  data-id="deleteZone"
                  style={{ whiteSpace: "nowrap" }}
                >
                  <i className="fa-fw fa fa-trash"></i> Delete
                </Button>
                <Modal
                  show={this.state.showDeleteModal === this.props.zone.id}
                  onHide={this.closeZoneModal}
                  backdrop="static"
                >
                  <Modal.Header
                    closeButton
                  >{`Are you sure you want to delete ?`}</Modal.Header>
                  <Modal.Footer>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={this.closeZoneModal}
                    >
                      Cancel
                    </Button>
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={() => {
                        this.deleteZone(this.props.zone.id);
                      }}
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal>
              </div>
            )}
          </div>
          <br />
          <p className="text-break">{this.props.zone.description}</p>
          <div>
            <Accordion defaultActiveKey="0">
              <Card>
                <div className="border-bottom">
                  <Accordion.Toggle
                    as={Card.Header}
                    eventKey="0"
                    onClick={this.firstAccordian}
                    className="border-bottom-0"
                    data-id="panel"
                    data-cy="panel"
                  >
                    Zone Administrations
                    {this.state.eventKey0 ? (
                      <i className="fa fa-angle-up pull-right fa-lg font-weight-bold"></i>
                    ) : (
                      <i className="fa fa-angle-down pull-right fa-lg font-weight-bold"></i>
                    )}
                  </Accordion.Toggle>
                </div>
                <Accordion.Collapse eventKey="0">
                  <Card.Body className="p-0">
                    <Form className="border border-white shadow-none p-0">
                      <Form.Group as={Row} className="mb-3 ">
                        <Form.Label className="text-right" column sm="3">
                          Admin Users
                        </Form.Label>
                        <Col sm="9" className="pt-2">
                          {this.props?.zone?.adminUsers?.length > 0 ? (
                            this.props?.zone.adminUsers?.map((obj) => {
                              return (
                                <Badge
                                  variant="info"
                                  className="mr-1 more-less-width text-truncate"
                                  key={obj}
                                  title={obj}
                                >
                                  {obj}
                                </Badge>
                              );
                            })
                          ) : (
                            <p className="mt-1">--</p>
                          )}
                        </Col>
                      </Form.Group>
                      <Form.Group as={Row} className="mb-3">
                        <Form.Label className="text-right" column sm="3">
                          Admin Usergroups
                        </Form.Label>
                        <Col sm="9" className="pt-2">
                          {this.props?.zone?.adminUserGroups?.length > 0 ? (
                            this.props?.zone?.adminUserGroups?.map(
                              (obj, index) => {
                                return (
                                  <Badge
                                    variant="secondary"
                                    className="mr-1 more-less-width text-truncate"
                                    key={obj}
                                    title={obj}
                                  >
                                    {obj}
                                  </Badge>
                                );
                              }
                            )
                          ) : (
                            <span className="mt-1">--</span>
                          )}
                        </Col>
                      </Form.Group>
                      <Form.Group as={Row} className="mb-3">
                        <Form.Label className="text-right" column sm="3">
                          Auditor Users
                        </Form.Label>
                        <Col sm="9" className="pt-2">
                          {this.props?.zone.auditUsers?.length > 0 ? (
                            this.props?.zone?.auditUsers?.map((obj) => {
                              return (
                                <Badge
                                  variant="info"
                                  className="mr-1 more-less-width text-truncate"
                                  key={obj}
                                  title={obj}
                                >
                                  {obj}
                                </Badge>
                              );
                            })
                          ) : (
                            <span className="mt-1">--</span>
                          )}
                        </Col>
                      </Form.Group>
                      <Form.Group as={Row} className="mb-3">
                        <Form.Label className="text-right" column sm="3">
                          Auditor Usergroups
                        </Form.Label>
                        <Col sm="9" className="pt-2">
                          {this.props?.zone?.auditUserGroups?.length > 0 ? (
                            this.props?.zone?.auditUserGroups?.map(
                              (obj, index) => {
                                return (
                                  <Badge
                                    variant="secondary"
                                    className="mr-1 more-less-width text-truncate"
                                    key={obj}
                                    title={obj}
                                  >
                                    {obj}
                                  </Badge>
                                );
                              }
                            )
                          ) : (
                            <span className="mt-1">--</span>
                          )}
                        </Col>
                      </Form.Group>
                    </Form>
                  </Card.Body>
                </Accordion.Collapse>
              </Card>
            </Accordion>
          </div>
          <br />
          <div>
            <Accordion defaultActiveKey="1">
              <Card>
                <div className="border-bottom">
                  <Accordion.Toggle
                    as={Card.Header}
                    eventKey="1"
                    onClick={this.secondAccordian}
                    className="border-bottom-0"
                    data-id="panel"
                    data-cy="panel"
                  >
                    Zone Tag Services
                    {this.state.eventKey1 ? (
                      <i className="fa fa-angle-up pull-right fa-lg font-weight-bold"></i>
                    ) : (
                      <i className="fa fa-angle-down pull-right fa-lg font-weight-bold"></i>
                    )}
                  </Accordion.Toggle>
                </div>
                <Accordion.Collapse eventKey="1">
                  <Card.Body>
                    {this.props?.zone?.tagServices?.length > 0 ? (
                      this?.props?.zone?.tagServices?.map((obj, index) => (
                        <h6 key={index} className="d-inline mr-1">
                          <Badge variant="info">{obj}</Badge>
                        </h6>
                      ))
                    ) : (
                      <h6 className="text-muted large mt-2">
                        No tag based services are associated with this zone
                      </h6>
                    )}
                  </Card.Body>
                </Accordion.Collapse>
              </Card>
            </Accordion>
          </div>
          <br />
          <div>
            <Accordion defaultActiveKey="2">
              <Card>
                <div className="border-bottom">
                  <Accordion.Toggle
                    as={Card.Header}
                    eventKey="2"
                    onClick={this.thirdAccordian}
                    className="border-bottom-0"
                    data-id="panel"
                    data-cy="panel"
                  >
                    Services
                    {this.state.eventKey2 ? (
                      <i className="fa fa-angle-up pull-right fa-lg font-weight-bold"></i>
                    ) : (
                      <i className="fa fa-angle-down pull-right fa-lg font-weight-bold"></i>
                    )}
                  </Accordion.Toggle>
                </div>
                <Accordion.Collapse eventKey="2">
                  <Card.Body>
                    <Table striped bordered>
                      <thead>
                        <tr>
                          <th className="p-3 mb-2 bg-white text-dark  align-middle text-center">
                            Service Name
                          </th>
                          <th className="p-3 mb-2 bg-white text-dark align-middle text-center">
                            Service Type
                          </th>
                          <th className="p-3 mb-2 bg-white text-dark align-middle text-center">
                            Resource
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.keys(this.props?.zone?.services)?.map(
                          (key, index) => {
                            let servicetype = find(this.props.services, {
                              name: key
                            });

                            return (
                              <tr className="bg-white" key={index}>
                                <td className="align-middle" width="20%">
                                  {key}
                                </td>
                                <td className="align-middle" width="20%">
                                  {servicetype &&
                                    servicetype.type.toUpperCase()}
                                </td>
                                <td
                                  className="text-center"
                                  width="32%"
                                  height="55px"
                                >
                                  {this.props?.zone?.services[
                                    key
                                  ]?.resources?.map((resource, index) => (
                                    <div className="resource-group" key={index}>
                                      {Object.keys(resource)?.map(
                                        (resourceKey, index) => (
                                          <p key={index} className="text-break">
                                            <strong>{`${resourceKey} : `}</strong>
                                            {resource[resourceKey].join(", ")}
                                          </p>
                                        )
                                      )}
                                    </div>
                                  ))}
                                </td>
                              </tr>
                            );
                          }
                        )}
                      </tbody>
                    </Table>
                  </Card.Body>
                </Accordion.Collapse>
              </Card>
            </Accordion>
          </div>
        </div>
        <br />
      </div>
    );
  }
}
export default ZoneDisplay;
