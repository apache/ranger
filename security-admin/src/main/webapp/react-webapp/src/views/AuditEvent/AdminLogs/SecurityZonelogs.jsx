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

import React from "react";
import { Table, Badge } from "react-bootstrap";
import dateFormat from "dateformat";
import { ClassTypes } from "../../../utils/XAEnums";
import { isEmpty } from "lodash";

export const SecurityZonelogs = ({ data, reportdata }) => {
  const { objectName, objectClassType, createDate, owner, action } = data;

  const updateZoneDetails = reportdata.filter(
    (zone) => zone.action == "update" && zone.attributeName !== "Zone Services"
  );

  const updateZoneServices = reportdata.filter(
    (zone) => zone.action == "update" && zone.attributeName == "Zone Services"
  );

  const createZoneDetails = (newvalue) => {
    return !isEmpty(newvalue.replace(/[[\]]/g, ""))
      ? newvalue.replace(/[[\]]/g, "")
      : "--";
  };

  return (
    <div>
      {/* CREATE  */}
      {action == "create" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value && (
          <div>
            <div className="font-weight-bolder">Name: {objectName}</div>
            <div className="font-weight-bolder">
              Date: {dateFormat(createDate, "mm/dd/yyyy hh:MM:ss TT ")}
              India Standard Time
            </div>
            <div className="font-weight-bolder">Created By: {owner}</div>
            <br />
            <h5 className="bold wrap-header m-t-sm">Zone Details:</h5>

            <Table className="table table-striped table-bordered w-auto">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>

                  <th>New Value</th>
                </tr>
              </thead>
              {reportdata
                .filter((c) => {
                  return c.attributeName != "Zone Services";
                })
                .map((obj) => {
                  return (
                    <tbody>
                      <tr>
                        <td className="table-warning">{obj.attributeName}</td>
                        <td className="table-warning">
                          {!isEmpty(obj.newValue)
                            ? createZoneDetails(obj.newValue)
                            : "--"}
                        </td>
                      </tr>
                    </tbody>
                  );
                })}
            </Table>
            <br />
            <h5 className="bold wrap-header m-t-sm">Zone Service Details:</h5>
            <Table className="table table-striped table-bordered w-75">
              <thead className="thead-light">
                <tr>
                  <th>Service Name</th>

                  <th>Zone Service Resources</th>
                </tr>
              </thead>

              {reportdata
                .filter((obj) => obj.attributeName == "Zone Services")
                .map((key) => {
                  return (
                    !isEmpty(key.newValue) &&
                    Object.keys(JSON.parse(key.newValue)).map((c) => {
                      return (
                        <tbody>
                          <tr>
                            <td className="table-warning align-middle">
                              <strong> {c}</strong>
                            </td>
                            <td className="table-warning ">
                              {Object.values(
                                JSON.parse(key.newValue)[c].resources
                              ).map((resource) => (
                                <div className="zone-resource">
                                  {Object.keys(resource).map((policy) => {
                                    return (
                                      <>
                                        <strong>{`${policy} : `}</strong>
                                        {resource[policy].join(", ")}
                                        <br />
                                      </>
                                    );
                                  })}
                                </div>
                              ))}
                            </td>
                          </tr>
                        </tbody>
                      );
                    })
                  );
                })}
            </Table>
          </div>
        )}

      {/* UPDATE */}

      {action == "update" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value && (
          <div>
            <div className="row">
              <div className="col-md-6">
                <div className="font-weight-bolder">Name: {objectName}</div>
                <div className="font-weight-bolder">
                  Date: {dateFormat(createDate, "mm/dd/yyyy hh:MM:ss TT ")}
                  India Standard Time
                </div>
                <div className="font-weight-bolder">Updated By: {owner}</div>
              </div>
              <div className="col-md-6 text-right">
                <div className="bg-success legend"></div> {" Added "}
                <div className="bg-danger legend"></div> {" Deleted "}
              </div>
            </div>
            <br />
            {action == "update" && !isEmpty(updateZoneDetails) && (
              <>
                <h5 className="bold wrap-header m-t-sm">Zone Details:</h5>

                <Table className="table  table-bordered table-striped  w-auto">
                  <thead className="thead-light">
                    <tr>
                      <th>Fields</th>
                      <th>Old Value</th>
                      <th>New Value</th>
                    </tr>
                  </thead>
                  {updateZoneDetails.map((obj) => (
                    <tbody>
                      <tr key={obj.id}>
                        <td className="table-warning text-nowrap">
                          {obj.attributeName}
                        </td>

                        <td className="table-warning text-nowrap">
                          {!isEmpty(obj.previousValue.replace(/[[\]]/g, "")) ? (
                            isEmpty(obj.newValue.replace(/[[\]]/g, "")) ? (
                              <h6>
                                <Badge
                                  className="d-inline mr-1"
                                  variant="danger"
                                >
                                  {obj.previousValue.replace(/[[\]]/g, "")}
                                </Badge>
                              </h6>
                            ) : (
                              obj.previousValue.replace(/[[\]]/g, "")
                            )
                          ) : (
                            "--"
                          )}
                        </td>
                        <td className="table-warning text-nowrap">
                          {!isEmpty(obj.newValue.replace(/[[\]]/g, "")) ? (
                            isEmpty(obj.previousValue.replace(/[[\]]/g, "")) ? (
                              <h6>
                                <Badge
                                  className="d-inline mr-1"
                                  variant="success"
                                >
                                  {obj.newValue.replace(/[[\]]/g, "")}
                                </Badge>
                              </h6>
                            ) : (
                              obj.newValue.replace(/[[\]]/g, "")
                            )
                          ) : (
                            "--"
                          )}
                        </td>
                      </tr>
                    </tbody>
                  ))}
                </Table>
                <br />
              </>
            )}
          </div>
        )}

      {action == "update" && !isEmpty(updateZoneServices) && (
        <div className="row">
          <div className="col">
            <h5 className="bold wrap-header m-t-sm">
              Old Zone Service Details:
            </h5>

            <Table className="table  table-bordered table-striped w-100">
              <thead className="thead-light">
                <tr>
                  <th>Service Name</th>

                  <th> Zone Service Resources</th>
                </tr>
              </thead>
              {updateZoneServices.map((key) => {
                return (
                  !isEmpty(key.previousValue) &&
                  Object.keys(JSON.parse(key.previousValue)).map((c) => {
                    return (
                      <tbody>
                        <tr>
                          <td className="old-value-bg">
                            <strong> {c}</strong>
                          </td>
                          <td className="old-value-bg">
                            {Object.values(
                              JSON.parse(key.previousValue)[c].resources
                            ).map((resource) => (
                              <div className="zone-resource">
                                {Object.keys(resource).map((policy) => {
                                  return (
                                    <>
                                      <strong>{`${policy} : `}</strong>
                                      {resource[policy].join(", ")}
                                      <br />
                                    </>
                                  );
                                })}
                              </div>
                            ))}
                          </td>
                        </tr>
                      </tbody>
                    );
                  })
                );
              })}
            </Table>
          </div>
          <div className="col">
            <h5 className="bold wrap-header m-t-sm">
              New Zone Service Details:
            </h5>

            <Table className="table  table-bordered table-striped w-100">
              <thead className="thead-light">
                <tr>
                  <th>Service Name </th>

                  <th> Zone Service Resources</th>
                </tr>
              </thead>
              {updateZoneServices.map((key) => {
                return (
                  !isEmpty(key.newValue) &&
                  Object.keys(JSON.parse(key.newValue)).map((c) => {
                    return (
                      <tbody>
                        <tr>
                          <td className="table-warning align-middle">
                            <strong> {c}</strong>
                          </td>
                          <td className="table-warning ">
                            {Object.values(
                              JSON.parse(key.newValue)[c].resources
                            ).map((resource) => (
                              <div className="zone-resource">
                                {Object.keys(resource).map((policy) => {
                                  return (
                                    <>
                                      <strong>{`${policy} : `}</strong>
                                      {resource[policy].join(", ")}
                                      <br />
                                    </>
                                  );
                                })}
                              </div>
                            ))}
                          </td>
                        </tr>
                      </tbody>
                    );
                  })
                );
              })}
            </Table>
            <br />
          </div>
        </div>
      )}

      {/* DELETE  */}

      {action == "delete" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value && (
          <div>
            <div className="font-weight-bolder">Name: {objectName}</div>
            <div className="font-weight-bolder">
              Date: {dateFormat(createDate, "mm/dd/yyyy hh:MM:ss TT ")}
              India Standard Time
            </div>
            <div className="font-weight-bolder">Deleted By: {owner}</div>
            <br />
            <h5 className="bold wrap-header m-t-sm">Zone Details:</h5>
            <Table className="table table-striped table-bordered w-50">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>
                  <th>Old Value</th>
                </tr>
              </thead>
              {reportdata
                .filter((obj) => {
                  return obj.attributeName != "Zone Services";
                })
                .map((c) => {
                  return (
                    <tbody>
                      <tr key={c.id}>
                        <td className="table-warning">{c.attributeName}</td>

                        <td className="table-warning">
                          {!isEmpty(c.previousValue)
                            ? createZoneDetails(c.previousValue)
                            : "--"}
                        </td>
                      </tr>
                    </tbody>
                  );
                })}
            </Table>
            <br />
            <h5 className="bold wrap-header m-t-sm">Zone Service Details:</h5>
            <Table className="table table-striped table-bordered w-75">
              <thead className="thead-light">
                <tr>
                  <th>Service Name</th>

                  <th>Zone Service Resources</th>
                </tr>
              </thead>

              {reportdata
                .filter((obj) => obj.attributeName == "Zone Services")
                .map((key) => {
                  return (
                    !isEmpty(key.previousValue) &&
                    Object.keys(JSON.parse(key.previousValue)).map((c) => {
                      return (
                        <tbody>
                          <tr>
                            <td className="table-warning align-middle">
                              <strong> {c}</strong>
                            </td>
                            <td className="table-warning">
                              {Object.values(
                                JSON.parse(key.previousValue)[c].resources
                              ).map((resource) => (
                                <div className="zone-resource">
                                  {Object.keys(resource).map((policy) => {
                                    return (
                                      <>
                                        <strong>{`${policy} : `}</strong>
                                        {resource[policy].join(", ")}
                                        <br />
                                      </>
                                    );
                                  })}
                                </div>
                              ))}
                            </td>
                          </tr>
                        </tbody>
                      );
                    })
                  );
                })}
            </Table>
          </div>
        )}
    </div>
  );
};

export default SecurityZonelogs;
