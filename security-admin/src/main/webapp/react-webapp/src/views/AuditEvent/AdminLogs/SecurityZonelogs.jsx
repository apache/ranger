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
import { currentTimeZone } from "../../../utils/XAUtils";

export const SecurityZonelogs = ({ data, reportdata }) => {
  const { objectName, objectClassType, createDate, owner, action } = data;
  const createZoneDetails = (newvalue) => {
    return !isEmpty(newvalue.replace(/[[\]]/g, ""))
      ? newvalue.replace(/[[\]]/g, "")
      : "--";
  };

  const zoneServiceData = (reportdata, action, attributeName) => {
    return reportdata.filter(
      (zone) => zone.action == action && zone.attributeName == attributeName
    );
  };
  const createZoneServices = zoneServiceData(
    reportdata,
    "create",
    "Zone Services"
  );

  const deleteZoneServices = zoneServiceData(
    reportdata,
    "delete",
    "Zone Services"
  );
  const parseAndFilterData = (zones, zoneValues) => {
    return zones.map(
      (zone) => !isEmpty(zone[zoneValues]) && JSON.parse(zone[zoneValues])
    );
  };

  const newZoneServiceData = parseAndFilterData(createZoneServices, "newValue");
  const oldZoneServiceData = parseAndFilterData(
    deleteZoneServices,
    "previousValue"
  );
  const updateZoneDetails = reportdata.filter(
    (zone) => zone.action == "update" && zone.attributeName != "Zone Services"
  );

  const updateZoneServices = zoneServiceData(
    reportdata,
    "update",
    "Zone Services"
  );

  const updateZoneOldServices = parseAndFilterData(
    updateZoneServices,
    "previousValue"
  );
  const updateZoneNewServices = parseAndFilterData(
    updateZoneServices,
    "newValue"
  );
  return (
    <div>
      {/* CREATE  */}
      {action == "create" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value && (
          <div>
            <div className="fw-bolder">Name: {objectName}</div>
            <div className="fw-bolder">Date: {currentTimeZone(createDate)}</div>
            <div className="fw-bolder">Created By: {owner}</div>
            <br />
            <h5 className="bold wrap-header m-t-sm">Zone Details:</h5>

            <Table className="table table-bordered w-auto">
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
            {!isEmpty(Object.keys(newZoneServiceData[0] || {})) && (
              <>
                <h5 className="bold wrap-header m-t-sm">
                  Zone Service Details:
                </h5>
                <Table className="table table-bordered w-75">
                  <thead className="thead-light">
                    <tr>
                      <th>Service Name</th>

                      <th>Zone Service Resources</th>
                    </tr>
                  </thead>

                  {Object.entries(newZoneServiceData[0])?.map(([key]) => {
                    return (
                      <tbody>
                        <tr>
                          <td className="table-warning align-middle">
                            <strong> {key}</strong>
                          </td>
                          {!isEmpty(newZoneServiceData[0][key].resources) ? (
                            <td className="table-warning">
                              {Object.values(
                                newZoneServiceData[0][key].resources
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
                          ) : (
                            <td className="text-center table-warning">--</td>
                          )}
                        </tr>
                      </tbody>
                    );
                  })}
                </Table>
              </>
            )}
          </div>
        )}

      {/* UPDATE */}

      {action == "update" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value && (
          <>
            <div>
              <div className="row">
                <div className="col-md-6">
                  <div className="fw-bolder">Name: {objectName}</div>
                  <div className="fw-bolder">
                    Date: {currentTimeZone(createDate)}
                  </div>
                  <div className="fw-bolder">Updated By: {owner}</div>
                </div>
                <div className="col-md-6 text-end">
                  <div className="bg-success legend"></div> {" Added "}
                  <div className="bg-danger legend"></div> {" Deleted "}
                </div>
              </div>
              <br />
              {action == "update" && !isEmpty(updateZoneDetails) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Zone Details:</h5>

                  <Table className="table table-bordered w-auto">
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
                            {!isEmpty(obj.attributeName)
                              ? obj.attributeName
                              : "--"}
                          </td>

                          <td className="table-warning text-nowrap">
                            {!isEmpty(
                              obj.previousValue.replace(/[[\]]/g, "")
                            ) ? (
                              isEmpty(obj.newValue.replace(/[[\]]/g, "")) ? (
                                <h6>
                                  <Badge className="d-inline me-1" bg="danger">
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
                              isEmpty(
                                obj.previousValue.replace(/[[\]]/g, "")
                              ) ? (
                                <h6>
                                  <Badge className="d-inline me-1" bg="success">
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
            {(!isEmpty(Object.keys(updateZoneOldServices[0] || {})) ||
              !isEmpty(Object.keys(updateZoneNewServices[0] || {}))) && (
              <div className="row">
                <div className="col">
                  <h5 className="bold wrap-header m-t-sm">
                    Old Zone Service Details:
                  </h5>

                  <Table className="table table-bordered w-100">
                    <thead className="thead-light">
                      <tr>
                        <th>Service Name</th>

                        <th> Zone Service Resources</th>
                      </tr>
                    </thead>
                    {!isEmpty(Object.keys(updateZoneOldServices[0] || {})) ? (
                      Object.entries(updateZoneOldServices[0]).map(([key]) => {
                        return (
                          <tbody>
                            <tr>
                              <td className="old-value-bg">
                                <strong> {key}</strong>
                              </td>
                              {!isEmpty(
                                updateZoneOldServices[0][key].resources
                              ) ? (
                                <td className="old-value-bg">
                                  {Object.values(
                                    updateZoneOldServices[0][key].resources
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
                              ) : (
                                <td className="text-center old-value-bg">--</td>
                              )}
                            </tr>
                          </tbody>
                        );
                      })
                    ) : (
                      <tbody>
                        <tr>
                          <td colSpan={2} className="text-center old-value-bg">
                            --
                          </td>
                        </tr>
                      </tbody>
                    )}
                  </Table>
                </div>
                <div className="col">
                  <h5 className="bold wrap-header m-t-sm">
                    New Zone Service Details:
                  </h5>

                  <Table className="table table-bordered w-100">
                    <thead className="thead-light">
                      <tr>
                        <th>Service Name </th>

                        <th> Zone Service Resources</th>
                      </tr>
                    </thead>
                    {!isEmpty(Object.keys(updateZoneNewServices[0] || {})) ? (
                      Object.entries(updateZoneNewServices[0]).map(([key]) => {
                        return (
                          <tbody>
                            <tr>
                              <td className="table-warning align-middle">
                                <strong> {key}</strong>
                              </td>
                              {!isEmpty(
                                updateZoneNewServices[0][key].resources
                              ) ? (
                                <td className="table-warning ">
                                  {Object.values(
                                    updateZoneNewServices[0][key].resources
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
                              ) : (
                                <td className="text-center table-warning">
                                  --
                                </td>
                              )}
                            </tr>
                          </tbody>
                        );
                      })
                    ) : (
                      <tbody>
                        <tr>
                          <td colSpan={2} className="text-center table-warning">
                            --
                          </td>
                        </tr>
                      </tbody>
                    )}
                  </Table>
                  <br />
                </div>
              </div>
            )}
          </>
        )}

      {/* DELETE  */}

      {action == "delete" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value && (
          <div>
            <div className="fw-bolder">Name: {objectName}</div>
            <div className="fw-bolder">Date: {currentTimeZone(createDate)}</div>
            <div className="fw-bolder">Deleted By: {owner}</div>
            <br />
            <h5 className="bold wrap-header m-t-sm">Zone Details:</h5>
            <Table className="table table-bordered w-50">
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
            {!isEmpty(Object.keys(oldZoneServiceData[0] || {})) && (
              <>
                <h5 className="bold wrap-header m-t-sm">
                  Zone Service Details:
                </h5>
                <Table className="table table-bordered w-75">
                  <thead className="thead-light">
                    <tr>
                      <th>Service Name</th>

                      <th>Zone Service Resources</th>
                    </tr>
                  </thead>

                  {Object.entries(oldZoneServiceData[0]).map(([key]) => {
                    return (
                      <tbody>
                        <tr>
                          <td className="table-warning align-middle">
                            <strong> {key}</strong>
                          </td>
                          {!isEmpty(oldZoneServiceData[0][key].resources) ? (
                            <td className="table-warning">
                              {Object.values(
                                oldZoneServiceData[0][key].resources
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
                          ) : (
                            <td className="text-center table-warning">--</td>
                          )}
                        </tr>
                      </tbody>
                    );
                  })}
                </Table>
              </>
            )}
          </div>
        )}
    </div>
  );
};

export default SecurityZonelogs;
