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
import { Badge, Table } from "react-bootstrap";
import dateFormat from "dateformat";
import { ClassTypes } from "../../../utils/XAEnums";
import { currentTimeZone } from "../../../utils/XAUtils";
import { isEmpty, isUndefined, sortBy } from "lodash";

export const ServiceLogs = ({ data, reportdata }) => {
  const { objectName, objectClassType, createDate, owner, action } = data;
  const serviceCreate = sortBy(
    reportdata.filter((obj) => {
      return (
        obj.attributeName != "Connection Configurations" &&
        obj.action == "create"
      );
    }),
    ["id"]
  );
  const serviceUpdate = reportdata.filter((obj) => {
    return (
      obj.attributeName != "Connection Configurations" && obj.action == "update"
    );
  });
  const serviceDelete = sortBy(
    reportdata.filter((obj) => {
      return (
        obj.attributeName != "Connection Configurations" &&
        obj.action == "delete"
      );
    }),
    ["id"]
  );

  const connectionCreate = reportdata.filter((obj) => {
    return (
      obj.attributeName == "Connection Configurations" && obj.action == "create"
    );
  });
  const connectionUpdate = reportdata.filter((obj) => {
    return (
      obj.attributeName == "Connection Configurations" && obj.action == "update"
    );
  });
  const connectionDelete = reportdata.filter((obj) => {
    return (
      obj.attributeName == "Connection Configurations" && obj.action == "delete"
    );
  });
  const serviceDetails = (val, newValue) => {
    if (val == "Service Status") {
      return newValue == "false" ? "Disabled" : "Enabled";
    }
    return newValue;
  };
  const updatePolicyOldNew = (service) => {
    var tablerow = [];

    const getfilteredoldval = (val, oldvals) => {
      if (val == "Service Status") {
        return <>{oldvals == "false" ? "Disabled" : "Enabled"}</>;
      }

      return !isEmpty(oldvals) ? oldvals : "--";
    };

    const getfilterednewval = (val, newvals) => {
      if (val == "Service Status") {
        return <>{newvals == "false" ? "Disabled" : "Enabled"}</>;
      }

      return !isEmpty(newvals) ? newvals : "--";
    };

    service.map((val) => {
      return tablerow.push(
        <>
          <tr key={val.id}>
            <td className="table-warning">{val.attributeName}</td>
            {val && val.previousValue && !isEmpty(val.previousValue) ? (
              <td className="table-warning text-nowrap">
                {val && val.previousValue && !isEmpty(val.previousValue) ? (
                  isEmpty(val.newValue) ? (
                    <h6>
                      <Badge className="d-inline me-1" bg="danger">
                        {getfilteredoldval(
                          val.attributeName,
                          val.previousValue
                        )}
                      </Badge>
                    </h6>
                  ) : (
                    getfilteredoldval(val.attributeName, val.previousValue)
                  )
                ) : (
                  "--"
                )}
              </td>
            ) : (
              <td>{"--"}</td>
            )}
            {val && val.newValue && !isEmpty(val.newValue) ? (
              <td className="table-warning text-nowrap">
                {val && val.newValue && !isEmpty(val.newValue) ? (
                  isEmpty(val.previousValue) ? (
                    <h6>
                      <Badge className="d-inline me-1" bg="success">
                        {getfilterednewval(val.attributeName, val.newValue)}
                      </Badge>
                    </h6>
                  ) : (
                    getfilterednewval(val.attributeName, val.newValue)
                  )
                ) : (
                  "--"
                )}
              </td>
            ) : (
              <td>{"--"}</td>
            )}
          </tr>
        </>
      );
    });

    return tablerow;
  };

  return (
    <div>
      {/* CREATE  */}
      {action == "create" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SERVICE.value && (
          <div>
            <div className="fw-bolder">Name: {objectName || ""}</div>
            <div className="fw-bolder">Date:{currentTimeZone(createDate)}</div>
            <div className="fw-bolder">Created By: {owner}</div>
            <br />
            {action == "create" && (
              <>
                <h5 className="bold wrap-header m-t-sm">Service Details:</h5>
                <Table className="table table-bordered w-50">
                  <thead className="thead-light">
                    <tr>
                      <th>Fields</th>
                      <th>New Value</th>
                    </tr>
                  </thead>
                  {serviceCreate.map((obj) => {
                    return (
                      <tbody>
                        <tr key={obj.id}>
                          <td className="table-warning">{obj.attributeName}</td>

                          <td className="table-warning">
                            {obj && obj.newValue && !isEmpty(obj.newValue)
                              ? serviceDetails(obj.attributeName, obj.newValue)
                              : "--"}
                          </td>
                        </tr>
                      </tbody>
                    );
                  })}
                </Table>
                <br />
              </>
            )}

            {action == "create" &&
              !isEmpty(connectionCreate) &&
              !isUndefined(connectionCreate) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Connection Configurations :
                  </h5>
                  <Table className="table  table-bordered w-auto">
                    <>
                      <thead className="thead-light">
                        <tr>
                          <th>Fields</th>
                          <th>New Value</th>
                        </tr>
                      </thead>

                      <tbody>
                        {connectionCreate.map((config) => {
                          return config &&
                            config.newValue &&
                            !isEmpty(
                              Object.keys(JSON.parse(config.newValue))
                            ) ? (
                            Object.keys(JSON.parse(config.newValue))?.map(
                              (obj, key) => (
                                <tr key={key}>
                                  <td className="table-warning">{obj}</td>
                                  <td className="table-warning text-nowrap">
                                    {config &&
                                    config.newValue &&
                                    !isEmpty(JSON.parse(config.newValue))
                                      ? JSON.parse(config.newValue)[obj]
                                      : "--"}
                                  </td>
                                </tr>
                              )
                            )
                          ) : (
                            <tr>
                              <td className="table-warning">
                                <strong>{"<empty>"}</strong>
                              </td>
                              <td className="table-warning">
                                <strong>{"<empty>"}</strong>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </>
                  </Table>
                </>
              )}
          </div>
        )}

      {/* UPDATE */}

      {action == "update" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SERVICE.value && (
          <div>
            <div className="row">
              <div className="col-md-6">
                <div className="fw-bolder">Name : {objectName}</div>
                <div className="fw-bolder">
                  Date:{currentTimeZone(createDate)}
                </div>
                <div className="fw-bolder">Updated By: {owner}</div>
              </div>
              <div className="col-md-6 text-end">
                <div className="bg-success legend"></div> {" Added "}
                <div className="bg-danger legend"></div> {" Deleted "}
              </div>
            </div>
            <br />
            {action == "update" &&
              !isEmpty(serviceUpdate) &&
              !isUndefined(serviceUpdate) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Service Details:</h5>
                  <Table className="table table-bordered w-50">
                    <thead className="thead-light">
                      <tr>
                        <th>Fields</th>
                        <th>Old Value</th>
                        <th>New Value</th>
                      </tr>
                    </thead>
                    <tbody>{updatePolicyOldNew(serviceUpdate)}</tbody>
                  </Table>
                  <br />
                </>
              )}

            {action == "update" &&
              !isEmpty(connectionUpdate) &&
              !isUndefined(connectionUpdate) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Connection Configurations :
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Fields</th>
                        <th>Old Value</th>
                        <th>New Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {connectionUpdate.map((config) => {
                        return (
                          config &&
                          config.newValue &&
                          Object.keys(JSON.parse(config.newValue)).map(
                            (obj, key) => (
                              <tr key={key}>
                                <td className="table-warning">{obj}</td>
                                <td className="table-warning text-nowrap">
                                  {config &&
                                  config.previousValue &&
                                  !isEmpty(JSON.parse(config.previousValue)) ? (
                                    isEmpty(
                                      JSON.parse(config.newValue)[obj]
                                    ) ? (
                                      <h6>
                                        <Badge
                                          className="d-inline me-1"
                                          bg="danger"
                                        >
                                          {
                                            JSON.parse(config.previousValue)[
                                              obj
                                            ]
                                          }
                                        </Badge>
                                      </h6>
                                    ) : (
                                      JSON.parse(config.previousValue)[obj]
                                    )
                                  ) : (
                                    "--"
                                  )}
                                </td>
                                <td className="table-warning text-nowrap">
                                  {config &&
                                  config.newValue &&
                                  !isEmpty(JSON.parse(config.newValue)) ? (
                                    isEmpty(
                                      JSON.parse(config.previousValue)[obj]
                                    ) ? (
                                      <h6>
                                        <Badge
                                          className="d-inline me-1"
                                          bg="success"
                                        >
                                          {JSON.parse(config.newValue)[obj]}
                                        </Badge>
                                      </h6>
                                    ) : (
                                      JSON.parse(config.newValue)[obj]
                                    )
                                  ) : (
                                    "--"
                                  )}
                                </td>
                              </tr>
                            )
                          )
                        );
                      })}
                    </tbody>
                  </Table>
                </>
              )}
          </div>
        )}
      {/* DELETE  */}

      {action == "delete" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_SERVICE.value && (
          <div>
            <div className="fw-bolder">Name: {objectName || ""}</div>
            <div className="fw-bolder">Date: {currentTimeZone(createDate)}</div>
            <div className="fw-bolder">Deleted By: {owner}</div>
            <br />
            {action == "delete" &&
              !isEmpty(serviceDelete) &&
              !isUndefined(serviceDelete) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Service Details:</h5>
                  <Table className="table table-bordered w-50">
                    <thead className="thead-light">
                      <tr>
                        <th>Fields</th>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    <tbody>
                      {serviceDelete.map((obj) => {
                        return (
                          <tr key={obj}>
                            <td className="table-warning">
                              {obj.attributeName}
                            </td>
                            <td className="table-warning">
                              {obj &&
                              obj.previousValue &&
                              !isEmpty(obj.previousValue)
                                ? serviceDetails(
                                    obj.attributeName,
                                    obj.previousValue
                                  )
                                : "--"}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </Table>
                  <br />
                </>
              )}

            {action == "delete" &&
              !isEmpty(connectionDelete) &&
              !isUndefined(connectionDelete) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Connection Configurations :
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Fields</th>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    <tbody>
                      {connectionDelete.map((config) => {
                        return (
                          config &&
                          config.previousValue &&
                          Object.keys(JSON.parse(config.previousValue)).map(
                            (obj, key) => (
                              <tr key={key}>
                                <td className="table-warning">{obj}</td>
                                <td className="table-warning text-nowrap">
                                  {config &&
                                  config.previousValue &&
                                  !isEmpty(JSON.parse(config.previousValue))
                                    ? JSON.parse(config.previousValue)[obj]
                                    : "--"}
                                </td>
                              </tr>
                            )
                          )
                        );
                      })}
                    </tbody>
                  </Table>
                </>
              )}
          </div>
        )}
    </div>
  );
};

export default ServiceLogs;
