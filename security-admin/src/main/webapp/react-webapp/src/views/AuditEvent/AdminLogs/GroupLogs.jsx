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
import { ClassTypes } from "../../../utils/XAEnums";
import { isEmpty } from "lodash";
import { currentTimeZone } from "../../../utils/XAUtils";

export const GroupLogs = ({ data, reportdata }) => {
  const { objectName, objectClassType, createDate, owner, action } = data;

  const updateGrpOldNew = (userDetails) => {
    var tablerow = [];

    const getfilteredoldval = (oldvals) => {
      return !isEmpty(oldvals) ? oldvals : "--";
    };

    const getfilterednewval = (newvals) => {
      return !isEmpty(newvals) ? newvals : "--";
    };

    userDetails.map((val) => {
      return tablerow.push(
        <>
          <tr key={val.id}>
            <td className="table-warning">{val.attributeName}</td>
            {val && val.previousValue && !isEmpty(val.previousValue) ? (
              <td className="table-warning">
                {val && val.previousValue && !isEmpty(val.previousValue) ? (
                  isEmpty(val.newValue) ? (
                    <h6>
                      <Badge className="d-inline me-1" bg="danger">
                        {getfilteredoldval(val.previousValue)}
                      </Badge>
                    </h6>
                  ) : (
                    getfilteredoldval(val.previousValue)
                  )
                ) : (
                  "--"
                )}
              </td>
            ) : (
              <td>{"--"}</td>
            )}
            {val && val.newValue && !isEmpty(val.newValue) ? (
              <td className="table-warning">
                {val && val.newValue && !isEmpty(val.newValue) ? (
                  isEmpty(val.previousValue) ? (
                    <h6>
                      <Badge className="d-inline me-1" bg="success">
                        {getfilterednewval(val.newValue)}
                      </Badge>
                    </h6>
                  ) : (
                    getfilterednewval(val.newValue)
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
        objectClassType == ClassTypes.CLASS_TYPE_XA_GROUP.value && (
          <div>
            <div className="fw-bolder">Name : {objectName}</div>
            <div className="fw-bolder">Date: {currentTimeZone(createDate)}</div>
            <div className="fw-bolder">Created By: {owner}</div>
            <h5 className="bold wrap-header m-t-sm">Group Detail:</h5>

            <Table className="table table-bordered w-50">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>

                  <th>New Value</th>
                </tr>
              </thead>

              {reportdata.map((grp) => {
                return (
                  <tbody>
                    <tr key={grp.id}>
                      <td className="table-warning">{grp.attributeName}</td>

                      <td className="table-warning">
                        {!isEmpty(grp.newValue) ? grp.newValue : "--"}
                      </td>
                    </tr>
                  </tbody>
                );
              })}
            </Table>
          </div>
        )}

      {/* UPDATE */}

      {action == "update" &&
        objectClassType == ClassTypes.CLASS_TYPE_XA_GROUP.value && (
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
            <h5 className="bold wrap-header m-t-sm">Group Detail:</h5>

            <Table className="table table-bordered w-75 ">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>
                  <th>Old Value</th>
                  <th>New Value</th>
                </tr>
              </thead>

              <tbody>{updateGrpOldNew(reportdata)}</tbody>
            </Table>
          </div>
        )}
      {/* DELETE  */}
      {action == "delete" &&
        objectClassType == ClassTypes.CLASS_TYPE_XA_GROUP.value && (
          <div>
            <div className="fw-bolder">Name : {objectName}</div>
            <div className="fw-bolder">Date: {currentTimeZone(createDate)}</div>
            <div className="fw-bolder">Deleted By: {owner} </div>
            <br />
            <h5 className="bold wrap-header m-t-sm">Group Details:</h5>

            <Table className="table table-bordered w-50">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>
                  <th>Old Value</th>
                </tr>
              </thead>
              {reportdata.map((grp) => {
                return (
                  <tbody>
                    <tr>
                      <td className="table-warning">{grp.attributeName}</td>
                      <td className="table-warning">
                        {!isEmpty(grp.previousValue) ? grp.previousValue : "--"}
                      </td>
                    </tr>
                  </tbody>
                );
              })}
            </Table>
          </div>
        )}
    </div>
  );
};

export default GroupLogs;
