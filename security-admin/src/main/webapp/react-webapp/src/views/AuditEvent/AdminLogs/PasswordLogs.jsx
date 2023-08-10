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
import { Table } from "react-bootstrap";
import dateFormat from "dateformat";
import { ClassTypes } from "../../../utils/XAEnums";

export const PasswordLogs = ({ data, reportdata }) => {
  const { objectName, objectClassType, createDate, owner, action } = data;

  return (
    <div>
      {/* PASSWORD CHANGE  */}

      {action == "password change" &&
        objectClassType == ClassTypes.CLASS_TYPE_PASSWORD_CHANGE.value && (
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
            <h5 className="bold wrap-header m-t-sm">User Details:</h5>
            <Table striped bordered hover>
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>
                  <th>Old Value</th>
                  <th>New Value</th>
                </tr>
              </thead>
              {reportdata.map((obj) => (
                <tbody>
                  <tr>
                    <td className="table-warning">{obj.attributeName}</td>
                    <td className="table-warning">{obj.previousValue}</td>
                    <td className="table-warning">{obj.newValue}</td>
                  </tr>
                </tbody>
              ))}
            </Table>
          </div>
        )}
    </div>
  );
};

export default PasswordLogs;
