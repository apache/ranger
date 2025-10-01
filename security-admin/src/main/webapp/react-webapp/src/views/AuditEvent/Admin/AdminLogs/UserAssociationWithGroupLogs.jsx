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
import { currentTimeZone } from "Utils/XAUtils";
import { find } from "lodash";

export const UserAssociationWithGroupLogs = ({ data, reportdata }) => {
  const { objectName, createDate, owner, action, parentObjectName } = data;
  let actionType = action == "create" ? "Created By" : "Deleted By";
  let reportFinalData = find(reportdata, {
    parentObjectName: parentObjectName
  });
  let operation =
    action == "create" ? (
      <span>
        User <strong> {objectName} </strong>added to group{" "}
        <strong>{reportFinalData?.newValue || " "}</strong>
      </span>
    ) : (
      <span>
        User <strong> {objectName} </strong>removed from group{" "}
        <strong>{reportFinalData?.previousValue || " "}</strong>
      </span>
    );
  return (
    <div>
      <div className="fw-bolder">Name : {objectName}</div>
      <div className="fw-bolder">Date: {currentTimeZone(createDate)}</div>
      <div className="fw-bolder">
        {actionType}: {owner}
      </div>
      <br />
      <h5 className="bold wrap-header m-t-sm">User Details:</h5>

      <Table className="table table-bordered w-50">
        <thead className="thead-light">
          <tr>
            <th>Fields</th>
            <th>Value</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td className="table-warning">Group</td>
            <td className="table-warning">{operation}</td>
          </tr>
        </tbody>
      </Table>
    </div>
  );
};
export default UserAssociationWithGroupLogs;
