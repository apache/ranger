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
import { Button, Table } from "react-bootstrap";
import dateFormat from "dateformat";
import { isEmpty } from "lodash";
import { toast } from "react-toastify";
import {
  ServiceRequestDataRangerAcl,
  ServiceRequestDataHadoopAcl
} from "../../utils/XAEnums";
import { requestDataTitle } from "../../utils/XAUtils";

export const AccessLogsTable = ({ data = {} }) => {
  const {
    eventTime,
    eventId,
    accessResult,
    clientIP,
    clusterName,
    action,
    policyId,
    requestUser,
    repoName,
    serviceType,
    serviceTypeDisplayName,
    resourcePath,
    resourceType,
    aclEnforcer,
    policyVersion,
    accessType,
    agentId,
    agentHost,
    eventCount,
    zoneName,
    requestData,
    tags
  } = data;

  const copyText = (val) => {
    !isEmpty(val) && toast.success("Copied successfully!!");
    return val;
  };

  return (
    <Table bordered hover>
      <tbody>
        <tr>
          <td>Audit ID</td>
          <td>{!isEmpty(eventId) ? eventId : "--"}</td>
        </tr>
        <tr>
          <td>Policy ID</td>
          <td>{policyId > 0 ? policyId : "--"}</td>
        </tr>
        <tr>
          <td>Policy Version</td>
          <td>{policyVersion || "--"}</td>
        </tr>
        <tr>
          <td>Event Time</td>
          <td>
            {!isEmpty(eventTime)
              ? dateFormat(eventTime, "mm/dd/yyyy hh:MM:ss TT ")
              : "--"}
          </td>
        </tr>
        <tr>
          <td>Application</td>
          <td>{!isEmpty(agentId) ? agentId : "--"}</td>
        </tr>
        <tr>
          <td>User</td>
          <td>{!isEmpty(requestUser) ? requestUser : "--"}</td>
        </tr>
        <tr>
          <td>Service Name </td>
          <td>{!isEmpty(repoName) ? repoName : "--"}</td>
        </tr>
        <tr>
          <td>Service Type </td>
          <td>
            {!isEmpty(serviceTypeDisplayName) ? serviceTypeDisplayName : "--"}
          </td>
        </tr>
        <tr>
          <td>Resource Path</td>
          <td>{!isEmpty(resourcePath) ? resourcePath : "--"}</td>
        </tr>
        <tr>
          <td>Resource Type</td>
          <td>{!isEmpty(resourceType) ? resourceType : "--"}</td>
        </tr>
        {ServiceRequestDataRangerAcl.includes(serviceType) &&
          aclEnforcer === "ranger-acl" &&
          !isEmpty(requestData) && (
            <tr>
              <td>{requestDataTitle(serviceType)}</td>
              <td>
                {!isEmpty(requestData) ? (
                  <>
                    <Button
                      className="float-end link-tag query-icon btn btn-sm"
                      size="sm"
                      variant="link"
                      title="Copy"
                      onClick={() =>
                        navigator.clipboard.writeText(copyText(requestData))
                      }
                    >
                      <i className="fa-fw fa fa-copy"> </i>
                    </Button>
                    <span>{requestData}</span>
                  </>
                ) : (
                  "--"
                )}
              </td>
            </tr>
          )}
        {ServiceRequestDataHadoopAcl.includes(serviceType) &&
          aclEnforcer === "hadoop-acl" &&
          !isEmpty(requestData) && (
            <tr>
              <td>{requestDataTitle(serviceType)}</td>
              <td>
                {!isEmpty(requestData) ? (
                  <>
                    <Button
                      className="float-end link-tag query-icon btn btn-sm"
                      size="sm"
                      variant="link"
                      title="Copy"
                      onClick={() =>
                        navigator.clipboard.writeText(copyText(requestData))
                      }
                    >
                      <i className="fa-fw fa fa-copy"> </i>
                    </Button>
                    <span>{requestData}</span>
                  </>
                ) : (
                  "--"
                )}
              </td>
            </tr>
          )}
        <tr>
          <td>Access Type</td>
          <td>{!isEmpty(accessType) ? accessType : "--"}</td>
        </tr>
        <tr>
          <td>Permission</td>
          <td>{!isEmpty(action) ? action : "--"}</td>
        </tr>
        <tr>
          <td>Result</td>
          <td>
            {accessResult !== undefined
              ? accessResult == 1
                ? "Allowed"
                : "Denied"
              : "--"}
          </td>
        </tr>
        <tr>
          <td>Access Enforcer</td>
          <td>{!isEmpty(aclEnforcer) ? aclEnforcer : "--"}</td>
        </tr>
        <tr>
          <td>Agent Host Name </td>
          <td>{!isEmpty(agentHost) ? agentHost : "--"}</td>
        </tr>
        <tr>
          <td>Client IP </td>
          <td>{!isEmpty(clientIP) ? clientIP : "--"}</td>
        </tr>
        <tr>
          <td>Cluster Name</td>
          <td>{!isEmpty(clusterName) ? clusterName : "--"}</td>
        </tr>
        <tr>
          <td>Zone Name</td>
          <td>{!isEmpty(zoneName) ? zoneName : "--"}</td>
        </tr>
        <tr>
          <td>Event Count</td>
          <td>{eventCount > 0 ? eventCount : "--"}</td>
        </tr>
        <tr>
          <td>Tags</td>
          <td>
            {!isEmpty(tags)
              ? JSON.parse(tags)
                  .map((val) => {
                    return val.type;
                  })
                  .sort()
                  .join(", ")
              : "--"}
          </td>
        </tr>
      </tbody>
    </Table>
  );
};
export default AccessLogsTable;
