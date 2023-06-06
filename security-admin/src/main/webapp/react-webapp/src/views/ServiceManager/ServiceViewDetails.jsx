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
import { Row, Col, Table, Badge } from "react-bootstrap";
import { difference, isEmpty, keys, map, omit, pick, upperCase } from "lodash";

export const ServiceViewDetails = (props) => {
  let { serviceData, serviceDefData } = props;
  const getServiceConfigs = (serviceDef, serviceConfigs) => {
    let tableRow = [];
    let configs = {};
    let customConfigs = {};

    let serviceDefConfigs = serviceDef?.configs?.filter(
      (config) => config.name !== "ranger.plugin.audit.filters"
    );

    serviceConfigs = omit(serviceConfigs, "ranger.plugin.audit.filters");

    let serviceConfigsKey = keys(serviceConfigs);
    let serviceDefConfigsKey = map(serviceDefConfigs, "name");
    let customConfigsKey = difference(serviceConfigsKey, serviceDefConfigsKey);

    serviceDefConfigs?.map(
      (config) =>
        (configs[config.label !== undefined ? config.label : config.name] =
          serviceConfigs[config.name])
    );

    Object.entries(configs)?.map(([key, value]) =>
      tableRow.push(
        <tr key={key}>
          <td>{key}</td>
          <td>{value ? value : "--"}</td>
        </tr>
      )
    );

    customConfigsKey.map(
      (config) => (customConfigs[config] = serviceConfigs[config])
    );

    tableRow.push(
      <tr key="custom-configs-title">
        <td colSpan="2">
          <b>Add New Configurations :</b>
        </td>
      </tr>
    );

    if (isEmpty(customConfigs)) {
      tableRow.push(
        <tr key="custom-configs-empty">
          <td>--</td>
          <td>--</td>
        </tr>
      );
    }

    Object.entries(customConfigs)?.map(([key, value]) =>
      tableRow.push(
        <tr key={key}>
          <td>{key}</td>
          <td>{value ? value : "--"}</td>
        </tr>
      )
    );

    return tableRow;
  };
  const getFilterResources = (resources) => {
    let keyname = Object.keys(resources);
    return keyname.map((key, index) => {
      let val = resources[key].values;
      return (
        <div key={index} className="clearfix mb-2">
          <span className="float-left">
            <b>{key}: </b>
            {val.join()}
          </span>
          {resources[key].isExcludes !== undefined ? (
            <h6 className="d-inline">
              {resources[key].isExcludes ? (
                <span className="badge badge-dark float-right">Include</span>
              ) : (
                <span className="badge badge-dark float-right">Exclude</span>
              )}
            </h6>
          ) : (
            ""
          )}
          {resources[key].isRecursive !== undefined ? (
            <h6 className="d-inline">
              {resources[key].isRecursive ? (
                <span className="badge badge-dark float-right">Recursive</span>
              ) : (
                <span className="badge badge-dark float-right">
                  Non Recursive
                </span>
              )}
            </h6>
          ) : (
            ""
          )}
        </div>
      );
    });
  };
  const getAuditFilters = (serviceConfigs) => {
    let tableRow = [];
    let auditFilters = pick(serviceConfigs, "ranger.plugin.audit.filters");

    if (isEmpty(auditFilters)) {
      return tableRow;
    }

    if (isEmpty(auditFilters["ranger.plugin.audit.filters"])) {
      return tableRow;
    }

    auditFilters = JSON.parse(
      auditFilters["ranger.plugin.audit.filters"].replace(/'/g, '"')
    );

    auditFilters?.map((a, index) =>
      tableRow.push(
        <tr key={index}>
          <td className="text-center">
            {a.isAudited == true ? (
              <h6>
                <Badge variant="info">Yes</Badge>
              </h6>
            ) : (
              <h6>
                <Badge variant="info">No</Badge>
              </h6>
            )}
          </td>
          <td className="text-center">
            {a.accessResult !== undefined ? (
              <h6>
                <Badge variant="info">{a.accessResult}</Badge>
              </h6>
            ) : (
              "--"
            )}
          </td>
          <td className="text-center">
            {a.resources !== undefined ? (
              <div className="resource-grp">
                {getFilterResources(a.resources)}
              </div>
            ) : (
              "--"
            )}
          </td>
          <td className="text-center">
            {a.actions !== undefined
              ? a.actions.map((action) => (
                  <h6 key={action}>
                    <Badge variant="info">{action}</Badge>
                  </h6>
                ))
              : "--"}
          </td>
          <td className="text-center">
            {a.accessTypes !== undefined && a.accessTypes.length > 0
              ? a.accessTypes.map((accessType) => (
                  <h6 key={accessType}>
                    <Badge variant="info">{accessType}</Badge>
                  </h6>
                ))
              : "--"}
          </td>
          <td className="text-center">
            {a.users !== undefined
              ? a.users.map((user) => (
                  <h6 key={user}>
                    <Badge
                      variant="info"
                      className="m-1 text-truncate more-less-width"
                      title={user}
                      key={user}
                    >
                      {user}
                    </Badge>
                  </h6>
                ))
              : "--"}
          </td>
          <td className="text-center">
            {a.groups !== undefined
              ? a.groups.map((group) => (
                  <h6 key={group}>
                    <Badge
                      variant="info"
                      className="m-1 text-truncate more-less-width"
                      title={group}
                      key={group}
                    >
                      {group}
                    </Badge>
                  </h6>
                ))
              : "--"}
          </td>
          <td className="text-center">
            {a.roles !== undefined
              ? a.roles.map((role) => (
                  <h6 key={role}>
                    <Badge variant="info">{role}</Badge>
                  </h6>
                ))
              : "--"}
          </td>
        </tr>
      )
    );

    return tableRow;
  };
  return (
    <Row>
      <Col sm={12}>
        <p className="form-header">Service Details :</p>
        <Table bordered size="sm">
          <tbody className="service-details">
            <tr>
              <td>Service Name</td>
              <td>{serviceData.name}</td>
            </tr>
            <tr>
              <td>Display Name</td>
              <td>{serviceData.displayName}</td>
            </tr>
            <tr>
              <td>Description</td>
              <td>
                {serviceData.description ? serviceData.description : "--"}
              </td>
            </tr>
            <tr>
              <td>Active Status</td>
              <td>
                <h6>
                  <Badge variant="info">
                    {serviceData.isEnabled ? `Enabled` : `Disabled`}
                  </Badge>
                </h6>
              </td>
            </tr>
            <tr>
              <td>Tag Service</td>
              <td>
                {serviceData.tagService ? (
                  <h6>
                    <Badge variant="info">{serviceData.tagService}</Badge>
                  </h6>
                ) : (
                  "--"
                )}
              </td>
            </tr>
          </tbody>
        </Table>
        <p className="form-header">Config Properties :</p>
        <Table bordered size="sm">
          <tbody className="service-config">
            {getServiceConfigs(serviceDefData, serviceData.configs)}
          </tbody>
        </Table>
        <p className="form-header">Audit Filter :</p>
        <div className="table-responsive">
          <Table bordered size="sm" className="table-audit-filter-ready-only">
            <thead>
              <tr>
                <th>Is Audited</th>
                <th>Access Result</th>
                <th>Resources</th>
                <th>Operations</th>
                <th>Permissions</th>
                <th>Users</th>
                <th>Groups</th>
                <th>Roles</th>
              </tr>
            </thead>
            <tbody>{getAuditFilters(serviceData.configs)}</tbody>
          </Table>
        </div>
      </Col>
    </Row>
  );
};
export default ServiceViewDetails;
