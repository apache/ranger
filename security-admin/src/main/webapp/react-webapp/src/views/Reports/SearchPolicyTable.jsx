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

import React, { useState, useCallback, useRef } from "react";
import { Link } from "react-router-dom";
import {
  Accordion,
  Badge,
  Button,
  Col,
  Modal,
  Row,
  Table
} from "react-bootstrap";
import { isEmpty, find } from "lodash";
import { MoreLess } from "Components/CommonComponents";
import XATableLayout from "Components/XATableLayout";
import { fetchApi } from "Utils/fetchAPI";
import { Loader } from "../../components/CommonComponents";
import { isAuditor, isKMSAuditor } from "../../utils/XAUtils";

function SearchPolicyTable(props) {
  const [searchPoliciesData, setSearchPolicies] = useState([]);
  const fetchIdRef = useRef(0);
  const [loader, setLoader] = useState(true);
  const [totalCount, setTotalCount] = useState(0);
  const [pageCount, setPageCount] = React.useState(0);
  const [policyData, setPolicyData] = useState(null);
  const [showModal, setShowModal] = useState(false);

  const showPolicyConditionModal = (policyData) => {
    setShowModal(true);
    setPolicyData(policyData);
  };

  const hidePolicyConditionModal = () => setShowModal(false);
  const fetchSearchPolicies = useCallback(
    async ({ pageSize, pageIndex }) => {
      setLoader(true);
      const fetchId = ++fetchIdRef.current;
      let searchPoliciesResp;
      let searchPolicies = [];
      let totalPoliciesCount = 0;
      let params = { ...props.searchParams };

      if (fetchId === fetchIdRef.current) {
        params["serviceType"] = props.serviceDef.name;
        params["pageSize"] = pageSize;
        params["startIndex"] = pageIndex * pageSize;
        try {
          searchPoliciesResp = await fetchApi({
            url: `plugins/policies`,
            params: params
          });
          searchPolicies = searchPoliciesResp.data.policies;
          totalPoliciesCount = searchPoliciesResp.data.totalCount;
        } catch (error) {
          `Error occurred while fetching Service Policies ! ${error}`;
        }

        setSearchPolicies(searchPolicies);
        setTotalCount(totalPoliciesCount);
        setPageCount(Math.ceil(totalPoliciesCount / pageSize));
        setLoader(false);
      }
    },
    [props.searchParams]
  );

  const getServiceId = (serviceName) => {
    let service = find(props.services, { name: serviceName });

    return service.id;
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Policy ID",
        accessor: "id",
        Cell: (rawValue) => {
          if (isAuditor() || isKMSAuditor()) {
            return (
              <div className="position-relative text-center">
                {rawValue.value}
              </div>
            );
          } else {
            return (
              <div className="position-relative text-center">
                <Link
                  title="Edit"
                  to={`/service/${getServiceId(
                    rawValue.row.original.service
                  )}/policies/${rawValue.value}/edit`}
                >
                  {rawValue.value}
                </Link>
              </div>
            );
          }
        },
        width: 65
      },
      {
        Header: "Policy Name",
        accessor: "name",
        Cell: (val) => {
          return (
            <span
              className="text-truncate"
              style={{ maxWidth: "120px", display: "inline-block" }}
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Policy Label",
        accessor: "policyLabels",
        Cell: (rawValue) => {
          let policyLabels = rawValue?.value?.map((label, index) => (
            <span key={index}>{label}</span>
          ));
          return !isEmpty(policyLabels) ? (
            <MoreLess data={policyLabels} key={rawValue.row.original.id} />
          ) : (
            <div className="text-center">--</div>
          );
        }
      },
      {
        Header: "Resources",
        accessor: "resources",
        Cell: (rawValue) => {
          if (rawValue.value) {
            let keyName = Object.keys(rawValue.value);
            return keyName?.map((key, index) => {
              let val = rawValue.value[key].values;
              return (
                <div key={index} className="text-center overflow-text">
                  <b>{key}: </b>
                  {val.join()}
                </div>
              );
            });
          }
        }
      },
      {
        Header: "Policy Type",
        accessor: "policyType",
        Cell: (rawValue) => {
          if (rawValue.value == 1) {
            return (
              <h6 className="text-center">
                <Badge bg="primary">Masking</Badge>
              </h6>
            );
          } else if (rawValue.value == 2) {
            return (
              <h6 className="text-center">
                <Badge bg="primary">Row Level Filter</Badge>
              </h6>
            );
          } else
            return (
              <h6 className="text-center">
                <Badge bg="primary">Access</Badge>
              </h6>
            );
        }
      },
      {
        Header: "Status",
        accessor: "isEnabled",
        Cell: (rawValue) => {
          if (rawValue.value)
            return (
              <h6 className="text-center">
                <Badge bg="success">Enabled</Badge>
              </h6>
            );
          else
            return (
              <h6 className="text-center">
                <Badge bg="danger">Disabled</Badge>
              </h6>
            );
        }
      },
      {
        Header: "Zone Name",
        accessor: "zoneName",
        Cell: (rawValue) => {
          return !isEmpty(rawValue.value) ? (
            <Badge bg="dark" className="text-truncate mw-100">
              {rawValue.value}
            </Badge>
          ) : (
            <div className="text-center">--</div>
          );
        }
      },
      {
        Header: "Policy Conditions",
        Cell: ({ row: { original } }) => {
          return (
            <div className="text-center">
              <Button
                variant="outline-dark"
                size="sm"
                title="View"
                onClick={(e) => {
                  e.stopPropagation();
                  showPolicyConditionModal(original);
                }}
              >
                <div className="text-center">
                  <i className="fa-fw fa fa-plus"></i>
                </div>
              </Button>
            </div>
          );
        }
      }
    ],
    []
  );

  return (
    <Row>
      <Col sm={12} className="mt-3">
        <Accordion defaultActiveKey="0">
          <Accordion.Item eventKey="0">
            <Accordion.Header data-js="hdfsHeader" data-cy="hdfsHeader">
              <div className="clearfix">
                <span className="bold float-start text-uppercase">
                  {props.serviceDef.name}
                </span>
                <span className="float-end"></span>
              </div>
            </Accordion.Header>
            <Accordion.Body>
              {props.contentLoader ? (
                <Loader />
              ) : (
                <XATableLayout
                  columnHide={false}
                  loading={loader}
                  data={searchPoliciesData}
                  columns={columns}
                  fetchData={fetchSearchPolicies}
                  pagination
                  pageCount={pageCount}
                  totalCount={totalCount}
                />
              )}
            </Accordion.Body>
          </Accordion.Item>
        </Accordion>
        <Modal show={showModal} onHide={hidePolicyConditionModal} size="lg">
          <Modal.Header closeButton>
            <Modal.Title>Policy Condition Details</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <PolicyConditionData
              policyData={policyData}
              serviceDef={props.serviceDef}
            />
          </Modal.Body>
        </Modal>
      </Col>
    </Row>
  );
}

export default SearchPolicyTable;

function PolicyConditionData(props) {
  const getPolicyData = (policyItem) => {
    let tableRow = [];
    let access = [];

    if (!isEmpty(policyItem)) {
      tableRow = policyItem?.map((items, index) => {
        access = items.accesses?.map((obj) => obj.type);
        return (
          <tr key={index}>
            <td>
              {!isEmpty(items.roles) ? (
                <MoreLess data={items.roles} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(items.groups) ? (
                <MoreLess data={items.groups} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(items.users) ? (
                <MoreLess data={items.users} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            {!isEmpty(props?.serviceDef?.policyConditions) && (
              <td className="text-center">
                {!isEmpty(items.conditions)
                  ? items.conditions.map((obj, index) => {
                      return (
                        <h6 className="d-inline me-1" key={index}>
                          <Badge
                            bg="info"
                            className="d-inline me-1"
                            key={obj.values}
                          >{`${obj.type}: ${obj.values.join(", ")}`}</Badge>
                        </h6>
                      );
                    })
                  : "--"}
              </td>
            )}
            <td>
              {!isEmpty(access) ? (
                <MoreLess data={access} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
          </tr>
        );
      });
    } else {
      tableRow.push(
        <tr key="no-data">
          <td
            className="text-center"
            colSpan={!isEmpty(props?.serviceDef?.policyConditions) ? "5" : "4"}
          >
            <span className="text-muted">&quot;No data to show!!&quot;</span>
          </td>
        </tr>
      );
    }
    return tableRow;
  };

  const getMaskingPolicyData = (policyItem) => {
    let tableRow = [];
    let access = [];
    if (!isEmpty(policyItem)) {
      tableRow = policyItem?.map((items, index) => {
        access = items.accesses?.map((obj) => obj.type);
        return (
          <tr key={index}>
            <td>
              {!isEmpty(items.roles) ? (
                <MoreLess data={items.roles} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(items.groups) ? (
                <MoreLess data={items.groups} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(items.users) ? (
                <MoreLess data={items.users} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            {!isEmpty(props?.serviceDef?.policyConditions) && (
              <td className="text-center">
                {!isEmpty(items.conditions)
                  ? items.conditions.map((obj, index) => {
                      return (
                        <h6 className="d-inline me-1" key={index}>
                          <Badge
                            bg="info"
                            className="d-inline me-1"
                            key={obj.values}
                          >{`${obj.type}: ${obj.values.join(", ")}`}</Badge>
                        </h6>
                      );
                    })
                  : "--"}
              </td>
            )}
            <td>
              {!isEmpty(access) ? (
                <MoreLess data={access} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(items.dataMaskInfo) ? (
                <h6 className="d-inline">
                  <Badge bg="info" className="me-1">
                    {items.dataMaskInfo["dataMaskType"]}
                  </Badge>
                </h6>
              ) : (
                "--"
              )}
            </td>
          </tr>
        );
      });
    } else {
      tableRow.push(
        <tr key="no-data">
          <td
            className="text-center"
            colSpan={!isEmpty(props?.serviceDef?.policyConditions) ? "5" : "4"}
          >
            <span className="text-muted">&quot;No data to show!!&quot;</span>
          </td>
        </tr>
      );
    }
    return tableRow;
  };

  const getRowLevelPolicyData = (policyItem) => {
    let tableRow = [];
    let access = [];
    if (!isEmpty(policyItem)) {
      tableRow = policyItem?.map((items, index) => {
        access = items.accesses?.map((obj) => obj.type);
        return (
          <tr key={index}>
            <td>
              {!isEmpty(items.roles) ? (
                <MoreLess data={items.roles} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(items.groups) ? (
                <MoreLess data={items.groups} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(items.users) ? (
                <MoreLess data={items.users} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(access) ? (
                <MoreLess data={access} />
              ) : (
                <div className="text-center">--</div>
              )}
            </td>
            <td>
              {!isEmpty(items.rowFilterInfo) ? (
                <h6 className="d-inline">
                  <Badge bg="info" className="me-1">
                    {items.rowFilterInfo["filterExpr"]}
                  </Badge>
                </h6>
              ) : (
                "--"
              )}
            </td>
          </tr>
        );
      });
    } else {
      tableRow.push(
        <tr key="no-data">
          <td className="text-center" colSpan="4">
            <span className="text-muted">&quot;No data to show!!&quot;</span>
          </td>
        </tr>
      );
    }
    return tableRow;
  };

  return (
    <React.Fragment>
      {props.policyData.policyType == 0 && (
        <>
          <p className="form-header">Allow Conditions</p>
          <div className="overflow-x-auto">
            <Table
              bordered
              size="sm"
              className="mb-3 table-audit-filter-ready-only"
            >
              <thead>
                <tr>
                  <th>Roles</th>
                  <th>Groups</th>
                  <th>Users</th>
                  {!isEmpty(props?.serviceDef?.policyConditions) && (
                    <th className="text-center text-nowrap">
                      Policy Conditions
                    </th>
                  )}
                  <th>Accesses</th>
                </tr>
              </thead>
              <tbody>{getPolicyData(props.policyData.policyItems)}</tbody>
            </Table>
          </div>
          {props.serviceDef?.options?.enableDenyAndExceptionsInPolicies ==
            "true" && (
            <>
              <p className="form-header">Allow Exclude</p>
              <div className="overflow-x-auto">
                <Table
                  bordered
                  size="sm"
                  className="mb-3 table-audit-filter-ready-only"
                >
                  <thead>
                    <tr>
                      <th>Roles</th>
                      <th>Groups</th>
                      <th>Users</th>
                      {!isEmpty(props?.serviceDef?.policyConditions) && (
                        <th className="text-center text-nowrap">
                          Policy Conditions
                        </th>
                      )}
                      <th>Accesses</th>
                    </tr>
                  </thead>
                  <tbody>
                    {getPolicyData(props.policyData.allowExceptions)}
                  </tbody>
                </Table>
              </div>
              <p className="form-header">Deny Conditions</p>
              <div className="overflow-x-auto">
                <Table
                  bordered
                  size="sm"
                  className="mb-3 table-audit-filter-ready-only"
                >
                  <thead>
                    <tr>
                      <th>Roles</th>
                      <th>Groups</th>
                      <th>Users</th>
                      {!isEmpty(props?.serviceDef?.policyConditions) && (
                        <th className="text-center text-nowrap">
                          Policy Conditions
                        </th>
                      )}
                      <th>Accesses</th>
                    </tr>
                  </thead>
                  <tbody>
                    {getPolicyData(props.policyData.denyPolicyItems)}
                  </tbody>
                </Table>
              </div>
              <p className="form-header">Deny Exclude</p>
              <div className="overflow-x-auto">
                <Table
                  bordered
                  size="sm"
                  className="mb-3 table-audit-filter-ready-only"
                >
                  <thead>
                    <tr>
                      <th>Roles</th>
                      <th>Groups</th>
                      <th>Users</th>
                      {!isEmpty(props?.serviceDef?.policyConditions) && (
                        <th className="text-center text-nowrap">
                          Policy Conditions
                        </th>
                      )}
                      <th>Accesses</th>
                    </tr>
                  </thead>
                  <tbody>
                    {getPolicyData(props.policyData.denyExceptions)}
                  </tbody>
                </Table>
              </div>
            </>
          )}
        </>
      )}

      {props.policyData.policyType == 1 && (
        <>
          <p className="form-header ">Masking Conditions</p>
          <div className="overflow-x-auto">
            <Table
              bordered
              size="sm"
              className="mb-3 table-audit-filter-ready-only"
            >
              <thead>
                <tr>
                  <th>Roles</th>
                  <th>Groups</th>
                  <th>Users</th>
                  {!isEmpty(props?.serviceDef?.policyConditions) && (
                    <th className="text-center text-nowrap">
                      Policy Conditions
                    </th>
                  )}
                  <th>Accesses</th>
                  <th>Masking Condition</th>
                </tr>
              </thead>
              <tbody>
                {getMaskingPolicyData(props.policyData.dataMaskPolicyItems)}
              </tbody>
            </Table>
          </div>
        </>
      )}

      {props.policyData.policyType == 2 && (
        <>
          <p className="form-header">Row Level Conditions</p>
          <div className="overflow-x-auto">
            <Table
              bordered
              size="sm"
              className="mb-3 table-audit-filter-ready-only"
            >
              <thead>
                <tr>
                  <th>Roles</th>
                  <th>Groups</th>
                  <th>Users</th>
                  <th>Accesses</th>
                  <th>Row Level Filter</th>
                </tr>
              </thead>
              <tbody>
                {getRowLevelPolicyData(props.policyData.rowFilterPolicyItems)}
              </tbody>
            </Table>
          </div>
        </>
      )}
    </React.Fragment>
  );
}
