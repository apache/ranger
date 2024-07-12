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

import React, { useState, useEffect } from "react";
import { fetchApi } from "Utils/fetchAPI";
import { Table, Badge, Row, Col } from "react-bootstrap";
import { RangerPolicyType, DefStatus } from "../../../utils/XAEnums";
import dateFormat from "dateformat";
import { toast } from "react-toastify";
import { cloneDeep, find, isEmpty, map, sortBy } from "lodash";
import { getResourcesDefVal, serverError } from "../../../utils/XAUtils";
import { ModalLoader } from "../../../components/CommonComponents";
import { getServiceDef } from "../../../utils/appState";

export function PolicyViewDetails(props) {
  const isMultiResources = true;
  const [access, setAccess] = useState([]);
  const [loader, SetLoader] = useState(true);
  const [serviceDef, setServiceDef] = useState({});
  const { updateServices } = props;
  let { allServiceDefs } = cloneDeep(getServiceDef());

  useEffect(() => {
    if (props.paramsData.isRevert) {
      fetchPolicyVersions();
    } else {
      if (props.paramsData.isChangeVersion) {
        fetchByVersion();
      } else {
        fetchInitialData();
      }
    }
  }, [
    props.paramsData.isRevert
      ? props.paramsData.isRevert
      : props.paramsData.version || props.paramsData.policyVersion
  ]);

  const fetchInitialData = async () => {
    await fetchByEventTime();
  };

  const fetchByEventTime = async () => {
    let accesslogs;
    let { policyView, paramsData } = props;
    let paramsVal = !policyView
      ? {
          eventTime: paramsData.eventTime,
          policyId: paramsData.policyId,
          versionNo: paramsData.policyVersion
        }
      : "";
    let accessLogsServiceDef;

    try {
      accesslogs = await fetchApi({
        url: policyView
          ? `plugins/policies/${paramsData.id}`
          : "plugins/policies/eventTime",
        params: paramsVal
      });
    } catch (error) {
      console.error(`eventTime can not be undefined ${error}`);
    }
    accessLogsServiceDef = allServiceDefs?.find((servicedef) => {
      return servicedef.name == accesslogs?.data?.serviceType;
    });
    setAccess(accesslogs?.data);
    setServiceDef(accessLogsServiceDef);
    SetLoader(false);
  };

  const fetchByVersion = async () => {
    let accesslogs;
    try {
      accesslogs = await fetchApi({
        url: `plugins/policy/${
          props.paramsData.id || props.paramsData.policyId
        }/version/${props.paramsData.version || props.paramsData.policyVersion}`
      });
    } catch (error) {
      console.error(`versionNo can not be undefined ${error}`);
    }
    setAccess(accesslogs?.data);
  };

  const fetchPolicyVersions = async () => {
    let accesslogs;
    let policyId = props.paramsData.id;
    try {
      accesslogs = await fetchApi({
        url: `plugins/policies/${policyId}`,
        method: "PUT",
        data: access
      });
      updateServices();
      toast.success("Policy reverted successfully");
    } catch (error) {
      console.error(
        `Error occurred while fetching Policy Version or CSRF headers! ${error}`
      );
      serverError(error);
    }
    setAccess(accesslogs?.data);
  };

  const {
    service,
    serviceType,
    policyType,
    id,
    version,
    policyLabels,
    name,
    description,
    resources,
    conditions,
    dataMaskPolicyItems,
    rowFilterPolicyItems,
    policyItems,
    isEnabled,
    allowExceptions,
    denyPolicyItems,
    denyExceptions,
    isDenyAllElse,
    isAuditEnabled,
    policyPriority,
    updatedBy,
    updateTime,
    createdBy,
    createTime,
    validitySchedules,
    zoneName,
    additionalResources
  } = access;
  let additionalResourcesVal = [];
  if (isMultiResources) {
    additionalResourcesVal = [resources, ...(additionalResources || [])];
  }

  const getPolicyResources = (policyType, resourceval) => {
    var filterResources = [];
    let serviceTypeData = serviceDef;
    var resourceDef = getResourcesDefVal(serviceTypeData, policyType);
    for (let key in resourceval) {
      let filterResourcesVal = find(resourceDef, { name: key });
      let resource = {};
      resource.label = filterResourcesVal && filterResourcesVal.label;
      resource.level = filterResourcesVal && filterResourcesVal.level;
      resource.values = resourceval[key].values;
      if (filterResourcesVal && filterResourcesVal.recursiveSupported) {
        resource.Rec_Recursive = resourceval[key].isRecursive
          ? DefStatus.RecursiveStatus.STATUS_RECURSIVE.label
          : DefStatus.RecursiveStatus.STATUS_NONRECURSIVE.label;
      }
      if (filterResourcesVal && filterResourcesVal.excludesSupported) {
        resource.Rec_Exc = resourceval[key].isExcludes
          ? DefStatus.ExcludeStatus.STATUS_EXCLUDE.label
          : DefStatus.ExcludeStatus.STATUS_INCLUDE.label;
      }
      filterResources.push(resource);
    }
    return (
      <>
        {sortBy(filterResources, "level").map((obj) => (
          <tr key={obj.level}>
            <td>{obj.label}</td>
            <td>
              <Row>
                <Col md={9} className="d-flex flex-wrap">
                  {obj.values.map((val, index) => (
                    <Badge
                      className="d-inline me-1 text-start"
                      bg="info"
                      key={index}
                    >
                      <span className="d-inline me-1 item" key={val}>
                        {val}
                      </span>
                    </Badge>
                  ))}
                </Col>
                <Col className="text-end" md={3}>
                  <h6 className="d-inline me-1">
                    <Badge bg="dark text-capitalize">{obj.Rec_Exc}</Badge>
                  </h6>

                  <h6 className="d-inline me-1">
                    <Badge bg="dark text-capitalize">{obj.Rec_Recursive}</Badge>
                  </h6>
                </Col>
              </Row>
            </td>
          </tr>
        ))}
      </>
    );
  };

  const getPolicyDetails = () => {
    const getPolicyType = (policyTypeVal) => {
      if (policyTypeVal == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value) {
        return (policyTypeVal = (
          <h6>
            <Badge bg="primary">
              {RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.label}
            </Badge>
          </h6>
        ));
      }
      if (policyTypeVal == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value) {
        return (policyTypeVal = (
          <h6>
            <Badge bg="primary">
              {RangerPolicyType.RANGER_MASKING_POLICY_TYPE.label}
            </Badge>
          </h6>
        ));
      }
      if (
        policyTypeVal == RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value
      ) {
        return (policyTypeVal = (
          <h6>
            <Badge bg="primary">
              {RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.label}
            </Badge>
          </h6>
        ));
      }
      return (
        <h6>
          <Badge bg="primary">{policyTypeVal}</Badge>
        </h6>
      );
    };

    const getPolicyName = (name) => {
      return (
        <>
          <span className="float-start">{name}</span>
          <br />
          <div className="text-end">
            <h6 className="d-inline me-1">
              <Badge bg="dark">
                {policyPriority == 1 ? "Override" : "Normal"}
              </Badge>
            </h6>

            <h6 className="d-inline me-1">
              <Badge bg="dark">
                {isEnabled == true ? "Enabled" : "Disabled"}
              </Badge>
            </h6>
          </div>
        </>
      );
    };

    return loader ? (
      <ModalLoader />
    ) : (
      <>
        <tr>
          <td className="text-nowrap">Policy Type</td>
          <td>{getPolicyType(policyType)}</td>
        </tr>
        <tr>
          <td className="text-nowrap">Policy ID</td>
          <td>
            <h6 className="d-inline me-1">
              <Badge bg="primary">{id}</Badge>
            </h6>
          </td>
        </tr>
        <tr>
          <td className="text-nowrap">Version</td>
          <td>
            <h6 className="d-inline me-1">
              <Badge bg="primary">{version}</Badge>
            </h6>
          </td>
        </tr>
        <tr>
          <td className="text-nowrap">Policy Name </td>
          <td className="text-break">{getPolicyName(name)}</td>
        </tr>
        <tr>
          <td className="text-nowrap">Policy Labels </td>
          <td className="text-break">
            {!isEmpty(policyLabels)
              ? policyLabels.map((policyLabel, index) => (
                  <Badge
                    bg="dark"
                    className="me-1 more-less-width text-truncate"
                    key={index}
                  >
                    {policyLabel}
                  </Badge>
                ))
              : "--"}
          </td>
        </tr>
        {!isMultiResources && getPolicyResources(policyType, resources)}
        <tr>
          <td className="text-nowrap">Description</td>
          <td className="text-break">
            {!isEmpty(description) ? description : "--"}
          </td>
        </tr>
        <tr>
          <td className="text-nowrap">Audit Logging </td>
          <td>
            <h6 className="d-inline me-1">
              <Badge bg="info">{isAuditEnabled == true ? "Yes" : "No"}</Badge>
            </h6>
          </td>
        </tr>
        {!isEmpty(zoneName) && (
          <tr>
            <td className="text-nowrap">Zone Name </td>
            <td className="text-break">
              <h6 className="d-inline me-1">
                <Badge bg="dark">{zoneName}</Badge>
              </h6>
            </td>
          </tr>
        )}
      </>
    );
  };

  const getFilterPolicy = (
    policyItemsVal,
    serviceDef,
    serviceType,
    noTblDataMsg
  ) => {
    let tableRow = [];
    let filterServiceDef = serviceDef;
    const getMaskingLabel = (label) => {
      let filterLabel = "";
      filterServiceDef.dataMaskDef.maskTypes.map((obj) => {
        return obj.name == label ? (filterLabel = obj.label) : "--";
      });
      return (
        <h6 className="d-inline me-1">
          <Badge bg="info">{filterLabel}</Badge>
        </h6>
      );
    };
    tableRow.push(
      <>
        <thead>
          <tr>
            <th className="text-center text-nowrap">Select Role </th>
            <th className="text-center text-nowrap">Select Group </th>
            <th className="text-center text-nowrap">Select User</th>
            {!isEmpty(
              filterServiceDef && filterServiceDef.policyConditions
            ) && <th className="text-center text-nowrap">Policy Conditions</th>}
            <th className="text-center text-nowrap">
              {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value
                ? "Permissions"
                : "Access Types"}
            </th>
            {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value &&
              serviceType != "tag" && (
                <th className="text-center text-nowrap">Delegate Admin</th>
              )}
            {policyType ==
              RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value && (
              <th className="text-center text-nowrap">Select Masking Option</th>
            )}
            {policyType ==
              RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value && (
              <th className="text-center text-nowrap">Row Level Filter</th>
            )}
          </tr>
        </thead>
      </>
    );

    {
      !isEmpty(policyItemsVal)
        ? policyItemsVal.map((items, index) =>
            tableRow.push(
              <>
                <tbody>
                  <tr key={index}>
                    <td className="text-center">
                      {!isEmpty(items.roles)
                        ? items.roles.map((role) => (
                            <Badge
                              bg="info"
                              className="text-truncate me-1 more-less-width"
                              key={role}
                            >
                              {role}
                            </Badge>
                          ))
                        : "--"}
                    </td>

                    <td className="text-center">
                      {!isEmpty(items.groups)
                        ? items.groups.map((group) => (
                            <Badge
                              bg="info"
                              className="text-truncate me-1 more-less-width"
                              key={group}
                            >
                              {group}
                            </Badge>
                          ))
                        : "--"}
                    </td>
                    <td className="text-center">
                      {!isEmpty(items.users)
                        ? items.users.map((user) => (
                            <Badge
                              bg="info"
                              key={user}
                              className="text-truncate me-1 more-less-width "
                            >
                              {user}
                            </Badge>
                          ))
                        : "--"}
                    </td>
                    {!isEmpty(
                      filterServiceDef && filterServiceDef.policyConditions
                    ) && (
                      <td className="text-center">
                        {!isEmpty(items.conditions)
                          ? items.conditions.map((obj, index) => {
                              let conditionObj =
                                filterServiceDef.policyConditions.find((e) => {
                                  return e.name == obj.type;
                                });
                              return (
                                <h6 className="d-inline me-1" key={index}>
                                  <Badge
                                    bg="info"
                                    className="d-inline me-1"
                                    key={obj.values}
                                  >{`${conditionObj.label}: ${obj.values.join(
                                    ", "
                                  )}`}</Badge>
                                </h6>
                              );
                            })
                          : "--"}
                      </td>
                    )}

                    {!isEmpty(items.accesses) ? (
                      <td className="text-center d-flex flex-wrap policyview-permission-wrap">
                        {" "}
                        {items.accesses.map((obj, index) => (
                          <h6 className="d-inline me-1" key={index}>
                            <Badge
                              bg="info"
                              className="d-inline me-1"
                              key={obj.type}
                            >
                              {obj.type}
                            </Badge>
                          </h6>
                        ))}
                      </td>
                    ) : (
                      <td className="text-center">--</td>
                    )}

                    {policyType ==
                      RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value &&
                      serviceType != "tag" && (
                        <td className="text-center">
                          <input
                            type="checkbox"
                            checked={
                              items.delegateAdmin === false ? false : true
                            }
                            disabled="disabled"
                          />
                        </td>
                      )}
                    {policyType ==
                      RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value && (
                      <td className="text-center">
                        {getMaskingLabel(items.dataMaskInfo.dataMaskType)}
                      </td>
                    )}
                    {policyType ==
                      RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value && (
                      <td className="text-center">
                        {items.rowFilterInfo.filterExp == undefined ? (
                          <h6 className="d-inline me-1">
                            <Badge bg="info">
                              {items.rowFilterInfo.filterExpr}
                            </Badge>
                          </h6>
                        ) : (
                          "--"
                        )}
                      </td>
                    )}
                  </tr>
                </tbody>
              </>
            )
          )
        : tableRow.push(
            <tbody>
              <tr>
                <td className="text-center text-muted" colSpan={6}>
                  {noTblDataMsg}
                </td>
              </tr>
            </tbody>
          );
    }

    return tableRow;
  };

  const getPolicyConditions = (conditions, serviceDef) => {
    const getConditionLabel = (label) => {
      let filterLabel = find(serviceDef.policyConditions, { name: label });

      return filterLabel && filterLabel?.label ? filterLabel.label : "";
    };
    return (
      !isEmpty(conditions) && (
        <>
          <p className="form-header">Policy Conditions :</p>
          <div className="overflow-auto">
            <Table bordered size="sm" className="table-audit-filter-ready-only">
              <tbody>
                {conditions.map((obj) => (
                  <tr key={obj.type} colSpan="2">
                    <td width="40%">{getConditionLabel(obj.type)}</td>
                    <td width="60% text-truncate">{obj.values.join(", ")}</td>
                  </tr>
                ))}
              </tbody>
            </Table>
          </div>
        </>
      )
    );
  };

  const getValidityPeriod = (validity) => {
    let filterValidity = validity.map(function (obj) {
      return {
        startTime: obj.startTime,
        endTime: obj.endTime,
        timeZone: obj.timeZone
      };
    });
    return (
      !isEmpty(validity) && (
        <>
          <p className="form-header">Validity Period :</p>
          <Table bordered size="sm" className="table-audit-filter-ready-only">
            <thead>
              <tr>
                <th className="text-center text-nowrap">Start Time</th>
                <th className="text-center text-nowrap">End Time</th>
                <th className="text-center text-nowrap">Time zone</th>
              </tr>
            </thead>
            <tbody>
              {filterValidity.map((obj, index) => (
                <tr key={index}>
                  <td className="text-center">
                    {!isEmpty(obj.startTime) ? (
                      <strong>{obj.startTime}</strong>
                    ) : (
                      "--"
                    )}
                  </td>
                  <td className="text-center">
                    {!isEmpty(obj.endTime) ? (
                      <strong>{obj.endTime}</strong>
                    ) : (
                      "--"
                    )}
                  </td>
                  <td className="text-center">
                    {!isEmpty(obj.timeZone) ? (
                      <h6 className="d-inline">
                        <Badge bg="info">{obj.timeZone}</Badge>
                      </h6>
                    ) : (
                      "--"
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </Table>
        </>
      )
    );
  };

  return loader ? (
    <ModalLoader />
  ) : (
    <>
      <div>
        <div>
          <p>
            <strong>Service Name :</strong> {service}
          </p>
        </div>
        <div>
          <p>
            <strong>Service Type :</strong> {serviceType}
          </p>
        </div>
      </div>
      <p className="form-header">Policy Details :</p>
      <div className="overflow-auto">
        <Table bordered size="sm" className="table-audit-filter-ready-only">
          <tbody>{getPolicyDetails(serviceDef)}</tbody>
        </Table>
      </div>
      {isMultiResources && (
        <>
          <p className="form-header">Policy Resource :</p>
          {additionalResourcesVal &&
            map(additionalResourcesVal, (resourcesVal, index) => (
              <>
                <Table
                  bordered
                  size="sm"
                  className="table-audit-filter-ready-only"
                  key={index}
                >
                  <thead>
                    <tr>
                      <th className="text-start" colSpan={2}>
                        #{index + 1}
                      </th>
                    </tr>
                  </thead>
                  <tbody>{getPolicyResources(policyType, resourcesVal)}</tbody>
                </Table>
              </>
            ))}
        </>
      )}
      {(policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value ||
        RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value) &&
        !isEmpty(validitySchedules) &&
        getValidityPeriod(validitySchedules)}
      {/* Get Policy Condition */}
      {getPolicyConditions(conditions, serviceDef)}
      {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value && (
        <>
          <p className="form-header">Allow Conditions :</p>
          <div className="overflow-auto">
            <Table bordered size="sm" className="table-audit-filter-ready-only">
              {getFilterPolicy(
                policyItems,
                serviceDef,
                serviceType,
                ` No policy items of "Allow Conditions" are present`
              )}
            </Table>
          </div>

          <br />
        </>
      )}
      {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value &&
        serviceDef?.options?.enableDenyAndExceptionsInPolicies == "true" && (
          <>
            <p className="form-header">Exclude from Allow Conditions :</p>
            <div className="overflow-auto">
              <Table
                bordered
                size="sm"
                className="table-audit-filter-ready-only"
              >
                {getFilterPolicy(
                  allowExceptions,
                  serviceDef,
                  serviceType,
                  `No policy items of "Exclude from Allow Conditions" are present`
                )}
              </Table>
            </div>
            <br />
          </>
        )}
      {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value &&
        serviceDef?.options?.enableDenyAndExceptionsInPolicies == "true" && (
          <>
            <b>
              Deny All Other Accesses :{" "}
              {isDenyAllElse == false ? (
                <h6 className="d-inline">
                  <Badge bg="dark">FALSE</Badge>
                </h6>
              ) : (
                <h6 className="d-inline">
                  <Badge bg="dark">TRUE</Badge>
                </h6>
              )}
              <br />
            </b>

            <br />
          </>
        )}
      {isDenyAllElse == false &&
        policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value &&
        serviceDef?.options?.enableDenyAndExceptionsInPolicies == "true" && (
          <>
            <p className="form-header">Deny Conditions :</p>
            <div className="overflow-auto">
              <Table
                bordered
                size="sm"
                className="table-audit-filter-ready-only"
              >
                {getFilterPolicy(
                  denyPolicyItems,
                  serviceDef,
                  serviceType,
                  ` No policy items of "Deny Conditions" are present`
                )}
              </Table>
            </div>
            <br />
          </>
        )}
      {isDenyAllElse == false &&
        policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value &&
        serviceDef?.options?.enableDenyAndExceptionsInPolicies == "true" && (
          <>
            <p className="form-header">Exclude from Deny Conditions :</p>
            <div className="overflow-auto">
              <Table
                bordered
                size="sm"
                className="table-audit-filter-ready-only"
              >
                {getFilterPolicy(
                  denyExceptions,
                  serviceDef,
                  serviceType,
                  `No policy items of "Exclude from Deny Conditions" are present`
                )}
              </Table>
            </div>
          </>
        )}
      {policyType == RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value && (
        <>
          <p className="form-header">Row Level Conditions :</p>
          <div className="overflow-auto">
            <Table bordered size="sm" className="table-audit-filter-ready-only">
              {getFilterPolicy(
                rowFilterPolicyItems,
                serviceDef,
                serviceType,
                `No policy items of "Row Level Conditions" are present`
              )}
            </Table>
          </div>
        </>
      )}
      {policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value && (
        <>
          <p className="form-header">Masking Conditions :</p>
          <div className="overflow-auto">
            <Table bordered size="sm" className="table-audit-filter-ready-only">
              {getFilterPolicy(
                dataMaskPolicyItems,
                serviceDef,
                serviceType,
                ` No policy items of "Masking Conditions" are present`
              )}
            </Table>
          </div>
        </>
      )}
      <div className="updateInfo clearfix">
        <div className="float-start">
          <p>
            <strong>Updated By : </strong> {updatedBy}
          </p>
          <p>
            <strong>Updated On : </strong>
            {dateFormat(updateTime, "mm/dd/yyyy hh:MM TT ")}
          </p>
        </div>
        <div className="float-end">
          <p>
            <strong>Created By : </strong> {createdBy}
          </p>
          <p>
            <strong>Created On : </strong>{" "}
            {dateFormat(createTime, "mm/dd/yyyy hh:MM TT ")}
          </p>
        </div>
      </div>
    </>
  );
}

export default PolicyViewDetails;
