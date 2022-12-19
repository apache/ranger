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
import { Loader } from "Components/CommonComponents";
import { RangerPolicyType, DefStatus } from "../../../utils/XAEnums";
import dateFormat from "dateformat";
import { toast } from "react-toastify";
import { find, isEmpty, sortBy } from "lodash";
import { serverError } from "../../../utils/XAUtils";
import { ContentLoader } from "../../../components/CommonComponents";

export function PolicyViewDetails(props) {
  const [access, setAccess] = useState([]);
  const [loader, SetLoader] = useState(true);
  const { serviceDef, updateServices } = props;

  useEffect(() => {
    if (props.paramsData.isRevert) {
      fetchPolicyVersions();
    } else {
      if (props.paramsData.isChangeVersion) {
        fetchByVersion();
      } else {
        fetchInitalData();
      }
    }
  }, [
    props.paramsData.isRevert
      ? props.paramsData.isRevert
      : props.paramsData.version || props.paramsData.policyVersion
  ]);
  const fetchInitalData = async () => {
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

    setAccess(accesslogs.data);
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
    setAccess(accesslogs.data);
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
    setAccess(accesslogs && accesslogs.data);
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
    validitySchedules
  } = access;

  const getPolicyDetails = () => {
    const getPolicyType = (policyTypeVal) => {
      if (policyTypeVal == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value) {
        return (policyTypeVal = (
          <h6>
            <Badge variant="primary">
              {RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.label}
            </Badge>
          </h6>
        ));
      }
      if (policyTypeVal == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value) {
        return (policyTypeVal = (
          <h6>
            <Badge variant="primary">
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
            <Badge variant="primary">
              {RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.label}
            </Badge>
          </h6>
        ));
      }
      return (
        <h6>
          <Badge variant="primary">{policyTypeVal}</Badge>
        </h6>
      );
    };

    const getPolicyName = (name) => {
      return (
        <>
          <span className="float-left">{name}</span>
          <br />
          <div className="text-right">
            <h6 className="d-inline mr-1">
              <Badge variant="dark">
                {policyPriority == 1 ? "Override" : "Normal"}
              </Badge>
            </h6>

            <h6 className="d-inline mr-1">
              <Badge variant="dark">
                {isEnabled == true ? "Enabled" : "Disabled"}
              </Badge>
            </h6>
          </div>
        </>
      );
    };

    const getPolicyResources = (policyType, resourceval) => {
      var resourceDef;
      var filterResources = [];
      let serviceTypeData = serviceDef;
      if (policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value) {
        resourceDef = serviceTypeData && serviceTypeData.resources;
      }
      if (policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value) {
        resourceDef = serviceTypeData && serviceTypeData.dataMaskDef.resources;
      }
      if (policyType == RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value) {
        resourceDef = serviceTypeData && serviceTypeData.rowFilterDef.resources;
      }
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
                  <Col>
                    {obj.values.map((val) => (
                      <h6 className="d-inline mr-1">
                        <Badge
                          className="d-inline mr-1"
                          variant="info"
                          key={val}
                        >
                          {val}
                        </Badge>
                      </h6>
                    ))}
                  </Col>
                  <Col className="text-right">
                    <h6 className="d-inline mr-1">
                      <Badge variant="dark">{obj.Rec_Exc}</Badge>
                    </h6>

                    <h6 className="d-inline mr-1">
                      <Badge variant="dark">{obj.Rec_Recursive}</Badge>
                    </h6>
                  </Col>
                </Row>
              </td>
            </tr>
          ))}
        </>
      );
    };

    return loader ? (
      <Loader />
    ) : (
      <>
        <tr>
          <td className="text-nowrap">Policy Type</td>
          <td>{getPolicyType(policyType)}</td>
        </tr>
        <tr>
          <td className="text-nowrap">Policy ID</td>
          <td>
            <h6 className="d-inline mr-1">
              <Badge variant="primary">{id}</Badge>
            </h6>
          </td>
        </tr>
        <tr>
          <td className="text-nowrap">Version</td>
          <td>
            <h6 className="d-inline mr-1">
              <Badge variant="primary">{version}</Badge>
            </h6>
          </td>
        </tr>
        <tr>
          <td className="text-nowrap">Policy Name </td>
          <td>{getPolicyName(name)}</td>
        </tr>
        <tr>
          <td className="text-nowrap">Policy Labels </td>
          <td>
            {!isEmpty(policyLabels)
              ? policyLabels.map((policyLabel) => (
                  <Badge
                    variant="dark"
                    className="mr-1 more-less-width text-truncate"
                  >
                    {policyLabel}
                  </Badge>
                ))
              : "--"}
          </td>
        </tr>
        {getPolicyResources(policyType, resources)}
        <tr>
          <td className="text-nowrap">Description</td>
          <td>{description}</td>
        </tr>
        <tr>
          <td className="text-nowrap">Audit Logging </td>
          <td>
            <h6 className="d-inline mr-1">
              <Badge variant="info">
                {isAuditEnabled == true ? "Yes" : "No"}
              </Badge>
            </h6>
          </td>
        </tr>
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
        <h6 className="d-inline mr-1">
          <Badge variant="info">{filterLabel}</Badge>
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
                              variant="info"
                              className="text-truncate mr-1 more-less-width"
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
                              variant="info"
                              className="text-truncate mr-1 more-less-width"
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
                              variant="info"
                              key={user}
                              className="text-truncate mr-1 more-less-width "
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
                          ? items.conditions.map((obj) => {
                              return (
                                <h6 className="d-inline mr-1">
                                  <Badge
                                    variant="info"
                                    className="d-inline mr-1"
                                    key={obj.values}
                                  >{`${obj.type}: ${obj.values.join(
                                    ", "
                                  )}`}</Badge>
                                </h6>
                              );
                            })
                          : "--"}
                      </td>
                    )}
                    <td className="text-center">
                      {!isEmpty(items.accesses)
                        ? items.accesses.map((obj) => (
                            <h6 className="d-inline mr-1">
                              <Badge
                                variant="info"
                                className="d-inline mr-1"
                                key={obj.type}
                              >
                                {obj.type}
                              </Badge>
                            </h6>
                          ))
                        : "--"}
                    </td>
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
                          <h6 className="d-inline mr-1">
                            <Badge variant="info">
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

  const getPolicyConditions = (conditions, serviceDef, serviceType) => {
    let filterServiceDef = serviceDef;
    const getConditionLabel = (label) => {
      let filterLabel = "";
      filterServiceDef.policyConditions.map((obj) =>
        obj.name == label ? (filterLabel = obj.label) : ""
      );
      return filterLabel;
    };
    return (
      !isEmpty(conditions) && (
        <>
          <p className="form-header">Policy Conditions :</p>
          <Table bordered size="sm" className="table-audit-filter-ready-only">
            <tbody>
              {serviceType == "tag" ? (
                conditions.map((obj) => (
                  <tr key={obj.type} colSpan="2">
                    <td width="40%">{getConditionLabel(obj.type)}</td>
                    <td width="60%">{obj.values}</td>
                  </tr>
                ))
              ) : (
                <tr colSpan="2">
                  <td width="20%">
                    {filterServiceDef.policyConditions.map((obj) => obj.label)}
                  </td>
                  <td className="text-left">
                    {conditions.map((val) => val.values).join("")}
                  </td>
                </tr>
              )}
            </tbody>
          </Table>
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
                        <Badge variant="info">{obj.timeZone}</Badge>
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
    <ContentLoader size="50px" />
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
      <div className="overflow-auto">
        <p className="form-header">Policy Details :</p>
        <Table bordered size="sm" className="table-audit-filter-ready-only">
          <tbody>{getPolicyDetails(serviceDef)}</tbody>
        </Table>
      </div>
      {(policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value ||
        RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value) &&
        !isEmpty(validitySchedules) &&
        getValidityPeriod(validitySchedules)}
      {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value && (
        <>{getPolicyConditions(conditions, serviceDef, serviceType)}</>
      )}
      {policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value &&
        serviceType == "tag" && (
          <>{getPolicyConditions(conditions, serviceDef, serviceType)}</>
        )}

      {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value && (
        <>
          <p className="form-header">Allow Condition :</p>
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

      {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value && (
        <>
          <p className="form-header">Exclude from Allow Conditions :</p>
          <div className="overflow-auto">
            <Table bordered size="sm" className="table-audit-filter-ready-only">
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
      {policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value && (
        <>
          <b>
            Deny All Other Accesses :{" "}
            {isDenyAllElse == false ? (
              <h6 className="d-inline">
                <Badge variant="dark">FALSE</Badge>
              </h6>
            ) : (
              <h6 className="d-inline">
                <Badge variant="dark">TRUE</Badge>
              </h6>
            )}
            <br />
          </b>

          <br />
        </>
      )}
      {isDenyAllElse == false &&
        policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value && (
          <>
            <p className="form-header">Deny Condition :</p>
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
                  ` No policy items of "Deny Condition" are present`
                )}
              </Table>
            </div>
            <br />
          </>
        )}
      {isDenyAllElse == false &&
        policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value && (
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
      <div class="updateInfo">
        <div class="pull-left">
          <p>
            <strong>Updated By : </strong> {updatedBy}
          </p>
          <p>
            <strong>Updated On : </strong>
            {dateFormat(updateTime, "mm/dd/yyyy hh:MM TT ")}
          </p>
        </div>
        <div class="pull-right">
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
