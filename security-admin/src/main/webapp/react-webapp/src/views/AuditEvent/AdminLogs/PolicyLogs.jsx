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
import { Table, Badge, Row, Col } from "react-bootstrap";
import dateFormat from "dateformat";
import { ClassTypes } from "../../../utils/XAEnums";
import {
  isEmpty,
  isEqual,
  isUndefined,
  unionBy,
  difference,
  differenceWith,
  forEach,
  filter,
  map,
  includes,
  split
} from "lodash";
import { currentTimeZone } from "../../../utils/XAUtils";

export const PolicyLogs = ({ data, reportdata }) => {
  const {
    objectName,
    objectClassType,
    parentObjectName,
    createDate,
    owner,
    action,
    objectId
  } = data;

  /* CREATE LOGS VARIABLES */
  const createPolicyDetails = reportdata.filter((policy) => {
    return (
      policy.action == "create" &&
      policy.attributeName != "Policy Resources" &&
      policy.attributeName != "Policy Conditions" &&
      policy.attributeName != "Policy Items" &&
      policy.attributeName != "DenyPolicy Items" &&
      policy.attributeName != "Allow Exceptions" &&
      policy.attributeName != "Deny Exceptions" &&
      policy.attributeName != "Masked Policy Items" &&
      policy.attributeName != "Row level filter Policy Items" &&
      policy.attributeName != "Validity Schedules"
    );
  });

  const createPolicyResources = reportdata.filter((resources) => {
    return (
      resources.attributeName == "Policy Resources" &&
      resources.action == "create"
    );
  });
  const createPolicyItems = reportdata.filter(
    (obj) => obj.attributeName == "Policy Items" && obj.action == "create"
  );
  const policyDetails = (details, resources) => {
    let tablerow = [];

    const createDetailsPolicy = (val, newVal) => {
      if (val == "Policy Labels") {
        return (newVal = !isEmpty(JSON.parse(newVal))
          ? JSON.parse(newVal).join(", ")
          : "--");
      }
      if (val == "Policy Status") {
        return (newVal = newVal === "false" ? "disabled" : "enabled");
      }
      return !isEmpty(newVal) ? newVal : "--";
    };

    details.map((policy) => {
      return tablerow.push(
        <tr>
          <td className="table-warning policyitem-field">
            {policy.attributeName}
          </td>
          <td className="table-warning policyitem-field">
            {!isEmpty(policy.newValue)
              ? createDetailsPolicy(policy.attributeName, policy.newValue)
              : "--"}
          </td>
        </tr>
      );
    });

    let newVal = {};
    resources.map((obj) => {
      newVal = !isEmpty(obj.newValue) && JSON.parse(obj.newValue);
    });

    !isEmpty(newVal) &&
      Object.keys(newVal).map((key, index) => {
        return tablerow.push(
          <>
            <tr key={index}>
              <td className="table-warning text-nowrap policyitem-field">
                {key}
              </td>
              <td className="table-warning">
                {" "}
                {newVal[key].values.join(", ")}
              </td>
            </tr>
            <tr>
              <td className="table-warning text-nowrap policyitem-field">
                {key + " " + "exclude"}
              </td>
              <td className="table-warning">
                {newVal[key].isExcludes == false ? "false" : "true"}
              </td>
            </tr>
            <tr>
              <td className="table-warning text-nowrap policyitem-field">
                {key + " " + "recursive"}
              </td>
              <td className="table-warning policyitem-field">
                {newVal[key].isRecursive == false ? "false" : "true"}
              </td>
            </tr>
          </>
        );
      });
    return tablerow;
  };
  const createValidity = reportdata.filter(
    (obj) => obj.attributeName == "Validity Schedules" && obj.action == "create"
  );
  const createValidityNew = createValidity.map(
    (obj) => !isEmpty(obj.newValue) && JSON.parse(obj.newValue)
  );
  const createCondition = reportdata.filter((obj) => {
    return obj.attributeName == "Policy Conditions" && obj.action == "create";
  });
  const createConditionNew = createCondition.map((obj) => obj.newValue);
  const createRowMask = reportdata.filter(
    (obj) =>
      obj.attributeName == "Row level filter Policy Items" &&
      obj.action == "create"
  );
  const createRowMaskNew = createRowMask.map((newval) => newval.newValue);

  const createException = reportdata.filter(
    (obj) => obj.attributeName == "Allow Exceptions" && obj.action == "create"
  );
  const createExceptionNew = createException.map((newval) => newval.newValue);
  const createDenyPolicy = reportdata.filter(
    (obj) => obj.attributeName == "DenyPolicy Items" && obj.action == "create"
  );
  const createDenyPolicyNew = createDenyPolicy.map((newval) => newval.newValue);

  const cerateDenyException = reportdata.filter(
    (obj) => obj.attributeName == "Deny Exceptions" && obj.action == "create"
  );
  const createDenyExceptionNew = cerateDenyException.map(
    (newval) => newval.newValue
  );
  const createMaskPolicy = reportdata.filter(
    (obj) => obj.attributeName == "Masked Policy Items"
  );
  const createMaskPolicyNew = createMaskPolicy.map((newval) => newval.newValue);

  const getDataMaskType = (dataMaskLabel, dataMaskInfo) => {
    let maskType = dataMaskInfo?.dataMaskType;

    if (!isEmpty(dataMaskLabel)) {
      if (dataMaskLabel == "Custom") {
        maskType = dataMaskLabel + " : " + dataMaskInfo?.valueExpr;
      } else {
        maskType = dataMaskLabel;
      }
    } else {
      if (includes(dataMaskInfo.dataMaskType, "CUSTOM")) {
        maskType =
          split(dataMaskInfo.dataMaskType, ":").pop() +
          " : " +
          dataMaskInfo?.valueExpr;
      } else {
        maskType = split(dataMaskInfo.dataMaskType, ":").pop();
      }
    }

    return maskType;
  };

  /* CREATE END */

  /* UPDATE LOGS VARIABLES */

  const updateValidity = reportdata.filter(
    (obj) => obj.attributeName == "Validity Schedules" && obj.action == "update"
  );
  const updateValidityOld = updateValidity.map((obj) => obj.previousValue);
  const updateValidityNew = updateValidity.map((obj) => obj.newValue);

  const updateMaskPolicy = reportdata.filter(
    (obj) =>
      obj.attributeName == "Masked Policy Items" && obj.action == "update"
  );
  const updateRowMask = reportdata.filter(
    (obj) =>
      obj.attributeName == "Row level filter Policy Items" &&
      obj.action == "update"
  );
  const updateRowMaskOld = updateRowMask.map(
    (obj) => obj.previousValue && obj.action == "update"
  );
  const updateRowMaskNew = updateRowMask.map(
    (obj) => obj.newValue && obj.action == "update"
  );
  const updatePolicyItems = reportdata.filter(
    (obj) => obj.attributeName == "Policy Items" && obj.action == "update"
  );
  const updateException = reportdata.filter(
    (obj) => obj.attributeName == "Allow Exceptions" && obj.action == "update"
  );
  const updateDenyPolicy = reportdata.filter(
    (obj) => obj.attributeName == "DenyPolicy Items" && obj.action == "update"
  );
  const updateDenyException = reportdata.filter(
    (obj) => obj.attributeName == "Deny Exceptions" && obj.action == "update"
  );
  const updatePolicyCondition = reportdata.filter((obj) => {
    return obj.attributeName == "Policy Conditions" && obj.action == "update";
  });

  const updatePolicyConditionOld = updatePolicyCondition.map(
    (obj) => obj.previousValue
  );
  const updatePolicyConditionNew = updatePolicyCondition.map(
    (obj) => obj.newValue
  );

  const updatePolicyDetails = reportdata.filter((policy) => {
    return (
      policy.action == "update" &&
      policy.attributeName != "Policy Resources" &&
      policy.attributeName != "Policy Conditions" &&
      policy.attributeName != "Policy Items" &&
      policy.attributeName != "DenyPolicy Items" &&
      policy.attributeName != "Allow Exceptions" &&
      policy.attributeName != "Deny Exceptions" &&
      policy.attributeName != "Masked Policy Items" &&
      policy.attributeName != "Row level filter Policy Items" &&
      policy.attributeName != "Validity Schedules"
    );
  });
  const updatePolicyResources = reportdata.filter((obj) => {
    return obj.attributeName == "Policy Resources" && obj.action == "update";
  });

  /* UPDATES */

  const policyDetailsUpdate = (details, resources) => {
    let tablerow = [];
    const policyOldVal = (val, oldVal) => {
      if (val == "Policy Labels") {
        return (oldVal = !isEmpty(JSON.parse(oldVal))
          ? JSON.parse(oldVal).join(", ")
          : "--");
      }
      if (val == "Policy Status") {
        return (oldVal = oldVal === "false" ? "disabled" : "enabled");
      }

      return !isEmpty(oldVal) ? oldVal : "--";
    };
    const policyNewVal = (val, newVal) => {
      if (val == "Policy Labels") {
        return (newVal = !isEmpty(JSON.parse(newVal))
          ? JSON.parse(newVal).join(", ")
          : "--");
      }
      if (val == "Policy Status") {
        return (newVal = newVal === "false" ? "disabled" : "enabled");
      }
      return !isEmpty(newVal) ? newVal : "--";
    };

    details.map((o, index) => {
      return tablerow.push(
        <tr key={index}>
          <td className="table-warning text-nowrap policyitem-field">
            {o.attributeName}
          </td>
          <td className="table-warning text-nowrap policyitem-field">
            {!isEmpty(o.previousValue)
              ? policyOldVal(o.attributeName, o.previousValue)
              : "--"}
          </td>
          <td className="table-warning text-nowrap  policyitem-field">
            {!isEmpty(o.newValue)
              ? policyNewVal(o.attributeName, o.newValue)
              : "--"}
          </td>
        </tr>
      );
    });

    let oldVal = {};
    let newVal = {};
    resources.map((obj) => {
      oldVal = !isEmpty(obj.previousValue) && JSON.parse(obj.previousValue);
      newVal = !isEmpty(obj.newValue) && JSON.parse(obj.newValue);
    });

    const diffVal = (obj1, obj2) => {
      let diff = {};
      if (!isEmpty(obj1)) {
        forEach(obj2, function (value, key) {
          if (obj1[key] !== undefined) {
            diff[key] = differenceWith(value.values, obj1[key].values, isEqual);
          }
        });
      } else {
        return (diff = obj2);
      }
      return diff;
    };
    let removedUsers = diffVal(newVal, oldVal);
    let addUsers = diffVal(oldVal, newVal);

    const getfilteredoldval = (val, oldvals) => {
      let filterdiff = null;
      !isEmpty(removedUsers[val])
        ? (filterdiff = difference(oldvals?.values, removedUsers[val]))
        : (filterdiff = oldvals?.values);
      return (
        <>
          {!isEqual(oldvals, newVal[val])
            ? !isEmpty(removedUsers[val])
              ? unionBy(
                  filterdiff.map((obj) => {
                    return (
                      <>
                        <span>{obj}</span>
                        {`, `}
                      </>
                    );
                  }),
                  removedUsers[val]?.map((obj) => {
                    return (
                      <>
                        <h6 className="d-inline">
                          <Badge className="d-inline-flex me-1" bg="danger">
                            {obj}
                          </Badge>
                        </h6>
                      </>
                    );
                  })
                )
              : !isEmpty(filterdiff)
              ? filterdiff.map((obj) => obj).join(", ")
              : "--"
            : !isEmpty(oldvals)
            ? oldvals.values.map((obj) => obj).join(", ")
            : "--"}
        </>
      );
    };
    const getfilterednewval = (val, newvals) => {
      let filterdiff = null;
      !isEmpty(addUsers[val])
        ? (filterdiff = difference(newvals?.values, addUsers[val]))
        : (filterdiff = newvals?.values);
      return (
        <>
          {!isEqual(newvals, oldVal[val])
            ? !isEmpty(addUsers[val])
              ? unionBy(
                  filterdiff.map((obj) => {
                    return (
                      <>
                        <span>{obj}</span>
                        {`, `}
                      </>
                    );
                  }),
                  addUsers[val].map((obj) => {
                    return (
                      <>
                        <h6 className="d-inline">
                          <Badge className="d-inline-flex me-1" bg="success">
                            {obj}
                          </Badge>
                        </h6>
                      </>
                    );
                  })
                )
              : !isEmpty(filterdiff)
              ? filterdiff.map((obj) => obj).join(", ")
              : "--"
            : !isEmpty(newvals)
            ? newvals.values.map((obj) => obj).join(", ")
            : "--"}
        </>
      );
    };
    if (
      JSON.stringify(Object.keys(oldVal)) == JSON.stringify(Object.keys(newVal))
    ) {
      Object.keys(oldVal)?.map((key, index) => {
        return tablerow.push(
          <>
            {!isEqual(oldVal[key]?.values, newVal[key]?.values) && (
              <tr>
                <td className="table-warning policyitem-field">{key}</td>
                {oldVal[key] && !isEmpty(oldVal[key].values) && (
                  <td className="table-warning policyitem-field">
                    {getfilteredoldval(key, oldVal[key])}
                  </td>
                )}
                {newVal[key] && !isEmpty(newVal[key].values) && (
                  <td className="table-warning">
                    {getfilterednewval(key, newVal[key])}
                  </td>
                )}
              </tr>
            )}
            {oldVal[key]?.isExcludes != newVal[key]?.isExcludes && (
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  {key + " " + "exclude"}
                </td>

                <td className="table-warning text-nowrap policyitem-field">
                  <h6 className="d-inline">
                    <Badge className="d-inline-flex me-1" bg="danger">
                      {oldVal[key]?.isExcludes === false ? "false" : "true"}
                    </Badge>
                  </h6>
                </td>

                <td className="table-warning  text-nowrap policyitem-field">
                  <h6 className="d-inline">
                    <Badge className="d-inline-flex me-1" bg="success">
                      {newVal[key]?.isExcludes === false ? "false" : "true"}
                    </Badge>
                  </h6>
                </td>
              </tr>
            )}
            {oldVal[key]?.isRecursive != newVal[key]?.isRecursive && (
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  {key + " " + "recursive"}
                </td>

                <td className="table-warning  text-nowrap policyitem-field">
                  <h6 className="d-inline">
                    <Badge className="d-inline-flex me-1" bg="danger">
                      {oldVal[key]?.isRecursive === false ? "false" : "true"}
                    </Badge>
                  </h6>
                </td>

                <td className="table-warning text-nowrap  policyitem-field">
                  <h6 className="d-inline">
                    <Badge className="d-inline-flex me-1" bg="success">
                      {newVal[key]?.isRecursive === false ? "false" : "true"}
                    </Badge>
                  </h6>
                </td>
              </tr>
            )}
          </>
        );
      });
    }

    if (
      JSON.stringify(Object.keys(oldVal)) !==
      JSON.stringify(Object.keys(newVal))
    ) {
      Object.keys(newVal)?.map((key, index) => {
        return tablerow.push(
          <>
            {!isEqual(newVal[key]?.values, oldVal[key]?.values) && (
              <tr>
                <td className="table-warning policyitem-field">{key}</td>
                <td className="table-warning  policyitem-field">--</td>
                {newVal[key] && !isEmpty(newVal[key].values) && (
                  <td className="table-warning policyitem-field">
                    {newVal[key].values?.map((values) => (
                      <Badge className="d-inline-flex me-1" bg="success">
                        {values}
                      </Badge>
                    ))}
                  </td>
                )}
              </tr>
            )}
            {
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  {key + " " + "exclude"}
                </td>
                <td className="table-warning  policyitem-field">--</td>
                <td className="table-warning text-nowrap policyitem-field">
                  <h6 className="d-inline">
                    <Badge className="d-inline-flex me-1" bg="success">
                      {newVal[key]?.isExcludes === false ? "false" : "true"}
                    </Badge>
                  </h6>
                </td>
              </tr>
            }
            {
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  {key + " " + "recursive"}
                </td>
                <td className="table-warning  policyitem-field">--</td>
                <td className="table-warning  text-nowrap policyitem-field">
                  <h6 className="d-inline">
                    <Badge className="d-inline-flex me-1" bg="success">
                      {newVal[key]?.isRecursive === false ? "false" : "true"}
                    </Badge>
                  </h6>
                </td>
              </tr>
            }
          </>
        );
      });

      Object.keys(oldVal)?.map((key, index) => {
        return tablerow.push(
          <>
            {!isEqual(oldVal[key]?.values, newVal[key]?.values) && (
              <tr>
                <td className="table-warning policyitem-field">{key}</td>
                {oldVal[key] && !isEmpty(oldVal[key].values) && (
                  <td className="table-warning policyitem-field">
                    {oldVal[key]?.values?.map((values) => (
                      <Badge className="d-inline-flex me-1" bg="danger">
                        {values}
                      </Badge>
                    ))}
                  </td>
                )}
                <td className="table-warning  policyitem-field">--</td>
              </tr>
            )}
            {
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  {key + " " + "exclude"}
                </td>

                <td className="table-warning text-nowrap policyitem-field">
                  <h6 className="d-inline">
                    <Badge className="d-inline-flex me-1" bg="danger">
                      {oldVal[key]?.isExcludes === false ? "false" : "true"}
                    </Badge>
                  </h6>
                </td>
                <td className="table-warning  policyitem-field">--</td>
              </tr>
            }
            {
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  {key + " " + "recursive"}
                </td>

                <td className="table-warning  text-nowrap policyitem-field">
                  <h6 className="d-inline">
                    <Badge className="d-inline-flex me-1" bg="danger">
                      {oldVal[key]?.isRecursive === false ? "false" : "true"}
                    </Badge>
                  </h6>
                </td>
                <td className="table-warning   policyitem-field">--</td>
              </tr>
            }
          </>
        );
      });
    }

    return tablerow;
  };

  const updateConditionOldNew = (policy) => {
    var tablerow = [];
    let oldVal = [];
    let newVal = [];

    oldVal =
      !isEmpty(policy.previousValue) &&
      JSON.parse(policy.previousValue).map(
        (obj) => `${obj.type}: ${obj.values.join(", ")}`
      );

    newVal =
      !isEmpty(policy.newValue) &&
      JSON.parse(policy.newValue).map(
        (obj) => `${obj.type}: ${obj.values.join(", ")}`
      );
    tablerow.push(
      <Row className="d-flex flex-nowrap">
        <Col className="d-table" xs={6} md={4}>
          <Table className="table table-bordered w-auto">
            <thead className="thead-light">
              <tr>
                <th>Old Value</th>
              </tr>
            </thead>

            <tbody>
              {!isEmpty(oldVal) ? (
                oldVal.map((val) => {
                  return (
                    <tr key={val.id}>
                      {policy &&
                      policy.previousValue &&
                      !isEmpty(JSON.parse(policy.previousValue)) ? (
                        <td className="table-warning text-nowrap policyitem-field">
                          {val}
                        </td>
                      ) : (
                        <td>
                          <strong>--</strong>
                        </td>
                      )}
                    </tr>
                  );
                })
              ) : (
                <td className="table-warning text-center policycondition-empty">
                  <strong>{"<empty>"} </strong>
                </td>
              )}
            </tbody>
          </Table>
        </Col>
        <Col className="d-table" xs={6} md={4}>
          <Table className="table table-bordered w-auto">
            <thead className="thead-light">
              <tr>
                <th>New Value</th>
              </tr>
            </thead>
            <tbody>
              {!isEmpty(newVal) ? (
                newVal.map((val) => {
                  return policy &&
                    policy.newValue &&
                    !isEmpty(JSON.parse(policy.newValue)) ? (
                    <tr key={val.id}>
                      <td className="table-warning policyitem-field">{val}</td>
                    </tr>
                  ) : (
                    <tr>
                      <td>--</td>
                    </tr>
                  );
                })
              ) : (
                <td className="table-warning  text-center policycondition-empty">
                  <strong>{"<empty>"} </strong>
                </td>
              )}
            </tbody>
          </Table>
        </Col>
      </Row>
    );
    return tablerow;
  };
  const updatePolicyValidityNew = (policy) => {
    var tableRow = [];
    let newPolicyItems = [];
    let oldPolicyItems = [];
    let newPolicyItemsDiff = [];
    let oldPolicyItemsDiff = [];
    if (!isUndefined(policy.newValue) && !isEmpty(policy.newValue)) {
      newPolicyItems = policy.newValue && JSON.parse(policy.newValue);
    }
    if (!isUndefined(policy.previousValue) && !isEmpty(policy.previousValue)) {
      oldPolicyItems = policy.previousValue && JSON.parse(policy.previousValue);
    }
    newPolicyItemsDiff = differenceWith(
      newPolicyItems,
      oldPolicyItems,
      isEqual
    );
    oldPolicyItemsDiff = differenceWith(
      oldPolicyItems,
      newPolicyItems,
      isEqual
    );
    const diffVal = (obj1, obj2) =>
      Object.fromEntries(
        Object.entries(obj1).reduce((r, [key, value]) => {
          if (value && typeof value === "object") {
            let temp = diffVal(value, obj2[key] || {});
            if (Object.keys(temp).length) r.push([key, temp]);
          } else {
            if (value !== obj2[key]) r.push([key, value]);
          }
          return r;
        }, [])
      );
    const getStartTime = (startTime, index) => {
      var added = diffVal(newPolicyItemsDiff, oldPolicyItemsDiff);

      return !isEmpty(added[index] && added[index].startTime) ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="success">
            {added[index] && added[index].startTime}
          </Badge>
        </h6>
      ) : (
        startTime
      );
    };
    const getEndTime = (endTime, index) => {
      var added = diffVal(newPolicyItemsDiff, oldPolicyItemsDiff);

      return !isEmpty(added[index] && added[index].endTime) ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="success">
            {added[index] && added[index].endTime}
          </Badge>
        </h6>
      ) : (
        endTime
      );
    };
    const getTimeZone = (timeZone, index) => {
      var added = diffVal(newPolicyItemsDiff, oldPolicyItemsDiff);

      return !isEmpty(added[index] && added[index].timeZone) ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="success">
            {added[index] && added[index].timeZone}
          </Badge>
        </h6>
      ) : (
        timeZone
      );
    };
    tableRow.push(
      !isEmpty(newPolicyItemsDiff) ? (
        newPolicyItemsDiff.map(({ startTime, endTime, timeZone }, index) => (
          <tbody>
            <tr>
              <td className="table-warning text-nowrap validity-field">
                <i>Start Date: </i>
                {!isEmpty(startTime) ? getStartTime(startTime, index) : "--"}
              </td>
            </tr>
            <tr>
              <td className="table-warning text-nowrap validity-field">
                <i>End Date: </i>
                {!isEmpty(endTime) ? getEndTime(endTime, index) : "--"}
              </td>
            </tr>
            <tr>
              <td className="table-warning text-nowrap validity-field">
                <i>Time Zone: </i>
                {!isEmpty(timeZone) ? getTimeZone(timeZone, index) : "--"}
              </td>
            </tr>
          </tbody>
        ))
      ) : (
        <tbody>
          <tr>
            <td className="validity-empty-field text-center table-warning">
              <b>{"<empty>"}</b>
            </td>
          </tr>
        </tbody>
      )
    );

    return tableRow;
  };

  const updatePolicyValidityOld = (policy) => {
    var tableRow = [];
    let newPolicyItems = [];
    let oldPolicyItems = [];
    let newPolicyItemsDiff = [];
    let oldPolicyItemsDiff = [];
    if (!isUndefined(policy.newValue) && !isEmpty(policy.newValue)) {
      newPolicyItems = policy.newValue && JSON.parse(policy.newValue);
    }
    if (!isUndefined(policy.previousValue) && !isEmpty(policy.previousValue)) {
      oldPolicyItems = policy.previousValue && JSON.parse(policy.previousValue);
    }
    newPolicyItemsDiff = differenceWith(
      newPolicyItems,
      oldPolicyItems,
      isEqual
    );
    oldPolicyItemsDiff = differenceWith(
      oldPolicyItems,
      newPolicyItems,
      isEqual
    );
    const diffVal = (obj1, obj2) =>
      Object.fromEntries(
        Object.entries(obj1).reduce((r, [key, value]) => {
          if (value && typeof value === "object") {
            let temp = diffVal(value, obj2[key] || {});
            if (Object.keys(temp).length) r.push([key, temp]);
          } else {
            if (value !== obj2[key]) r.push([key, value]);
          }
          return r;
        }, [])
      );
    const getStartTime = (startTime, index) => {
      var removed = diffVal(oldPolicyItemsDiff, newPolicyItemsDiff);
      return !isEmpty(removed[index] && removed[index].startTime) ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="danger">
            {removed[index] && removed[index].startTime}
          </Badge>
        </h6>
      ) : (
        startTime
      );
    };
    const getEndTime = (endTime, index) => {
      var removed = diffVal(oldPolicyItemsDiff, newPolicyItemsDiff);

      return !isEmpty(removed[index] && removed[index].endTime) ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="danger">
            {removed[index] && removed[index].endTime}
          </Badge>
        </h6>
      ) : (
        endTime
      );
    };
    const getTimeZone = (timeZone, index) => {
      var removed = diffVal(oldPolicyItemsDiff, newPolicyItemsDiff);

      return !isEmpty(removed[index] && removed[index].timeZone) ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="danger">
            {removed[index] && removed[index].timeZone}
          </Badge>
        </h6>
      ) : (
        timeZone
      );
    };
    tableRow.push(
      !isEmpty(oldPolicyItemsDiff) ? (
        oldPolicyItemsDiff.map(({ startTime, endTime, timeZone }, index) => (
          <tbody>
            <tr>
              <td className="table-warning text-nowrap validity-field">
                <i>Start Date: </i>
                {!isEmpty(startTime) ? getStartTime(startTime, index) : "--"}
              </td>
            </tr>
            <tr>
              <td className="table-warning text-nowrap validity-field">
                <i>End Date: </i>
                {!isEmpty(endTime) ? getEndTime(endTime, index) : "--"}
              </td>
            </tr>
            <tr>
              <td className="table-warning text-nowrap validity-field">
                <i>Time Zone: </i>
                {!isEmpty(timeZone) ? getTimeZone(timeZone, index) : "--"}
              </td>
            </tr>
          </tbody>
        ))
      ) : (
        <tbody>
          <tr>
            <td className="validity-empty-field text-center table-warning">
              <b>{"<empty>"}</b>
            </td>
          </tr>
        </tbody>
      )
    );

    return tableRow;
  };

  const updatePolicyNew = (policy) => {
    var tableRow = [];
    let newPolicyItems = [];
    let oldPolicyItems = [];
    let newPolicyItemsDiff = [];
    let oldPolicyItemsDiff = [];
    if (!isUndefined(policy.newValue) && !isEmpty(policy.newValue)) {
      newPolicyItems = policy.newValue && JSON.parse(policy.newValue);
      newPolicyItems.map((obj) => {
        if (!isUndefined(obj.accesses)) {
          let permissions = map(
            filter(obj.accesses, { isAllowed: true }),
            function (t) {
              return t.type;
            }
          );
          obj["permissions"] = permissions;
        }
        obj["delegateAdmin"] =
          obj.delegateAdmin == true ? "enabled" : "disabled";
      });
    }

    if (!isUndefined(policy.previousValue) && !isEmpty(policy.previousValue)) {
      oldPolicyItems = policy.previousValue && JSON.parse(policy.previousValue);
      oldPolicyItems.map((obj) => {
        if (!isUndefined(obj.accesses)) {
          let permissions = map(
            filter(obj.accesses, { isAllowed: true }),
            function (t) {
              return t.type;
            }
          );
          obj["permissions"] = permissions;
          obj["delegateAdmin"] = obj.delegateAdmin ? "enabled" : "disabled";
        }
      });
    }
    newPolicyItemsDiff = differenceWith(
      newPolicyItems,
      oldPolicyItems,
      isEqual
    );
    oldPolicyItemsDiff = differenceWith(
      oldPolicyItems,
      newPolicyItems,
      isEqual
    );

    const getRoles = (roles, index) => {
      let filterdiff = [];
      var added = difference(
        newPolicyItemsDiff[index] && newPolicyItemsDiff[index].roles,
        oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].roles
      );
      !isEmpty(added)
        ? (filterdiff = differenceWith(Object.values(roles), added, isEqual))
        : (filterdiff = newPolicyItemsDiff);
      return !isEmpty(added)
        ? unionBy(
            filterdiff.map((val) => `${val}, `),
            added.map((obj) => {
              return (
                <h6 className="d-inline">
                  <Badge className="d-inline-flex me-1" bg="success">
                    {obj}
                  </Badge>
                </h6>
              );
            })
          )
        : filterdiff[index].roles.map((obj) => obj).join(", ");
    };

    const getGroups = (group, index) => {
      let filterdiff = [];
      var added = difference(
        newPolicyItemsDiff[index] && newPolicyItemsDiff[index].groups,
        oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].groups
      );
      !isEmpty(added)
        ? (filterdiff = differenceWith(Object.values(group), added, isEqual))
        : (filterdiff = newPolicyItemsDiff);
      return !isEmpty(added)
        ? unionBy(
            filterdiff.map((val) => `${val}, `),
            added.map((obj) => {
              return (
                <h6 className="d-inline">
                  <Badge className="d-inline-flex me-1" bg="success">
                    {obj}
                  </Badge>
                </h6>
              );
            })
          )
        : filterdiff[index].groups.map((obj) => obj).join(", ");
    };
    const getUsers = (users, index) => {
      let filterdiff = [];
      var added = difference(
        newPolicyItemsDiff[index] && newPolicyItemsDiff[index].users,
        oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].users
      );
      !isEmpty(added)
        ? (filterdiff = differenceWith(Object.values(users), added, isEqual))
        : (filterdiff = newPolicyItemsDiff);
      return !isEmpty(added)
        ? unionBy(
            filterdiff.map((val) => `${val}, `),
            added.map((obj) => {
              return (
                <h6 className="d-inline">
                  <Badge className="d-inline-flex me-1" bg="success">
                    {obj}
                  </Badge>
                </h6>
              );
            })
          )
        : filterdiff[index].users.map((obj) => obj).join(", ");
    };
    const getPermissions = (permissions, index) => {
      let filterdiff = [];
      var added = difference(
        newPolicyItemsDiff[index] && newPolicyItemsDiff[index].permissions,
        oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].permissions
      );
      !isEmpty(added)
        ? (filterdiff = differenceWith(
            Object.values(permissions),
            added,
            isEqual
          ))
        : (filterdiff = newPolicyItemsDiff);
      return !isEmpty(added)
        ? unionBy(
            filterdiff.map((val) => `${val}, `),
            added.map((obj) => {
              return (
                <h6 className="d-inline">
                  <Badge className="d-inline-flex me-1" bg="success">
                    {obj}
                  </Badge>
                </h6>
              );
            })
          )
        : filterdiff[index]?.permissions !== undefined
        ? filterdiff[index]?.permissions?.map((obj) => obj).join(", ")
        : "<empty>";
    };

    const getMaskingLabel = (DataMasklabel, dataMaskInfo, index) => {
      if (dataMaskInfo) {
        if (
          !isEqual(
            newPolicyItemsDiff[index] && newPolicyItemsDiff[index].dataMaskInfo,
            oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].dataMaskInfo
          )
        ) {
          return (
            <h6 className="d-inline">
              <Badge className="d-inline-flex me-1" bg="success">
                {getDataMaskType(DataMasklabel, dataMaskInfo)}
              </Badge>
            </h6>
          );
        } else {
          return getDataMaskType(DataMasklabel, dataMaskInfo);
        }
      }
    };
    const getRowLevelFilter = (rowFilter, index) => {
      var added = isEqual(
        newPolicyItemsDiff[index] &&
          newPolicyItemsDiff[index].rowFilterInfo.filterExpr,
        oldPolicyItemsDiff[index] &&
          oldPolicyItemsDiff[index].rowFilterInfo.filterExpr
      );
      return !added ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="success">
            {rowFilter}
          </Badge>
        </h6>
      ) : (
        rowFilter
      );
    };
    const getCondition = (conditions, index) => {
      var added = isEqual(
        oldPolicyItemsDiff[index] &&
          oldPolicyItemsDiff[index]?.conditions?.map((obj) => obj.values),
        newPolicyItemsDiff[index] &&
          newPolicyItemsDiff[index]?.conditions?.map((obj) => obj.values)
      );
      return !added ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="success">
            {`${conditions.type}: ${conditions.values.join(", ")}`}
          </Badge>
        </h6>
      ) : (
        `${conditions.type}: ${conditions.values.join(", ")} `
      );
    };
    tableRow.push(
      !isEmpty(newPolicyItemsDiff) ? (
        newPolicyItemsDiff.map(
          (
            {
              roles,
              groups,
              users,
              permissions,
              delegateAdmin,
              dataMaskInfo,
              DataMasklabel,
              conditions,
              rowFilterInfo
            },
            index
          ) => (
            <tbody>
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  <i>Roles: </i>
                  {!isEmpty(roles) ? getRoles(roles, index) : "<empty>"}{" "}
                </td>
              </tr>
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  <i>Groups: </i>
                  {!isEmpty(groups) ? getGroups(groups, index) : "<empty>"}{" "}
                </td>
              </tr>
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  <i>Users: </i>
                  {!isEmpty(users) ? getUsers(users, index) : "<empty>"}{" "}
                </td>
              </tr>
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  {policy.attributeName == "Masked Policy Items" ||
                  policy.attributeName == "Row level filter Policy Items" ? (
                    <i>Accesses: </i>
                  ) : (
                    <i>Permissions: </i>
                  )}
                  {getPermissions(permissions, index)}
                </td>
              </tr>
              {(policy.attributeName != "Masked Policy Items" ||
                policy.attributeName != "Row level filter Policy Items") &&
                !isEmpty(conditions) && (
                  <tr>
                    <td className="table-warning text-nowrap policyitem-field">
                      <i>Conditions: </i>
                      {!isEmpty(conditions)
                        ? conditions.map((obj) => {
                            return getCondition(obj, index);
                          })
                        : "<empty>"}
                    </td>
                  </tr>
                )}
              {policy.attributeName != "Masked Policy Items" &&
                policy.attributeName != "Row level filter Policy Items" && (
                  <tr>
                    <td className="table-warning text-nowrap policyitem-field">
                      <i>Delegate Admin: </i>
                      {delegateAdmin}
                    </td>
                  </tr>
                )}
              {policy.attributeName == "Masked Policy Items" && (
                <tr>
                  <td className="table-warning text-nowrap policyitem-field">
                    <i>Data Mask Type: </i>
                    {getMaskingLabel(DataMasklabel, dataMaskInfo, index)}
                  </td>
                </tr>
              )}
              {policy.attributeName == "Row level filter Policy Items" && (
                <tr>
                  <td className="table-warning text-nowrap">
                    <i>Row Level Filter: </i>
                    {!isEmpty(rowFilterInfo)
                      ? getRowLevelFilter(rowFilterInfo.filterExpr, index)
                      : "--"}
                  </td>
                </tr>
              )}
              {newPolicyItemsDiff.length - 1 != index && (
                <tr>
                  <td>
                    <br />
                  </td>
                </tr>
              )}
            </tbody>
          )
        )
      ) : (
        <tbody>
          <tr>
            <td className="policyitem-empty-field text-center table-warning">
              <b>{"<empty>"}</b>
            </td>
          </tr>
        </tbody>
      )
    );

    return tableRow;
  };
  const updatePolicyOld = (policy) => {
    var tableRow = [];
    let oldPolicyItems = [];
    let newPolicyItems = [];
    let newPolicyItemsDiff = [];
    let oldPolicyItemsDiff = [];

    if (!isUndefined(policy.previousValue) && !isEmpty(policy.previousValue)) {
      oldPolicyItems = policy.previousValue && JSON.parse(policy.previousValue);
      oldPolicyItems.map((obj) => {
        if (!isUndefined(obj.accesses)) {
          let permissions = map(
            filter(obj.accesses, { isAllowed: true }),
            function (t) {
              return t.type;
            }
          );
          obj["permissions"] = permissions;
        }
        obj["delegateAdmin"] =
          obj.delegateAdmin == true ? "enabled" : "disabled";
      });
    }
    if (!isUndefined(policy.newValue) && !isEmpty(policy.newValue)) {
      newPolicyItems = policy.newValue && JSON.parse(policy.newValue);
      newPolicyItems.map((obj) => {
        if (!isUndefined(obj.accesses)) {
          let permissions = map(
            filter(obj.accesses, { isAllowed: true }),
            (t) => {
              return t.type;
            }
          );
          obj["permissions"] = permissions;
          obj["delegateAdmin"] =
            obj.delegateAdmin == true ? "enabled" : "disabled";
        }
      });
    }
    newPolicyItemsDiff = differenceWith(
      newPolicyItems,
      oldPolicyItems,
      isEqual
    );
    oldPolicyItemsDiff = differenceWith(
      oldPolicyItems,
      newPolicyItems,
      isEqual
    );

    const getRoles = (roles, index) => {
      let filterdiff = [];
      var removed = difference(
        oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].roles,
        newPolicyItemsDiff[index] && newPolicyItemsDiff[index].roles
      );
      !isEmpty(removed)
        ? (filterdiff = differenceWith(Object.values(roles), removed, isEqual))
        : (filterdiff = oldPolicyItemsDiff);
      return !isEmpty(removed)
        ? unionBy(
            filterdiff.map((val) => `${val}, `),
            removed.map((obj) => {
              return (
                <h6 className="d-inline">
                  <Badge className="d-inline-flex me-1" bg="danger">
                    {obj}
                  </Badge>
                </h6>
              );
            })
          )
        : filterdiff[index]?.roles?.map((obj) => obj).join(", ");
    };
    const getGroups = (group, index) => {
      let filterdiff = [];
      var removed = difference(
        oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].groups,
        newPolicyItemsDiff[index] && newPolicyItemsDiff[index].groups
      );
      !isEmpty(removed)
        ? (filterdiff = differenceWith(Object.values(group), removed, isEqual))
        : (filterdiff = oldPolicyItemsDiff);
      return !isEmpty(removed)
        ? unionBy(
            filterdiff.map((val) => `${val}, `),
            removed.map((obj) => {
              return (
                <h6 className="d-inline">
                  <Badge className="d-inline-flex me-1" bg="danger">
                    {obj}
                  </Badge>
                </h6>
              );
            })
          )
        : filterdiff[index]?.groups?.map((obj) => obj).join(", ");
    };
    const getUsers = (users, index) => {
      let filterdiff = [];
      var removed = difference(
        oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].users,
        newPolicyItemsDiff[index] && newPolicyItemsDiff[index].users
      );
      !isEmpty(removed)
        ? (filterdiff = differenceWith(Object.values(users), removed, isEqual))
        : (filterdiff = oldPolicyItemsDiff);
      return !isEmpty(removed)
        ? unionBy(
            filterdiff.map((val) => `${val}, `),
            removed.map((obj) => {
              return (
                <h6 className="d-inline">
                  <Badge className="d-inline-flex me-1" bg="danger">
                    {obj}
                  </Badge>
                </h6>
              );
            })
          )
        : filterdiff[index]?.users?.map((obj) => obj).join(", ");
    };
    const getPermissions = (permissions, index) => {
      let filterdiff = [];
      var removed = difference(
        oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].permissions,
        newPolicyItemsDiff[index] && newPolicyItemsDiff[index].permissions
      );
      !isEmpty(removed)
        ? (filterdiff = differenceWith(
            Object.values(permissions),
            removed,
            isEqual
          ))
        : (filterdiff = oldPolicyItemsDiff);
      return !isEmpty(removed)
        ? unionBy(
            filterdiff.map((val) => `${val}, `),
            removed.map((obj) => {
              return (
                <h6 className="d-inline">
                  <Badge className="d-inline-flex me-1" bg="danger">
                    {obj}
                  </Badge>
                </h6>
              );
            })
          )
        : filterdiff[index]?.permissions !== undefined
        ? filterdiff[index]?.permissions?.map((obj) => obj).join(", ")
        : "<empty>";
    };

    const getMaskingLabel = (DataMasklabel, dataMaskInfo, index) => {
      if (dataMaskInfo) {
        if (
          !isEqual(
            oldPolicyItemsDiff[index] && oldPolicyItemsDiff[index].dataMaskInfo,
            newPolicyItemsDiff[index] && newPolicyItemsDiff[index].dataMaskInfo
          )
        ) {
          return (
            <h6 className="d-inline">
              <Badge className="d-inline-flex me-1" bg="danger">
                {getDataMaskType(DataMasklabel, dataMaskInfo)}
              </Badge>
            </h6>
          );
        } else {
          return getDataMaskType(DataMasklabel, dataMaskInfo);
        }
      }
    };
    const getRowLevelFilter = (rowFilter, index) => {
      var removed = isEqual(
        oldPolicyItemsDiff[index] &&
          oldPolicyItemsDiff[index].rowFilterInfo.filterExpr,
        newPolicyItemsDiff[index] &&
          newPolicyItemsDiff[index].rowFilterInfo.filterExpr
      );
      return !removed ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="danger">
            {rowFilter}
          </Badge>
        </h6>
      ) : (
        rowFilter
      );
    };
    const getCondition = (conditions, index) => {
      var removed = isEqual(
        newPolicyItemsDiff[index] &&
          newPolicyItemsDiff[index]?.conditions?.map((obj) => obj.values),
        oldPolicyItemsDiff[index] &&
          oldPolicyItemsDiff[index]?.conditions?.map((obj) => obj.values)
      );
      return !removed ? (
        <h6 className="d-inline">
          <Badge className="d-inline-flex me-1" bg="danger">
            {`${conditions.type}: ${conditions.values.join(", ")}`}
          </Badge>
        </h6>
      ) : (
        `${conditions.type}: ${conditions.values.join(", ")} `
      );
    };
    tableRow.push(
      !isEmpty(oldPolicyItemsDiff) ? (
        oldPolicyItemsDiff.map(
          (
            {
              roles,
              groups,
              users,
              permissions,
              delegateAdmin,
              DataMasklabel,
              dataMaskInfo,
              conditions,
              rowFilterInfo
            },
            index
          ) => (
            <tbody>
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  <i>Roles: </i>
                  {!isEmpty(roles) ? getRoles(roles, index) : "<empty>"}{" "}
                </td>
              </tr>
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  <i>Groups: </i>
                  {!isEmpty(groups) ? getGroups(groups, index) : "<empty>"}{" "}
                </td>
              </tr>
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  <i>Users: </i>
                  {!isEmpty(users) ? getUsers(users, index) : "<empty>"}{" "}
                </td>
              </tr>
              <tr>
                <td className="table-warning text-nowrap policyitem-field">
                  {policy.attributeName == "Masked Policy Items" ||
                  policy.attributeName == "Row level filter Policy Items" ? (
                    <i>Accesses: </i>
                  ) : (
                    <i>Permissions: </i>
                  )}

                  {getPermissions(permissions, index)}
                </td>
              </tr>
              {(policy.attributeName != "Masked Policy Items" ||
                policy.attributeName != "Row level filter Policy Items") &&
                !isEmpty(conditions) && (
                  <tr>
                    <td className="table-warning text-nowrap policyitem-field">
                      <i>Conditions: </i>
                      {!isEmpty(conditions)
                        ? conditions.map((obj) => {
                            return getCondition(obj, index);
                          })
                        : "<empty>"}
                    </td>
                  </tr>
                )}
              {policy.attributeName != "Masked Policy Items" &&
                policy.attributeName != "Row level filter Policy Items" && (
                  <tr>
                    <td className="table-warning text-nowrap policyitem-field">
                      <i>Delegate Admin: </i>
                      {delegateAdmin}
                    </td>
                  </tr>
                )}
              {policy.attributeName == "Masked Policy Items" && (
                <tr>
                  <td className="table-warning text-nowrap policyitem-field">
                    <i>Data Mask Type: </i>
                    {getMaskingLabel(DataMasklabel, dataMaskInfo, index)}
                  </td>
                </tr>
              )}
              {policy.attributeName == "Row level filter Policy Items" && (
                <tr>
                  <td className="table-warning text-nowrap policyitem-field">
                    <i>Row Level Filter: </i>
                    {!isEmpty(rowFilterInfo)
                      ? getRowLevelFilter(rowFilterInfo.filterExpr, index)
                      : "--"}
                  </td>
                </tr>
              )}
              {oldPolicyItemsDiff.length - 1 != index && (
                <tr>
                  <td>
                    <br />
                  </td>
                </tr>
              )}
            </tbody>
          )
        )
      ) : (
        <tbody>
          <tr>
            <td className="policyitem-empty-field text-center table-warning">
              <b>{"<empty>"}</b>
            </td>
          </tr>
        </tbody>
      )
    );

    return tableRow;
  };

  /*UPDATE END */

  /* DELETE POLICY VARIABLES */

  const deletePolicyDetails = reportdata.filter((policy) => {
    return (
      policy.action == "delete" &&
      policy.attributeName != "Policy Resources" &&
      policy.attributeName != "Policy Conditions" &&
      policy.attributeName != "Policy Items" &&
      policy.attributeName != "DenyPolicy Items" &&
      policy.attributeName != "Allow Exceptions" &&
      policy.attributeName != "Deny Exceptions" &&
      policy.attributeName != "Zone Name" &&
      policy.attributeName != "Masked Policy Items" &&
      policy.attributeName != "Row level filter Policy Items" &&
      policy.attributeName != "Validity Schedules"
    );
  });
  const deletePolicyResources = reportdata.filter((obj) => {
    return obj.attributeName == "Policy Resources" && obj.action == "delete";
  });
  const deleteDetails = (details, resources) => {
    let tablerow = [];

    const policyOldVal = (val, oldVal) => {
      if (val == "Policy Labels") {
        return (oldVal = !isEmpty(JSON.parse(oldVal))
          ? JSON.parse(oldVal).join(", ")
          : "--");
      }
      if (val == "Policy Status") {
        return (oldVal = oldVal === "false" ? "disabled" : "enabled");
      }
      return !isEmpty(oldVal) ? oldVal : "--";
    };

    details.map((policy) => {
      return tablerow.push(
        <tr>
          <td className="table-warning text-nowrap">{policy.attributeName}</td>
          <td className="table-warning text-nowrap">
            {!isEmpty(policy.previousValue)
              ? policyOldVal(policy.attributeName, policy.previousValue)
              : "--"}
          </td>
        </tr>
      );
    });

    let keynew = {};
    resources.map((obj) => {
      keynew = !isEmpty(obj.previousValue) && JSON.parse(obj.previousValue);
    });

    Object.keys(keynew).map((key, index) => {
      return tablerow.push(
        <>
          <tr key={index}>
            <td className="table-warning">{key}</td>
            <td className="table-warning">
              {!isEmpty(keynew[key].values)
                ? keynew[key].values.join(", ")
                : "--"}
            </td>
          </tr>
          <tr>
            <td className="table-warning">{key + " " + "exclude"}</td>
            <td className="table-warning">
              {keynew[key].isExcludes == false ? "false" : "true"}
            </td>
          </tr>
          <tr>
            <td className="table-warning">{key + " " + "recursive"}</td>
            <td className="table-warning">
              {keynew[key].isRecursive == false ? "false" : "true"}
            </td>
          </tr>
        </>
      );
    });
    return tablerow;
  };

  const deleteRowMask = reportdata.filter(
    (obj) =>
      obj.attributeName == "Row level filter Policy Items" &&
      obj.action == "delete"
  );

  const deleteRowMaskOld = deleteRowMask.map((obj) => obj.previousValue);
  const deleteValidity = reportdata.filter(
    (obj) => obj.attributeName == "Validity Schedules" && obj.action == "delete"
  );

  const deleteValidityOld = deleteValidity.map((obj) => obj.previousValue);

  const deleteCondition = reportdata.filter((obj) => {
    return obj.attributeName == "Policy Conditions" && obj.action == "delete";
  });

  const deleteConditionOld = deleteCondition.map((obj) => obj.previousValue);
  const deleteMaskPolicy = reportdata.filter(
    (obj) =>
      obj.attributeName == "Masked Policy Items" && obj.action == "delete"
  );
  const deletemaskPolicyOld = deleteMaskPolicy.map((obj) => obj.previousValue);
  const deletePolicyItems = reportdata.filter(
    (obj) => obj.attributeName == "Policy Items" && obj.action == "delete"
  );

  const deletePolicyItemsOld = deletePolicyItems.map(
    (obj) => obj.previousValue
  );

  const deleteException = reportdata.filter(
    (obj) => obj.attributeName == "Allow Exceptions" && obj.action == "delete"
  );
  const deleteExceptionOld = deleteException.map((obj) => obj.previousValue);

  const deleteDenyPolicy = reportdata.filter(
    (obj) => obj.attributeName == "DenyPolicy Items" && obj.action == "delete"
  );
  const deleteDenyPolicyOld = deleteDenyPolicy.map((obj) => obj.previousValue);

  const deleteDenyException = reportdata.filter(
    (obj) => obj.attributeName == "Deny Exceptions" && obj.action == "delete"
  );
  const deleteDenyExceptionOld = deleteDenyException.map(
    (obj) => obj.previousValue
  );

  /*DELETE END */

  /* IMPORT DELETE LOGS VARIABLES */

  const importDeleteDetails = reportdata.filter((c) => {
    return (
      c.action == "Import Delete" &&
      c.attributeName != "Policy Resources" &&
      c.attributeName != "Policy Conditions" &&
      c.attributeName != "Policy Items" &&
      c.attributeName != "DenyPolicy Items" &&
      c.attributeName != "Zone Name" &&
      c.attributeName != "Allow Exceptions" &&
      c.attributeName != "Deny Exceptions" &&
      c.attributeName != "Masked Policy Items" &&
      c.attributeName != "Row level filter Policy Items" &&
      c.attributeName != "Validity Schedules"
    );
  });

  const ImportDeleteDetails = (details, resources) => {
    let tablerow = [];
    const createDetailsPolicy = (val, newVal) => {
      if (val == "Policy Labels") {
        return (newVal = !isEmpty(JSON.parse(newVal))
          ? JSON.parse(newVal).join(", ")
          : "--");
      }
      if (val == "Policy Status") {
        return (newVal = newVal === "false" ? "disabled" : "enabled");
      }
      return !isEmpty(newVal) ? newVal : "--";
    };
    details.map((policy) => {
      return tablerow.push(
        <tr>
          <td className="table-warning policyitem-field">
            {policy.attributeName}
          </td>
          <td className="table-warning policyitem-field">
            {!isEmpty(policy.previousValue)
              ? createDetailsPolicy(policy.attributeName, policy.previousValue)
              : "--"}
          </td>
        </tr>
      );
    });

    let keynew = {};
    resources.map((obj) => {
      keynew = !isEmpty(obj.previousValue) && JSON.parse(obj.previousValue);
    });

    Object.keys(keynew).map((key, index) => {
      return tablerow.push(
        <>
          <tr>
            <td className="table-warning policyitem-field">{key}</td>
            <td className="table-warning policyitem-field">
              {" "}
              {keynew[key].values}
            </td>
          </tr>
          <tr>
            <td className="table-warning policyitem-field">
              {key + " " + "exclude"}
            </td>
            <td className="table-warning policyitem-field">
              {keynew[key].isExcludes == false ? "false" : "true"}
            </td>
          </tr>
          <tr>
            <td className="table-warning policyitem-field">
              {key + " " + "recursive"}
            </td>
            <td className="table-warning policyitem-field">
              {keynew[key].isRecursive == false ? "false" : "true"}
            </td>
          </tr>
        </>
      );
    });
    return tablerow;
  };

  const importDelItems = reportdata.filter((obj) => {
    return obj.attributeName == "Policy Items";
  });
  const importDeleteItemsOld = importDelItems.map((obj) => obj.previousValue);

  const importDelValidity = reportdata.filter(
    (obj) =>
      obj.attributeName == "Validity Schedules" && obj.action == "Import Delete"
  );
  const importDelValidityOld = importDelValidity.map(
    (obj) => obj.previousValue
  );

  const importDelPolicyCondition = reportdata.filter((obj) => {
    return (
      obj.attributeName == "Policy Conditions" && obj.action == "Import Delete"
    );
  });
  const importPolicyConditionOld = importDelPolicyCondition.map(
    (obj) => obj.previousValue
  );

  const importDelPolicyException = reportdata.filter(
    (obj) => obj.attributeName == "Allow Exceptions"
  );
  const importdelPolicyExceptionOld = importDelPolicyException.map(
    (obj) => obj.previousValue
  );

  const importdeldenyPolicy = reportdata.filter(
    (obj) =>
      obj.attributeName == "DenyPolicy Items" && obj.action == "Import Delete"
  );
  const importdeldenyPolicyold = importdeldenyPolicy.map(
    (obj) => obj.previousValue
  );

  const importDelDenyExceptions = reportdata.filter(
    (obj) =>
      obj.attributeName == "Deny Exceptions" && obj.action == "Import Delete"
  );
  const importDeldenyExceptionsold = importDelDenyExceptions.map(
    (obj) => obj.previousValue
  );

  const importdelmaskPolicyItem = reportdata.filter(
    (obj) =>
      obj.attributeName == "Masked Policy Items" &&
      obj.action == "Import Delete"
  );
  const importdelmaskpolicyold = importdelmaskPolicyItem.map(
    (obj) => obj.previousValue
  );
  const importDeleteRowPolicyItem = reportdata.filter(
    (obj) =>
      obj.attributeName == "Row level filter Policy Items" &&
      obj.action == "Import Delete"
  );
  const importDeleteRowPolicyOld = importDeleteRowPolicyItem.map(
    (obj) => obj.previousValue
  );
  /*IMPORT DELETE END*/

  /* IMPORT END LOGS*/

  const importEnd = reportdata.filter((obj) => obj.action == "IMPORT END");

  /* EXPORT JSON | CSV | EXCEL  LOGS */

  const exportVal = reportdata.filter(
    (obj) =>
      obj.action == "EXPORT JSON" ||
      obj.action == "EXPORT EXCEL" ||
      obj.action == "EXPORT CSV"
  );
  const exportOldVal = (val, oldVal) => {
    if (val == "Export time") {
      return (oldVal = dateFormat(createDate, "mm/dd/yyyy hh:MM:ss TT "));
    }
    return oldVal;
  };

  const importdeleteresources = reportdata.filter((c) => {
    return c.attributeName == "Policy Resources" && c.action == "Import Delete";
  });

  return (
    <div>
      {/* CREATE LOGS */}

      {action == "create" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_POLICY.value && (
          <div>
            <div className="fw-bolder">
              Policy ID :{" "}
              <Badge className="d-inline-flex me-1" bg="info">
                {objectId}
              </Badge>
            </div>
            <div className="fw-bolder">Policy Name: {objectName}</div>
            <div className="fw-bolder">Service Name: {parentObjectName}</div>
            <div className="fw-bolder">
              Created Date: {currentTimeZone(createDate)}
            </div>
            <div className="fw-bolder">Created By: {owner}</div>
            <h5 className="bold wrap-header mt-3">Policy Details:</h5>

            <Table className="table table-bordered w-auto">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>

                  <th>New Value</th>
                </tr>
              </thead>
              <tbody>
                {policyDetails(createPolicyDetails, createPolicyResources)}
              </tbody>
            </Table>
            <br />
            {action == "create" &&
              !isEmpty(createValidityNew) &&
              !isUndefined(createValidityNew) &&
              createValidityNew != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Validity Period:</h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>New Value</th>
                      </tr>
                    </thead>
                    {createValidityNew.map((policyitem) => {
                      return policyitem.map((policy, index) => (
                        <tbody>
                          <tr key={index}>
                            <td className="table-warning policyitem-field">
                              {`Start Date: ${
                                !isEmpty(policy.startTime)
                                  ? policy.startTime
                                  : "--"
                              } `}
                            </td>
                          </tr>
                          <tr>
                            <td className="table-warning policyitem-field">
                              {`End Date: ${
                                !isEmpty(policy.endTime) ? policy.endTime : "--"
                              } `}
                            </td>
                          </tr>
                          <tr>
                            <td className="table-warning policyitem-field">
                              {`Time Zone: ${
                                !isEmpty(policy.timeZone)
                                  ? policy.timeZone
                                  : "--"
                              } `}
                            </td>
                          </tr>
                        </tbody>
                      ));
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "create" &&
              !isEmpty(createConditionNew) &&
              !isUndefined(createConditionNew) &&
              createConditionNew != 0 &&
              createConditionNew != "[]" && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Policy Conditions:
                  </h5>
                  <Table className="table table-bordered w-25">
                    <thead className="thead-light">
                      <tr>
                        <th>New Value</th>
                      </tr>
                    </thead>

                    {createConditionNew.map((policyitem) => {
                      return !isEmpty(JSON.parse(policyitem)) ? (
                        JSON.parse(policyitem).map((policy, index) => (
                          <tbody>
                            <tr key={index}>
                              <td className="table-warning policyitem-field">
                                {" "}
                                {`${policy.type}: ${policy.values}`}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      ) : (
                        <tbody>
                          <tr>
                            <td className="table-warning policyitem-field">
                              <strong>{"<empty>"}</strong>
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
              !isEmpty(createPolicyItems) &&
              !isUndefined(createPolicyItems) &&
              createPolicyItems != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Allow PolicyItems:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>New Value</th>
                      </tr>
                    </thead>

                    {createPolicyItems.map((policyitem) => {
                      return (
                        !isEmpty(policyitem.newValue) &&
                        JSON.parse(policyitem.newValue).map((policy, index) => (
                          <tbody>
                            <tr key={index}>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Permissions`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {policy.conditions &&
                                policy.conditions.length > 0 && (
                                  <td className="table-warning text-nowrap policyitem-field">
                                    <i>{`Conditions`}</i>
                                    {`: ${policy.conditions.map(
                                      (type) => `${type.type} : ${type.values}`
                                    )}`}
                                  </td>
                                )}
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Delegate Admin`}</i>
                                {`: ${
                                  policy.delegateAdmin == true
                                    ? "enabled"
                                    : "disabled"
                                }`}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "create" &&
              !isEmpty(createRowMaskNew) &&
              !isUndefined(createRowMaskNew) &&
              createRowMaskNew != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Row Level Filter Policy Items:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>New Value</th>
                      </tr>
                    </thead>

                    {createRowMaskNew.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy, index) => (
                          <tbody>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Accesses`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {!isEmpty(policy.conditions) && (
                                <td className="table-warning text-nowrap policyitem-field">
                                  <i>{`Conditions`}</i>
                                  {`: ${policy.conditions.map(
                                    (type) =>
                                      `${type.type} : ${type.values.join(", ")}`
                                  )} `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Row Level Filter`}</i>
                                {!isEmpty(policy.rowFilterInfo.filterExpr)
                                  ? `: ${policy.rowFilterInfo.filterExpr} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            {JSON.parse(policyitem).length - 1 != index && (
                              <tr>
                                <td>
                                  <br />
                                </td>
                              </tr>
                            )}
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "create" &&
              !isEmpty(createExceptionNew) &&
              !isUndefined(createExceptionNew) &&
              createExceptionNew != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Allow Exceptions:</h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>New Value</th>
                      </tr>
                    </thead>

                    {createExceptionNew.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Permissions`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {policy.conditions &&
                                policy.conditions.length > 0 && (
                                  <td className="table-warning text-nowrap policyitem-field">
                                    <i>{`Conditions`}</i>
                                    {`: ${policy.conditions
                                      .map(
                                        (type) =>
                                          `${type.type} : ${type.values}`
                                      )
                                      .join(", ")}`}
                                  </td>
                                )}
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Delegate Admin`}</i>
                                {`: ${
                                  policy.delegateAdmin == true
                                    ? "enabled"
                                    : "disabled"
                                }`}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "create" &&
              !isEmpty(createDenyPolicyNew) &&
              !isUndefined(createDenyPolicyNew) &&
              createDenyPolicyNew != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Deny PolicyItems:</h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>New Value</th>
                      </tr>
                    </thead>

                    {createDenyPolicyNew.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Permissions`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {policy.conditions &&
                                policy.conditions.length > 0 && (
                                  <td className="table-warning text-nowrap policyitem-field">
                                    <i>{`Conditions`}</i>
                                    {`: ${policy.conditions
                                      .map(
                                        (type) =>
                                          `${type.type} : ${type.values}`
                                      )
                                      .join(", ")}`}
                                  </td>
                                )}
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Delegate Admin`}</i>
                                {`: ${
                                  policy.delegateAdmin == true
                                    ? "enabled"
                                    : "disabled"
                                }`}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "create" &&
              !isEmpty(createDenyExceptionNew) &&
              !isUndefined(createDenyExceptionNew) &&
              createDenyExceptionNew != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Deny Exception PolicyItems:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>New Value</th>
                      </tr>
                    </thead>

                    {createDenyExceptionNew.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Permissions`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {policy.conditions &&
                                policy.conditions.length > 0 && (
                                  <td className="table-warning text-nowrap policyitem-field">
                                    <i>{`Conditions`}</i>
                                    {`: ${policy.conditions.map(
                                      (type) => `${type.type} : ${type.values}`
                                    )}`}
                                  </td>
                                )}
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Delegate Admin`}</i>
                                {`: ${
                                  policy.delegateAdmin == true
                                    ? "enabled"
                                    : "disabled"
                                }`}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "create" &&
              !isEmpty(createMaskPolicyNew) &&
              !isUndefined(createMaskPolicyNew) &&
              createMaskPolicyNew != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Masking Policy Items:
                  </h5>
                  <Table className="table table-bordered w-auto ">
                    <thead className="thead-light">
                      <tr>
                        <th>New Value</th>
                      </tr>
                    </thead>

                    {createMaskPolicyNew.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy, index) => (
                          <tbody>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning text-nowrap policyitem-field">
                                <i>{`Accesses`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {!isEmpty(policy.conditions) && (
                                <td className="table-warning policyitem-field">
                                  <i>{`Conditions`}</i>
                                  {`: ${policy.conditions.map(
                                    (type) =>
                                      `${type.type} : ${type.values.join(", ")}`
                                  )} `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              {policy.delegateAdmin == true && (
                                <td className="table-warning text-nowrap policyitem-field">
                                  <i>{`Delegate Admin`}</i>
                                  {`: ${
                                    policy.delegateAdmin == true
                                      ? "enabled"
                                      : "disabled"
                                  }`}
                                </td>
                              )}
                            </tr>
                            <tr>
                              {!isEmpty(policy.dataMaskInfo) && (
                                <td className="table-warning text-nowrap policyitem-field">
                                  <i>{`Data Mask Types: `}</i>
                                  {getDataMaskType(
                                    policy.DataMasklabel,
                                    policy.dataMaskInfo
                                  )}
                                </td>
                              )}
                            </tr>
                            {JSON.parse(policyitem).length - 1 != index && (
                              <tr>
                                <td>
                                  <br />
                                </td>
                              </tr>
                            )}
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                </>
              )}
          </div>
        )}

      {/* UPDATE Policy Logs*/}

      {action == "update" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_POLICY.value && (
          <div>
            <div className="row">
              <div className="col-md-6">
                <div className="fw-bolder">
                  Policy ID :{" "}
                  <Badge className="d-inline-flex me-1" bg="info">
                    {objectId}
                  </Badge>
                </div>
                <div className="fw-bolder">Policy Name: {objectName}</div>
                <div className="fw-bolder">
                  Service Name: {parentObjectName}
                </div>
                <div className="fw-bolder">
                  Updated Date: {currentTimeZone(createDate)}
                </div>
                <div className="fw-bolder">Updated By: {owner} </div>
              </div>
              <div className="col-md-6 text-end">
                <div className="bg-success legend"></div> {" Added "}
                <div className="bg-danger legend"></div> {" Deleted "}
              </div>
            </div>
            <br />

            {(!isEmpty(updatePolicyDetails) ||
              !isEmpty(updatePolicyResources)) && (
              <>
                <h5 className="bold wrap-header m-t-sm">Policy details</h5>
                <Table className="table table-bordered w-auto">
                  <thead className="thead-light">
                    <tr>
                      <th>Fields</th>
                      <th>Old Value</th>
                      <th>New Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {policyDetailsUpdate(
                      updatePolicyDetails,
                      updatePolicyResources
                    )}
                  </tbody>
                </Table>
                <br />
              </>
            )}
            {action == "update" &&
              !isEqual(updatePolicyConditionOld, updatePolicyConditionNew) &&
              (!updatePolicyConditionOld.includes("") ||
                !updatePolicyConditionNew.includes("")) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Policy Conditions:
                  </h5>
                  {updatePolicyCondition.map((policyitem) => {
                    return updateConditionOldNew(policyitem);
                  })}
                </>
              )}
            {action == "update" &&
              !isEqual(updateValidityOld, updateValidityNew) &&
              (!updateValidityOld.includes("") ||
                !updateValidityNew.includes("")) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Validity Period:</h5>

                  <Row className="d-flex flex-nowrap">
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th>Old Value</th>
                          </tr>
                        </thead>

                        {updateValidity.map((policy) => {
                          return updatePolicyValidityOld(policy);
                        })}
                      </Table>
                    </Col>
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th>New Value</th>
                          </tr>
                        </thead>

                        {updateValidity.map((policy) => {
                          return updatePolicyValidityNew(policy);
                        })}
                      </Table>
                    </Col>
                  </Row>
                  <br />
                </>
              )}

            {action == "update" &&
              (!isEmpty(updateRowMaskOld) || !isEmpty(updateRowMaskNew)) &&
              (!updateRowMaskOld.includes("") ||
                !updateRowMaskNew.includes("")) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Row Level Filter Policy Items:
                  </h5>

                  <Row className="d-flex flex-nowrap">
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th>Old Value</th>
                          </tr>
                        </thead>

                        {updateRowMask.map((policy) => {
                          return updatePolicyOld(policy);
                        })}
                      </Table>
                    </Col>
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th>New Value</th>
                          </tr>
                        </thead>

                        {updateRowMask.map((policy) => {
                          return updatePolicyNew(policy);
                        })}
                      </Table>
                    </Col>
                  </Row>
                  <br />
                </>
              )}

            {action == "update" &&
              !isEmpty(updateMaskPolicy) &&
              !isUndefined(updateMaskPolicy) &&
              updateMaskPolicy != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Masking Policy Items:
                  </h5>

                  <Row className="d-flex flex-nowrap">
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">Old Value</th>
                          </tr>
                        </thead>

                        {updateMaskPolicy.map((policy) => {
                          return updatePolicyOld(policy);
                        })}
                      </Table>
                    </Col>
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">New Value</th>
                          </tr>
                        </thead>

                        {updateMaskPolicy.map((policy) => {
                          return updatePolicyNew(policy);
                        })}
                      </Table>
                    </Col>
                  </Row>
                  <br />
                </>
              )}

            {action == "update" &&
              !isEmpty(updatePolicyItems) &&
              !isUndefined(updatePolicyItems) &&
              updatePolicyItems != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Allow PolicyItems:
                  </h5>

                  <Row className="d-flex flex-nowrap">
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">Old Value</th>
                          </tr>
                        </thead>

                        {updatePolicyItems.map((policy) => {
                          return updatePolicyOld(policy);
                        })}
                      </Table>
                    </Col>
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">New Value</th>
                          </tr>
                        </thead>

                        {updatePolicyItems.map((policy) => {
                          return updatePolicyNew(policy);
                        })}
                      </Table>
                    </Col>
                  </Row>
                  <br />
                </>
              )}

            {action == "update" &&
              !isEmpty(updateException) &&
              !isUndefined(updateException) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Allow Exception PolicyItems:
                  </h5>

                  <Row className="d-flex flex-nowrap">
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">Old Value</th>
                          </tr>
                        </thead>

                        {updateException.map((policy) => {
                          return updatePolicyOld(policy);
                        })}
                      </Table>
                    </Col>
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">New Value</th>
                          </tr>
                        </thead>

                        {updateException.map((policy) => {
                          return updatePolicyNew(policy);
                        })}
                      </Table>
                    </Col>
                  </Row>
                  <br />
                </>
              )}

            {action == "update" &&
              !isEmpty(updateDenyPolicy) &&
              !isUndefined(updateDenyPolicy) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Deny PolicyItems:</h5>

                  <Row className="d-flex flex-nowrap">
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">Old Value</th>
                          </tr>
                        </thead>

                        {updateDenyPolicy.map((policy) => {
                          return updatePolicyOld(policy);
                        })}
                      </Table>
                    </Col>
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">New Value</th>
                          </tr>
                        </thead>

                        {updateDenyPolicy.map((policy) => {
                          return updatePolicyNew(policy);
                        })}
                      </Table>
                    </Col>
                  </Row>
                  <br />
                </>
              )}

            {action == "update" &&
              !isEmpty(updateDenyException) &&
              !isUndefined(updateDenyException) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Deny Exception PolicyItems:
                  </h5>

                  <Row className="d-flex flex-nowrap">
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">Old Value</th>
                          </tr>
                        </thead>

                        {updateDenyException.map((policy) => {
                          return updatePolicyOld(policy);
                        })}
                      </Table>
                    </Col>
                    <Col className="d-table" xs={6} md={4}>
                      <Table className="table table-bordered w-auto">
                        <thead className="thead-light">
                          <tr>
                            <th className="text-nowrap">New Value</th>
                          </tr>
                        </thead>

                        {updateDenyException.map((policy) => {
                          return updatePolicyNew(policy);
                        })}
                      </Table>
                    </Col>
                  </Row>
                </>
              )}
          </div>
        )}

      {/* DELETE POLICY LOGS  */}

      {action == "delete" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_POLICY.value && (
          <div>
            <div className="fw-bolder">
              Policy ID :{" "}
              <Badge className="d-inline-flex me-1" bg="info">
                {objectId}
              </Badge>
            </div>
            <div className="fw-bolder">Policy Name: {objectName}</div>
            <div className="fw-bolder">Service Name: {parentObjectName}</div>
            <div className="fw-bolder">
              Deleted Date:{currentTimeZone(createDate)}
            </div>
            <div className="fw-bolder">Deleted By: {owner} </div>
            <br />
            <h5 className="bold wrap-header m-t-sm">Policy Details:</h5>

            <Table className="table table-bordered w-auto">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>

                  <th>Old Value</th>
                </tr>
              </thead>
              <tbody>
                {deleteDetails(deletePolicyDetails, deletePolicyResources)}
              </tbody>
            </Table>
            <br />
            {action == "delete" &&
              !isEmpty(deleteValidityOld) &&
              !isUndefined(deleteValidityOld) &&
              deleteValidityOld != "[]" &&
              deleteValidityOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Validity Period:</h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>old Value</th>
                      </tr>
                    </thead>
                    {deleteValidityOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Start Date`}</i>
                                {`: ${
                                  !isEmpty(policy.startTime)
                                    ? policy.startTime
                                    : "--"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`End Date`}</i>
                                {`: ${
                                  !isEmpty(policy.endTime)
                                    ? policy.endTime
                                    : "--"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Time Zone`}</i>
                                {`: ${
                                  !isEmpty(policy.timeZone)
                                    ? policy.timeZone
                                    : "--"
                                } `}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}
            {action == "delete" &&
              !isEmpty(deleteConditionOld) &&
              !isUndefined(deleteConditionOld) &&
              deleteConditionOld != "[]" &&
              deleteConditionOld != "" && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Policy Conditions:
                  </h5>
                  <Table className="table table-bordered w-25">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {deleteConditionOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                {`${policy.type}: ${policy.values.join(", ")}`}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}
            {action == "delete" &&
              !isEmpty(deleteRowMaskOld) &&
              !isUndefined(deleteRowMaskOld) &&
              deleteRowMaskOld != 0 &&
              deleteRowMaskOld.length > 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Row Level Filter Policy Items:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {deleteRowMaskOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy, index) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Accesses`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {!isEmpty(policy.conditions) && (
                                <td className="table-warning text-nowrap policyitem-field">
                                  <i>{`Conditions`}</i>
                                  {`: ${policy.conditions.map(
                                    (type) =>
                                      `${type.type} : ${type.values.join(", ")}`
                                  )} `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Row Level Filter`}</i>
                                {!isEmpty(policy.rowFilterInfo.filterExpr)
                                  ? `: ${policy.rowFilterInfo.filterExpr}`
                                  : "<empty>"}
                              </td>
                            </tr>
                            {JSON.parse(policyitem).length - 1 != index && (
                              <tr>
                                <td>
                                  <br />
                                </td>
                              </tr>
                            )}
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}
            {action == "delete" &&
              !isEmpty(deletemaskPolicyOld) &&
              !isUndefined(deletemaskPolicyOld) &&
              deletemaskPolicyOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Masking Policy Items:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {deletemaskPolicyOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy, index) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Accesses`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {!isEmpty(policy.conditions) && (
                                <td className="table-warning policyitem-field">
                                  <i>{`Conditions`}</i>
                                  {`: ${policy.conditions.map(
                                    (type) =>
                                      `${type.type} : ${type.values.join(", ")}`
                                  )} `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              {policy.delegateAdmin == true && (
                                <td className="table-warning policyitem-field">
                                  <i>{`Delegate Admin`}</i>
                                  {`: ${
                                    policy.delegateAdmin == true
                                      ? "enabled"
                                      : "disabled"
                                  } `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              {!isEmpty(policy.dataMaskInfo) && (
                                <td className="table-warning text-nowrap policyitem-field">
                                  <i>{`Data Mask Types: `}</i>
                                  {getDataMaskType(
                                    policy.DataMasklabel,
                                    policy.dataMaskInfo
                                  )}
                                </td>
                              )}
                            </tr>
                            {JSON.parse(policyitem).length - 1 != index && (
                              <tr>
                                <td>
                                  <br />
                                </td>
                              </tr>
                            )}
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                </>
              )}
            {action == "delete" &&
              !isEmpty(deletePolicyItemsOld) &&
              !isUndefined(deletePolicyItemsOld) &&
              deletePolicyItemsOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Allow PolicyItems:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {deletePolicyItemsOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Permissions`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {policy.conditions &&
                                policy.conditions.length > 0 && (
                                  <td className="table-warning policyitem-field">
                                    <i>{`Conditions`}</i>
                                    {`: ${policy.conditions.map(
                                      (type) =>
                                        `${type.type} : ${type.values.join(
                                          ", "
                                        )}`
                                    )} `}
                                  </td>
                                )}
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Delegate Admin`}</i>
                                {`: ${
                                  policy.delegateAdmin == true
                                    ? "enabled"
                                    : "disabled"
                                } `}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}
            {action == "delete" &&
              !isEmpty(deleteExceptionOld) &&
              !isUndefined(deleteExceptionOld) &&
              deleteExceptionOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Allow Exception PolicyItems:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {deleteExceptionOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Permissions`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {policy.conditions &&
                                policy.conditions.length > 0 && (
                                  <td className="table-warning policyitem-field">
                                    <i>{`Conditions`}</i>
                                    {`: ${policy.conditions.map(
                                      (type) =>
                                        `${type.type} : ${type.values.join(
                                          ", "
                                        )}`
                                    )} `}
                                  </td>
                                )}
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Delegate Admin`}</i>
                                {`: ${
                                  policy.delegateAdmin == true
                                    ? "enabled"
                                    : "disabled"
                                } `}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}
            {action == "delete" &&
              !isEmpty(deleteDenyPolicyOld) &&
              !isUndefined(deleteDenyPolicyOld) &&
              deleteDenyPolicyOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Deny PolicyItems:</h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {deleteDenyPolicyOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Permissions`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {policy.conditions &&
                                policy.conditions.length > 0 && (
                                  <td className="table-warning policyitem-field">
                                    <i>{`Conditions`}</i>
                                    {`: ${policy.conditions.map(
                                      (type) =>
                                        `${type.type} : ${type.values.join(
                                          ", "
                                        )}`
                                    )} `}
                                  </td>
                                )}
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Delegate Admin`}</i>
                                {`: ${
                                  policy.delegateAdmin == true
                                    ? "enabled"
                                    : "disabled"
                                } `}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "delete" &&
              !isEmpty(deleteDenyExceptionOld) &&
              !isUndefined(deleteDenyExceptionOld) &&
              deleteDenyExceptionOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Deny Exception PolicyItems:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {deleteDenyExceptionOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Permissions`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {policy.conditions &&
                                policy.conditions.length > 0 && (
                                  <td className="table-warning policyitem-field">
                                    <i>{`Conditions`}</i>
                                    {`: ${policy.conditions.map(
                                      (type) =>
                                        `${type.type} : ${type.values.join(
                                          ", "
                                        )}`
                                    )} `}
                                  </td>
                                )}
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Delegate Admin`}</i>
                                {`: ${
                                  policy.delegateAdmin == true
                                    ? "enabled"
                                    : "disabled"
                                } `}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}
          </div>
        )}

      {/* IMPORT END */}
      {action == "IMPORT END" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_POLICY.value && (
          <div>
            <h5 className="bold wrap-header m-t-sm">Details:</h5>
            <Table className="table table-bordered w-50">
              {importEnd.map(
                (c) =>
                  !isEmpty(c.previousValue) &&
                  Object.keys(JSON.parse(c.previousValue)).map((s, index) => (
                    <tbody key={index}>
                      <tr>
                        <td className="table-warning policyitem-field">{s}</td>
                        <td className="table-warning policyitem-field">
                          {JSON.parse(c.previousValue)[s]}
                        </td>
                      </tr>
                    </tbody>
                  ))
              )}
            </Table>
          </div>
        )}

      {/* IMPORT START */}
      {action == "IMPORT START" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_POLICY.value && (
          <div>
            <p className="text-center">
              Importing policies from file is started...
            </p>
          </div>
        )}
      {/* Export JSON */}
      {(action == "EXPORT JSON" ||
        action == "EXPORT CSV" ||
        action == "EXPORT EXCEL") &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_POLICY.value && (
          <>
            <h5 className="bold wrap-header m-t-sm">Details:</h5>
            <Table className="table table-bordered w-50">
              {exportVal.map(
                (c) =>
                  !isEmpty(c.previousValue) &&
                  Object.keys(JSON.parse(c.previousValue)).map((s, index) => (
                    <tbody key={index}>
                      <tr>
                        <td className="table-warning">{s}</td>
                        <td className="table-warning">
                          {c &&
                          c.previousValue &&
                          !isEmpty(JSON.parse(c.previousValue))
                            ? exportOldVal(s, JSON.parse(c.previousValue)[s])
                            : "--"}
                        </td>
                      </tr>
                    </tbody>
                  ))
              )}
            </Table>
          </>
        )}

      {/* Import Delete */}

      {action == "Import Delete" &&
        objectClassType == ClassTypes.CLASS_TYPE_RANGER_POLICY.value && (
          <div>
            <div className="fw-bolder">
              Policy ID :{" "}
              <Badge className="d-inline-flex me-1" bg="info">
                {objectId}
              </Badge>
            </div>
            <div className="fw-bolder">Policy Name: {objectName}</div>
            <div className="fw-bolder">Service Name: {parentObjectName}</div>
            <div className="fw-bolder">
              Deleted Date: {currentTimeZone(createDate)}
            </div>
            <div className="fw-bolder">Deleted By: {owner} </div>
            <br />
            <h5 className="bold wrap-header m-t-sm">Policy Details:</h5>
            <Table className="table table-bordered w-auto">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>
                  <th>Old Value</th>
                </tr>
              </thead>
              <tbody>
                {ImportDeleteDetails(
                  importDeleteDetails,
                  importdeleteresources
                )}
              </tbody>
            </Table>
            <br />

            {action == "Import Delete" &&
              !isEmpty(importDelValidityOld) &&
              !isUndefined(importDelValidityOld) &&
              importDelValidityOld != "[]" &&
              importDelValidityOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">Validity Period:</h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>old Value</th>
                      </tr>
                    </thead>
                    {importDelValidityOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Start Date`}</i>
                                {`: ${
                                  !isEmpty(policy.startTime)
                                    ? policy.startTime
                                    : "--"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`End Date`}</i>
                                {`: ${
                                  !isEmpty(policy.endTime)
                                    ? policy.endTime
                                    : "--"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Time Zone`}</i>
                                {`: ${
                                  !isEmpty(policy.timeZone)
                                    ? policy.timeZone
                                    : "--"
                                } `}
                              </td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "Import Delete" &&
              !isEmpty(importdelmaskpolicyold) &&
              !isUndefined(importdelmaskpolicyold) &&
              importdelmaskpolicyold != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Masking Policy Items:
                  </h5>

                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {importdelmaskpolicyold.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy, index) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Accesses`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {!isEmpty(policy.conditions) && (
                                <td className="table-warning policyitem-field">
                                  <i>{`Conditions`}</i>
                                  {`: ${policy.conditions.map(
                                    (type) =>
                                      `${type.type} : ${type.values.join(", ")}`
                                  )} `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              {policy.delegateAdmin == true && (
                                <td className="table-warning policyitem-field">
                                  <i>{`Delegate Admin`}</i>
                                  {`: ${
                                    policy.delegateAdmin == true
                                      ? "enabled"
                                      : "disabled"
                                  } `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              {!isEmpty(policy.dataMaskInfo) && (
                                <td className="table-warning text-nowrap policyitem-field">
                                  <i>{`Data Mask Types: `}</i>
                                  {getDataMaskType(
                                    policy.DataMasklabel,
                                    policy.dataMaskInfo
                                  )}
                                </td>
                              )}
                            </tr>
                            {JSON.parse(policyitem).length - 1 != index && (
                              <tr>
                                <td>
                                  <br />
                                </td>
                              </tr>
                            )}
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "Import Delete" &&
              !isEmpty(importDeleteRowPolicyOld) &&
              !isUndefined(importDeleteRowPolicyOld) &&
              importDeleteRowPolicyOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Row Level Filter Policy Items:
                  </h5>

                  <Table className="table table-striped  table-bordered   w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {importDeleteRowPolicyOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy, index) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Roles`}</i>
                                {`: ${
                                  !isEmpty(policy.roles)
                                    ? policy.roles.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Groups`}</i>
                                {`: ${
                                  !isEmpty(policy.groups)
                                    ? policy.groups.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Users`}</i>
                                {`: ${
                                  !isEmpty(policy.users)
                                    ? policy.users.join(", ")
                                    : "<empty>"
                                } `}
                              </td>
                            </tr>
                            <tr>
                              <td className="table-warning policyitem-field">
                                <i>{`Accesses`}</i>
                                {!isEmpty(policy.accesses)
                                  ? `: ${policy.accesses
                                      .map((obj) => obj.type)
                                      .join(", ")} `
                                  : "<empty>"}
                              </td>
                            </tr>
                            <tr>
                              {!isEmpty(policy.conditions) && (
                                <td className="table-warning policyitem-field">
                                  <i>{`Conditions`}</i>
                                  {`: ${policy.conditions.map(
                                    (type) =>
                                      `${type.type} : ${type.values.join(", ")}`
                                  )} `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              {policy.delegateAdmin == true && (
                                <td className="table-warning policyitem-field">
                                  <i>{`Delegate Admin`}</i>
                                  {`: ${
                                    policy.delegateAdmin == true
                                      ? "enabled"
                                      : "disabled"
                                  } `}
                                </td>
                              )}
                            </tr>
                            <tr>
                              {policy.DataMasklabel &&
                                policy.DataMasklabel.length > 0 && (
                                  <td className="table-warning policyitem-field">
                                    <i>{`Data Mask Types`}</i>
                                    {`: ${policy.DataMasklabel} `}
                                  </td>
                                )}
                            </tr>
                            {JSON.parse(policyitem).length - 1 != index && (
                              <tr>
                                <td>
                                  <br />
                                </td>
                              </tr>
                            )}
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "Import Delete" &&
              !isEmpty(importPolicyConditionOld) &&
              !isUndefined(importPolicyConditionOld) &&
              importPolicyConditionOld != 0 &&
              importPolicyConditionOld != "[]" && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Policy Conditions:
                  </h5>
                  <Table className="table table-bordered w-25">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>

                    {importPolicyConditionOld.map((policyitem) => {
                      return (
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => (
                          <tbody>
                            <tr>
                              <td className="table-warning policyitem-field">{`${policy.type}: ${policy.values}`}</td>
                            </tr>
                          </tbody>
                        ))
                      );
                    })}
                  </Table>
                  <br />
                </>
              )}

            {action == "Import Delete" &&
              !isEmpty(importDeleteItemsOld) &&
              !isUndefined(importDeleteItemsOld) &&
              importDeleteItemsOld != 0 && (
                <>
                  <h5 className="bold wrap-header m-t-sm">
                    Allow PolicyItems:
                  </h5>
                  <Table className="table table-bordered w-auto">
                    <thead className="thead-light">
                      <tr>
                        <th>Old Value</th>
                      </tr>
                    </thead>
                    {importDeleteItemsOld.map(
                      (policyitem) =>
                        !isEmpty(policyitem) &&
                        JSON.parse(policyitem).map((policy) => {
                          return (
                            <>
                              <tbody>
                                <tr>
                                  <td className="table-warning policyitem-field">
                                    <i>{`Roles`}</i>
                                    {`: ${
                                      !isEmpty(policy.roles)
                                        ? policy.roles.join(", ")
                                        : "<empty>"
                                    } `}
                                  </td>
                                </tr>
                                <tr>
                                  <td className="table-warning policyitem-field">
                                    <i>{`Groups`}</i>
                                    {`: ${
                                      !isEmpty(policy.groups)
                                        ? policy.groups.join(", ")
                                        : "<empty>"
                                    } `}
                                  </td>
                                </tr>
                                <tr>
                                  <td className="table-warning policyitem-field">
                                    <i>{`Users`}</i>
                                    {`: ${
                                      !isEmpty(policy.users)
                                        ? policy.users.join(", ")
                                        : "<empty>"
                                    } `}
                                  </td>
                                </tr>
                                <tr>
                                  <td className="table-warning policyitem-field">
                                    <i>{`Permissions`}</i>
                                    {!isEmpty(policy.accesses)
                                      ? `: ${policy.accesses
                                          .map((obj) => obj.type)
                                          .join(", ")} `
                                      : "<empty>"}
                                  </td>
                                </tr>
                                <tr>
                                  {policy.conditions &&
                                    policy.conditions.length > 0 && (
                                      <td className="table-warning policyitem-field">
                                        <i>{`Conditions`}</i>
                                        {`: ${policy.conditions.map(
                                          (type) =>
                                            `${type.type} : ${type.values}`
                                        )} `}
                                      </td>
                                    )}
                                </tr>
                                <tr>
                                  <td className="table-warning policyitem-field">
                                    <i>{`Delegate Admin`}</i>
                                    {`: ${
                                      policy.delegateAdmin == false
                                        ? "enabled"
                                        : "disabled"
                                    } `}
                                  </td>
                                </tr>
                              </tbody>
                            </>
                          );
                        })
                    )}
                  </Table>
                  <br />
                </>
              )}
          </div>
        )}

      {action == "Import Delete" &&
        !isEmpty(importdelPolicyExceptionOld) &&
        !isUndefined(importdelPolicyExceptionOld) &&
        importdelPolicyExceptionOld != 0 && (
          <>
            <h5 className="bold wrap-header m-t-sm">
              Allow Exception PolicyItems:
            </h5>
            <Table className="table table-bordered w-auto">
              <thead className="thead-light">
                <tr>
                  <th>Old Value</th>
                </tr>
              </thead>

              {importdelPolicyExceptionOld.map((policyitem) => {
                return (
                  !isEmpty(policyitem) &&
                  JSON.parse(policyitem).map((policy) => (
                    <tbody>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Roles`}</i>
                          {`: ${
                            !isEmpty(policy.roles)
                              ? policy.roles.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Groups`}</i>
                          {`: ${
                            !isEmpty(policy.groups)
                              ? policy.groups.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Users`}</i>
                          {`: ${
                            !isEmpty(policy.users)
                              ? policy.users.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Permissions`}</i>
                          {!isEmpty(policy.accesses)
                            ? `: ${policy.accesses
                                .map((obj) => obj.type)
                                .join(", ")} `
                            : "<empty>"}
                        </td>
                      </tr>
                      <tr>
                        {policy.conditions && policy.conditions.length > 0 && (
                          <td className="table-warning policyitem-field">
                            <i>{`Conditions`}</i>
                            {`: ${policy.conditions.map(
                              (type) => `${type.type} : ${type.values}`
                            )} `}
                          </td>
                        )}
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Delegate Admin`}</i>
                          {`: ${
                            policy.delegateAdmin == true
                              ? "enabled"
                              : "disabled"
                          } `}
                        </td>
                      </tr>
                    </tbody>
                  ))
                );
              })}
            </Table>
            <br />
          </>
        )}

      {action == "Import Delete" &&
        !isEmpty(importdeldenyPolicyold) &&
        !isUndefined(importdeldenyPolicyold) &&
        importdeldenyPolicyold != 0 && (
          <>
            <h5 className="bold wrap-header m-t-sm">Deny PolicyItems:</h5>
            <Table className="table table-bordered w-auto">
              <thead className="thead-light">
                <tr>
                  <th>Old Value</th>
                </tr>
              </thead>

              {importdeldenyPolicyold.map((policyitem) => {
                return (
                  !isEmpty(policyitem) &&
                  JSON.parse(policyitem).map((policy) => (
                    <tbody>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Roles`}</i>
                          {`: ${
                            !isEmpty(policy.roles)
                              ? policy.roles.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Groups`}</i>
                          {`: ${
                            !isEmpty(policy.groups)
                              ? policy.groups.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Users`}</i>
                          {`: ${
                            !isEmpty(policy.users)
                              ? policy.users.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Permissions`}</i>
                          {!isEmpty(policy.accesses)
                            ? `: ${policy.accesses
                                .map((obj) => obj.type)
                                .join(", ")} `
                            : "<empty>"}
                        </td>
                      </tr>
                      <tr>
                        {policy.conditions && policy.conditions.length > 0 && (
                          <td className="table-warning policyitem-field">
                            <i>{`Conditions`}</i>
                            {`: ${policy.conditions.map(
                              (type) => `${type.type} : ${type.values}`
                            )}`}
                          </td>
                        )}
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Delegate Admin`}</i>
                          {`: ${
                            policy.delegateAdmin == true
                              ? "enabled"
                              : "disabled"
                          }`}
                        </td>
                      </tr>
                    </tbody>
                  ))
                );
              })}
            </Table>
            <br />
          </>
        )}

      {action == "Import Delete" &&
        !isEmpty(importDeldenyExceptionsold) &&
        !isUndefined(importDeldenyExceptionsold) &&
        importDeldenyExceptionsold != 0 && (
          <>
            <h5 className="bold wrap-header m-t-sm">
              Deny Exception PolicyItems:
            </h5>
            <Table className="table table-bordered w-auto">
              <thead className="thead-light">
                <tr>
                  <th>Old Value</th>
                </tr>
              </thead>

              {importDeldenyExceptionsold.map((policyitem) => {
                return (
                  !isEmpty(policyitem) &&
                  JSON.parse(policyitem).map((policy) => (
                    <tbody>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Roles`}</i>
                          {`: ${
                            !isEmpty(policy.roles)
                              ? policy.roles.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Groups`}</i>
                          {`: ${
                            !isEmpty(policy.groups)
                              ? policy.groups.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Users`}</i>
                          {`: ${
                            !isEmpty(policy.users)
                              ? policy.users.join(", ")
                              : "<empty>"
                          } `}
                        </td>
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Permissions`}</i>
                          {`: ${policy.accesses
                            .map((obj) => obj.type)
                            .join(", ")}`}
                        </td>
                      </tr>
                      <tr>
                        {policy.conditions && policy.conditions.length > 0 && (
                          <td className="table-warning policyitem-field">
                            <i>{`Conditions:`}</i>
                            {`: ${policy.conditions.map(
                              (type) => `${type.type} : ${type.values}`
                            )}`}
                          </td>
                        )}
                      </tr>
                      <tr>
                        <td className="table-warning policyitem-field">
                          <i>{`Delegate Admin`}</i>
                          {`: ${
                            policy.delegateAdmin == true
                              ? "enabled"
                              : "disabled"
                          }`}
                        </td>
                      </tr>
                      <br />
                    </tbody>
                  ))
                );
              })}
            </Table>
          </>
        )}
    </div>
  );
};

export default PolicyLogs;
