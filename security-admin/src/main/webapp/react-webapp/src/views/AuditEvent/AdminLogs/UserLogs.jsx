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
import { ClassTypes, UserRoles } from "../../../utils/XAEnums";
import { isEmpty, unionBy, difference, isEqual, without } from "lodash";
import { currentTimeZone } from "../../../utils/XAUtils";

export const UserLogs = ({ data, reportdata }) => {
  const { objectName, objectClassType, createDate, owner, action } = data;
  const userGrp = reportdata.filter((obj) => {
    return obj.attributeName == "Group Name";
  });
  const createUserDetails = reportdata.filter((obj) => {
    return obj.attributeName != "Group Name" && obj.action == "create";
  });
  const createUserGrp = reportdata.filter((obj) => {
    return obj.attributeName == "Group Name" && obj.action == "create";
  });

  const updateUserDetails = reportdata.filter((obj) => {
    return obj.attributeName != "Group Name" && obj.action == "update";
  });

  const updateUserDetailsOld = updateUserDetails.map(
    (obj) => obj.previousValue
  );
  const updateUserDetailsNew = updateUserDetails.map((obj) => obj.newValue);

  const updateUserGrp = reportdata.filter((obj) => {
    return obj.attributeName == "Group Name";
  });

  const updateGrpOld = without(
    updateUserGrp.map((obj) => obj.previousValue),
    ""
  );
  const updateGrpNew = without(
    updateUserGrp.map((obj) => obj.newValue),
    ""
  );
  const deleteUser = reportdata.filter((obj) => {
    return obj.action == "delete";
  });
  const updatePolicyOldNew = (userDetails) => {
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
            <td className="table-warning text-nowrap">{val.attributeName}</td>
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
              <td className="table-warning">{"--"}</td>
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
              <td className="table-warning">{"--"}</td>
            )}
          </tr>
        </>
      );
    });

    return tablerow;
  };

  const updateOldNew = (previousValue, newValue) => {
    var tablerow = [];
    let oldVal = previousValue;
    let newVal = newValue;

    const diffVal = (obj1, obj2) => {
      let diff = null;
      if (!isEmpty(obj1)) {
        diff = difference(obj1, obj2);
      } else {
        return (diff = obj2);
      }
      return diff;
    };

    var removedUsers = diffVal(previousValue, newValue);
    var addUsers = diffVal(newValue, previousValue);

    const getfilteredoldval = (oldVals) => {
      let filterdiff = null;
      !isEmpty(removedUsers)
        ? (filterdiff = difference(oldVals, removedUsers))
        : (filterdiff = oldVals);
      return (
        <>
          {!isEqual(oldVals, newVal)
            ? !isEmpty(removedUsers)
              ? unionBy(
                  filterdiff.map((obj) => {
                    return (
                      <>
                        <span>{obj}</span>
                        {`, `}
                      </>
                    );
                  }),
                  removedUsers.map((obj) => {
                    return (
                      <>
                        <Badge className="d-inline me-1" bg="danger">
                          {obj}
                        </Badge>
                      </>
                    );
                  })
                )
              : !isEmpty(filterdiff)
              ? filterdiff.map((obj) => obj).join(", ")
              : "--"
            : !isEmpty(oldVals)
            ? oldVals.map((obj) => obj).join(", ")
            : "--"}
        </>
      );
    };
    const getfilterednewval = (newVals) => {
      let filterdiff = null;
      !isEmpty(addUsers)
        ? (filterdiff = difference(newVals, addUsers))
        : (filterdiff = newVals);
      return (
        <>
          {!isEqual(newVals, oldVal)
            ? !isEmpty(addUsers)
              ? unionBy(
                  filterdiff.map((obj) => {
                    return (
                      <>
                        <span>{obj}</span>
                        {`, `}
                      </>
                    );
                  }),
                  addUsers.map((obj) => {
                    return (
                      <>
                        <Badge className="d-inline me-1" bg="success">
                          {obj}
                        </Badge>
                      </>
                    );
                  })
                )
              : !isEmpty(filterdiff)
              ? filterdiff.map((obj) => obj).join(", ")
              : "--"
            : !isEmpty(newVals)
            ? newVals.map((obj) => obj).join(", ")
            : "--"}
        </>
      );
    };

    tablerow.push(
      <tr>
        <td className="table-warning">Groups</td>
        {previousValue && !isEmpty(previousValue) ? (
          <td className="table-warning text-nowrap">
            {getfilteredoldval(previousValue)}
          </td>
        ) : (
          <td className="table-warning">{"--"}</td>
        )}

        {newValue && !isEmpty(newValue) ? (
          <td className="table-warning text-nowrap">
            {getfilterednewval(newValue)}
          </td>
        ) : (
          <td className="table-warning">{"--"}</td>
        )}
      </tr>
    );

    return tablerow;
  };
  const deleteOldNew = (attribute, val) => {
    if (attribute == "User Role") {
      val = val.replace(/[[\]]/g, "");
      if (val == "ROLE_USER") {
        val = UserRoles.ROLE_USER.label;
      }
      if (val == "ROLE_SYS_ADMIN") {
        val = UserRoles.ROLE_SYS_ADMIN.label;
      }
      if (val == "ROLE_KEY_ADMIN") {
        val = UserRoles.ROLE_KEY_ADMIN.label;
      }
      if (val == "ROLE_ADMIN_AUDITOR") {
        val = UserRoles.ROLE_ADMIN_AUDITOR.label;
      }
      if (val == "ROLE_KEY_ADMIN_AUDITOR") {
        val = UserRoles.ROLE_KEY_ADMIN_AUDITOR.label;
      }
    }
    return val;
  };
  return (
    <div>
      {/* CREATE  */}
      {action == "create" &&
        objectClassType == ClassTypes.CLASS_TYPE_XA_USER.value && (
          <div>
            <div className="fw-bolder">Name: {objectName}</div>
            <div className="fw-bolder">Date: {currentTimeZone(createDate)}</div>
            <div className="fw-bolder">Created By: {owner} </div>
            <br />
            <h5 className="bold wrap-header m-t-sm">User Details:</h5>

            <Table className="table table-bordered w-50">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>

                  <th>New Value</th>
                </tr>
              </thead>
              {createUserDetails.map((user, index) => {
                return (
                  <tbody>
                    <tr key={index}>
                      <td className="table-warning text-nowrap">
                        {user.attributeName}
                      </td>

                      <td className="table-warning">
                        {!isEmpty(user.newValue)
                          ? deleteOldNew(user.attributeName, user.newValue)
                          : "--"}
                      </td>
                    </tr>
                  </tbody>
                );
              })}
            </Table>
            <br />

            {action == "create" && !isEmpty(createUserGrp) && (
              <>
                <h5 className="bold wrap-header m-t-sm">Group: </h5>
                <Table className="table table-bordered w-50">
                  <thead className="thead-light">
                    <tr>
                      <th>Fields</th>
                      <th>New Value</th>
                    </tr>
                  </thead>

                  <tbody>
                    <tr>
                      <td className="table-warning">Groups</td>
                      <td className="table-warning">
                        {userGrp
                          .map((u) => {
                            return u.newValue;
                          })
                          .join(", ")}
                      </td>
                    </tr>
                  </tbody>
                </Table>
              </>
            )}
          </div>
        )}

      {/* UPDATE */}

      {action == "update" &&
        objectClassType == ClassTypes.CLASS_TYPE_XA_USER.value && (
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
            {action == "update" &&
              ((updateUserDetailsOld.length > 0 &&
                updateUserDetailsOld[0] != "") ||
                (updateUserDetailsNew.length > 0 &&
                  updateUserDetailsNew[0] != "")) && (
                <>
                  <h5 className="bold wrap-header m-t-sm">User Details:</h5>

                  <Table className="table table-bordered w-50">
                    <thead className="thead-light">
                      <tr>
                        <th>Fields</th>
                        <th>Old Value</th>
                        <th>New Value</th>
                      </tr>
                    </thead>

                    <tbody>{updatePolicyOldNew(updateUserDetails)}</tbody>
                  </Table>
                  <br />
                </>
              )}
            {action == "update" && !isEmpty(updateUserGrp) && (
              <>
                <h5 className="bold wrap-header m-t-sm">Group: </h5>
                <Table className="table table-bordered w-50">
                  <thead className="thead-light">
                    <tr>
                      <th>Fields</th>
                      <th>Old Value</th>
                      <th>New Value</th>
                    </tr>
                  </thead>
                  <tbody>{updateOldNew(updateGrpOld, updateGrpNew)}</tbody>
                </Table>
              </>
            )}
          </div>
        )}

      {/* DELETE  */}

      {action == "delete" &&
        objectClassType == ClassTypes.CLASS_TYPE_XA_USER.value && (
          <div>
            <div className="fw-bolder">Name : {objectName}</div>
            <div className="fw-bolder">Date: {currentTimeZone(createDate)}</div>
            <div className="fw-bolder">Deleted By: {owner} </div>
            <br />
            <h5 className="bold wrap-header m-t-sm">User Details:</h5>

            <Table className="table table-bordered w-50">
              <thead className="thead-light">
                <tr>
                  <th>Fields</th>
                  <th>Old Value</th>
                </tr>
              </thead>
              {deleteUser.map((obj, index) => {
                return (
                  <tbody>
                    <tr key={index}>
                      <td className="table-warning">{obj.attributeName}</td>
                      <td className="table-warning">
                        {!isEmpty(obj.previousValue)
                          ? deleteOldNew(obj.attributeName, obj.previousValue)
                          : "--"}
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

export default UserLogs;
