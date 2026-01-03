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
import { pluginStatusColumnInfoMsg } from "./XAEnums";

export const RegexMessage = {
  MESSAGE: {
    policyNameInfoIconMessage:
      "Please avoid these characters (&, <, >, ', \", `) for policy name.",
    userNameValidationMsg: (
      <>
        <p className="pd-10 mb-0" style={{ fontSize: "small" }}>
          1. User name should be start with alphabet / numeric / underscore /
          non-us characters.
          <br />
          2. Allowed special character ,._-+/@= and space.
          <br />
          3. Name length should be greater than one.
        </p>
      </>
    ),
    passwordvalidationinfomessage:
      "Password should be minimum 8 characters ,atleast one uppercase letter, one lowercase letter and one numeric. For FIPS environment password should be minimum 14 characters with atleast one uppercase letter, one special characters, one lowercase letter and one numeric.",
    emailvalidationinfomessage: (
      <>
        <p className="pd-10 mb-0" style={{ fontSize: "small" }}>
          1. Email address should be start with alphabet / numeric / underscore
          / non-us characters.
          <br /> 2. Allowed special character <b>.-@</b> .
          <br />
          3. Email address length should be greater than 9 characters.
          <br /> 4. Email address examples : abc@de.fg, A-C@D-.FG
        </p>
      </>
    ),
    policyConditionInfoIcon:
      "1. JavaScript Condition Examples :\
                      country_code == 'USA', time_range >= 900 time_range <= 1800 etc.\
                      2. Dragging bottom-right corner of javascript condition editor(Textarea) can resizable",
    firstNameValidationMsg: (
      <>
        <p className="pd-10 mb-0" style={{ fontSize: "small" }}>
          1. First name should be start with alphabet / numeric / underscore /
          non-us characters.
          <br />
          2. Allowed special character ,._-+/@= and space.
          <br />
          3. Name length should be greater than one.
        </p>
      </>
    ),
    lastNameValidationMsg: (
      <>
        <p className="pd-10 mb-0" style={{ fontSize: "small" }}>
          1. Last name should be start with alphabet / numeric / underscore /
          non-us characters.
          <br />
          2. Allowed special character ,._-+/@= and space.
          <br />
          3. Name length should be greater than one.
        </p>
      </>
    )
  }
};

/* External User Edit Role Change */

export const roleChngWarning = (user) => {
  return (
    <>
      <strong>Warning !! </strong> : Please make sure that {`${user}`} user's
      role change performed here is consistent with ranger.usersync.
      group.based.role.assignment.rules property in ranger usersync
      configuration.
    </>
  );
};

/* Policy Info Message */

export const policyInfoMessage = {
  maskingPolicyInfoMsg: (
    <p className="pd-l-10 d-inline">
      Please ensure that users/groups listed in this policy have access to the
      column via an <b>Access Policy</b>. This policy does not implicitly grant
      access to the column.
    </p>
  ),
  maskingPolicyInfoMsgForTagBased: (
    <p className="pd-l-10 d-inline">
      Please ensure that users/groups listed in this policy have access to the
      tag via an <b>Access Policy</b>. This policy does not implicitly grant
      access to the tag.
    </p>
  ),
  rowFilterPolicyInfoMsg: (
    <p className="pd-l-10 d-inline">
      Please ensure that users/groups listed in this policy have access to the
      table via an <b>Access Policy</b>. This policy does not implicitly grant
      access to the table.
    </p>
  )
};

/* UDF resource change warning */

export const udfResourceWarning = () => {
  return (
    <p>
      <strong>Warning !! </strong> : UDF create is a privileged operation.
      Please make sure you grant them to only trusted users.
    </p>
  );
};

/* PluginStatus Column Info */

export const pluginStatusColumnInfo = (colName) => {
  return (
    <ul className="list-inline">
      <li className="list-inline-item">
        <strong>Last Update: </strong>{" "}
        {pluginStatusColumnInfoMsg[colName].lastUpdated}
      </li>
      <li className="list-inline-item">
        <strong>Download: </strong>
        {pluginStatusColumnInfoMsg[colName].downloadTime}
      </li>
      <li className="list-inline-item">
        <strong>Active: </strong>{" "}
        {pluginStatusColumnInfoMsg[colName].activeTime}
      </li>
    </ul>
  );
};
