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

const appState = {};
const serviceDefData = {};

function getUserProfile() {
  return appState.userProfile;
}

function setUserProfile(profile = null) {
  appState.userProfile = profile;
}

function getServiceDef() {
  return serviceDefData;
}

function updateServiceDef(serviceDef, constraintDefs) {
  return serviceDef.map((item) => {
    if (item.id === 208) {
      return { ...item, constraintDefs: constraintDefs }; // or shorthand { ...item, constraintDefs }
    }
    return item;
  });
}

function setServiceDef(
  serviceDef,
  tagServiceDef,
  gdsServiceDef,
  allServiceDefs
) {
  let constraintDefs = [
    {
      itemId: 1,
      name: "requireApproval",
      label: "Require approval",
      resources: ["caiiservicecrn", "namespace", "endpoint"],
      accessTypes: ["view", "access"],
      parameters: [
        {
          itemId: 1,
          name: "approver",
          label: "Approver",
          type: "user",
          mandatory: true
        }
      ]
    },
    {
      itemId: 2,
      name: "maxRows",
      label: "Maximum rows",
      resources: ["caiiservicecrn", "namespace", "application"],
      accessTypes: ["access", "manage"],
      parameters: [
        {
          itemId: 1,
          name: "rowCount",
          type: "int",
          mandatory: true
        }
      ]
    }
  ];

  serviceDefData.serviceDefs = updateServiceDef(serviceDef, constraintDefs);
  serviceDefData.tagServiceDefs = tagServiceDef;
  serviceDefData.gdsServiceDef = gdsServiceDef;
  serviceDefData.allServiceDefs = updateServiceDef(
    allServiceDefs,
    constraintDefs
  );
}

export { getUserProfile, setUserProfile, getServiceDef, setServiceDef };
