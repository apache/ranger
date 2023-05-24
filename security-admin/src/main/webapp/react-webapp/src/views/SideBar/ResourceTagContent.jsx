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

import React, { useEffect } from "react";
import { NavLink } from "react-router-dom";
import { sortBy, capitalize } from "lodash";
import { RangerPolicyType } from "../../utils/XAEnums";
import Spinner from "react-bootstrap/Spinner";

export const ResourceTagContent = (props) => {
  const { serviceDefData, serviceData, closeCollapse, loader } = props;

  const handleCloseCollapse = () => {
    localStorage.removeItem("zoneDetails");
    closeCollapse();
  };

  return loader ? (
    <Spinner
      animation="border"
      className="position-absolute"
      style={{
        height: "48px",
        width: "48px",
        top: "50%",
        left: "37%"
      }}
      role="status"
    ></Spinner>
  ) : (
    <ul className="list-group list-group-flush overflow-y-auto">
      {serviceDefData?.map((servicedef, index) => {
        let filterService = sortBy(
          serviceData?.filter((service) => service?.type === servicedef?.name),
          "name"
        );

        return (
          <React.Fragment key={index}>
            <li className="list-servicedef-title">
              {capitalize(servicedef?.displayName)}
            </li>
            {filterService?.map((service, index) => {
              return (
                <React.Fragment key={index}>
                  <li className="list-group-item">
                    <NavLink
                      onClick={handleCloseCollapse}
                      to={`/service/${service.id}/policies/${RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value}`}
                      className="list-group-item"
                    >
                      {service?.displayName ?? service?.name}
                    </NavLink>
                  </li>
                </React.Fragment>
              );
            })}
          </React.Fragment>
        );
      })}
    </ul>
  );
};

export default ResourceTagContent;
