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
import moment from "moment-timezone";
import { Link } from "react-router-dom";
import { isEmpty } from "lodash";
import chevronIcon from "Images/sidebar/chevron.svg";

export const CustomBreadcrumb = (props) => {
  const { data, type } = props;

  return (
    <>
      <div>
        <span className="navbar-text last-response-time">
          <strong>Last Response Time</strong>
          <br />
          {moment(moment()).format("MM/DD/YYYY hh:mm:ss A")}
        </span>
      </div>
      {type?.length > 0 && (
        <div className="r_breadcrumbs">
          <ul className="breadcrumb-new">
            {type?.map((obj, index) => {
              let link = data[index][obj].href;
              return (
                <React.Fragment key={index}>
                  {" "}
                  <li
                    className={
                      index >= 1
                        ? "allow_nav breadcrumb-item"
                        : "breadcrumb-item"
                    }
                  >
                    {isEmpty(link) ? (
                      <Link
                        to="#"
                        onClick={(e) => e.preventDefault()}
                        className="text-decoration-none"
                      >
                        <p
                          className="trim-breadcrumb-content"
                          title={data[index][obj].text}
                        >
                          {data[index][obj].text}
                        </p>
                      </Link>
                    ) : (
                      <Link to={link} className="text-decoration-none">
                        <p
                          className="trim-breadcrumb-content"
                          title={data[index][obj].text}
                        >
                          {data[index][obj].text}
                        </p>
                      </Link>
                    )}
                    <img src={chevronIcon} className="breadcrumb-chevron" />
                  </li>
                </React.Fragment>
              );
            })}
          </ul>
        </div>
      )}
    </>
  );
};
export default CustomBreadcrumb;
