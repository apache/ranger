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

export const CustomBreadcrumb = (props) => {
  const { data, type } = props;

  return (
    <div className="mt-n1 mb-2 headerBreadcrumbs">
      <div className="r_breadcrumbs">
        <ul className="breadcrumb">
          {type.map((obj, index) => {
            let link = data[index][obj].href;
            return (
              <li
                className={
                  index >= 1 ? "allow_nav breadcrumb-item" : "breadcrumb-item"
                }
                key={index}
              >
                {isEmpty(link) ? (
                  <a href="javascript:void(0)">
                    <p
                      className="trim-containt-breadcrumb"
                      title={data[index][obj].text}
                    >
                      {data[index][obj].text}
                    </p>
                  </a>
                ) : (
                  <Link to={link}>
                    <p
                      className="trim-containt-breadcrumb"
                      title={data[index][obj].text}
                    >
                      {data[index][obj].text}
                    </p>
                  </Link>
                )}
              </li>
            );
          })}
        </ul>
      </div>
      <div className="text-right latestResponse">
        <b>Last Response Time: </b>
        {moment(moment()).format("MM/DD/YYYY hh:mm:ss A")}
      </div>
    </div>
  );
};
export default CustomBreadcrumb;
