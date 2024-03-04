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
import { useNavigate, useLocation } from "react-router-dom";
import errorIcon from "Images/error-404-icon.png";
import { Button } from "react-bootstrap";
import { getLandingPageURl } from "../utils/XAUtils";

export const ErrorPage = (props) => {
  const [errorCode, setErrorCode] = useState(null);
  const [errorInfo, setErrorInfo] = useState(null);
  let navigate = useNavigate();
  let currentURLObj = useLocation();
  let customURL = ["/dataNotFound", "/pageNotFound", "/forbidden"];

  useEffect(() => {
    if (props.errorCode == "401") {
      setErrorCode("Access Denied (401).");
      setErrorInfo(
        "Sorry, you don't have enough privileges to view this page."
      );
    }
    if (props.errorCode == "204") {
      setErrorCode("Content not found (204).");
      setErrorInfo(
        "Sorry, Please sync-up the users with your source directory."
      );
    }
    if (props.errorCode == "403") {
      setErrorCode("Forbidden (403).");
      setErrorInfo(
        "Sorry, you don't have enough privileges to view this page."
      );
    }
    if (props.errorCode == "checkSSOTrue") {
      setErrorCode("Sign Out Is Not Complete!");
      setErrorInfo(
        <span>
          Authentication to this instance of Ranger is managed externally(for
          example,Apache Knox). You can still open this instance of Ranger from
          the same web browser without re-authentication.To prevent additional
          access to Ranger,
          <strong>close all browser windows and exit the browser</strong>.
        </span>
      );
    }
    if (props.errorCode == "404") {
      setErrorCode("Page not found (404).");
      setErrorInfo("Sorry, this page isn't here or has moved.");
    }
    if (props.errorCode == "400") {
      setErrorCode("Data Not Found (400).");
      setErrorInfo("Data not found for given page.");
    }
  });

  return (
    <div data-id="pageNotFoundPage" className="new-error-page">
      <div className="new-error-box">
        <div className="error-white-bg">
          <div className="new-icon-box">
            <img src={errorIcon}></img>
          </div>
          <div className="new-description-box">
            <h4 className="m-t-xs m-b-xs" data-id="msg">
              {errorCode}
            </h4>
            <div data-id="moreInfo">{errorInfo}</div>
          </div>
        </div>
        <div className="mt-2">
          <Button
            size="sm"
            onClick={() =>
              customURL.indexOf(currentURLObj.pathname) >= 0
                ? navigate(-2)
                : navigate(-1)
            }
            className="me-1"
          >
            <i className="fa-fw fa fa-long-arrow-left"></i> Go back
          </Button>
          {props.errorCode !== "checkSSOTrue" && (
            <Button size="sm" onClick={() => navigate(getLandingPageURl())}>
              Home
            </Button>
          )}
        </div>
      </div>
    </div>
  );
};
export default ErrorPage;
