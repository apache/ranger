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
import { Button, Modal } from "react-bootstrap";
import {
  useLocation,
  Outlet,
  Navigate,
  useNavigate,
  matchRoutes
} from "react-router-dom";
import ErrorPage from "./ErrorPage";
import {
  hasAccessToPath,
  checkKnoxSSO,
  navigateTo,
  getLandingPageURl
} from "Utils/XAUtils";
import { useIdleTimer } from "react-idle-timer";
import { getUserProfile } from "Utils/appState";
import SideBar from "./SideBar/SideBar";
import { Loader } from "../components/CommonComponents";
import { Suspense } from "react";
import { PathAssociateWithModule } from "../utils/XAEnums";
import { flatMap, values } from "lodash";

const Layout = () => {
  let location = useLocation();
  const navigate = useNavigate();
  const userProfile = getUserProfile();
  navigateTo.navigate = useNavigate();
  const [open, setOpen] = useState(false);
  const [timer, setTimer] = useState(0);

  const timeout =
    1000 *
    (userProfile?.configProperties?.inactivityTimeout > 0
      ? userProfile?.configProperties?.inactivityTimeout
      : 900);
  const promptTimeout = 1000 * 15;

  const handleLogout = async () => {
    setOpen(false);
    checkKnoxSSO(navigate);
  };

  const onPrompt = () => {
    setOpen(true);
    setTimer(promptTimeout);
  };

  const onIdle = () => {
    setOpen(false);
    handleLogout();
    setTimer(0);
  };

  const onActive = () => {
    setOpen(false);
    setTimer(0);
  };

  const { getRemainingTime, isPrompted, activate } = useIdleTimer({
    timeout,
    promptTimeout,
    onPrompt,
    onIdle,
    onActive,
    crossTab: true,
    throttle: 1000,
    eventsThrottle: 1000,
    startOnMount: true
  });

  const handleStillHere = () => {
    setOpen(false);
    activate();
  };

  useEffect(() => {
    const interval = setInterval(() => {
      if (isPrompted()) {
        setTimer(Math.ceil(getRemainingTime() / 1000));
      }
    }, 1000);
    return () => {
      clearInterval(interval);
    };
  }, [getRemainingTime, isPrompted]);

  return (
    <React.Fragment>
      <Modal show={open}>
        <Modal.Header className="pd-10">
          <h6>
            <strong>Session Expiration Warning</strong>
          </h6>
        </Modal.Header>
        <Modal.Body className="fnt-14">
          <span className="d-inline-block">
            Because you have been inactive, your session is about to expire.
            <br />
            <div id="Timer">
              Time left : <b>{timer}</b> seconds remaining
            </div>
          </span>
        </Modal.Body>
        <Modal.Footer>
          <Button onClick={handleStillHere}>Stay Logged in</Button>
          <Button variant="danger" onClick={() => handleLogout()}>
            Log Out Now
          </Button>
        </Modal.Footer>
      </Modal>
      <React.Fragment>
        <div className="wrapper">
          <SideBar />
        </div>
        {location.pathname === "/" &&
          window.location.pathname !== "/locallogin" &&
          window.location.pathname != "/dataNotFound" &&
          window.location.pathname != "/pageNotFound" &&
          window.location.pathname != "/forbidden" && (
            <Navigate to={getLandingPageURl()} replace={true} />
          )}
        <div id="content" className="content-body">
          <div id="ranger-content">
            {matchRoutes(
              flatMap(values(PathAssociateWithModule)).map((val) => ({
                path: val
              })),
              location.pathname
            ) ? (
              hasAccessToPath(location.pathname) ? (
                <Suspense fallback={<Loader />}>
                  <Outlet />
                </Suspense>
              ) : (
                <ErrorPage errorCode="401" />
              )
            ) : (
              <ErrorPage errorCode="404" />
            )}
          </div>

          <footer>
            <div className="main-footer">
              <p className="text-start">
                <a
                  target="_blank"
                  href="http://www.apache.org/licenses/LICENSE-2.0"
                  rel="noopener noreferrer"
                >
                  Licensed under the Apache License, Version 2.0
                </a>
              </p>
            </div>
          </footer>
        </div>
      </React.Fragment>
    </React.Fragment>
  );
};

export default Layout;
