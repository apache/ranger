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

import React, { lazy, useState, useEffect } from "react";
import { Button, Modal } from "react-bootstrap";
import { useLocation, Outlet, Navigate } from "react-router-dom";
import ErrorPage from "./ErrorPage";
import { hasAccessToPath } from "Utils/XAUtils";
import { useIdleTimer } from "react-idle-timer";
import { setUserProfile, getUserProfile } from "Utils/appState";

const HeaderComp = lazy(() => import("Views/Header"));

const Layout = () => {
  let location = useLocation();
  const userProfile = getUserProfile();
  const [open, setOpen] = useState(false);
  const [timer, setTimer] = useState(0);
  const timeout =
    1000 *
    (userProfile?.configProperties?.inactivityTimeout > 0
      ? userProfile?.configProperties?.inactivityTimeout
      : 900);
  const promptTimeout = 1000 * 15;

  const handleLogout = async (e) => {
    try {
      const { fetchApi } = await import("Utils/fetchAPI");
      await fetchApi({
        url: "logout",
        baseURL: "",
        headers: {
          "cache-control": "no-cache"
        }
      });
      setUserProfile(null);
      window.localStorage.clear();
      setOpen(false);
      window.location.replace("login.jsp");
    } catch (error) {
      console.error(`Error occurred while login! ${error}`);
    }
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
    <>
      <Modal show={open}>
        <Modal.Header className="pd-10">
          <h6>
            <strong>Session Expiration Warning</strong>
          </h6>
        </Modal.Header>
        <Modal.Body className="fnt-14">
          <span class="d-inline-block">
            Because you have been inactive, your session is about to expire.
            <br />
            <div id="Timer">
              Time left : <b>{timer}</b> seconds remaining
            </div>
          </span>
        </Modal.Body>
        <Modal.Footer>
          <Button onClick={handleStillHere}>Stay Logged in</Button>
          <Button variant="danger" onClick={handleLogout}>
            Log Out Now
          </Button>
        </Modal.Footer>
      </Modal>
      <HeaderComp />
      {location.pathname === "/" && (
        <Navigate to="/policymanager/resource" replace={true} />
      )}
      <section className="container-fluid" style={{ minHeight: "80vh" }}>
        <div id="ranger-content">
          {hasAccessToPath(location.pathname) ? <Outlet /> : <ErrorPage />}
        </div>
      </section>
      <footer>
        <div className="main-footer">
          <div className="pull-left copy-right-text">
            <p className="text-left">
              <a
                target="_blank"
                href="http://www.apache.org/licenses/LICENSE-2.0"
                rel="noopener noreferrer"
              >
                Licensed under the Apache License, Version 2.0
              </a>
            </p>
          </div>
        </div>
      </footer>
    </>
  );
};

export default Layout;
