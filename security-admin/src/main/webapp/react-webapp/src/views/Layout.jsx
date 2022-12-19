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

import React, { lazy } from "react";
import { useLocation, Outlet, Navigate } from "react-router-dom";
import ErrorPage from "./ErrorPage";
import { hasAccessToPath } from "Utils/XAUtils";

const HeaderComp = lazy(() => import("Views/Header"));

const Layout = () => {
  let location = useLocation();
  return (
    <>
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
