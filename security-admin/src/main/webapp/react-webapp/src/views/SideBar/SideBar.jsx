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

import React, { useReducer, useEffect, useCallback, useState } from "react";
import {
  NavLink,
  matchRoutes,
  useLocation,
  useNavigate
} from "react-router-dom";
import Button from "react-bootstrap/Button";
import rangerIcon from "Images/sidebar/ranger.svg";
import keyIcon from "Images/sidebar/key.svg";
import tagsIcon from "Images/sidebar/tags.svg";
import reportsIcon from "Images/sidebar/reports.svg";
import auditsIcon from "Images/sidebar/audits.svg";
import zoneIcon from "Images/sidebar/zone.svg";
import settingsIcon from "Images/sidebar/settings.svg";
import accountIcon from "Images/sidebar/account.svg";
import Collapse from "react-bootstrap/Collapse";
import { fetchApi } from "Utils/fetchAPI";
import { getUserProfile, setUserProfile } from "Utils/appState";
import {
  hasAccessToTab,
  isAuditor,
  isKeyAdmin,
  isSystemAdmin,
  getBaseUrl,
  isKMSAuditor
} from "Utils/XAUtils";
import Select from "react-select";
import {
  cloneDeep,
  filter,
  isEmpty,
  map,
  sortBy,
  uniq,
  upperCase
} from "lodash";
import { toast } from "react-toastify";
import ResourceTagContent from "./ResourceTagContent";
import { PathAssociateWithModule } from "../../utils/XAEnums";
import { getServiceDef } from "../../utils/appState";
import { SideBarBody } from "./SideBarBody";
import { getLandingPageURl } from "../../utils/XAUtils";

function reducer(state, action) {
  switch (action.type) {
    case "SET_LOADER":
      return {
        ...state,
        loader: action.loader
      };
    case "SERVICEDEF_DATA":
      return {
        ...state,
        allserviceDefData: action.allserviceDefData,
        serviceDefData: action.serviceDefData,
        tagServiceDefData: action.tagServiceDefData
      };
    case "SERVICES_DATA":
      return {
        ...state,
        allServiceData: action.allServiceData,
        serviceData: action.serviceData,
        tagServiceData: action.tagServiceData
      };
    case "SELECTED_SERVCIEDEF_DATA":
      return {
        ...state,
        selectedServiceDef: action.selectedServiceDef
      };

    default:
      throw new Error();
  }
}

export const SideBar = () => {
  const { allServiceDefs, serviceDefs, tagServiceDefs } = cloneDeep(
    getServiceDef()
  );
  const isKMSRole = isKeyAdmin() || isKMSAuditor();
  const [keyState, dispatch] = useReducer(reducer, {
    loader: false,
    allserviceDefData: allServiceDefs,
    serviceDefData: serviceDefs,
    tagServiceDefData: tagServiceDefs,
    allServiceData: [],
    serviceData: [],
    tagServiceData: [],
    selectedServiceDef: [],
    serviceTypesOptions: sortBy(
      filter(allServiceDefs, (serviceDef) => serviceDef.name !== "tag"),
      "name"
    )
  });

  const {
    loader,
    serviceData,
    tagServiceData,
    allServiceData,
    serviceDefData,
    tagServiceDefData,
    allserviceDefData
  } = keyState;

  const userProps = getUserProfile();
  const loginId = (
    <span className="login-id user-name">{userProps?.loginId}</span>
  );

  let location = useLocation();
  let isListenerAttached = false;

  const [isActive, setActive] = useState(null);
  const [isDrawerOpen, setDrawer] = useState(false);
  const [accountDrawer, setAccountDrawer] = useState(false);
  const [isTagView, setTagView] = useState(false);

  const handleClickOutside = (e) => {
    if (
      document.getElementById("sidebar")?.contains(e?.target) == false &&
      document.getElementById("drawer-content")?.contains(e?.target) == false &&
      document.getElementById("account-drawer-content")?.contains(e?.target) ==
        false
    ) {
      setActive(null);
      setDrawer(false);
      setAccountDrawer(false);
    }
    e?.stopPropagation();
  };

  useEffect(() => {
    if (!isListenerAttached) {
      document?.addEventListener("mousedown", handleClickOutside);
      isListenerAttached = true;
      return;
    }
    return () => {
      document?.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  const fetchServicesData = async () => {
    dispatch({
      type: "SET_LOADER",
      loader: true
    });

    let servicesResp = [];
    let resourceServices = [];
    let tagServices = [];

    try {
      servicesResp = await fetchApi({
        url: "plugins/services"
      });
      tagServices = filter(servicesResp.data.services, ["type", "tag"]);
      if (isKMSRole) {
        resourceServices = filter(
          servicesResp.data.services,
          (service) => service.type == "kms"
        );
      } else {
        resourceServices = filter(
          servicesResp.data.services,
          (service) => service.type !== "tag" && service.type !== "kms"
        );
      }

      dispatch({
        type: "SERVICES_DATA",
        allServiceData: servicesResp.data.services,
        serviceData: resourceServices,
        tagServiceData: tagServices
      });
      dispatch({
        type: "SET_LOADER",
        loader: false
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Services or CSRF headers! ${error}`
      );
    }
  };

  const closeCollapse = () => {
    setActive(null);
    setDrawer(false);
    setAccountDrawer(false);
  };

  const activeClass = (modules) => {
    let allRouter = [];
    let PermissionPath = PathAssociateWithModule["Permission"]?.map((val) => ({
      path: val
    }));
    for (const key in PathAssociateWithModule) {
      if (key == modules) {
        allRouter = [...allRouter, ...PathAssociateWithModule[key]];
      }
    }
    let isValidRouter = matchRoutes(
      allRouter.map((val) => ({ path: val })),
      location.pathname
    );

    if (isValidRouter !== null && isValidRouter.length > 0) {
      return "navbar-active";
    } else if (
      matchRoutes(PermissionPath, location.pathname) &&
      modules == "Users/Groups"
    ) {
      return "navbar-active";
    } else {
      return;
    }
  };

  return (
    <React.Fragment>
      <nav id="sidebar">
        <div className="sidebar-header">
          <NavLink
            id="rangerIcon"
            to={getLandingPageURl()}
            onClick={() => {
              setActive(null);
              setDrawer(false);
              setAccountDrawer(false);
            }}
          >
            <img className="logo" src={rangerIcon} alt="Ranger logo" />
          </NavLink>
        </div>

        <ul className="list-unstyled components">
          {hasAccessToTab("Resource Based Policies") && (
            <li
              className={
                isActive !== null && isActive === "resourcesCollapse"
                  ? "selected"
                  : undefined
              }
            >
              <Button
                id="resourcesButton"
                className={activeClass("Resource Based Policies")}
                onClick={() => {
                  setActive("resourcesCollapse");
                  setAccountDrawer(false);
                  setDrawer(true);
                  setTagView(false);
                  fetchServicesData();
                }}
              >
                <img src={keyIcon} />
                <span>Resource Policies</span>
              </Button>
            </li>
          )}

          {hasAccessToTab("Tag Based Policies") && (
            <li
              className={
                isActive !== null && isActive === "tagCollapse"
                  ? "selected"
                  : undefined
              }
            >
              <Button
                id="tagButton"
                className={activeClass("Tag Based Policies")}
                onClick={() => {
                  setActive("tagCollapse");
                  setAccountDrawer(false);
                  setDrawer(true);
                  setTagView(true);
                  fetchServicesData();
                }}
              >
                <img src={tagsIcon} />
                <span>Tag Policies</span>
              </Button>
            </li>
          )}

          {hasAccessToTab("Reports") && (
            <li>
              <NavLink
                to="/reports/userAccess?policyType=0"
                className={activeClass("Reports")}
                onClick={() => {
                  setActive(null);
                  setDrawer(false);
                  setAccountDrawer(false);
                }}
              >
                <img src={reportsIcon} />
                <span>Reports</span>
              </NavLink>
            </li>
          )}

          {hasAccessToTab("Audit") && (
            <li
              className={
                isActive !== null && isActive === "auditCollapse"
                  ? "selected"
                  : undefined
              }
            >
              <Button
                className={activeClass("Audit")}
                onClick={() => {
                  setActive("auditCollapse");
                  setAccountDrawer(false);
                  setDrawer(true);
                }}
              >
                <img src={auditsIcon} />
                <span>Audits</span>
              </Button>
            </li>
          )}

          {hasAccessToTab("Security Zone") && (
            <React.Fragment>
              {!isKMSRole && (
                <li>
                  <NavLink
                    className={activeClass("Security Zone")}
                    to="/zones/zone/list"
                    onClick={() => {
                      setActive(null);
                      setDrawer(false);
                      setAccountDrawer(false);
                    }}
                  >
                    <img src={zoneIcon} />
                    <span>Security Zone</span>
                  </NavLink>
                </li>
              )}
            </React.Fragment>
          )}

          {hasAccessToTab("Key Manager") && (
            <React.Fragment>
              <li>
                <NavLink
                  className={activeClass("Key Manager")}
                  to="/kms/keys/new/manage/service"
                  onClick={() => {
                    setActive(null);
                    setDrawer(false);
                    setAccountDrawer(false);
                  }}
                >
                  <i className="fa fa-fw fa-key"></i>
                  <span>Key Manager</span>
                </NavLink>
              </li>
            </React.Fragment>
          )}

          {(hasAccessToTab("Users/Groups") ||
            isAuditor() ||
            isSystemAdmin()) && (
            <li
              className={
                isActive !== null && isActive === "settingsCollapse"
                  ? "selected"
                  : undefined
              }
            >
              <Button
                className={activeClass("Users/Groups")}
                onClick={() => {
                  setActive("settingsCollapse");
                  setAccountDrawer(false);
                  setDrawer(true);
                }}
              >
                <img src={settingsIcon} />
                <span>Settings</span>
              </Button>
            </li>
          )}

          <li
            className={
              isActive !== null && isActive === "accountCollapse"
                ? "selected"
                : undefined
            }
          >
            <Button
              className={activeClass("Profile")}
              onClick={() => {
                setActive("accountCollapse");
                setDrawer(false);
                setAccountDrawer(true);
              }}
            >
              <img src={accountIcon} />
              <span title={userProps?.loginId}>{loginId}</span>
            </Button>
          </li>
        </ul>
      </nav>

      <SideBarBody
        allServicesDefData={allserviceDefData}
        servicesDefData={serviceDefData}
        tagServicesDefData={tagServiceDefData}
        allServicesData={allServiceData}
        servicesData={serviceData}
        tagServicesData={tagServiceData}
        loader={loader}
        activeMenu={isActive}
        isDrawerOpen={isDrawerOpen}
        accountDrawer={accountDrawer}
        closeCollapse={closeCollapse}
        sideBarDispatch={dispatch}
      />
    </React.Fragment>
  );
};

export default SideBar;
