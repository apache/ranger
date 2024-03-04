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

import React, { useReducer } from "react";
import { NavLink, useNavigate } from "react-router-dom";
import { sortBy, filter, isEmpty, map, upperCase, groupBy } from "lodash";
import closeIcon from "Images/close.svg";
import { getUserProfile, setUserProfile } from "Utils/appState";
import { fetchApi } from "Utils/fetchAPI";
import Select from "react-select";
import {
  hasAccessToTab,
  isAuditor,
  isKeyAdmin,
  isSystemAdmin,
  getBaseUrl,
  isKMSAuditor
} from "Utils/XAUtils";
import ResourceTagContent from "./ResourceTagContent";
import { toast } from "react-toastify";

function reducer(state, action) {
  switch (action.type) {
    case "SELECTED_SERVCIEDEF_DATA":
      return {
        ...state,
        selectedServiceDef: action.selectedServiceDef
      };
    default:
      throw new Error();
  }
}

export const SideBarBody = (props) => {
  const {
    loader,
    activeMenu,
    isDrawerOpen,
    accountDrawer,
    allServicesDefData,
    servicesDefData,
    tagServicesDefData,
    allServicesData,
    servicesData,
    tagServicesData,
    sideBarDispatch
  } = props;
  const [keyState, dispatch] = useReducer(reducer, {
    selectedServiceDef: []
  });

  const { selectedServiceDef } = keyState;

  const isKMSRole = isKeyAdmin() || isKMSAuditor();
  const navigate = useNavigate();
  const apiUrl = getBaseUrl() + "apidocs/swagger.html";
  const backboneUrl = getBaseUrl() + "backbone-index.html";

  const serviceSelectThemes = (theme) => {
    return {
      ...theme,
      colors: {
        ...theme.colors,
        text: "#444444",
        primary25: "#0b7fad;",
        primary: "#0b7fad;"
      }
    };
  };

  const serviceSelectCustomStyle = {
    option: (provided, state) => ({
      ...provided,
      color: state.isFocused ? "white" : "black"
    })
  };

  const getCurrentServiceDefs = (value) => {
    let filterServiceDef = [];
    value.filter((servicedefData) => {
      return Object.keys(groupBy(servicesData, "type"))?.map((servicedef) => {
        if (servicedefData.value == servicedef) {
          return filterServiceDef.push(servicedefData);
        }
      });
    });
    if (filterServiceDef?.length == 0 && value?.length > 0) {
      handleServiceDefChange(filterServiceDef);
    } else {
      return filterServiceDef;
    }
  };

  const serviceDefOptions = () => {
    let filterServiceDef = [];
    allServicesDefData?.filter((servicedefData) => {
      return Object.keys(groupBy(servicesData, "type"))?.map((servicedef) => {
        if (servicedefData.name == servicedef) {
          return filterServiceDef.push(servicedefData);
        }
      });
    });
    return sortBy(
      map(filterServiceDef, function (serviceDef) {
        return {
          value: serviceDef.name,
          label: upperCase(serviceDef.name)
        };
      }),
      "value"
    );
  };

  const handleServiceDefChange = (value) => {
    if (value.length !== 0) {
      let selectedServiceDefs = [];
      let selectedService = [];
      let filterSelectedService = [];

      value.map((serviceDef) => {
        allServicesDefData?.filter((servicedefs) => {
          if (servicedefs.name === serviceDef.value) {
            selectedServiceDefs.push(servicedefs);
          }
        });
      });

      value.map((serviceDef) => {
        allServicesData.filter((services) => {
          if (services.type === serviceDef.value) {
            selectedService.push(services);
          }
        });
      });

      if (isKMSRole) {
        filterSelectedService = filter(
          selectedService,
          (service) => service.type == "kms"
        );
      } else {
        filterSelectedService = filter(
          selectedService,
          (service) => service.type !== "tag" && service.type !== "kms"
        );
      }

      sideBarDispatch({
        type: "SERVICEDEF_DATA",
        allserviceDefData: filter(
          allServicesDefData,
          (serviceDef) => serviceDef.name !== "tag"
        ),
        serviceDefData: sortBy(
          filter(
            selectedServiceDefs,
            (serviceDef) => serviceDef.name !== "tag"
          ),
          "id"
        ),
        tagServiceDefData: tagServicesDefData
      });
    }

    if (value.length == 0) {
      let filterSelectedService = [];
      if (isKMSRole) {
        filterSelectedService = filter(
          allServicesData,
          (service) => service.type == "kms"
        );
      } else {
        filterSelectedService = filter(
          allServicesData,
          (service) => service.type !== "tag" && service.type !== "kms"
        );
      }
      sideBarDispatch({
        type: "SERVICEDEF_DATA",
        allserviceDefData: allServicesDefData,
        serviceDefData: sortBy(
          filter(allServicesDefData, (serviceDef) => serviceDef.name !== "tag"),
          "id"
        ),
        tagServiceDefData: tagServicesDefData
      });
      sideBarDispatch({
        type: "SERVICES_DATA",
        allServiceData: allServicesData,
        serviceData: filterSelectedService,
        tagServiceData: tagServicesData
      });
    }
    dispatch({
      type: "SELECTED_SERVCIEDEF_DATA",
      selectedServiceDef: value
    });
  };

  const checkKnoxSSO = async (e) => {
    e.preventDefault();
    const userProfile = getUserProfile();
    let checkKnoxSSOresp = {};
    try {
      checkKnoxSSOresp = await fetchApi({
        url: "plugins/checksso",
        type: "GET",
        headers: {
          "cache-control": "no-cache"
        }
      });
      if (
        checkKnoxSSOresp?.data?.toString() == "true" &&
        userProfile?.configProperties?.inactivityTimeout > 0
      ) {
        window.location.replace("index.html?action=timeout");
      } else {
        handleLogout(checkKnoxSSOresp?.data);
      }
    } catch (error) {
      if (checkKnoxSSOresp?.status == "419") {
        setUserProfile(null);
        window.location.replace("login.jsp");
      }
      console.error(`Error occurred while logout! ${error}`);
    }
  };

  const handleLogout = async (checkKnoxSSOVal) => {
    try {
      await fetchApi({
        url: "logout",
        baseURL: "",
        headers: {
          "cache-control": "no-cache"
        }
      });
      if (checkKnoxSSOVal !== undefined || checkKnoxSSOVal !== null) {
        if (checkKnoxSSOVal?.toString() == "false") {
          window.location.replace("locallogin");
          window.localStorage.clear();
          setUserProfile(null);
        } else {
          navigate("/knoxSSOWarning");
        }
      } else {
        window.location.replace("login.jsp");
      }
    } catch (error) {
      toast.error(`Error occurred while logout! ${error}`);
    }
  };

  const closeCollapse = () => {
    props.closeCollapse();
  };

  return (
    <React.Fragment>
      <div
        className={`drawer ${isDrawerOpen ? "drawer-open" : "drawer-close"}`}
        id="drawer-content"
      >
        <div className="nav-drawer">
          <div
            id="resourcesCollapse"
            className={
              activeMenu !== null && activeMenu === "resourcesCollapse"
                ? "show-menu"
                : "hide-menu"
            }
          >
            <div className="drawer-menu-title">
              <span>RESOURCE POLICIES</span>
              <span className="drawer-menu-close">
                <img
                  src={closeIcon}
                  onClick={() => {
                    props.closeCollapse();
                  }}
                />
              </span>
            </div>
            <div
              title={`${isEmpty(servicesData) ? "Create Service first" : ""}`}
              className="position-relative"
            >
              <Select
                isMulti
                isClearable={true}
                placeholder="Select Service Types"
                menuPlacement="auto"
                className={`select-nav-drawer custom-nav-search  ${
                  loader ? "not-allowed" : ""
                }`}
                styles={serviceSelectCustomStyle}
                theme={serviceSelectThemes}
                isDisabled={loader || isEmpty(servicesData) ? true : false}
                isLoading={loader ? true : false}
                value={!loader ? getCurrentServiceDefs(selectedServiceDef) : ""}
                onChange={(e) => handleServiceDefChange(e)}
                options={serviceDefOptions()}
                components={{
                  DropdownIndicator: () => null,
                  IndicatorSeparator: () => null
                }}
              />
              {selectedServiceDef?.length > 0 && !loader && (
                <button
                  className={`custom-clear-btn ${loader ? "not-allowed" : ""}`}
                  disabled={loader ? true : false}
                  onClick={() => {
                    handleServiceDefChange([]);
                  }}
                >
                  X
                </button>
              )}
            </div>
            <ResourceTagContent
              serviceDefsData={sortBy(
                servicesDefData?.filter(Boolean)?.filter((serviceDef) => {
                  return serviceDef.name !== "tag";
                }),
                "name"
              )}
              servicesData={sortBy(
                servicesData?.filter(Boolean)?.filter((serviceDef) => {
                  return serviceDef.name !== "tag";
                }),
                "name"
              )}
              closeCollapse={closeCollapse}
              loader={loader}
            />
          </div>

          <div
            id="tagCollapse"
            className={
              activeMenu !== null && activeMenu === "tagCollapse"
                ? "show-menu"
                : "hide-menu"
            }
          >
            <div className="drawer-menu-title">
              <span>TAG POLICIES</span>
              <span className="drawer-menu-close">
                <img
                  src={closeIcon}
                  onClick={() => {
                    props.closeCollapse();
                  }}
                />
              </span>
            </div>
            <ResourceTagContent
              serviceDefsData={tagServicesDefData
                ?.filter(Boolean)
                .filter((serviceDef) => {
                  return serviceDef.name == "tag";
                })}
              servicesData={sortBy(
                tagServicesData.filter(Boolean)?.filter((serviceDef) => {
                  return serviceDef.type === "tag";
                }),
                "name"
              )}
              closeCollapse={closeCollapse}
              loader={loader}
            />
          </div>

          <div
            id="auditCollapse"
            className={
              activeMenu !== null && activeMenu === "auditCollapse"
                ? "show-menu"
                : "hide-menu"
            }
          >
            <div className="drawer-menu-title">
              <span>AUDITS</span>
              <span className="drawer-menu-close">
                <img
                  src={closeIcon}
                  onClick={() => {
                    props.closeCollapse();
                  }}
                />
              </span>
            </div>
            <ul className="list-group list-group-flush">
              {hasAccessToTab("Audit") && (
                <React.Fragment>
                  <li className="list-group-item">
                    <NavLink
                      to="/reports/audit/bigData"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      Access
                    </NavLink>
                  </li>
                  <li className="list-group-item">
                    <NavLink
                      to="/reports/audit/admin"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      Admin
                    </NavLink>
                  </li>
                  <li className="list-group-item">
                    <NavLink
                      to="/reports/audit/loginSession"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      Login Sessions
                    </NavLink>
                  </li>
                  <li className="list-group-item">
                    <NavLink
                      to="/reports/audit/agent"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      Plugins
                    </NavLink>
                  </li>
                  <li className="list-group-item">
                    <NavLink
                      to="/reports/audit/pluginStatus"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      Plugin Status
                    </NavLink>
                  </li>
                  <li className="list-group-item">
                    <NavLink
                      to="/reports/audit/userSync"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      User Sync
                    </NavLink>
                  </li>
                </React.Fragment>
              )}
            </ul>
          </div>

          <div
            id="settingsCollapse"
            className={
              activeMenu !== null && activeMenu === "settingsCollapse"
                ? "show-menu"
                : "hide-menu"
            }
          >
            <div className="drawer-menu-title">
              <span>SETTINGS</span>
              <span className="drawer-menu-close">
                <img
                  src={closeIcon}
                  onClick={() => {
                    props.closeCollapse();
                  }}
                />
              </span>
            </div>
            <ul className="list-group list-group-flush">
              {hasAccessToTab("Users/Groups") && (
                <React.Fragment>
                  <li className="list-group-item">
                    <NavLink
                      to="/users/usertab"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      Users
                    </NavLink>
                  </li>

                  <li className="list-group-item">
                    <NavLink
                      to="/users/grouptab"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      Groups
                    </NavLink>
                  </li>

                  <li className="list-group-item">
                    <NavLink
                      to="/users/roletab"
                      onClick={() => {
                        props.closeCollapse();
                      }}
                      className="list-group-item"
                    >
                      Roles
                    </NavLink>
                  </li>
                </React.Fragment>
              )}
              {(isAuditor() || isSystemAdmin()) && (
                <li className="list-group-item">
                  <NavLink
                    to="/permissions/models"
                    onClick={() => {
                      props.closeCollapse();
                    }}
                    className="list-group-item"
                  >
                    Permissions
                  </NavLink>
                </li>
              )}
            </ul>
          </div>
        </div>
      </div>
      <div
        className={`account-drawer ${
          accountDrawer ? "account-drawer-open" : "account-drawer-close"
        }`}
        id="account-drawer-content"
      >
        <div className="account-nav-drawer">
          <div
            id="accountCollapse"
            className={
              activeMenu !== null && activeMenu === "accountCollapse"
                ? "show-account-menu"
                : "hide-account-menu"
            }
          >
            <div className="drawer-menu-title">
              <span>USER PROFILE</span>
              <span className="drawer-menu-close">
                <img
                  src={closeIcon}
                  onClick={() => {
                    props.closeCollapse();
                  }}
                />
              </span>
            </div>
            <ul className="list-group list-group-flush">
              <li className="list-group-item">
                <NavLink
                  to="/userprofile"
                  onClick={() => {
                    props.closeCollapse();
                  }}
                  className="list-group-item"
                >
                  Profile
                </NavLink>
              </li>
              <li className="list-group-item">
                <a
                  href={backboneUrl}
                  onClick={() => {
                    props.closeCollapse();
                    localStorage.clear();
                  }}
                  className="text-decoration-none"
                >
                  Backbone Classic UI
                </a>
              </li>
              <li className="list-group-item">
                <a
                  href={apiUrl}
                  target="_blank"
                  onClick={() => {
                    props.closeCollapse();
                  }}
                  className="text-decoration-none"
                >
                  API Documentation
                </a>
              </li>
              <li className="list-group-item">
                <NavLink
                  onClick={checkKnoxSSO}
                  to="#"
                  className="text-decoration-none"
                >
                  Log Out
                </NavLink>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </React.Fragment>
  );
};
