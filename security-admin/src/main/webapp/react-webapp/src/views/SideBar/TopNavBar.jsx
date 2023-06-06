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
import { Link, useNavigate } from "react-router-dom";
import { Button, Modal } from "react-bootstrap";
import { isEmpty, upperCase } from "lodash";
import Select from "react-select";
import ServiceViewDetails from "../ServiceManager/ServiceViewDetails";
import { fetchApi } from "Utils/fetchAPI";
import moment from "moment-timezone";
import { toast } from "react-toastify";
import {
  serverError,
  isKeyAdmin,
  isKMSAuditor,
  isUser,
  isSystemAdmin
} from "../../utils/XAUtils";

function reducer(state, action) {
  switch (action.type) {
    case "SHOW_VIEW_MODAL":
      return {
        ...state,
        showView: action.showView
      };
    case "SHOW_DELETE_MODAL":
      return {
        ...state,
        showDelete: action.showDelete
      };
    default:
      throw new Error();
  }
}

export const TopNavBar = (props) => {
  const isKMSRole = isKeyAdmin() || isKMSAuditor();
  const isUserRole = isUser();
  const isAdminRole = isSystemAdmin() || isKeyAdmin();
  const navigate = useNavigate();
  const [policyState, dispatch] = useReducer(reducer, {
    showView: null,
    showDelete: false
  });
  const { showView, showDelete } = policyState;

  let {
    serviceDefData,
    serviceData,
    handleServiceChange,
    getServices,
    allServicesData,
    policyLoader,
    currentServiceZone,
    handleZoneChange,
    getZones,
    allZonesData
    //zoneServicesData
  } = props;

  //let localStorageZoneDetails = localStorage.getItem("zoneDetails");

  const serviceSelectCustomStyles = {
    option: (provided, state) => ({
      ...provided,
      color: state.isSelected ? "white" : "black"
    }),
    control: (provided) => ({
      ...provided,
      maxHeight: "32px",
      minHeight: "32px"
    }),
    indicatorsContainer: (provided) => ({
      ...provided,
      maxHeight: "30px"
    }),
    dropdownIndicator: (provided) => ({
      ...provided,
      padding: "5px"
    }),
    clearIndicator: (provided) => ({
      ...provided,
      padding: "5px"
    }),
    container: (styles) => ({ ...styles, width: "240px" })
  };

  const serviceSelectTheme = (theme) => {
    return {
      ...theme,
      colors: {
        ...theme.colors,
        primary: "#0081ab"
      }
    };
  };

  const showViewModal = (id) => {
    dispatch({
      type: "SHOW_VIEW_MODAL",
      showView: id
    });
  };

  const hideViewModal = () => {
    dispatch({
      type: "SHOW_VIEW_MODAL",
      showView: null
    });
  };

  const showDeleteModal = () => {
    dispatch({
      type: "SHOW_DELETE_MODAL",
      showDelete: true
    });
  };

  const hideDeleteModal = () => {
    dispatch({
      type: "SHOW_DELETE_MODAL",
      showDelete: false
    });
  };

  const getCurrentZone = (zone) => {
    if (zone !== null) {
      return {
        label: `Security Zone : ${zone?.label}`,
        value: zone.value
      };
    } else {
      return "";
    }
  };

  const getCurrentService = (service) => {
    if (!isEmpty(service)) {
      return {
        label: `Service : ${service?.displayName}`,
        value: service.displayName
      };
    } else {
      return "";
    }
  };

  const deleteService = async (serviceId) => {
    let localStorageZoneDetails = localStorage.getItem("zoneDetails");
    let zonesResp = [];
    hideDeleteModal();
    try {
      await fetchApi({
        url: `plugins/services/${serviceId}`,
        method: "delete"
      });
      if (
        localStorageZoneDetails !== undefined &&
        localStorageZoneDetails !== null
      ) {
        zonesResp = await fetchApi({
          url: `public/v2/api/zones/${
            JSON.parse(localStorageZoneDetails)?.value
          }/service-headers`
        });

        if (isEmpty(zonesResp?.data)) {
          localStorage.removeItem("zoneDetails");
        }
      }
      toast.success("Successfully deleted the service");
      navigate(
        serviceDefData?.name === "tag"
          ? "/policymanager/tag"
          : "/policymanager/resource"
      );
    } catch (error) {
      serverError(error);
      console.error(
        `Error occurred while deleting Service id - ${serviceData?.id}!  ${error}`
      );
    }
  };

  return (
    <nav className="navbar navbar-expand-lg navbar-light bg-light content-top-nav">
      <div className="top-nav-title-wrapper">
        <span className="top-nav-title">
          {`${upperCase(serviceDefData?.name)} Policies`}
        </span>
        <span className="pipe show-on-mobile"></span>
        <Select
          theme={serviceSelectTheme}
          styles={serviceSelectCustomStyles}
          className={`${policyLoader ? "not-allowed" : ""}`}
          isDisabled={policyLoader ? true : false}
          /* options={getServices(
            localStorageZoneDetails != null ? zoneServicesData : allServicesData
          )} */
          options={getServices(allServicesData)}
          onChange={(e) => handleServiceChange(e)}
          value={!policyLoader ? getCurrentService(serviceData) : ""}
          menuPlacement="auto"
          placeholder="Select Service Name"
        />
        {!isKMSRole && (
          <React.Fragment>
            <span className="pipe show-on-mobile"></span>
            <Select
              theme={serviceSelectTheme}
              styles={serviceSelectCustomStyles}
              className={`${policyLoader ? "not-allowed" : ""}`}
              isDisabled={policyLoader ? true : false}
              options={getZones(allZonesData)}
              onChange={(e) => handleZoneChange(e)}
              value={!policyLoader ? getCurrentZone(currentServiceZone) : ""}
              menuPlacement="auto"
              placeholder="Select Zone Name"
              isClearable
            />
          </React.Fragment>
        )}
      </div>
      <div className="collapse navbar-collapse" id="navbarText">
        <ul className="navbar-nav ml-auto">
          {!isUserRole && (
            <li className="nav-item" title="Service View">
              <Button
                variant="outline-dark"
                className={`${
                  policyLoader ? "not-allowed" : ""
                } btn btn-sm m-r-5`}
                onClick={() => {
                  showViewModal(serviceData?.id);
                }}
                disabled={policyLoader ? true : false}
                data-name="viewService"
                data-id={serviceData?.id}
                data-cy={serviceData?.id}
              >
                <i className="fa-fw fa fa-eye fa-fw fa fa-large"></i>
              </Button>
            </li>
          )}
          {isAdminRole && (
            <li className="nav-item" title="Service Edit">
              <Link
                to={`/service/${serviceDefData.id}/edit/${serviceData?.id}`}
                onClick={(e) => policyLoader && e.preventDefault()}
                state={allServicesData[0]?.id}
                disabled={policyLoader ? true : false}
                className={`${
                  policyLoader ? "not-allowed" : ""
                } btn btn-sm m-r-5`}
                data-name="editService"
                data-id={serviceData?.id}
                data-cy={serviceData?.id}
              >
                <i className="fa-fw fa fa-edit fa-fw fa fa-large"></i>
              </Link>
            </li>
          )}
          {isAdminRole && (
            <li className="nav-item" title="Service Delete">
              <Button
                variant="danger"
                disabled={policyLoader ? true : false}
                className={`${policyLoader ? "not-allowed" : ""} btn-sm`}
                onClick={() => {
                  showDeleteModal();
                }}
                data-name="deleteService"
                data-id={serviceData?.id}
                data-cy={serviceData?.id}
              >
                <i className="fa-fw fa fa-trash fa-fw fa fa-large"></i>
              </Button>
            </li>
          )}
        </ul>
        {(!isUserRole || isAdminRole) && <span className="pipe"></span>}
        <span className="navbar-text last-response-time">
          <strong>Last Response Time</strong>
          <br />
          {moment(moment()).format("MM/DD/YYYY hh:mm:ss A")}
        </span>
      </div>
      <Modal
        show={showView === serviceData?.id}
        onHide={hideViewModal}
        size="xl"
      >
        <Modal.Header closeButton>
          <Modal.Title>Service Details</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <ServiceViewDetails
            serviceDefData={serviceDefData}
            serviceData={serviceData}
          />
        </Modal.Body>
        <Modal.Footer>
          <Button variant="primary" size="sm" onClick={hideViewModal}>
            OK
          </Button>
        </Modal.Footer>
      </Modal>
      <Modal show={showDelete} onHide={hideDeleteModal}>
        <Modal.Header closeButton>
          {`Are you sure want to delete ?`}
        </Modal.Header>
        <Modal.Footer>
          <Button
            variant="secondary"
            size="sm"
            title="Cancel"
            onClick={hideDeleteModal}
          >
            Cancel
          </Button>
          <Button
            variant="primary"
            size="sm"
            title="Yes"
            onClick={() => deleteService(serviceData?.id)}
          >
            Yes
          </Button>
        </Modal.Footer>
      </Modal>
    </nav>
  );
};

export default TopNavBar;
