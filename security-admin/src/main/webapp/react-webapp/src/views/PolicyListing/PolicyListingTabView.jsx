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

import React, { useState, useReducer, useEffect } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Tab, Tabs } from "react-bootstrap";
import PolicyListing from "./PolicyListing";
import { fetchApi } from "Utils/fetchAPI";
import {
  isRenderMasking,
  isRenderRowFilter,
  isKeyAdmin,
  isKMSAuditor,
  updateTagActive
} from "Utils/XAUtils";
import { Loader } from "Components/CommonComponents";
import TopNavBar from "../SideBar/TopNavBar";
import { cloneDeep, isEmpty, map, sortBy } from "lodash";
import { RangerPolicyType } from "../../utils/XAEnums";
import { getServiceDef } from "../../utils/appState";

function reducer(state, action) {
  switch (action.type) {
    case "SET_LOADER":
      return {
        ...state,
        loader: action.loader
      };
    case "SERVICES_DATA":
      return {
        ...state,
        serviceDefData: action.serviceDefData,
        serviceData: action.serviceData,
        allServicesData: action.allServicesData
      };
    default:
      throw new Error();
  }
}

export const PolicyListingTabView = () => {
  const isKMSRole = isKeyAdmin() || isKMSAuditor();
  const serviceDefs = cloneDeep(getServiceDef());
  const navigate = useNavigate();
  const params = useParams();
  const [policyState, dispatch] = useReducer(reducer, {
    loader: true,
    serviceData: {},
    serviceDefData: {},
    allServicesData: []
  });
  const { loader, serviceDefData, serviceData, allServicesData } = policyState;
  const [zones, setZones] = useState([]);

  let localStorageZoneDetails = localStorage.getItem("zoneDetails");

  useEffect(() => {
    fetchServiceDetails();
  }, [
    params?.serviceId,
    params?.policyType,
    JSON.parse(localStorageZoneDetails)?.value
  ]);

  const fetchServiceDetails = async () => {
    document
      .getElementById("resourceSelectedZone")
      ?.classList?.add("disabledEvents");
    document
      .getElementById("tagSelectedZone")
      ?.classList?.add("disabledEvents");

    dispatch({
      type: "SET_LOADER",
      loader: true
    });

    let getServiceData = {};
    let getAllServicesData = [];
    let getServiceDefData = {};
    let getZones = [];
    let isTagView = false;

    try {
      getServiceData = await fetchApi({
        url: `plugins/services/${params.serviceId}`
      });
      getAllServicesData = await fetchApi({
        url: `plugins/services?serviceType=${getServiceData?.data?.type}`
      });
      getServiceDefData = serviceDefs?.allServiceDefs?.find((serviceDef) => {
        return serviceDef.name == getServiceData?.data?.type;
      });

      if (!isKMSRole) {
        getZones = await fetchApi({
          url: `public/v2/api/zones/zone-headers/for-service/${params.serviceId}`,
          params: {
            isTagService: getServiceData.data?.type == "tag" ? true : false
          }
        });
      }
    } catch (error) {
      console.error(`Error occurred while fetching service details ! ${error}`);
    }

    isTagView =
      getServiceDefData?.name !== undefined && getServiceDefData?.name === "tag"
        ? true
        : false;

    updateTagActive(isTagView);

    setZones(getZones?.data || []);

    dispatch({
      type: "SERVICES_DATA",
      allServicesData: getAllServicesData?.data?.services,
      serviceData: getServiceData?.data,
      serviceDefData: getServiceDefData
    });
    if (!isEmpty(getServiceData) && !isEmpty(getServiceDefData)) {
      dispatch({
        type: "SET_LOADER",
        loader: false
      });
    }
    document
      .getElementById("resourceSelectedZone")
      ?.classList?.remove("disabledEvents");
    document
      .getElementById("tagSelectedZone")
      ?.classList?.remove("disabledEvents");
  };

  const getServices = (services) => {
    let filterService = [];

    filterService = sortBy(
      map(services, function ({ displayName }) {
        return { label: displayName, value: displayName };
      }),
      "label"
    );

    return filterService;
  };

  const handleServiceChange = async (e) => {
    if (e !== "") {
      let selectedServiceData = allServicesData?.find((service) => {
        if (service.displayName == e?.label) {
          return service;
        }
      });
      navigate(
        `/service/${selectedServiceData?.id}/policies/${params.policyType}`
      );
      localStorage.removeItem("zoneDetails");
    }
  };

  const getZones = (zones) => {
    let filterZones = [];

    filterZones = sortBy(
      map(zones, function (zone) {
        return { label: zone.name, value: zone.id };
      }),
      "label"
    );

    return filterZones;
  };

  const handleZoneChange = async (e) => {
    if (e && e !== undefined) {
      let zoneDetails = {};
      zoneDetails["label"] = e.label;
      zoneDetails["value"] = e.value;
      localStorage.setItem("zoneDetails", JSON.stringify(zoneDetails));
    } else {
      localStorage.removeItem("zoneDetails");
    }
    navigate(`/service/${params.serviceId}/policies/${params.policyType}`);
  };

  const getServiceZone = () => {
    let zoneName = null;

    if (
      localStorageZoneDetails !== undefined &&
      localStorageZoneDetails !== null
    ) {
      for (const zone of zones) {
        if (zone.name === JSON.parse(localStorageZoneDetails)?.label) {
          zoneName = { label: zone.name, value: zone.id };
          break;
        }
      }
    }
    return zoneName;
  };

  const tabChange = (tabName) => {
    navigate(`/service/${params?.serviceId}/policies/${tabName}`);
  };

  return (
    <React.Fragment>
      <TopNavBar
        serviceDefData={serviceDefData}
        serviceData={serviceData}
        handleServiceChange={handleServiceChange}
        getServices={getServices}
        allServicesData={sortBy(allServicesData, "name")}
        policyLoader={loader}
        currentServiceZone={getServiceZone()}
        handleZoneChange={handleZoneChange}
        getZones={getZones}
        allZonesData={zones}
      />
      {loader ? (
        <Loader />
      ) : (isRenderMasking(serviceDefData.dataMaskDef) ||
          isRenderRowFilter(serviceDefData.rowFilterDef)) &&
        params.policyType < 3 ? (
        <Tabs
          id="PolicyListing"
          activeKey={params.policyType}
          onSelect={(k) => tabChange(k)}
        >
          <Tab eventKey="0" title="Access">
            {params.policyType == "0" && (
              <PolicyListing
                serviceDef={serviceDefData}
                serviceData={serviceData}
                serviceZone={getServiceZone()}
              />
            )}
          </Tab>
          {isRenderMasking(serviceDefData?.dataMaskDef) && (
            <Tab eventKey="1" title="Masking">
              {params.policyType == "1" && (
                <PolicyListing
                  serviceDef={serviceDefData}
                  serviceData={serviceData}
                  serviceZone={getServiceZone()}
                />
              )}
            </Tab>
          )}
          {isRenderRowFilter(serviceDefData.rowFilterDef) && (
            <Tab eventKey="2" title="Row Level Filter">
              {params.policyType == "2" && (
                <PolicyListing
                  serviceDef={serviceDefData}
                  serviceData={serviceData}
                  serviceZone={getServiceZone()}
                />
              )}
            </Tab>
          )}
        </Tabs>
      ) : (
        <PolicyListing
          serviceDef={serviceDefData}
          serviceData={serviceData}
          serviceZone={getServiceZone()}
        />
      )}
    </React.Fragment>
  );
};

export default PolicyListingTabView;
