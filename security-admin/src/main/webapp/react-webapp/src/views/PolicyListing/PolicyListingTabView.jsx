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
import { compact, has, map, sortBy, includes, filter } from "lodash";
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
  const serviceDefs = getServiceDef();
  const navigate = useNavigate();
  const params = useParams();
  const [policyState, dispatch] = useReducer(reducer, {
    loader: true,
    serviceData: {},
    serviceDefData: {},
    allServicesData: []
  });
  const { loader, serviceDefData, serviceData, allServicesData } = policyState;
  //const [zoneServicesData, setZoneServicesData] = useState([]);
  const [zones, setZones] = useState([]);

  let localStorageZoneDetails = localStorage.getItem("zoneDetails");

  useEffect(() => {
    fetchServiceDetails();
  }, [params?.serviceId, JSON.parse(localStorageZoneDetails)?.value]);

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
        url: `plugins/services?serviceType=${getServiceData.data.type}`
      });
      getServiceDefData = serviceDefs?.allServiceDefs?.find((serviceDef) => {
        return serviceDef.name == getServiceData.data.type;
      });

      isTagView =
        getServiceDefData?.name !== undefined &&
        getServiceDefData?.name === "tag"
          ? true
          : false;

      updateTagActive(isTagView);

      if (!isKMSRole) {
        getZones = await fetchApi({
          url: `zones/zones`
        });
      }

      setZones(getZones?.data?.securityZones || []);

      dispatch({
        type: "SERVICES_DATA",
        allServicesData: getAllServicesData.data.services,
        serviceData: getServiceData.data,
        serviceDefData: getServiceDefData
      });

      /* zoneServices(
        getServiceData.data,
        getAllServicesData.data.services,
        getZones?.data?.securityZones || []
      ); */

      dispatch({
        type: "SET_LOADER",
        loader: false
      });

      document
        .getElementById("resourceSelectedZone")
        ?.classList?.remove("disabledEvents");
      document
        .getElementById("tagSelectedZone")
        ?.classList?.remove("disabledEvents");
    } catch (error) {
      console.error(`Error occurred while fetching service details ! ${error}`);
    }
  };

  /* const zoneServices = (serviceData, servicesData, zonesData) => {
    let filterZoneService = [];
    if (
      localStorageZoneDetails !== undefined &&
      localStorageZoneDetails !== null
    ) {
      let filterZones = find(zonesData, {
        name: JSON.parse(localStorageZoneDetails)?.label
      });

      if (serviceData.type !== "tag") {
        filterZoneService =
          filterZones !== undefined ? Object.keys(filterZones.services) : [];
      } else {
        filterZoneService =
          filterZones !== undefined ? filterZones?.tagServices : [];
      }

      let zoneServices = filterZoneService?.map((zoneService) => {
        return servicesData?.filter((service) => {
          return service.name === zoneService;
        });
      });

      setZoneServicesData(zoneServices?.flat());
    }
  }; */

  const getServices = (services) => {
    return sortBy(
      services?.map(({ displayName }) => ({
        label: displayName,
        value: displayName
      })),
      "label"
    );
  };

  const handleServiceChange = async (e) => {
    if (e !== "") {
      let selectedServiceData = allServicesData?.find((service) => {
        if (service.displayName == e?.label) {
          return service;
        }
      });
      navigate(
        `/service/${selectedServiceData?.id}/policies/${RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value}`,
        {
          replace: true
        }
      );
      localStorage.removeItem("zoneDetails");
    }
  };

  const getZones = (zones) => {
    let filterZones = [];

    if (serviceData.type !== "tag") {
      filterZones = compact(
        zones?.map((zone) => {
          if (has(zone?.services, serviceData?.name)) {
            return { label: zone.name, value: zone.id };
          }
        })
      );
    } else {
      filterZones = zones
        ?.map((zone) => {
          return compact(
            zone?.tagServices?.map((tagService) => {
              if (tagService === serviceData?.name) {
                return { label: zone.name, value: zone.id };
              }
            })
          );
        })
        ?.flat();
    }

    return sortBy(
      filterZones?.map((zone) => ({
        label: zone.label,
        value: zone.value
      })),
      "label"
    );
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
    navigate(
      `/service/${params.serviceId}/policies/${RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value}`,
      {
        replace: true
      }
    );
  };

  const getServiceZone = () => {
    let zoneName = null;

    if (
      localStorageZoneDetails !== undefined &&
      localStorageZoneDetails !== null
    ) {
      if (serviceData.type !== "tag") {
        zoneName = compact(
          map(zones, function (zone) {
            if (
              zone.name === JSON.parse(localStorageZoneDetails)?.label &&
              has(zone?.services, serviceData?.name)
            ) {
              return { label: zone.name, value: zone.id };
            }
          })
        );

        zoneName = zoneName.length == 1 ? zoneName[0] : null;
      } else {
        zoneName = filter(zones, {
          name: JSON.parse(localStorageZoneDetails)?.label
        });

        zoneName =
          zoneName.length == 1 &&
          includes(zoneName[0]?.tagServices, serviceData?.name)
            ? { label: zoneName[0].name, value: zoneName[0].id }
            : null;
      }
    }

    return zoneName;
  };

  const tabChange = (tabName) => {
    navigate(`/service/${params?.serviceId}/policies/${tabName}`, {
      replace: true
    });
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
        //zoneServicesData={zoneServicesData}
      />
      {loader ? (
        <Loader />
      ) : isRenderMasking(serviceDefData.dataMaskDef) ||
        isRenderRowFilter(serviceDefData.rowFilterDef) ? (
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
