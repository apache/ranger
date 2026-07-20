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

import React, { useEffect, useState } from "react";
import { Outlet, useParams, useNavigate, useLocation } from "react-router-dom";
import { cloneDeep, pick } from "lodash";
import { Loader } from "Components/CommonComponents";
import { fetchApi } from "Utils/fetchAPI";
import { getServiceDef } from "Utils/appState";
import { commonBreadcrumb } from "Utils/XAUtils";

const PolicyFormContext = () => {
  const { serviceId, policyType, policyId } = useParams();
  const navigate = useNavigate();
  const location = useLocation();
  const serviceDefs = cloneDeep(getServiceDef());

  const [loading, setLoading] = useState(true);
  const [serviceDetails, setServiceDetails] = useState(null);
  const [serviceCompDetails, setServiceCompDetails] = useState(null);
  const [policyData, setPolicyData] = useState(null);
  const isNewForm = location.pathname.endsWith("new-policy-form-edit");

  useEffect(() => {
    fetchInitialData();
  }, [serviceId, policyType, policyId]);

  const fetchServiceDetails = async () => {
    let data = null;
    try {
      const resp = await fetchApi({
        url: `plugins/services/${serviceId}`
      });
      data = resp.data || null;
    } catch (error) {
      console.error(`Error occurred while fetching policy details ! ${error}`);
    }
    return data;
  };

  const fetchPolicyData = async () => {
    let data = null;
    try {
      const resp = await fetchApi({
        url: `plugins/policies/${policyId}`
      });
      data = resp.data || null;
    } catch (error) {
      console.error(`Error occurred while fetching service details ! ${error}`);
    }
    return data;
  };

  const fetchInitialData = async () => {
    setLoading(true);
    let serviceData = await fetchServiceDetails();

    let serviceCompData = serviceDefs?.allServiceDefs?.find((serviceDef) => {
      return serviceDef.name == serviceData.type;
    });
    if (serviceCompData) {
      let serviceDefPolicyType = 0;
      if (
        serviceCompData?.dataMaskDef &&
        Object.keys(serviceCompData.dataMaskDef).length != 0
      )
        serviceDefPolicyType++;
      if (
        serviceCompData?.rowFilterDef &&
        Object.keys(serviceCompData.rowFilterDef).length != 0
      )
        serviceDefPolicyType++;
      if (+policyType > serviceDefPolicyType) navigate("/pageNotFound");
    }
    let fetchedPolicyData = null;
    if (policyId) {
      fetchedPolicyData = await fetchPolicyData();
    }

    setServiceDetails(serviceData);
    setServiceCompDetails(serviceCompData);
    setPolicyData(fetchedPolicyData);
    setLoading(false);
  };

  const policyBreadcrumb = () => {
    if (!serviceDetails || !serviceCompDetails) return null;

    let policyDetails = {};
    policyDetails["serviceId"] = serviceId;
    policyDetails["policyType"] = policyId
      ? policyData?.policyType
      : policyType;
    policyDetails["serviceName"] = serviceDetails?.displayName;
    policyDetails["selectedZone"] = JSON.parse(
      localStorage.getItem("zoneDetails")
    );

    if (serviceCompDetails?.name === "tag") {
      if (policyDetails?.selectedZone) {
        return commonBreadcrumb(
          [
            "TagBasedServiceManager",
            "ManagePolicies",
            policyId ? "PolicyEdit" : "PolicyCreate"
          ],
          policyDetails
        );
      } else {
        return commonBreadcrumb(
          [
            "TagBasedServiceManager",
            "ManagePolicies",
            policyId ? "PolicyEdit" : "PolicyCreate"
          ],
          pick(policyDetails, ["serviceId", "policyType", "serviceName"])
        );
      }
    } else {
      if (policyDetails?.selectedZone) {
        return commonBreadcrumb(
          [
            "ServiceManager",
            "ManagePolicies",
            policyId ? "PolicyEdit" : "PolicyCreate"
          ],
          policyDetails
        );
      } else {
        return commonBreadcrumb(
          [
            "ServiceManager",
            "ManagePolicies",
            policyId ? "PolicyEdit" : "PolicyCreate"
          ],
          pick(policyDetails, ["serviceId", "policyType", "serviceName"])
        );
      }
    }
  };

  const handleToggleForm = () => {
    navigate(
      isNewForm
        ? `/service/${serviceId}/policies/${policyId}/edit`
        : `/service/${serviceId}/policies/${policyId}/new-policy-form-edit`
    );
  };

  if (loading) {
    return <Loader />;
  }

  return (
    <div>
      {/* Shared Header */}
      <div className="header-wraper">
        <h3 className="wrap-header bold">{`${
          policyId ? "Edit" : "Create"
        } Policy`}</h3>

        {/* Switch toggle (only show during edit) */}
        {policyId && (
          <div className="ms-2 me-2 d-inline-flex align-items-center policy-form-toggle-switch">
            <div
              className="custom-rectangle-switch"
              onClick={handleToggleForm}
              style={{
                width: "216px",
                height: "31px",
                borderRadius: "4px",
                border: `1px solid ${isNewForm ? "#0d6efd" : "#dee2e6"}`,
                backgroundColor: "#fff",
                position: "relative",
                cursor: "pointer",
                transition: "all 0.2s ease",
                display: "flex",
                alignItems: "center",
                overflow: "hidden",
                userSelect: "none"
              }}
            >
              {/* Active side background */}
              <div
                style={{
                  position: "absolute",
                  top: "0",
                  left: isNewForm ? "50%" : "0",
                  width: "50%",
                  height: "100%",
                  backgroundColor: isNewForm ? "#0d6efd" : "#0d6efd",
                  transition: "left 0.3s ease",
                  zIndex: 1,
                  padding: "4px 10px"
                }}
              />

              {/* Left side - Old Form */}
              <div
                style={{
                  width: "50%",
                  height: "100%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  position: "relative",
                  zIndex: 2
                }}
              >
                <span
                  style={{
                    fontSize: "14px",
                    color: !isNewForm ? "#fff" : "#6c757d",
                    transition: "color 0.3s ease"
                  }}
                >
                  Old Form
                </span>
              </div>

              {/* Right side - New Form */}
              <div
                style={{
                  width: "50%",
                  height: "100%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  position: "relative",
                  zIndex: 2
                }}
              >
                <span
                  style={{
                    fontSize: "14px",
                    fontWeight: "500",
                    color: isNewForm ? "#fff" : "#6c757d",
                    transition: "color 0.3s ease"
                  }}
                >
                  New Form
                </span>
              </div>
            </div>
          </div>
        )}

        {policyBreadcrumb()}
      </div>

      {/* Child form content */}
      <Outlet
        context={{
          serviceDetails,
          serviceCompDetails,
          policyData: policyData ? cloneDeep(policyData) : null,
          serviceDefs
        }}
      />
    </div>
  );
};

export default PolicyFormContext;
