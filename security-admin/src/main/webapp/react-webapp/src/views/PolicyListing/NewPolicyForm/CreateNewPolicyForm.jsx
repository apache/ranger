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

import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { toast } from "react-toastify";
import { getServiceDef } from "Utils/appState";
import { fetchApi } from "Utils/fetchAPI";
import {
  getResourcesDefVal,
  prunePolicyItemAccessesToAllowedTypes,
  pruneConstraintPolicyItemsToMatchedResources,
  policyConditionUpdatedJSON,
  buildConstraintInfoForApi
} from "Utils/XAUtils";
import {
  buildActionReqsMapFromConditionDef,
  pruneStaleActionMatcherInPolicyItemCondition
} from "Utils/policyConditionUtils";
import { isEmpty, isArray, map, reject, groupBy } from "lodash";
import moment from "moment";
import StepperForm from "../../../components/StepperFormComponents";
import CustomBreadcrumb from "../../CustomBreadcrumb";
import { BlockUi } from "Components/CommonComponents";
import { RangerPolicyType } from "Utils/XAEnums";
import {
  validatePolicyItems,
  validatePolicyDetails,
  hasMissingAccessPermissionOnPolicyItems,
  hasMissingConstraintFieldsOnPolicyItems
} from "./policyFormValidation";
import ServiceStep from "./PolicyCreateStepperStepComp/ServiceStep";
import ResourcesStep from "./PolicyCreateStepperStepComp/ResourcesStep";
import AccessRulesStep from "./PolicyCreateStepperStepComp/AccessRulesStep";
import PolicyDetailsStep from "./PolicyCreateStepperStepComp/PolicyDetailsStep";
import ReviewConfirmStep from "./PolicyCreateStepperStepComp/ReviewConfirmStep";

const CreateNewPolicyForm = () => {
  const navigate = useNavigate();
  const [selectedServiceComponentDef, setSelectedServiceComponentDef] =
    useState({});
  const [blockUI, setBlockUI] = useState(false);
  const [preventUnBlock, setPreventUnBlock] = useState(false);

  // Function to get service def by service type
  const handleServiceTypeChange = (serviceType) => {
    if (serviceType) {
      const { allServiceDefs } = getServiceDef();
      const serviceDef = allServiceDefs.find(
        (def) => def.name === serviceType.value
      );
      setSelectedServiceComponentDef(serviceDef || {});
    } else {
      setSelectedServiceComponentDef({});
    }
  };

  const principalName = (p) => {
    if (typeof p !== "object") return p;
    if (p.name) return p.name;
    if (p.value) {
      const colonIdx = p.value.indexOf(":");
      return colonIdx !== -1 ? p.value.slice(colonIdx + 1) : p.value;
    }
    return p;
  };

  const appendAccessesPrincipalsAndConditions = (obj, key) => {
    if (key?.accesses) {
      if (key.accesses.tableList && isArray(key.accesses.tableList)) {
        obj.accesses = key.accesses.tableList.flatMap((tableItem) => {
          if (tableItem.permission && isArray(tableItem.permission)) {
            return tableItem.permission.map((perm) => ({
              type: perm,
              isAllowed: true
            }));
          }
          return [];
        });
      } else if (isArray(key.accesses) && key.accesses.length > 0) {
        obj.accesses = key.accesses.map((access) => {
          if (access.type && access.isAllowed !== undefined) {
            return {
              type: access.type,
              isAllowed: access.isAllowed
            };
          }
          return {
            type:
              typeof access === "object" && access.value
                ? access.value
                : access.type || access,
            isAllowed: true
          };
        });
      }
    }

    if (key.users) {
      if (isArray(key.users)) {
        obj.users = key.users.map(principalName);
      } else if (key.users.users && isArray(key.users.users)) {
        obj.users = key.users.users.map(principalName);
      }
    }

    if (
      key.users?.groups &&
      isArray(key.users.groups) &&
      key.users.groups.length > 0
    ) {
      obj.groups = key.users.groups.map(principalName);
    } else if (key.groups && isArray(key.groups) && key.groups.length > 0) {
      obj.groups = key.groups.map(principalName);
    }

    if (
      key.users?.roles &&
      isArray(key.users.roles) &&
      key.users.roles.length > 0
    ) {
      obj.roles = key.users.roles.map(principalName);
    } else if (key.roles && isArray(key.roles) && key.roles.length > 0) {
      obj.roles = key.roles.map(principalName);
    }

    if (key?.conditions && !isEmpty(key.conditions)) {
      obj.conditions = [];
      Object.entries(key.conditions).forEach(
        ([conditionKey, conditionValue]) => {
          if (conditionValue != "" && conditionValue != null) {
            obj.conditions.push({
              type: conditionKey,
              values: isArray(conditionValue)
                ? conditionValue.map((m) =>
                    typeof m === "object" && m.value ? m.value : m
                  )
                : [conditionValue]
            });
          }
        }
      );
    }
  };

  /** Constraint policy (type 4) — API shape for constraintPolicyItems */
  const getConstraintPolicyItemsVal = (formData) => {
    const items = formData?.constraintPolicyItems;
    if (!items || items.length === 0) {
      return [];
    }

    const policyResourceItem = [];
    for (const key of items) {
      if (isEmpty(key) || Object.entries(key).length === 0) {
        continue;
      }

      const obj = {};
      appendAccessesPrincipalsAndConditions(obj, key);

      const constraintInfo = buildConstraintInfoForApi(key.constraintInfo);
      if (constraintInfo) {
        obj.constraintInfo = constraintInfo;
      }

      if (!isEmpty(obj)) {
        policyResourceItem.push(obj);
      }
    }

    return policyResourceItem;
  };

  // Transform policy items to API format
  const getPolicyItemsVal = (formData, name) => {
    if (!formData[name] || formData[name].length === 0) {
      return [];
    }

    const policyResourceItem = [];
    for (let key of formData[name]) {
      if (!isEmpty(key) && Object.entries(key).length > 0) {
        let obj = {};

        // Delegate Admin
        if (key.delegateAdmin != "undefined" && key.delegateAdmin != null) {
          obj.delegateAdmin = key.delegateAdmin;
        } else {
          if (
            RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value ==
              formData?.policyType &&
            formData?.serviceType?.value != "tag"
          ) {
            obj.delegateAdmin = false;
          }
        }

        appendAccessesPrincipalsAndConditions(obj, key);

        // Row Filter Info (for row filter policies)
        if (
          key.rowFilterInfo != "undefined" &&
          key.rowFilterInfo != null &&
          key.rowFilterInfo !== ""
        ) {
          obj.rowFilterInfo = {};
          if (
            typeof key.rowFilterInfo === "object" &&
            key.rowFilterInfo.filterExpr
          ) {
            obj.rowFilterInfo.filterExpr = key.rowFilterInfo.filterExpr;
          } else {
            obj.rowFilterInfo.filterExpr = key.rowFilterInfo;
          }
        }

        // Data Mask Info (for masking policies)
        if (
          key.dataMaskInfo != "undefined" &&
          key.dataMaskInfo != null &&
          !isEmpty(key.dataMaskInfo)
        ) {
          obj.dataMaskInfo = {};
          if (typeof key.dataMaskInfo === "object") {
            obj.dataMaskInfo.dataMaskType =
              key.dataMaskInfo.value || key.dataMaskInfo.dataMaskType;
            if (key.dataMaskInfo.valueExpr) {
              obj.dataMaskInfo.valueExpr = key.dataMaskInfo.valueExpr;
            }
          } else {
            obj.dataMaskInfo.dataMaskType = key.dataMaskInfo;
          }
        }

        policyResourceItem.push(obj);
      }
    }

    return policyResourceItem;
  };

  const handleSubmit = async (values) => {
    if (hasMissingAccessPermissionOnPolicyItems(values)) {
      toast.error(
        "Please select a policy permission for each access rule in the Access Rule step. This is required to assign permissions to the specified resources."
      );
      return;
    }
    if (
      hasMissingConstraintFieldsOnPolicyItems(
        values,
        selectedServiceComponentDef
      )
    ) {
      toast.error(
        "Please complete each constraint rule in the Access Rules step: select a constraint name, choose permissions, and fill all required parameters."
      );
      return;
    }
    try {
      setBlockUI(true);
      let data = {};
      const isConstraintPolicy =
        values.policyType ===
        RangerPolicyType.RANGER_CONSTRAINT_POLICY_TYPE.value;

      if (isConstraintPolicy) {
        data.constraintPolicyItems = getConstraintPolicyItemsVal(values);
        data.policyItems = [];
        data.allowExceptions = [];
        data.denyPolicyItems = [];
        data.denyExceptions = [];
        data.dataMaskPolicyItems = [];
        data.rowFilterPolicyItems = [];
      } else {
        data.policyItems = getPolicyItemsVal(values, "policyItems");
        data.allowExceptions = getPolicyItemsVal(values, "allowExceptions");

        if (values?.isDenyAllElse) {
          data.denyPolicyItems = [];
          data.denyExceptions = [];
        } else {
          data.denyPolicyItems = getPolicyItemsVal(values, "denyPolicyItems");
          data.denyExceptions = getPolicyItemsVal(values, "denyExceptions");
        }

        data.dataMaskPolicyItems = getPolicyItemsVal(
          values,
          "dataMaskPolicyItems"
        );
        data.rowFilterPolicyItems = getPolicyItemsVal(
          values,
          "rowFilterPolicyItems"
        );
      }

      // Basic policy fields
      data.name = values.policyName;
      data.description = values.description || "";
      data.isAuditEnabled =
        values.isAuditEnabled !== false ? true : values.isAuditEnabled;
      data.isEnabled = values.isEnabled !== false ? true : values.isEnabled;
      data.isDenyAllElse = values.isDenyAllElse || false;
      data.policyPriority = values.policyPriority === 1 ? 1 : 0;
      data.policyType = values.policyType || 0;

      // Service name - extract from serviceName object
      data.service = values?.serviceName?.value;

      // Policy Labels
      if (values.policyLabels && isArray(values.policyLabels)) {
        data.policyLabels = values.policyLabels.map((label) =>
          typeof label === "object" ? label.value || label.label : label
        );
      }

      // Get resource definitions
      let serviceCompRes;
      if (values.policyType != null) {
        serviceCompRes = getResourcesDefVal(
          selectedServiceComponentDef,
          values.policyType
        );
      }

      const grpResources = groupBy(serviceCompRes || [], "level");
      let grpResourcesKeys = [];
      for (const resourceKey in grpResources) {
        grpResourcesKeys.push(+resourceKey);
      }
      grpResourcesKeys = grpResourcesKeys.sort();

      // Process resources (multi-resource support)
      data.resources = {};
      data.additionalResources = [];

      if (values.additionalResources && values.additionalResources.length > 0) {
        map(values.additionalResources, (resourceValue) => {
          let additionalResourcesObj = {};
          for (const level of grpResourcesKeys) {
            if (
              resourceValue[`resourceName-${level}`] &&
              resourceValue[`value-${level}`] &&
              !isEmpty(resourceValue[`value-${level}`])
            ) {
              let defObj = serviceCompRes.find(function (m) {
                return m.name == resourceValue[`resourceName-${level}`].name;
              });

              if (defObj) {
                additionalResourcesObj[
                  resourceValue[`resourceName-${level}`].name
                ] = {
                  values: isArray(resourceValue[`value-${level}`])
                    ? resourceValue[`value-${level}`].map((v) => v.value || v)
                    : [
                        resourceValue[`value-${level}`].value ||
                          resourceValue[`value-${level}`]
                      ]
                };

                if (defObj?.excludesSupported) {
                  additionalResourcesObj[
                    resourceValue[`resourceName-${level}`].name
                  ]["isExcludes"] =
                    defObj.excludesSupported &&
                    resourceValue[`isExcludesSupport-${level}`] == false;
                }
                if (defObj?.recursiveSupported) {
                  additionalResourcesObj[
                    resourceValue[`resourceName-${level}`].name
                  ]["isRecursive"] =
                    defObj.recursiveSupported &&
                    !(resourceValue[`isRecursiveSupport-${level}`] === false);
                }
              }
            }
          }
          if (!isEmpty(additionalResourcesObj)) {
            data.additionalResources.push(additionalResourcesObj);
          }
        });
      }

      // Move first resource to data.resources
      if (data?.additionalResources?.length > 0) {
        data.additionalResources = reject(data.additionalResources, isEmpty);
        data.resources = data.additionalResources[0];
        data.additionalResources.shift();
      }

      // Remove empty resources
      if (isEmpty(data.resources)) {
        delete data.resources;
      }

      // Validity Schedules
      if (values?.validitySchedules && values.validitySchedules.length > 0) {
        data.validitySchedules = [];
        values.validitySchedules.filter((val) => {
          if (val) {
            let timeObj = {};
            if (val.startTime) {
              timeObj.startTime = moment(val.startTime).format(
                "YYYY/MM/DD HH:mm:ss"
              );
            }
            if (val.endTime) {
              timeObj.endTime = moment(val.endTime).format(
                "YYYY/MM/DD HH:mm:ss"
              );
            }
            if (val.timeZone) {
              timeObj.timeZone = val.timeZone.id;
            }
            if (!isEmpty(timeObj)) {
              data.validitySchedules.push(timeObj);
            }
          }
          return null;
        });
      }

      // Policy Conditions
      if (values?.conditions && !isEmpty(values.conditions)) {
        data.conditions = [];
        Object.entries(values.conditions).map(([key, value]) => {
          if (value != "" && value != null) {
            return data.conditions.push({
              type: key,
              values: isArray(value) ? value : [value]
            });
          }
          return null;
        });
      } else {
        data.conditions = [];
      }

      // For zone policy
      if (values?.zoneName?.value) {
        data.zoneName = values?.zoneName?.label;
        let zoneDetails = {};
        zoneDetails["label"] = values?.zoneName?.value;
        zoneDetails["value"] = values?.zoneName?.zoneId;
        localStorage.setItem("zoneDetails", JSON.stringify(zoneDetails));
      } else {
        localStorage.removeItem("zoneDetails");
      }

      // eslint-disable-next-line no-console
      console.log("Create policy payload:", JSON.stringify(data, null, 2));

      const getPolicyCount = await fetchApi({
        url: "plugins/policies/count",
        method: "GET",
        params: {
          serviceName: data.service,
          policyType: data.policyType
        }
      });

      // Create new policy
      await fetchApi({
        url: "plugins/policies",
        method: "POST",
        data: data
      });

      toast.success("Policy created successfully!");

      // Navigate to policy listing page using service ID
      const serviceId = values.serviceName?.serviceId;
      const policyType = values.policyType || 0;
      let tablePageData = {};
      tablePageData["pageRecords"] = getPolicyCount.data;
      tablePageData["pageSize"] = 25;
      if (getPolicyCount.data % 25 == 0) {
        tablePageData["totalPage"] = Math.ceil(getPolicyCount.data / 25) + 1;
      } else {
        tablePageData["totalPage"] = Math.ceil(getPolicyCount.data / 25);
      }
      setBlockUI(false);
      setPreventUnBlock(true);
      navigate(`/service/${serviceId}/policies/${policyType}`, {
        state: { showLastPage: true, addPageData: tablePageData }
      });
    } catch (error) {
      setBlockUI(false);
      console.error("Error creating policy:", error);
      let errorMsg = "Failed to create policy";
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
    }
  };

  // Define the steps configuration
  const steps = [
    {
      title: "Service",
      subtitle: "Add service details",
      component: (props) => (
        <ServiceStep
          {...props}
          selectedServiceComponentDef={selectedServiceComponentDef}
          onServiceTypeChange={handleServiceTypeChange}
        />
      ),
      validate: (values) => {
        const errors = {};
        if (!values.serviceType) {
          errors.serviceType = "Service Type is required";
        }
        if (!values.serviceName) {
          errors.serviceName = "Service Name is required";
        }
        return errors;
      }
    },
    {
      title: "Resources",
      subtitle: "Add resource details",
      component: (props) => (
        <ResourcesStep
          {...props}
          selectedServiceComponentDef={selectedServiceComponentDef}
        />
      ),
      validate: (values) => {
        const errors = {};

        // Check if additionalResources array exists
        if (
          !values.additionalResources ||
          values.additionalResources.length === 0
        ) {
          return errors; // Let the form handle this naturally
        }

        // Get resource definitions to know which fields are required
        if (
          !selectedServiceComponentDef ||
          !selectedServiceComponentDef.resources
        ) {
          return errors;
        }

        const serviceCompRes = getResourcesDefVal(
          selectedServiceComponentDef,
          values.policyType || 0
        );

        const grpResources = groupBy(serviceCompRes || [], "level");
        let grpResourcesKeys = [];
        for (const resourceKey in grpResources) {
          grpResourcesKeys.push(+resourceKey);
        }
        grpResourcesKeys = grpResourcesKeys.sort();

        // Initialize errors for additionalResources array
        errors.additionalResources = [];

        // Validate each resource
        values.additionalResources.forEach((resource, resourceIndex) => {
          const resourceErrors = {};

          if (!resource || typeof resource !== "object") {
            return; // Skip invalid resources
          }

          // Check each resource level
          grpResourcesKeys.forEach((levelKey) => {
            const resourceNameKey = `resourceName-${levelKey}`;
            const resourceValueKey = `value-${levelKey}`;

            const resourceName = resource[resourceNameKey];
            const resourceValue = resource[resourceValueKey];

            // If resourceName is selected, check if value is required and provided
            if (resourceName && resourceName.mandatory) {
              if (!resourceValue || isEmpty(resourceValue)) {
                resourceErrors[resourceValueKey] =
                  `${resourceName.label || "Resource"} value is required`;
              }
            }
          });

          // Add errors for this resource if any exist
          if (Object.keys(resourceErrors).length > 0) {
            errors.additionalResources[resourceIndex] = resourceErrors;
          }
        });

        // Clean up empty error arrays
        if (
          errors.additionalResources &&
          errors.additionalResources.every((item) => !item)
        ) {
          delete errors.additionalResources;
        }

        return errors;
      }
    },
    {
      title: "Access Rules",
      subtitle: "Configure access rules",
      component: (props) => (
        <AccessRulesStep
          {...props}
          selectedServiceComponentDef={selectedServiceComponentDef}
        />
      ),
      validate: (values) =>
        validatePolicyItems(values, selectedServiceComponentDef)
    },
    {
      title: "Policy Details",
      subtitle: "Add policy details",
      component: PolicyDetailsStep,
      componentProps: {
        selectedServiceComponentDef
      },
      validate: validatePolicyDetails
    },
    {
      title: "Review & Confirm",
      subtitle: "Verify details",
      component: ReviewConfirmStep,
      componentProps: {
        selectedServiceComponentDef
      }
    }
  ];

  return (
    <>
      <div className="header-wraper">
        <h3 className="wrap-header bold">Policy Editor</h3>
        <CustomBreadcrumb />
      </div>
      <div className="container-fluid p-xxl-4 p-xl-3 p-lg-2">
        <div className="row">
          <div className="col-md-12">
            <div className="card">
              <div className="card-body">
                <h5 className="mb-3 mt-2">Create policy</h5>
                <hr />
                <StepperForm
                  steps={steps}
                  onSubmit={handleSubmit}
                  onBeforeNavigate={({ fromStep, form }) => {
                    if (fromStep === 1) {
                      prunePolicyItemAccessesToAllowedTypes(
                        form,
                        selectedServiceComponentDef
                      );
                      pruneConstraintPolicyItemsToMatchedResources(
                        form,
                        selectedServiceComponentDef
                      );

                      const conditionDefVal = policyConditionUpdatedJSON(
                        selectedServiceComponentDef?.policyConditions
                      );
                      const actionReqsMap =
                        buildActionReqsMapFromConditionDef(conditionDefVal);
                      const isDenyAllElse =
                        form.getState().values?.isDenyAllElse;
                      pruneStaleActionMatcherInPolicyItemCondition({
                        form,
                        attrNames: [
                          "policyItems",
                          "allowExceptions",
                          ...(isDenyAllElse
                            ? []
                            : ["denyPolicyItems", "denyExceptions"])
                        ],
                        serviceCompDetails: selectedServiceComponentDef,
                        conditionDefVal,
                        actionReqsMap
                      });
                    }
                  }}
                  initialValues={{
                    policyType: 0,
                    additionalResources: [{}], // Initialize with one empty resource
                    policyItems: [{}], // For allow conditions
                    allowExceptions: [{}], // For allow exceptions
                    denyPolicyItems: [{}], // For deny conditions
                    denyExceptions: [{}], // For deny exceptions
                    dataMaskPolicyItems: [{}], // For masking rules
                    rowFilterPolicyItems: [{}], // For row filter rules
                    isDenyAllElse: false, // For deny all toggle
                    isEnabled: true, // Policy enabled by default
                    isAuditEnabled: true, // Audit enabled by default
                    policyPriority: 0, // Normal priority by default
                    constraintPolicyItems: [{}] // For constraint
                  }}
                  blockUI={blockUI}
                  preventUnBlock={preventUnBlock}
                />
              </div>
            </div>
          </div>
        </div>
        <BlockUi isUiBlock={blockUI} />
      </div>
    </>
  );
};

export default CreateNewPolicyForm;
