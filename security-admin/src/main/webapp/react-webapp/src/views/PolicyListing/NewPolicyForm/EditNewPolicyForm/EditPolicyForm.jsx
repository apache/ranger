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
import { useParams, useNavigate, useOutletContext } from "react-router-dom";
import { Form } from "react-final-form";
import arrayMutators from "final-form-arrays";
import { toast } from "react-toastify";
import { Loader, BlockUi, scrollToError } from "Components/CommonComponents";
import { fetchApi } from "Utils/fetchAPI";
import usePrompt from "Hooks/usePrompt";
import {
  getResourcesDefVal,
  getAllTimeZoneList,
  policyConditionUpdatedJSON,
  isPolicyExpired,
  policyInfo,
  getAccessTypesByResource
} from "Utils/XAUtils";
import { RangerPolicyType } from "Utils/XAEnums";
import {
  isEmpty,
  isArray,
  find,
  map,
  groupBy,
  has,
  maxBy,
  forIn,
  includes,
  filter,
  isEqual
} from "lodash";
import moment from "moment";
import {
  Accordion,
  Alert,
  Badge,
  Button,
  Col,
  Row,
  Modal
} from "react-bootstrap";
import { validatePolicyForm } from "../policyFormValidation";
import ResourcesStep from "../PolicyCreateStepperStepComp/ResourcesStep";
import AccessRulesStep from "../PolicyCreateStepperStepComp/AccessRulesStep";
import PolicyDetailsStep from "../PolicyCreateStepperStepComp/PolicyDetailsStep";

// ─── helpers ─────────────────────────────────────────────────────────────────

const PromptDialog = (props) => {
  const { isDirtyField, isUnblock } = props;
  usePrompt("Are you sure you want to leave", isDirtyField && !isUnblock);
  return null;
};

const noneOptions = { label: "None", value: "none" };

/** Convert API policy items into form state compatible with the new components */
const setPolicyItemVal = (
  formData,
  accessTypes,
  maskTypes,
  serviceType,
  serviceData,
  finalAccessTypes
) => {
  return formData.map((val) => {
    let obj = {};

    if (has(val, "delegateAdmin")) {
      obj.delegateAdmin = val.delegateAdmin;
    }

    // Accesses
    if (serviceType === "tag") {
      let accessTypesObj = [];
      for (let i = 0; val.accesses.length > i; i++) {
        accessTypes.map((opt) => {
          if (val.accesses[i].type === opt.name) accessTypesObj.push(opt.name);
        });
      }
      let tableList = [];
      let tagAccessType = groupBy(accessTypesObj, (m) =>
        m.substr(0, m.indexOf(":"))
      );
      for (let tagObjData in tagAccessType) {
        tableList.push({
          serviceName: tagObjData,
          permission: tagAccessType[tagObjData]
        });
      }
      obj.accesses = { tableList };
    } else {
      // New format: {type, isAllowed}
      let accessTypesObj = [];
      for (let i = 0; val?.accesses?.length > i; i++) {
        finalAccessTypes.map((opt) => {
          if (val.accesses[i].type === opt.value) {
            accessTypesObj.push({ type: opt.value, isAllowed: true });
          }
        });
      }
      obj.accesses = accessTypesObj;
    }

    // Users / Groups / Roles – merge into UsersGroupsAndRolesSelectionComponent format
    const toNormalisedPrincipal = (name, type) => ({
      value: `${type}:${name}`,
      name: name,
      type: type
    });

    obj.users = {
      users: (val?.users || []).map((u) => toNormalisedPrincipal(u, "USER")),
      groups: (val?.groups || []).map((g) => toNormalisedPrincipal(g, "GROUP")),
      roles: (val?.roles || []).map((r) => toNormalisedPrincipal(r, "ROLE"))
    };

    // Row filter
    if (val?.rowFilterInfo?.filterExpr) {
      obj.rowFilterInfo = val.rowFilterInfo.filterExpr;
    }

    // Data mask
    if (val?.dataMaskInfo?.dataMaskType) {
      obj.dataMaskInfo = {};
      let maskDataType = maskTypes?.find(
        (m) => m.name === val.dataMaskInfo.dataMaskType
      );
      if (maskDataType) {
        obj.dataMaskInfo.label = maskDataType.label;
        obj.dataMaskInfo.value = maskDataType.name;
      }
      if (val?.dataMaskInfo?.valueExpr)
        obj.dataMaskInfo.valueExpr = val.dataMaskInfo.valueExpr;
    }

    // Conditions
    if (val?.conditions?.length > 0) {
      obj.conditions = {};
      for (let data of val.conditions) {
        let conditionObj = find(
          policyConditionUpdatedJSON(serviceData?.policyConditions),
          (m) => m.name === data.type
        );
        if (conditionObj && !isEmpty(conditionObj.uiHint)) {
          obj.conditions[data.type] = JSON.parse(conditionObj.uiHint)
            .isMultiValue
            ? data.values
            : data.values.toString().trim();
        }
      }
    }

    return obj;
  });
};

/** Transform API response into React Final Form initial values */
const generateFormData = (policyData, serviceCompData) => {
  const data = {};
  data.policyType = policyData?.policyType ?? 0;
  // Basic fields
  data.policyName = policyData?.name;
  data.isEnabled = policyData?.isEnabled ?? true;
  data.policyPriority = policyData?.policyPriority === 1 ? 1 : 0;
  data.description = policyData?.description?.trim() ?? "";
  data.isAuditEnabled = policyData?.isAuditEnabled ?? true;
  data.isDenyAllElse = policyData?.isDenyAllElse ?? false;
  data.policyLabels = (policyData?.policyLabels || []).map((v) => ({
    label: v.trim(),
    value: v.trim()
  }));

  // Resources (multi-resource)
  let serviceCompResourcesDetails = getResourcesDefVal(
    serviceCompData,
    data.policyType
  );
  if (policyData.additionalResources || policyData.resources) {
    policyData.additionalResources = [
      policyData.resources,
      ...(policyData.additionalResources || [])
    ];
    data.additionalResources = [];
    map(policyData.additionalResources, function (resourceObj) {
      let lastResourceLevel = [],
        additionalResourcesObj = {};
      Object.entries(resourceObj).map(([key, value]) => {
        let setResources = find(serviceCompResourcesDetails, ["name", key]);
        additionalResourcesObj[`resourceName-${setResources?.level}`] =
          setResources;
        additionalResourcesObj[`value-${setResources?.level}`] =
          value.values.map((m) => {
            return { label: m?.trim(), value: m?.trim() };
          });
        if (setResources?.excludesSupported) {
          additionalResourcesObj[`isExcludesSupport-${setResources?.level}`] =
            value.isExcludes == false;
        }
        if (setResources?.recursiveSupported) {
          additionalResourcesObj[`isRecursiveSupport-${setResources?.level}`] =
            value?.isRecursive;
        }
        lastResourceLevel.push({
          level: setResources.level,
          name: setResources.name
        });
      });
      lastResourceLevel = maxBy(lastResourceLevel, "level");
      let setLastResources = find(serviceCompResourcesDetails, [
        "parent",
        lastResourceLevel.name
      ]);
      if (setLastResources && setLastResources?.isValidLeaf) {
        additionalResourcesObj[`resourceName-${setLastResources.level}`] = {
          label: "None",
          value: "none"
        };
      }
      data.additionalResources.push(additionalResourcesObj);
    });
  }

  // Validity schedules
  if (policyData?.validitySchedules) {
    data.validitySchedules = policyData.validitySchedules.map((val) => {
      const obj = {};
      if (val.startTime)
        obj.startTime = moment(val.startTime, "YYYY/MM/DD HH:mm:ss");
      if (val.endTime) obj.endTime = moment(val.endTime, "YYYY/MM/DD HH:mm:ss");
      if (val.timeZone)
        obj.timeZone = getAllTimeZoneList().find(
          (tz) => tz.id === val.timeZone
        );
      return obj;
    });
  }

  // Policy conditions
  if (policyData?.conditions?.length > 0) {
    data.conditions = {};
    for (const val of policyData.conditions) {
      const conditionObj = find(
        policyConditionUpdatedJSON(serviceCompData?.policyConditions),
        (m) => m.name === val.type
      );
      if (conditionObj && !isEmpty(conditionObj.uiHint)) {
        data.conditions[val.type] = JSON.parse(conditionObj.uiHint).isMultiValue
          ? val?.values
          : val.values.toString().trim();
      }
    }
  }

  //permissions Item
  const finalAccessTypes = getAccessTypesByResource(data, serviceCompData);

  const policyItemOpts = (arr, accTypes, maskTypes, finalAccessTypes) =>
    arr?.length > 0
      ? setPolicyItemVal(
          arr,
          accTypes,
          maskTypes,
          serviceCompData?.name,
          serviceCompData,
          finalAccessTypes
        )
      : [{}];

  data.policyItems = policyItemOpts(
    policyData?.policyItems,
    serviceCompData?.accessTypes,
    null,
    finalAccessTypes
  );
  data.allowExceptions = policyItemOpts(
    policyData?.allowExceptions,
    serviceCompData?.accessTypes,
    null,
    finalAccessTypes
  );
  data.denyPolicyItems = policyItemOpts(
    policyData?.denyPolicyItems,
    serviceCompData?.accessTypes,
    null,
    finalAccessTypes
  );
  data.denyExceptions = policyItemOpts(
    policyData?.denyExceptions,
    serviceCompData?.accessTypes,
    null,
    finalAccessTypes
  );
  data.dataMaskPolicyItems = policyItemOpts(
    policyData?.dataMaskPolicyItems,
    serviceCompData?.dataMaskDef?.accessTypes,
    serviceCompData?.dataMaskDef?.maskTypes,
    finalAccessTypes
  );
  data.rowFilterPolicyItems = policyItemOpts(
    policyData?.rowFilterPolicyItems,
    serviceCompData?.rowFilterDef?.accessTypes,
    null,
    finalAccessTypes
  );

  return data;
};

/** Resolve a stored principal object to its plain name */
const principalName = (p) => {
  if (typeof p !== "object") return p;
  if (p.name) return p.name;
  if (p.value) {
    const idx = p.value.indexOf(":");
    return idx !== -1 ? p.value.slice(idx + 1) : p.value;
  }
  return p;
};

/** Transform form policy items back to API format */
const getPolicyItemsVal = (formData, name, finalAccessTypes) => {
  if (!formData[name] || formData[name].length === 0) return [];
  const result = [];

  for (const key of formData[name]) {
    if (!isEmpty(key) && Object.entries(key).length > 0) {
      const obj = {};

      if (key.delegateAdmin != null && key.delegateAdmin !== "undefined") {
        obj.delegateAdmin = key.delegateAdmin;
      }

      // Accesses
      if (key?.accesses?.tableList && isArray(key.accesses.tableList)) {
        obj.accesses = key.accesses.tableList.flatMap((t) =>
          (t.permission || []).map((p) => ({ type: p, isAllowed: true }))
        );
      } else if (isArray(key.accesses) && key.accesses.length > 0) {
        const validValues = map(finalAccessTypes, "value");
        const result = filter(key.accesses, (item) =>
          includes(validValues, item.type)
        );
        obj.accesses = result.map((a) => {
          return { type: a.type, isAllowed: a.isAllowed === true };
        });
      }

      // Users / Groups / Roles (combined field from UsersGroupsAndRolesSelectionComponent)
      if (key.users) {
        if (isArray(key.users)) {
          obj.users = key.users.map(principalName);
        } else if (key.users.users && isArray(key.users.users)) {
          obj.users = key.users.users.map(principalName);
          if (key.users.groups?.length)
            obj.groups = key.users.groups.map(principalName);
          if (key.users.roles?.length)
            obj.roles = key.users.roles.map(principalName);
        }
      }
      if (!obj.groups && key.groups?.length)
        obj.groups = key.groups.map(principalName);
      if (!obj.roles && key.roles?.length)
        obj.roles = key.roles.map(principalName);

      // Row filter
      if (
        key.rowFilterInfo != null &&
        key.rowFilterInfo !== "" &&
        key.rowFilterInfo !== "undefined"
      ) {
        obj.rowFilterInfo = {
          filterExpr:
            typeof key.rowFilterInfo === "object"
              ? key.rowFilterInfo.filterExpr
              : key.rowFilterInfo
        };
      }

      // Data mask
      if (
        key.dataMaskInfo != null &&
        !isEmpty(key.dataMaskInfo) &&
        key.dataMaskInfo !== "undefined"
      ) {
        obj.dataMaskInfo = {
          dataMaskType: key.dataMaskInfo.value || key.dataMaskInfo.dataMaskType
        };
        if (key.dataMaskInfo.valueExpr)
          obj.dataMaskInfo.valueExpr = key.dataMaskInfo.valueExpr;
      }

      // Rule conditions
      if (key.conditions && !isEmpty(key.conditions)) {
        obj.conditions = Object.entries(key.conditions)
          .filter(([, v]) => v !== "" && v != null)
          .map(([k, v]) => ({
            type: k,
            values: isArray(v)
              ? v.map((m) => (typeof m === "object" ? m.value : m))
              : [v]
          }));
      }

      if (!isEmpty(obj)) result.push(obj);
    }
  }
  return result;
};

// ─── main component ──────────────────────────────────────────────────────────

export default function EditPolicyForm() {
  const { serviceId, policyId } = useParams();
  const navigate = useNavigate();

  // Get data from PolicyFormContext
  const contextData = useOutletContext();
  const {
    serviceDetails,
    serviceCompDetails: serviceCompDef,
    policyData
  } = contextData;

  const [blockUI, setBlockUI] = useState(false);
  const [formData, setFormData] = useState({});
  const [showDelete, setShowDelete] = useState(false);
  const [preventUnBlock, setPreventUnblock] = useState(false);
  const [showPolicyExpire, setShowPolicyExpire] = useState(true);
  const [show, setShow] = useState(true);

  useEffect(() => {
    if (policyData && serviceCompDef) {
      initializeFormData();
    }
  }, [policyData, serviceCompDef]);

  const initializeFormData = () => {
    try {
      const fd = generateFormData(policyData, serviceCompDef);
      // ResourcesStep reads values.serviceName; pass the plain name so it works without the stepper's service selector
      fd.serviceName = serviceDetails.name;
      setFormData(fd);
    } catch (err) {
      console.error("Error initializing edit policy form:", err);
      toast.error("Failed to initialize policy data.");
    }
  };

  // Helper functions for error handling and delete functionality
  const showDeleteModal = () => {
    setShowDelete(true);
  };

  const hideDeleteModal = () => {
    setShowDelete(false);
  };

  const getValidatePolicyItems = (errors) => {
    let errorField;
    errors?.find((value) => {
      if (value !== undefined) {
        return (errorField = value);
      }
    });

    return errorField !== undefined && errorField?.accesses
      ? toast.error(errorField?.accesses, { toastId: "error1" })
      : toast.error(errorField?.delegateAdmin, { toastId: "error1" });
  };

  const resourceErrorCheck = (errors) => {
    if (errors?.additionalResources?.length > 0) {
      return true;
    }

    return false;
  };

  const handleDeleteClick = async (policyID, serviceId) => {
    hideDeleteModal();
    try {
      setPreventUnblock(true);
      setBlockUI(true);
      await fetchApi({
        url: `plugins/policies/${policyID}`,
        method: "DELETE"
      });
      setBlockUI(false);
      toast.success("Success! Policy deleted successfully");
      navigate(`/service/${serviceId}/policies/${policyData?.policyType || 0}`);
    } catch (error) {
      setBlockUI(false);
      let errorMsg = `Error occurred during deleting policy`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error(`Error occurred during deleting policy! ${error}`);
    }
  };

  const handleSubmit = async (values) => {
    try {
      setPreventUnblock(true);
      setBlockUI(true);

      const data = {};
      const finalAccessTypes = getAccessTypesByResource(values, serviceCompDef);
      data.policyItems = getPolicyItemsVal(
        values,
        "policyItems",
        finalAccessTypes
      );
      data.allowExceptions = getPolicyItemsVal(
        values,
        "allowExceptions",
        finalAccessTypes
      );
      data.denyPolicyItems = values.isDenyAllElse
        ? []
        : getPolicyItemsVal(values, "denyPolicyItems", finalAccessTypes);
      data.denyExceptions = values.isDenyAllElse
        ? []
        : getPolicyItemsVal(values, "denyExceptions", finalAccessTypes);
      data.dataMaskPolicyItems = getPolicyItemsVal(
        values,
        "dataMaskPolicyItems",
        finalAccessTypes
      );
      data.rowFilterPolicyItems = getPolicyItemsVal(
        values,
        "rowFilterPolicyItems",
        finalAccessTypes
      );

      data.id = policyId;
      data.name = values.policyName;
      data.description = values.description || "";
      data.isEnabled = values.isEnabled !== false;
      data.isAuditEnabled = values.isAuditEnabled !== false;
      data.isDenyAllElse = values.isDenyAllElse || false;
      data.policyPriority = values.policyPriority === 1 ? 1 : 0;
      data.policyType = values.policyType ?? 0;
      data.service = serviceDetails?.name;
      data.policyLabels = (values.policyLabels || []).map((l) =>
        typeof l === "object" ? l.value || l.label : l
      );

      // Resources
      const serviceCompRes =
        getResourcesDefVal(serviceCompDef, data.policyType) || [];
      const levels = [...new Set(serviceCompRes.map((r) => r.level))].sort(
        (a, b) => a - b
      );

      data.additionalResources = [];
      map(values.additionalResources || [], (resourceValue) => {
        const obj = {};
        for (const level of levels) {
          const nameObj = resourceValue[`resourceName-${level}`];
          const valArr = resourceValue[`value-${level}`];
          if (
            !nameObj ||
            nameObj.value === noneOptions.value ||
            !valArr ||
            isEmpty(valArr)
          )
            continue;

          const defObj = find(
            serviceCompRes,
            (m) => m.name === (nameObj.name || nameObj.value)
          );
          const resName = nameObj.name || nameObj.value;
          obj[resName] = {
            values: isArray(valArr)
              ? valArr.map((v) => v.value || v)
              : [valArr.value || valArr]
          };
          if (defObj?.excludesSupported) {
            obj[resName].isExcludes =
              defObj.excludesSupported &&
              resourceValue[`isExcludesSupport-${level}`] === false;
          }
          if (defObj?.recursiveSupported) {
            obj[resName].isRecursive =
              defObj.recursiveSupported &&
              resourceValue[`isRecursiveSupport-${level}`] !== false;
          }
        }
        if (!isEmpty(obj)) data.additionalResources.push(obj);
      });

      if (data.additionalResources.length > 0) {
        data.resources = data.additionalResources[0];
        data.additionalResources = data.additionalResources.slice(1);
      }

      // Validity schedules
      if (values.validitySchedules?.length > 0) {
        data.validitySchedules = values.validitySchedules
          .filter(Boolean)
          .map((s) => ({
            startTime: s.startTime
              ? moment(s.startTime).format("YYYY/MM/DD HH:mm:ss")
              : "",
            endTime: s.endTime
              ? moment(s.endTime).format("YYYY/MM/DD HH:mm:ss")
              : "",
            timeZone: s.timeZone?.id || ""
          }));
      } else {
        data["validitySchedules"] = [];
      }

      // Policy conditions
      if (values.conditions && !isEmpty(values.conditions)) {
        data.conditions = Object.entries(values.conditions)
          .filter(([, v]) => v !== "" && v != null)
          .map(([k, v]) => ({
            type: k,
            values: isArray(v)
              ? v.map((m) => (typeof m === "object" ? m.value : m))
              : [v]
          }));
      } else {
        data["conditions"] = [];
      }

      /* For create zone policy */
      if (localStorage.getItem("zoneDetails") != null) {
        data["zoneName"] = JSON.parse(
          localStorage.getItem("zoneDetails")
        ).label;
      }

      const mergeFormData = {
        ...policyData,
        ...data
      };

      await fetchApi({
        url: `plugins/policies/${policyId}`,
        method: "PUT",
        data: mergeFormData
      });

      toast.success("Policy updated successfully!");
      setBlockUI(false);
      navigate(`/service/${serviceId}/policies/${formData.policyType ?? 0}`);
    } catch (err) {
      setBlockUI(false);
      const msg = err?.response?.data?.msgDesc || "Failed to update policy.";
      toast.error(`Error! ${msg}`);
    }
  };

  const handleCancel = () => {
    navigate(`/service/${serviceId}/policies/${formData.policyType ?? 0}`);
  };

  if (
    !serviceDetails ||
    !serviceCompDef ||
    !policyData ||
    !formData.policyName
  ) {
    return <Loader />;
  }

  const policyTypeLabel =
    formData.policyType === 1
      ? "Masking"
      : formData.policyType === 2
        ? "Row Filter"
        : "Access";
  const policyTypeBg =
    formData.policyType === 1
      ? "warning"
      : formData.policyType === 2
        ? "info"
        : "primary";

  return (
    <>
      <div className="wrap">
        <Form
          onSubmit={handleSubmit}
          mutators={{ ...arrayMutators }}
          initialValues={formData}
          validate={validatePolicyForm}
          render={({
            handleSubmit: submitForm,
            values,
            form,
            invalid,
            errors,
            dirty,
            pristine,
            initialValues
          }) => (
            <form onSubmit={submitForm} noValidate>
              <PromptDialog
                isDirtyField={
                  dirty && !isEqual(values, initialValues) && !pristine
                }
                isUnblock={preventUnBlock}
              />

              {(values.policyType ==
                RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value ||
                values.policyType ==
                  RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value) &&
                show && (
                  <Alert
                    variant="warning"
                    onClose={() => setShow(false)}
                    dismissible
                    data-js="policyInfoAlert"
                    data-cy="policyInfoAlert"
                  >
                    <i className="fa-fw fa fa-info-circle d-inline text-dark"></i>
                    {policyInfo(values.policyType, serviceCompDef.name)}
                  </Alert>
                )}

              {policyId &&
                !isEmpty(policyData?.validitySchedules) &&
                isPolicyExpired(policyData) &&
                showPolicyExpire && (
                  <Alert
                    variant="danger"
                    onClose={() => setShowPolicyExpire(false)}
                    dismissible
                  >
                    <i className="fa fa-clock-o fa-fw fa-lg time-clock" />
                    <p className="pd-l-6 d-inline"> Policy Expired</p>
                  </Alert>
                )}

              {/* Policy name + type badge */}
              <fieldset>
                <p className="formHeader" style={{ fontSize: "16px" }}>
                  {values.policyName}&nbsp; &nbsp;
                  <span
                    className="text-muted"
                    style={{ fontSize: "16px", fontWeight: "normal" }}
                  >
                    (id: {policyId})
                  </span>
                </p>
              </fieldset>

              <Accordion
                defaultActiveKey={["service", "resources", "access", "details"]}
                alwaysOpen
                className="policy-permission-items-accordion"
              >
                {/* ── Service ──────────────────────────────────── */}
                <Accordion.Item eventKey="service">
                  <Accordion.Header>Service</Accordion.Header>
                  <Accordion.Body>
                    <Row className="user-role-grp-form">
                      <Col md={5}>
                        <Row className="mb-2">
                          <Col sm={4} className="text-end fnt-14">
                            Service :
                          </Col>
                          <Col sm={8}>{serviceDetails?.name}</Col>
                        </Row>
                        <Row className="mb-2">
                          <Col sm={4} className="text-end fnt-14">
                            Service Type :
                          </Col>
                          <Col sm={8}>{serviceDetails?.type}</Col>
                        </Row>
                        {serviceDetails?.zoneName && (
                          <Row className="mb-2">
                            <Col sm={4} className="text-end fnt-14">
                              Security Zone :
                            </Col>
                            <Col sm={8}>{serviceDetails.zoneName}</Col>
                          </Row>
                        )}
                        <Row className="mb-2">
                          <Col sm={4} className="text-end fnt-14">
                            policy Type :
                          </Col>
                          <Col sm={8}>
                            <Badge
                              bg={policyTypeBg}
                              text={
                                formData.policyType === 1 ? "dark" : undefined
                              }
                            >
                              {policyTypeLabel}
                            </Badge>
                          </Col>
                        </Row>
                      </Col>
                    </Row>
                  </Accordion.Body>
                </Accordion.Item>

                {/* ── Resources ────────────────────────────────── */}
                <Accordion.Item eventKey="resources">
                  <Accordion.Header>Resources</Accordion.Header>
                  <Accordion.Body>
                    <ResourcesStep
                      values={values}
                      form={form}
                      selectedServiceComponentDef={serviceCompDef}
                    />
                  </Accordion.Body>
                </Accordion.Item>

                {/* ── Access Rules ─────────────────────────────── */}
                <Accordion.Item eventKey="access">
                  <Accordion.Header>Access rules</Accordion.Header>
                  <Accordion.Body>
                    <AccessRulesStep
                      values={values}
                      form={form}
                      selectedServiceComponentDef={serviceCompDef}
                    />
                  </Accordion.Body>
                </Accordion.Item>

                {/* ── Policy Details ───────────────────────────── */}
                <Accordion.Item eventKey="details">
                  <Accordion.Header>Policy details</Accordion.Header>
                  <Accordion.Body>
                    <PolicyDetailsStep
                      values={values}
                      form={form}
                      selectedServiceComponentDef={serviceCompDef}
                    />
                  </Accordion.Body>
                </Accordion.Item>
              </Accordion>

              {/* Footer */}
              <div className="row form-actions">
                <div className="col-md-9 offset-md-3">
                  <Button
                    onClick={(event) => {
                      if (invalid) {
                        forIn(errors, function (value, key) {
                          if (
                            has(errors, "policyName") ||
                            resourceErrorCheck(errors)
                          ) {
                            let selector =
                              document?.getElementById("isError") ||
                              document?.getElementById(key) ||
                              document?.querySelector(`input[name=${key}]`) ||
                              document?.querySelector(`input[id=${key}]`) ||
                              document?.querySelector(
                                `span[className="invalid-field"]`
                              );
                            scrollToError(selector);
                          } else {
                            getValidatePolicyItems(errors?.[key]);
                          }
                        });
                      }
                      submitForm(event);
                    }}
                    variant="primary"
                    size="sm"
                    disabled={blockUI}
                    data-id="save"
                    data-cy="save"
                  >
                    Save
                  </Button>
                  <Button
                    variant="secondary"
                    type="button"
                    size="sm"
                    onClick={handleCancel}
                    disabled={blockUI}
                    data-id="cancel"
                    data-cy="cancel"
                  >
                    Cancel
                  </Button>

                  {policyId && (
                    <Button
                      variant="danger"
                      type="button"
                      className="btn-sm"
                      size="sm"
                      onClick={showDeleteModal}
                      disabled={blockUI}
                    >
                      Delete
                    </Button>
                  )}
                </div>

                {/* Delete confirmation modal */}
                {policyId && (
                  <Modal show={showDelete} onHide={hideDeleteModal}>
                    <Modal.Header closeButton>
                      <span className="text-word-break">
                        Are you sure want to delete policy&nbsp;&quot;
                        <b>{`${values?.policyName}`}</b>&quot; ?
                      </span>
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
                        onClick={() => handleDeleteClick(policyId, serviceId)}
                      >
                        Yes
                      </Button>
                    </Modal.Footer>
                  </Modal>
                )}
              </div>
            </form>
          )}
        />
        <BlockUi isUiBlock={blockUI} />
      </div>
    </>
  );
}
