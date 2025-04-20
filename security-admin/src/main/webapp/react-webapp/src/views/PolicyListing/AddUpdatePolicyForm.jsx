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

import React, { useEffect, useReducer, useState, useContext } from "react";
import {
  Form as FormB,
  Row,
  Col,
  Button,
  Badge,
  Accordion,
  Alert,
  Modal
} from "react-bootstrap";
import { Form, Field } from "react-final-form";
import AsyncCreatableSelect from "react-select/async-creatable";
import BootstrapSwitchButton from "bootstrap-switch-button-react";
import arrayMutators from "final-form-arrays";
import {
  groupBy,
  find,
  isEmpty,
  pick,
  isArray,
  isEqual,
  forIn,
  has,
  maxBy,
  map,
  isUndefined,
  forEach,
  reject,
  cloneDeep
} from "lodash";
import { toast } from "react-toastify";
import {
  BlockUi,
  Loader,
  scrollToError,
  selectInputCustomStyles
} from "Components/CommonComponents";
import { fetchApi } from "Utils/fetchAPI";
import { RangerPolicyType, getEnumElementByValue } from "Utils/XAEnums";
import ResourceComp from "../Resources/ResourceComp";
import PolicyPermissionItem from "../PolicyListing/PolicyPermissionItem";
import { useParams, useNavigate, useLocation } from "react-router-dom";
import PolicyValidityPeriodComp from "./PolicyValidityPeriodComp";
import PolicyConditionsComp from "./PolicyConditionsComp";
import { getAllTimeZoneList, policyConditionUpdatedJSON } from "Utils/XAUtils";
import moment from "moment";
import {
  InfoIcon,
  commonBreadcrumb,
  isPolicyExpired,
  getResourcesDefVal
} from "Utils/XAUtils";
import { useAccordionButton } from "react-bootstrap/AccordionButton";
import AccordionContext from "react-bootstrap/AccordionContext";
import usePrompt from "Hooks/usePrompt";
import { RegexMessage } from "Utils/XAMessages";
import { policyInfo } from "Utils/XAUtils";
import { getServiceDef } from "Utils/appState";
import { FieldArray } from "react-final-form-arrays";

const noneOptions = {
  label: "None",
  value: "none"
};

const isMultiResources = true;

const initialState = {
  loader: true,
  serviceDetails: null,
  serviceCompDetails: null,
  policyData: null,
  formData: {}
};

const PromptDialog = (props) => {
  const { isDirtyField, isUnblock } = props;
  usePrompt("Are you sure you want to leave", isDirtyField && !isUnblock);
  return null;
};

function reducer(state, action) {
  switch (action.type) {
    case "SET_DATA":
      return {
        ...state,
        loader: false,
        serviceDetails: action.serviceDetails,
        serviceCompDetails: action.serviceCompDetails,
        policyData: action?.policyData,
        formData: action.formData
      };
    default:
      throw new Error();
  }
}

const Condition = ({ when, is, children }) => (
  <Field name={when} subscription={{ value: true }}>
    {({ input: { value } }) => (value === is ? children : null)}
  </Field>
);

export default function AddUpdatePolicyForm() {
  let { serviceId, policyType, policyId } = useParams();
  const navigate = useNavigate();
  const { state } = useLocation();
  const serviceDefs = cloneDeep(getServiceDef());
  const [policyState, dispatch] = useReducer(reducer, initialState);
  const { loader, serviceDetails, serviceCompDetails, policyData, formData } =
    policyState;
  const [defaultPolicyLabelOptions, setDefaultPolicyLabelOptions] = useState(
    []
  );
  const [showModal, policyConditionState] = useState(false);
  const [preventUnBlock, setPreventUnblock] = useState(false);
  const [show, setShow] = useState(true);
  const [showPolicyExpire, setShowPolicyExpire] = useState(true);
  const [showDelete, setShowDelete] = useState(false);
  const [blockUI, setBlockUI] = useState(false);
  const toastId = React.useRef(null);
  const [changePolicyItemPermissions, setChangePolicyItemPermissions] =
    useState(false);

  useEffect(() => {
    fetchInitialData();
  }, [serviceId, policyType, policyId]);

  const showDeleteModal = () => {
    setShowDelete(true);
  };

  const hideDeleteModal = () => {
    setShowDelete(false);
  };

  const isDirtyFieldCheck = (dirtyFields, modified, values, initialValues) => {
    let modifiedVal = false;
    if (!isEmpty(dirtyFields)) {
      for (let dirtyFieldVal in dirtyFields) {
        modifiedVal = modified?.[dirtyFieldVal];
        if (
          values?.validitySchedules ||
          modified?.validitySchedules ||
          values?.conditions ||
          modified?.conditions ||
          modifiedVal == true
        ) {
          modifiedVal = true;
          break;
        }
      }
    }
    if (
      !isEqual(values?.validitySchedules, initialValues?.validitySchedules) ||
      !isEqual(values?.conditions, initialValues?.conditions)
    ) {
      modifiedVal = true;
    }
    return modifiedVal;
  };

  const fetchUsersData = async (inputValue) => {
    let params = { name: inputValue || "", isVisible: 1 };
    let op = [];
    const userResp = await fetchApi({
      url: "xusers/lookup/users",
      params: params
    });
    op = userResp.data.vXStrings;

    return op.map((obj) => ({
      label: obj.value,
      value: obj.value
    }));
  };

  const fetchGroupsData = async (inputValue) => {
    let params = { name: inputValue || "", isVisible: 1 };
    let op = [];

    const userResp = await fetchApi({
      url: "xusers/lookup/groups",
      params: params
    });
    op = userResp.data.vXStrings;

    return op.map((obj) => ({
      label: obj.value,
      value: obj.value
    }));
  };

  const fetchRolesData = async (inputValue) => {
    let params = { roleNamePartial: inputValue || "" };
    let op = [];

    const roleResp = await fetchApi({
      url: "roles/roles",
      params: params
    });
    op = roleResp.data.roles;

    return op.map((obj) => ({
      label: obj.name,
      value: obj.name
    }));
  };

  const fetchInitialData = async () => {
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
    let policyData = null;
    if (policyId) {
      policyData = await fetchPolicyData();
    }

    dispatch({
      type: "SET_DATA",
      serviceDetails: serviceData,
      serviceCompDetails: serviceCompData,
      policyData: policyData || null,
      formData: generateFormData(policyData, serviceCompData)
    });
  };

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

  const fetchPolicyLabel = async (inputValue) => {
    let params = {};
    if (inputValue) {
      params["policyLabel"] = inputValue || "";
    }
    const policyLabelResp = await fetchApi({
      url: "plugins/policyLabels",
      params: params
    });

    return policyLabelResp.data.map((name) => ({
      label: name,
      value: name
    }));
  };

  const onFocusPolicyLabel = () => {
    fetchPolicyLabel().then((opts) => {
      setDefaultPolicyLabelOptions(opts);
    });
  };

  const generateFormData = (policyData, serviceCompData) => {
    let data = {};
    data.policyType = policyId ? policyData?.policyType : policyType;
    data.policyItems =
      policyId && policyData?.policyItems?.length > 0
        ? setPolicyItemVal(
            policyData?.policyItems,
            serviceCompData?.accessTypes,
            null,
            serviceCompData?.name,
            serviceCompData
          )
        : [{}];
    data.allowExceptions =
      policyId && policyData?.allowExceptions?.length > 0
        ? setPolicyItemVal(
            policyData?.allowExceptions,
            serviceCompData?.accessTypes,
            null,
            serviceCompData?.name,
            serviceCompData
          )
        : [{}];
    data.denyPolicyItems =
      policyId && policyData?.denyPolicyItems?.length > 0
        ? setPolicyItemVal(
            policyData?.denyPolicyItems,
            serviceCompData?.accessTypes,
            null,
            serviceCompData?.name,
            serviceCompData
          )
        : [{}];
    data.denyExceptions =
      policyId && policyData?.denyExceptions?.length > 0
        ? setPolicyItemVal(
            policyData.denyExceptions,
            serviceCompData?.accessTypes,
            null,
            serviceCompData?.name,
            serviceCompData
          )
        : [{}];
    data.dataMaskPolicyItems =
      policyId && policyData?.dataMaskPolicyItems?.length > 0
        ? setPolicyItemVal(
            policyData.dataMaskPolicyItems,
            serviceCompData?.dataMaskDef?.accessTypes,
            serviceCompData?.dataMaskDef?.maskTypes,
            serviceCompData?.name,
            serviceCompData
          )
        : [{}];
    data.rowFilterPolicyItems =
      policyId && policyData?.rowFilterPolicyItems?.length > 0
        ? setPolicyItemVal(
            policyData.rowFilterPolicyItems,
            serviceCompData?.rowFilterDef?.accessTypes,
            null,
            serviceCompData?.name,
            serviceCompData
          )
        : [{}];
    let serviceCompResourcesDetails = getResourcesDefVal(
      serviceCompData,
      data.policyType
    );
    if (serviceCompResourcesDetails) {
      forEach(serviceCompResourcesDetails, (val) => {
        if (
          val.accessTypeRestrictions &&
          val.accessTypeRestrictions.length > 0
        ) {
          setChangePolicyItemPermissions(true);
        }
      });
    }
    if (policyId) {
      data.policyName = policyData?.name;
      data.isEnabled = policyData?.isEnabled;
      data.policyPriority = policyData?.policyPriority == 0 ? false : true;
      data.description = policyData?.description;
      data.isAuditEnabled = policyData?.isAuditEnabled;
      data.policyLabel =
        policyData &&
        policyData?.policyLabels?.map((val) => {
          return { label: val, value: val };
        });
      if (policyData?.resources) {
        if (!isMultiResources) {
          let lastResourceLevel = [];
          Object.entries(policyData?.resources).map(([key, value]) => {
            let setResources = find(serviceCompResourcesDetails, ["name", key]);
            data[`resourceName-${setResources?.level}`] = setResources;
            data[`value-${setResources?.level}`] = value.values.map((m) => {
              return { label: m, value: m };
            });
            if (setResources?.excludesSupported) {
              data[`isExcludesSupport-${setResources?.level}`] =
                value.isExcludes == false;
            }
            if (setResources?.recursiveSupported) {
              data[`isRecursiveSupport-${setResources?.level}`] =
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
            data[`resourceName-${setLastResources.level}`] = {
              label: "None",
              value: "none"
            };
          }
        } else {
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
                let setResources = find(serviceCompResourcesDetails, [
                  "name",
                  key
                ]);
                additionalResourcesObj[`resourceName-${setResources?.level}`] =
                  setResources;
                additionalResourcesObj[`value-${setResources?.level}`] =
                  value.values.map((m) => {
                    return { label: m, value: m };
                  });
                if (setResources?.excludesSupported) {
                  additionalResourcesObj[
                    `isExcludesSupport-${setResources?.level}`
                  ] = value.isExcludes == false;
                }
                if (setResources?.recursiveSupported) {
                  additionalResourcesObj[
                    `isRecursiveSupport-${setResources?.level}`
                  ] = value?.isRecursive;
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
                additionalResourcesObj[
                  `resourceName-${setLastResources.level}`
                ] = {
                  label: "None",
                  value: "none"
                };
              }
              data.additionalResources.push(additionalResourcesObj);
            });
          }
        }
      }
      if (policyData?.validitySchedules) {
        data["validitySchedules"] = [];
        policyData?.validitySchedules.filter((val) => {
          let obj = {};
          if (val.endTime) {
            obj["endTime"] = moment(val.endTime, "YYYY/MM/DD HH:mm:ss");
          }
          if (val.startTime) {
            obj["startTime"] = moment(val.startTime, "YYYY/MM/DD HH:mm:ss");
          }
          if (val.timeZone) {
            obj["timeZone"] = getAllTimeZoneList().find((tZoneVal) => {
              return tZoneVal.id == val.timeZone;
            });
          }
          data["validitySchedules"].push(obj);
        });
      }

      /* Policy condition */
      if (policyData?.conditions?.length > 0) {
        data.conditions = {};
        for (let val of policyData.conditions) {
          let conditionObj = find(
            policyConditionUpdatedJSON(serviceCompData?.policyConditions),
            function (m) {
              if (m.name == val.type) {
                return m;
              }
            }
          );

          if (!isEmpty(conditionObj.uiHint)) {
            data.conditions[val?.type] = JSON.parse(conditionObj.uiHint)
              .isMultiValue
              ? val?.values
              : val?.values.toString();
          }
        }
      }
    }
    if (isMultiResources && isUndefined(data.additionalResources)) {
      data.additionalResources = [{}];
    }
    data.isDenyAllElse = policyData?.isDenyAllElse || false;
    return data;
  };

  const getPolicyItemsVal = (formData, name) => {
    var policyResourceItem = [];
    for (let key of formData[name]) {
      if (!isEmpty(key) && Object.entries(key).length > 0) {
        let obj = {};
        if (key.delegateAdmin != "undefined" && key.delegateAdmin != null) {
          obj.delegateAdmin = key.delegateAdmin;
        }
        if (key?.accesses?.length > 0 && serviceCompDetails.name !== "tag") {
          obj.accesses = key.accesses.map(({ value }) => ({
            type: value,
            isAllowed: true
          }));
        } else if (
          key?.accesses?.tableList?.length > 0 &&
          serviceCompDetails.name == "tag"
        ) {
          let accessesData = [];
          for (let data in key.accesses.tableList) {
            accessesData = [
              ...accessesData,
              ...key.accesses.tableList[data].permission
            ];
          }
          obj.accesses = accessesData.map((value) => ({
            type: value,
            isAllowed: true
          }));
        }
        if (key.users && key.users.length > 0) {
          obj.users = key.users.map(({ value }) => value);
        }
        if (key.groups && key.groups.length > 0) {
          obj.groups = key.groups.map(({ value }) => value);
        }
        if (key.roles && key.roles.length > 0) {
          obj.roles = key.roles.map(({ value }) => value);
        }
        if (key.rowFilterInfo != "undefined" && key.rowFilterInfo != null) {
          obj.rowFilterInfo = {};
          obj.rowFilterInfo.filterExpr = key.rowFilterInfo;
        }
        if (key.dataMaskInfo != "undefined" && key.dataMaskInfo != null) {
          obj.dataMaskInfo = {};
          obj.dataMaskInfo.dataMaskType = key.dataMaskInfo.value;

          if (key?.dataMaskInfo?.valueExpr) {
            obj.dataMaskInfo.valueExpr = key.dataMaskInfo.valueExpr;
          }
        }
        if (key?.conditions) {
          obj.conditions = [];
          Object.entries(key.conditions).map(
            ([conditionKey, conditionValue]) => {
              return obj.conditions.push({
                type: conditionKey,
                values: isArray(conditionValue)
                  ? conditionValue.map((m) => {
                      return m.value;
                    })
                  : [conditionValue]
              });
            }
          );
        }

        if (
          !isEmpty(obj) &&
          !isEmpty(obj?.delegateAdmin) &&
          Object.keys(obj)?.length > 1
        ) {
          policyResourceItem.push(obj);
        }
        if (
          !isEmpty(obj) &&
          isEmpty(obj?.delegateAdmin) &&
          Object.keys(obj)?.length > 1
        ) {
          policyResourceItem.push(obj);
        }
      }
    }
    return policyResourceItem;
  };

  const setPolicyItemVal = (
    formData,
    accessTypes,
    maskTypes,
    serviceType,
    serviceData
  ) => {
    return formData.map((val) => {
      let obj = {},
        accessTypesObj = [];

      if (has(val, "delegateAdmin")) {
        obj.delegateAdmin = val.delegateAdmin;
      }

      if (serviceType == "tag") {
        for (let i = 0; val.accesses.length > i; i++) {
          accessTypes.map((opt) => {
            if (val.accesses[i].type == opt.name) {
              accessTypesObj.push(opt.name);
            }
          });
        }
        let tableList = [];
        let tagAccessType = groupBy(accessTypesObj, function (m) {
          let tagVal = m;
          return tagVal.substr(0, tagVal.indexOf(":"));
        });
        for (let tagObjData in tagAccessType) {
          tableList.push({
            serviceName: tagObjData,
            permission: tagAccessType[tagObjData]
          });
        }
        obj["accesses"] = { tableList };
      } else {
        for (let i = 0; val?.accesses?.length > i; i++) {
          accessTypes.map((opt) => {
            if (val.accesses[i].type == opt.name) {
              accessTypesObj.push({ label: opt.label, value: opt.name });
            }
          });
        }
        obj["accesses"] = accessTypesObj;
      }
      if (val?.groups?.length > 0) {
        obj.groups = val.groups.map((opt) => {
          return { label: opt, value: opt };
        });
      }
      if (val?.users?.length > 0) {
        obj.users = val.users.map((opt) => {
          return { label: opt, value: opt };
        });
      }
      if (val?.roles?.length > 0) {
        obj.roles = val.roles.map((opt) => {
          return { label: opt, value: opt };
        });
      }
      if (val?.rowFilterInfo?.filterExpr) {
        obj.rowFilterInfo = val.rowFilterInfo.filterExpr;
      }
      if (val?.dataMaskInfo?.dataMaskType) {
        obj.dataMaskInfo = {};
        let maskDataType = maskTypes.find((m) => {
          return m.name == val.dataMaskInfo.dataMaskType;
        });
        obj.dataMaskInfo.label = maskDataType.label;
        obj.dataMaskInfo.value = maskDataType.name;
        if (val?.dataMaskInfo?.valueExpr) {
          obj.dataMaskInfo.valueExpr = val.dataMaskInfo.valueExpr;
        }
      }

      /* Policy Condition */
      if (val?.conditions?.length > 0) {
        obj.conditions = {};

        for (let data of val.conditions) {
          let conditionObj = find(
            policyConditionUpdatedJSON(serviceData?.policyConditions),
            function (m) {
              if (m.name == data.type) {
                return m;
              }
            }
          );

          if (!isEmpty(conditionObj.uiHint)) {
            obj.conditions[data?.type] = JSON.parse(conditionObj.uiHint)
              .isMultiValue
              ? data?.values.map((m) => {
                  return { value: m, label: m };
                })
              : data?.values.toString();
          }
        }
      }

      return obj;
    });
  };

  const handleSubmit = async (values) => {
    let data = {};

    data.allowExceptions = getPolicyItemsVal(values, "allowExceptions");

    data.policyItems = getPolicyItemsVal(values, "policyItems");
    if (values?.isDenyAllElse) {
      data.denyPolicyItems = [];
      data.denyExceptions = [];
    } else {
      data.denyPolicyItems = getPolicyItemsVal(values, "denyPolicyItems");
      data.denyExceptions = getPolicyItemsVal(values, "denyExceptions");
    }
    data.dataMaskPolicyItems = getPolicyItemsVal(values, "dataMaskPolicyItems");
    data.rowFilterPolicyItems = getPolicyItemsVal(
      values,
      "rowFilterPolicyItems"
    );
    data.description = values.description ?? "";
    data.isAuditEnabled = values.isAuditEnabled ?? true;
    data.isDenyAllElse = values.isDenyAllElse;
    data.isEnabled = values.isEnabled ?? true;
    data.name = values.policyName;
    data.policyLabels = (values.policyLabel || [])?.map(({ value }) => value);
    data.policyPriority = values.policyPriority ? "1" : "0";
    data.policyType = values.policyType;
    data.service = serviceDetails.name;
    let serviceCompRes;
    if (values.policyType != null) {
      serviceCompRes = getResourcesDefVal(
        serviceCompDetails,
        values.policyType
      );
    }
    const grpResources = groupBy(serviceCompRes || [], "level");
    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    data.resources = {};
    if (!isMultiResources) {
      for (const level of grpResourcesKeys) {
        if (
          values[`resourceName-${level}`] &&
          values[`resourceName-${level}`].value !== noneOptions.value &&
          values[`value-${level}`] &&
          !isEmpty(values[`value-${level}`])
        ) {
          let defObj = serviceCompRes.find(function (m) {
            if (m.name == values[`resourceName-${level}`].name) {
              return m;
            }
          });
          data.resources[values[`resourceName-${level}`].name] = {
            values: isArray(values[`value-${level}`])
              ? values[`value-${level}`]?.map(({ value }) => value)
              : [values[`value-${level}`].value]
          };
          if (defObj?.excludesSupported) {
            data.resources[values[`resourceName-${level}`].name]["isExcludes"] =
              defObj.excludesSupported &&
              values[`isExcludesSupport-${level}`] == false;
          }
          if (defObj?.recursiveSupported) {
            data.resources[values[`resourceName-${level}`].name][
              "isRecursive"
            ] =
              defObj.recursiveSupported &&
              !(values[`isRecursiveSupport-${level}`] === false);
          }
        }
      }
    } else {
      data["additionalResources"] = [];
      map(values.additionalResources, (resourceValue) => {
        let additionalResourcesObj = {};
        for (const level of grpResourcesKeys) {
          if (
            resourceValue[`resourceName-${level}`] &&
            resourceValue[`resourceName-${level}`].value !==
              noneOptions.value &&
            resourceValue[`value-${level}`] &&
            !isEmpty(resourceValue[`value-${level}`])
          ) {
            let defObj = serviceCompRes.find(function (m) {
              if (m.name == resourceValue[`resourceName-${level}`].name) {
                return m;
              }
            });

            additionalResourcesObj[
              resourceValue[`resourceName-${level}`].name
            ] = {
              values: isArray(resourceValue[`value-${level}`])
                ? resourceValue[`value-${level}`]?.map(({ value }) => value)
                : [resourceValue[`value-${level}`].value]
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
        data["additionalResources"].push(additionalResourcesObj);
      });
    }
    if (data?.additionalResources?.length > 0) {
      data.additionalResources = reject(data.additionalResources, isEmpty);
      data.resources = data.additionalResources[0];
      data.additionalResources.shift();
    }
    if (isEmpty(data.resources)) {
      delete data.resources;
      if (policyId) {
        delete policyData.resources;
      }
    }
    if (values?.validitySchedules) {
      data["validitySchedules"] = [];

      values.validitySchedules.filter((val) => {
        if (val) {
          let timeObj = {};
          if (val.startTime) {
            timeObj["startTime"] = moment(val.startTime).format(
              "YYYY/MM/DD HH:mm:ss"
            );
          }
          if (val.endTime) {
            timeObj["endTime"] = moment(val.endTime).format(
              "YYYY/MM/DD HH:mm:ss"
            );
          }
          if (val.timeZone) {
            timeObj["timeZone"] = val.timeZone.id;
          }
          if (!isEmpty(timeObj)) {
            data["validitySchedules"].push(timeObj);
          }
        }
      });
    }

    /*Policy Condition*/
    if (values?.conditions) {
      data.conditions = [];
      Object.entries(values.conditions).map(([key, value]) => {
        return data.conditions.push({
          type: key,
          values: isArray(value) ? value : [value]
        });
      });
    } else {
      data["conditions"] = [];
    }

    /* For create zoen policy*/
    if (localStorage.getItem("zoneDetails") != null) {
      data["zoneName"] = JSON.parse(localStorage.getItem("zoneDetails")).label;
    }
    setPreventUnblock(true);
    if (policyId) {
      let dataVal = {
        ...policyData,
        ...data
      };
      try {
        setBlockUI(true);
        await fetchApi({
          url: `plugins/policies/${policyId}`,
          method: "PUT",
          data: dataVal
        });
        setBlockUI(false);
        toast.dismiss(toastId.current);
        toastId.current = toast.success("Policy updated successfully!!");
        navigate(`/service/${serviceId}/policies/${policyData.policyType}`);
      } catch (error) {
        setBlockUI(false);
        let errorMsg = `Failed to save policy form`;
        if (error?.response?.data?.msgDesc) {
          errorMsg = `Error! ${error.response.data.msgDesc}`;
        }
        toast.error(errorMsg);
        console.error(`Error while saving policy form! ${error}`);
      }
    } else {
      try {
        setBlockUI(true);
        await fetchApi({
          url: "plugins/policies",
          method: "POST",
          data
        });
        let tblpageData = {};
        if (state && state != null) {
          tblpageData = state.tblpageData;
          if (state.tblpageData.pageRecords % state.tblpageData.pageSize == 0) {
            tblpageData["totalPage"] = state.tblpageData.totalPage + 1;
          } else {
            if (tblpageData !== undefined) {
              tblpageData["totalPage"] = state.tblpageData.totalPage;
            }
          }
        }
        setBlockUI(false);
        toast.dismiss(toastId.current);
        toastId.current = toast.success("Policy save successfully!!");
        navigate(`/service/${serviceId}/policies/${policyType}`, {
          state: {
            showLastPage: true,
            addPageData: tblpageData
          }
        });
      } catch (error) {
        setBlockUI(false);
        let errorMsg = `Failed to save policy form`;
        if (error?.response?.data?.msgDesc) {
          errorMsg = `Error! ${error.response.data.msgDesc}`;
        }
        toast.error(errorMsg);
        console.error(`Error while saving policy form! ${error}`);
      }
    }
  };

  const handleDeleteClick = async (policyID, serviceId) => {
    hideDeleteModal();
    let policyType = policyId ? policyData.policyType : policyType;
    try {
      setBlockUI(true);
      await fetchApi({
        url: `plugins/policies/${policyID}`,
        method: "DELETE"
      });
      setPreventUnblock(true);
      setBlockUI(false);
      toast.dismiss(toastId.current);
      toastId.current = toast.success(" Success! Policy deleted successfully");
      navigate(`/service/${serviceId}/policies/${policyType}`);
    } catch (error) {
      setBlockUI(false);
      let errorMsg = `Error occurred during deleting policy`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
      console.error(error);
    }
  };

  const closeForm = () => {
    let polType = policyId ? policyData.policyType : policyType;
    navigate(`/service/${serviceId}/policies/${polType}`);
  };

  const CustomToggle = ({ children, eventKey, callback }) => {
    const currentEventKey = useContext(AccordionContext);

    const decoratedOnClick = useAccordionButton(
      eventKey,
      () => callback && callback(eventKey)
    );
    const isCurrentEventKey = currentEventKey?.activeEventKey === eventKey;

    return (
      <a
        className="float-end text-decoration-none"
        role="button"
        onClick={decoratedOnClick}
      >
        {!isCurrentEventKey ? (
          <span className="wrap-expand">
            show <i className="fa-fw fa fa-caret-down"></i>
          </span>
        ) : (
          <span className="wrap-collapse text-decoration-none">
            hide <i className="fa-fw fa fa-caret-up"></i>
          </span>
        )}
      </a>
    );
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

  const resourceErrorCheck = (errors, values) => {
    let serviceCompResourcesDetails = getResourcesDefVal(
      serviceCompDetails,
      values.policyType
    );
    const grpResources = groupBy(serviceCompResourcesDetails || [], "level");
    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    for (const key of grpResourcesKeys) {
      if (isMultiResources) {
        if (errors?.additionalResources?.length > 0) {
          return true;
        }
      } else {
        if (errors[`value-${key}`] !== undefined) {
          return true;
        }
      }
    }
  };

  const policyBreadcrumb = () => {
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

  return (
    <>
      {loader ? (
        <Loader />
      ) : (
        <div>
          <div className="header-wraper">
            <h3 className="wrap-header bold">{`${
              policyId ? "Edit" : "Create"
            } Policy`}</h3>
            {policyBreadcrumb()}
          </div>

          <div className="wrap">
            <Form
              onSubmit={handleSubmit}
              mutators={{
                ...arrayMutators
              }}
              initialValues={formData}
              validate={(values) => {
                const errors = {};
                if (!values.policyName) {
                  errors.policyName = {
                    required: true,
                    text: "Required"
                  };
                }

                return errors;
              }}
              render={({
                handleSubmit,
                values,
                invalid,
                errors,
                dirty,
                form: {
                  mutators: { push: addPolicyItem }
                },
                dirtyFields,
                modified,
                initialValues
              }) => (
                <>
                  <PromptDialog
                    isDirtyField={
                      dirty == true || !isEqual(initialValues, values)
                        ? isDirtyFieldCheck(
                            dirtyFields,
                            modified,
                            values,
                            initialValues
                          )
                        : false
                    }
                    isUnblock={preventUnBlock}
                  />
                  <form
                    onSubmit={(event) => {
                      if (invalid) {
                        forIn(errors, function (value, key) {
                          if (
                            has(errors, "policyName") ||
                            resourceErrorCheck(errors, values)
                          ) {
                            let selector =
                              document.getElementById("isError") ||
                              document.getElementById(key) ||
                              document.querySelector(`input[name=${key}]`) ||
                              document.querySelector(`input[id=${key}]`) ||
                              document.querySelector(
                                `span[className="invalid-field"]`
                              );
                            scrollToError(selector);
                          } else {
                            getValidatePolicyItems(errors?.[key]);
                          }
                        });
                      }
                      handleSubmit(event);
                    }}
                  >
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
                          {policyInfo(
                            values.policyType,
                            serviceCompDetails.name
                          )}
                        </Alert>
                      )}
                    {policyId &&
                      !isEmpty(policyData.validitySchedules) &&
                      isPolicyExpired(policyData) &&
                      showPolicyExpire && (
                        <Alert
                          variant="danger"
                          onClose={() => setShowPolicyExpire(false)}
                          dismissible
                        >
                          <i className="fa fa-clock-o fa-fw  fa-lg time-clock"></i>
                          <p className="pd-l-6 d-inline"> Policy Expired</p>
                        </Alert>
                      )}
                    <fieldset>
                      <p className="formHeader">Policy Details : </p>
                    </fieldset>
                    <Row className="user-role-grp-form">
                      <Col md={8}>
                        <FormB.Group as={Row} className="mb-3">
                          <Field
                            className="form-control"
                            name="policyType"
                            render={({ input }) => (
                              <>
                                <FormB.Label column sm={3}>
                                  <span className="float-end fnt-14">
                                    Policy Type
                                  </span>
                                </FormB.Label>
                                <Col sm={5}>
                                  <h6 className="d-inline me-1">
                                    <Badge
                                      bg="primary"
                                      style={{ verticalAlign: "sub" }}
                                    >
                                      {
                                        getEnumElementByValue(
                                          RangerPolicyType,
                                          +input.value
                                        )?.label
                                      }
                                    </Badge>
                                  </h6>
                                </Col>
                              </>
                            )}
                          />
                        </FormB.Group>
                        {policyId && (
                          <FormB.Group as={Row} className="mb-3">
                            <FormB.Label column sm={3}>
                              <span className="float-end fnt-14">
                                Policy ID*
                              </span>
                            </FormB.Label>
                            <Col sm={5}>
                              <h6 className="d-inline me-1">
                                <span style={{ verticalAlign: "sub" }}>
                                  <Badge bg="primary">{policyData.id}</Badge>
                                </span>
                              </h6>
                            </Col>
                          </FormB.Group>
                        )}
                        <FormB.Group as={Row} className="mb-3">
                          <Field
                            className="form-control"
                            name="policyName"
                            render={({ input, meta }) => (
                              <>
                                <FormB.Label column sm={3}>
                                  <span className="float-end fnt-14">
                                    Policy Name*
                                  </span>
                                </FormB.Label>
                                <>
                                  <Col sm={5} className={"position-relative"}>
                                    <FormB.Control
                                      {...input}
                                      placeholder="Policy Name"
                                      id={
                                        meta.error && meta.touched
                                          ? "isError"
                                          : "name"
                                      }
                                      className={
                                        meta.error && meta.touched
                                          ? "form-control border-danger"
                                          : "form-control"
                                      }
                                      data-cy="policyName"
                                    />
                                    <InfoIcon
                                      css="input-box-info-icon"
                                      position="right"
                                      message={
                                        <p
                                          className="pd-10"
                                          style={{ fontSize: "small" }}
                                        >
                                          {
                                            RegexMessage.MESSAGE
                                              .policyNameInfoIconMessage
                                          }
                                        </p>
                                      }
                                    />
                                    {meta.touched && meta.error && (
                                      <span className="invalid-field">
                                        {meta.error.text}
                                      </span>
                                    )}
                                  </Col>
                                </>
                              </>
                            )}
                          />
                          <Col sm={4}>
                            <Row>
                              <Col sm={5}>
                                <Field
                                  className="form-control"
                                  name="isEnabled"
                                  render={({ input }) => (
                                    <BootstrapSwitchButton
                                      {...input}
                                      className="abcd"
                                      checked={!(input.value === false)}
                                      onlabel="Enabled"
                                      onstyle="primary"
                                      offlabel="Disabled"
                                      offstyle="outline-secondary"
                                      style="w-100"
                                      size="xs"
                                      key="isEnabled"
                                    />
                                  )}
                                />
                              </Col>
                              <Col sm={5}>
                                <Field
                                  className="form-control"
                                  name="policyPriority"
                                  render={({ input }) => (
                                    <BootstrapSwitchButton
                                      {...input}
                                      checked={input.value}
                                      onlabel="Override"
                                      onstyle="primary"
                                      offlabel="Normal"
                                      offstyle="outline-secondary"
                                      style="w-100"
                                      size="xs"
                                      key="policyPriority"
                                    />
                                  )}
                                />
                              </Col>
                            </Row>
                          </Col>
                        </FormB.Group>
                        <Field
                          className="form-control"
                          name="policyLabel"
                          render={({ input }) => (
                            <FormB.Group as={Row} className="mb-3">
                              <FormB.Label column sm={3}>
                                <span className="float-end fnt-14">
                                  Policy Label
                                </span>
                              </FormB.Label>
                              <Col sm={5}>
                                <AsyncCreatableSelect
                                  {...input}
                                  isMulti
                                  loadOptions={fetchPolicyLabel}
                                  onFocus={() => {
                                    onFocusPolicyLabel();
                                  }}
                                  defaultOptions={defaultPolicyLabelOptions}
                                  styles={selectInputCustomStyles}
                                />
                              </Col>
                            </FormB.Group>
                          )}
                        />
                        {!isMultiResources && (
                          <ResourceComp
                            serviceDetails={serviceDetails}
                            serviceCompDetails={serviceCompDetails}
                            formValues={values}
                            policyType={
                              policyId ? policyData.policyType : policyType
                            }
                            policyId={policyId}
                          />
                        )}
                        <Field
                          className="form-control"
                          name="description"
                          render={({ input }) => (
                            <FormB.Group as={Row} className="mb-3">
                              <FormB.Label column sm={3}>
                                <span className="float-end fnt-14">
                                  Description
                                </span>
                              </FormB.Label>
                              <Col sm={5}>
                                <FormB.Control
                                  {...input}
                                  as="textarea"
                                  rows={3}
                                  data-cy="description"
                                />
                              </Col>
                            </FormB.Group>
                          )}
                        />
                        <Field
                          className="form-control"
                          name="isAuditEnabled"
                          render={({ input }) => (
                            <FormB.Group as={Row} className="mb-3">
                              <FormB.Label column sm={3}>
                                <span className="float-end fnt-14">
                                  Audit Logging*
                                </span>
                              </FormB.Label>
                              <Col sm={5}>
                                <span style={{ verticalAlign: "sub" }}>
                                  <BootstrapSwitchButton
                                    {...input}
                                    checked={!(input.value === false)}
                                    onlabel="Yes"
                                    onstyle="primary"
                                    offlabel="No"
                                    offstyle="outline-secondary"
                                    size="xs"
                                    key="isAuditEnabled"
                                  />
                                </span>
                              </Col>
                            </FormB.Group>
                          )}
                        />
                      </Col>
                      {/* -------------------------------------------------------------- */}
                      <Col md={4}>
                        <div className="mb-4">
                          <PolicyValidityPeriodComp
                            addPolicyItem={addPolicyItem}
                          />
                        </div>
                        <br />
                        {serviceCompDetails?.policyConditions?.length > 0 && (
                          <div className="table-responsive">
                            <table className="table table-bordered condition-group-table">
                              <thead>
                                <tr>
                                  <th colSpan="2">
                                    Policy Conditions :
                                    {showModal && (
                                      <Field
                                        className="form-control"
                                        name="conditions"
                                        render={({ input }) => (
                                          <PolicyConditionsComp
                                            policyConditionDetails={policyConditionUpdatedJSON(
                                              serviceCompDetails.policyConditions
                                            )}
                                            inputVal={input}
                                            showModal={showModal}
                                            handleCloseModal={
                                              policyConditionState
                                            }
                                          />
                                        )}
                                      />
                                    )}
                                    <Button
                                      className="float-end btn btn-mini"
                                      onClick={() => {
                                        policyConditionState(true);
                                      }}
                                      data-js="customPolicyConditions"
                                      data-cy="customPolicyConditions"
                                    >
                                      <i className="fa-fw fa fa-plus"></i>
                                    </Button>
                                  </th>
                                </tr>
                              </thead>
                              <tbody data-id="conditionData">
                                <>
                                  {values?.conditions &&
                                  !isEmpty(values.conditions) ? (
                                    Object.keys(values.conditions).map(
                                      (keyName) => {
                                        if (
                                          values.conditions[keyName] != "" &&
                                          values.conditions[keyName] != null
                                        ) {
                                          let conditionObj = find(
                                            serviceCompDetails?.policyConditions,
                                            function (m) {
                                              if (m.name == keyName) {
                                                return m;
                                              }
                                            }
                                          );
                                          return (
                                            <tr key={keyName}>
                                              <td>
                                                <center>
                                                  {conditionObj.label}
                                                </center>
                                              </td>
                                              <td>
                                                {isArray(
                                                  values?.conditions[keyName]
                                                ) ? (
                                                  <center>
                                                    {values.conditions[
                                                      keyName
                                                    ].join(", ")}
                                                  </center>
                                                ) : (
                                                  <center>
                                                    {values.conditions[keyName]}
                                                  </center>
                                                )}
                                              </td>
                                            </tr>
                                          );
                                        }
                                      }
                                    )
                                  ) : (
                                    <tr>
                                      <td>
                                        <center> No Conditions </center>
                                      </td>
                                    </tr>
                                  )}
                                </>
                              </tbody>
                            </table>
                          </div>
                        )}
                      </Col>
                    </Row>
                    {/* --------------------- Multiple Resources ---------------------------- */}
                    {isMultiResources && (
                      <>
                        <fieldset>
                          <p className="formHeader">Resources :</p>
                        </fieldset>
                        <>
                          <FieldArray name="additionalResources">
                            {({ fields }) =>
                              fields.map((name, index) => (
                                <Row
                                  className="resource-block"
                                  key={`${name}-${index}`}
                                >
                                  <Col md={8}>
                                    <ResourceComp
                                      serviceDetails={serviceDetails}
                                      serviceCompDetails={serviceCompDetails}
                                      formValues={
                                        values.additionalResources[index]
                                      }
                                      policyType={
                                        policyId
                                          ? policyData.policyType
                                          : policyType
                                      }
                                      name={name}
                                      isMultiResources={isMultiResources}
                                    />
                                  </Col>
                                  {values.additionalResources.length > 1 && (
                                    <Col md={4}>
                                      <Button
                                        variant="danger"
                                        size="sm"
                                        title="Remove"
                                        onClick={() => fields.remove(index)}
                                        data-action="delete"
                                        data-cy="delete"
                                      >
                                        <i className="fa-fw fa fa-remove"></i>
                                      </Button>
                                    </Col>
                                  )}
                                </Row>
                              ))
                            }
                          </FieldArray>
                          <div className="wrap">
                            <Button
                              type="button"
                              className="btn-mini"
                              onClick={() =>
                                addPolicyItem("additionalResources", {})
                              }
                              data-action="addTime"
                              data-cy="addTime"
                            >
                              <i className="fa-fw fa fa-plus"></i> Add Resource
                            </Button>
                          </div>
                        </>
                      </>
                    )}
                    {/*----------------------- Policy Item --------------------------- */}
                    {values.policyType == 0 ? (
                      <div>
                        <div>
                          <Accordion defaultActiveKey="0">
                            <>
                              <p className="formHeader">
                                Allow Conditions:{" "}
                                <CustomToggle eventKey="0"></CustomToggle>
                              </p>
                              <Accordion.Collapse eventKey="0">
                                <>
                                  <div className="wrap">
                                    <PolicyPermissionItem
                                      serviceDetails={serviceDetails}
                                      serviceCompDetails={serviceCompDetails}
                                      formValues={values}
                                      addPolicyItem={addPolicyItem}
                                      attrName="policyItems"
                                      fetchUsersData={fetchUsersData}
                                      fetchGroupsData={fetchGroupsData}
                                      fetchRolesData={fetchRolesData}
                                      changePolicyItemPermissions={
                                        changePolicyItemPermissions
                                      }
                                      isMultiResources={isMultiResources}
                                    />
                                  </div>
                                  {serviceCompDetails?.options
                                    ?.enableDenyAndExceptionsInPolicies ==
                                    "true" && (
                                    <>
                                      <fieldset>
                                        <p className="wrap-header search-header">
                                          <i className="fa-fw fa fa-exclamation-triangle fa-fw fa fa-1 text-color-red"></i>
                                          Exclude from Allow Conditions:
                                        </p>
                                      </fieldset>
                                      <div className="wrap">
                                        <PolicyPermissionItem
                                          serviceDetails={serviceDetails}
                                          serviceCompDetails={
                                            serviceCompDetails
                                          }
                                          formValues={values}
                                          addPolicyItem={addPolicyItem}
                                          attrName="allowExceptions"
                                          fetchUsersData={fetchUsersData}
                                          fetchGroupsData={fetchGroupsData}
                                          fetchRolesData={fetchRolesData}
                                          changePolicyItemPermissions={
                                            changePolicyItemPermissions
                                          }
                                          isMultiResources={isMultiResources}
                                        />
                                      </div>
                                    </>
                                  )}
                                </>
                              </Accordion.Collapse>
                            </>
                          </Accordion>
                        </div>
                        {serviceCompDetails?.options
                          ?.enableDenyAndExceptionsInPolicies == "true" && (
                          <>
                            <Field
                              className="form-control"
                              name="isDenyAllElse"
                              render={({ input }) => (
                                <FormB.Group
                                  as={Row}
                                  className="mb-3"
                                  controlId="isDenyAllElse"
                                >
                                  <FormB.Label column sm={2}>
                                    Deny All Other Accesses: *
                                  </FormB.Label>
                                  <Col sm={1}>
                                    <span style={{ verticalAlign: "sub" }}>
                                      <BootstrapSwitchButton
                                        {...input}
                                        checked={input.value}
                                        onlabel="True"
                                        onstyle="primary"
                                        offlabel="False"
                                        offstyle="outline-secondary"
                                        size="xs"
                                        style="w-100"
                                        key="isDenyAllElse"
                                      />
                                    </span>
                                  </Col>
                                </FormB.Group>
                              )}
                            />
                            <Condition when="isDenyAllElse" is={false}>
                              <div>
                                <Accordion defaultActiveKey="0">
                                  <>
                                    <p className="formHeader">
                                      Deny Conditions:
                                      <CustomToggle eventKey="0"></CustomToggle>
                                    </p>
                                    <Accordion.Collapse eventKey="0">
                                      <>
                                        <div className="wrap">
                                          <PolicyPermissionItem
                                            serviceDetails={serviceDetails}
                                            serviceCompDetails={
                                              serviceCompDetails
                                            }
                                            formValues={values}
                                            addPolicyItem={addPolicyItem}
                                            attrName="denyPolicyItems"
                                            fetchUsersData={fetchUsersData}
                                            fetchGroupsData={fetchGroupsData}
                                            fetchRolesData={fetchRolesData}
                                            changePolicyItemPermissions={
                                              changePolicyItemPermissions
                                            }
                                            isMultiResources={isMultiResources}
                                          />
                                        </div>
                                        <fieldset>
                                          <p className="wrap-header search-header">
                                            <i className="fa-fw fa fa-exclamation-triangle fa-fw fa fa-1 text-color-red"></i>
                                            Exclude from Deny Conditions:
                                          </p>
                                        </fieldset>
                                        <div className="wrap">
                                          <PolicyPermissionItem
                                            serviceDetails={serviceDetails}
                                            serviceCompDetails={
                                              serviceCompDetails
                                            }
                                            formValues={values}
                                            addPolicyItem={addPolicyItem}
                                            attrName="denyExceptions"
                                            fetchUsersData={fetchUsersData}
                                            fetchGroupsData={fetchGroupsData}
                                            fetchRolesData={fetchRolesData}
                                            changePolicyItemPermissions={
                                              changePolicyItemPermissions
                                            }
                                            isMultiResources={isMultiResources}
                                          />
                                        </div>
                                      </>
                                    </Accordion.Collapse>
                                  </>
                                </Accordion>
                              </div>
                            </Condition>
                          </>
                        )}
                      </div>
                    ) : values.policyType == 1 ? (
                      <div>
                        <Accordion defaultActiveKey="0">
                          <>
                            <p className="formHeader">
                              Mask Conditions:
                              <CustomToggle eventKey="0"></CustomToggle>
                            </p>
                            <Accordion.Collapse eventKey="0">
                              <>
                                <div className="wrap">
                                  <PolicyPermissionItem
                                    serviceDetails={serviceDetails}
                                    serviceCompDetails={serviceCompDetails}
                                    formValues={values}
                                    addPolicyItem={addPolicyItem}
                                    attrName="dataMaskPolicyItems"
                                    fetchUsersData={fetchUsersData}
                                    fetchGroupsData={fetchGroupsData}
                                    fetchRolesData={fetchRolesData}
                                    changePolicyItemPermissions={
                                      changePolicyItemPermissions
                                    }
                                    isMultiResources={isMultiResources}
                                  />
                                </div>
                              </>
                            </Accordion.Collapse>
                          </>
                        </Accordion>
                      </div>
                    ) : (
                      <div>
                        <div>
                          <Accordion defaultActiveKey="0">
                            <>
                              <p className="wrap-header search-header">
                                Row Filter Conditions:
                                <CustomToggle eventKey="0"></CustomToggle>
                              </p>
                              <Accordion.Collapse eventKey="0">
                                <>
                                  <div className="wrap">
                                    <PolicyPermissionItem
                                      serviceDetails={serviceDetails}
                                      serviceCompDetails={serviceCompDetails}
                                      formValues={values}
                                      addPolicyItem={addPolicyItem}
                                      attrName="rowFilterPolicyItems"
                                      fetchUsersData={fetchUsersData}
                                      fetchGroupsData={fetchGroupsData}
                                      fetchRolesData={fetchRolesData}
                                      changePolicyItemPermissions={
                                        changePolicyItemPermissions
                                      }
                                      isMultiResources={isMultiResources}
                                    />
                                  </div>
                                </>
                              </Accordion.Collapse>
                            </>
                          </Accordion>
                        </div>
                      </div>
                    )}
                    <div className="row form-actions">
                      <div className="col-md-9 offset-md-3">
                        <Button
                          onClick={(event) => {
                            if (invalid) {
                              forIn(errors, function (value, key) {
                                if (
                                  has(errors, "policyName") ||
                                  resourceErrorCheck(errors, values)
                                ) {
                                  let selector =
                                    document?.getElementById("isError") ||
                                    document?.getElementById(key) ||
                                    document?.querySelector(
                                      `input[name=${key}]`
                                    ) ||
                                    document?.querySelector(
                                      `input[id=${key}]`
                                    ) ||
                                    document?.querySelector(
                                      `span[className="invalid-field"]`
                                    );
                                  scrollToError(selector);
                                } else {
                                  getValidatePolicyItems(errors?.[key]);
                                }
                              });
                            }
                            handleSubmit(event);
                          }}
                          variant="primary"
                          size="sm"
                          data-id="save"
                          data-cy="save"
                        >
                          Save
                        </Button>
                        <Button
                          variant="secondary"
                          type="button"
                          size="sm"
                          onClick={() => {
                            closeForm();
                          }}
                          data-id="cancel"
                          data-cy="cancel"
                        >
                          Cancel
                        </Button>

                        {policyId !== undefined && (
                          <Button
                            variant="danger"
                            type="button"
                            className="btn-sm"
                            size="sm"
                            onClick={() => {
                              showDeleteModal();
                            }}
                          >
                            Delete
                          </Button>
                        )}
                      </div>
                      {policyId !== undefined && (
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
                              onClick={() =>
                                handleDeleteClick(policyId, serviceId)
                              }
                            >
                              Yes
                            </Button>
                          </Modal.Footer>
                        </Modal>
                      )}
                    </div>
                  </form>
                </>
              )}
            />
            <BlockUi isUiBlock={blockUI} />
          </div>
        </div>
      )}
    </>
  );
}
