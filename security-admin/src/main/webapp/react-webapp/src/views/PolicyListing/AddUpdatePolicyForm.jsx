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
  isObject,
  isArray,
  isEqual,
  forIn,
  has,
  maxBy
} from "lodash";
import { toast } from "react-toastify";
import { Loader, scrollToError } from "Components/CommonComponents";
import { fetchApi } from "Utils/fetchAPI";
import { RangerPolicyType, getEnumElementByValue } from "Utils/XAEnums";
import ResourceComp from "../Resources/ResourceComp";
import PolicyPermissionItem from "../PolicyListing/PolicyPermissionItem";
import { useParams, useNavigate, useLocation } from "react-router-dom";
import PolicyValidityPeriodComp from "./PolicyValidityPeriodComp";
import PolicyConditionsComp from "./PolicyConditionsComp";
import { getAllTimeZoneList } from "Utils/XAUtils";
import moment from "moment";
import {
  InfoIcon,
  commonBreadcrumb,
  isPolicyExpired
} from "../../utils/XAUtils";
import { useAccordionToggle } from "react-bootstrap/AccordionToggle";
import AccordionContext from "react-bootstrap/AccordionContext";
import usePrompt from "Hooks/usePrompt";
import { RegexMessage } from "../../utils/XAMessages";
import { policyInfo } from "Utils/XAUtils";
import { BlockUi } from "../../components/CommonComponents";
import { getServiceDef } from "../../utils/appState";

const noneOptions = {
  label: "None",
  value: "none"
};

const initialState = {
  loader: true,
  serviceDetails: null,
  serviceCompDetails: null,
  policyData: null,
  formData: {}
};

const PromtDialog = (props) => {
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
        policyData: action.policyData,
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

export default function AddUpdatePolicyForm(props) {
  let { serviceId, policyType, policyId } = useParams();
  const navigate = useNavigate();
  const { state } = useLocation();
  const serviceDefs = getServiceDef();
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

  useEffect(() => {
    fetchInitalData();
  }, []);

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
    let params = { roleNamePartial: inputValue || "", isVisible: 1 };
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

  const fetchInitalData = async () => {
    let serviceData = await fetchServiceDetails();

    let serviceCompData = serviceDefs?.allServiceDefs?.find((servicedef) => {
      return servicedef.name == serviceData.type;
    });
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
      console.error(`Error occurred while fetching service details ! ${error}`);
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
    const policyLabalResp = await fetchApi({
      url: "plugins/policyLabels",
      params: params
    });

    return policyLabalResp.data.map((name) => ({
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
            policyData.policyItems,
            serviceCompData.accessTypes,
            null,
            serviceCompData.name
          )
        : [{}];
    data.allowExceptions =
      policyId && policyData?.allowExceptions?.length > 0
        ? setPolicyItemVal(
            policyData.allowExceptions,
            serviceCompData.accessTypes,
            null,
            serviceCompData.name
          )
        : [{}];
    data.denyPolicyItems =
      policyId && policyData?.denyPolicyItems?.length > 0
        ? setPolicyItemVal(
            policyData.denyPolicyItems,
            serviceCompData.accessTypes,
            null,
            serviceCompData.name
          )
        : [{}];
    data.denyExceptions =
      policyId && policyData?.denyExceptions?.length > 0
        ? setPolicyItemVal(
            policyData.denyExceptions,
            serviceCompData.accessTypes,
            null,
            serviceCompData.name
          )
        : [{}];
    data.dataMaskPolicyItems =
      policyId && policyData?.dataMaskPolicyItems?.length > 0
        ? setPolicyItemVal(
            policyData.dataMaskPolicyItems,
            serviceCompData.dataMaskDef.accessTypes,
            serviceCompData.dataMaskDef.maskTypes,
            serviceCompData.name
          )
        : [{}];
    data.rowFilterPolicyItems =
      policyId && policyData?.rowFilterPolicyItems?.length > 0
        ? setPolicyItemVal(
            policyData.rowFilterPolicyItems,
            serviceCompData.rowFilterDef.accessTypes,
            null,
            serviceCompData.name
          )
        : [{}];
    if (policyId) {
      data.policyName = policyData.name;
      data.isEnabled = policyData.isEnabled;
      data.policyPriority = policyData.policyPriority == 0 ? false : true;
      data.description = policyData.description;
      data.isAuditEnabled = policyData.isAuditEnabled;
      data.policyLabel =
        policyData &&
        policyData?.policyLabels?.map((val) => {
          return { label: val, value: val };
        });
      let serviceCompResourcesDetails;
      if (
        RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value ==
        policyData.policyType
      ) {
        serviceCompResourcesDetails = serviceCompData.dataMaskDef.resources;
      } else if (
        RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value ==
        policyData.policyType
      ) {
        serviceCompResourcesDetails = serviceCompData.rowFilterDef.resources;
      } else {
        serviceCompResourcesDetails = serviceCompData.resources;
      }
      if (policyData.resources) {
        let lastResourceLevel = [];
        Object.entries(policyData.resources).map(([key, value]) => {
          let setResources = find(serviceCompResourcesDetails, ["name", key]);
          data[`resourceName-${setResources.level}`] = setResources;
          data[`value-${setResources.level}`] = value.values.map((m) => {
            return { label: m, value: m };
          });
          if (setResources.excludesSupported) {
            data[`isExcludesSupport-${setResources.level}`] =
              value.isExcludes == false;
          }
          if (setResources.recursiveSupported) {
            data[`isRecursiveSupport-${setResources.level}`] =
              value.isRecursive;
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
        if (setLastResources) {
          data[`resourceName-${setLastResources.level}`] = {
            label: "None",
            value: "none"
          };
        }
      }
      if (policyData.validitySchedules) {
        data["validitySchedules"] = [];
        policyData.validitySchedules.filter((val) => {
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
          data.conditions[val?.type] = val?.values?.join(",");
        }
      }
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

        if (
          key?.conditions &&
          isObject(key.conditions) &&
          serviceCompDetails.name == "tag"
        ) {
          obj.conditions = [];
          Object.entries(key.conditions).map(([key, value]) => {
            if (!isEmpty(value)) {
              return obj.conditions.push({
                type: key,
                values: value?.split(", ")
              });
            }
          });
        } else if (
          !isEmpty(key?.conditions) &&
          isObject(key.conditions) &&
          serviceCompDetails.name == "knox"
        ) {
          obj.conditions = [
            {
              type: "ip-range",
              values:
                !isEmpty(Object.keys(key.conditions)) &&
                !isArray(key.conditions)
                  ? key.conditions["ip-range"]?.split(", ")
                  : key.conditions.map((value) => {
                      return value.value;
                    })
            }
          ];
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

  const setPolicyItemVal = (formData, accessTypes, maskTypes, serviceType) => {
    return formData.map((val) => {
      let obj = {},
        accessTypesObj = [];

      if (val.hasOwnProperty("delegateAdmin")) {
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
          let tagval = m;
          return tagval.substr(0, tagval.indexOf(":"));
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
      if (
        val.hasOwnProperty("rowFilterInfo") &&
        val.rowFilterInfo &&
        val.rowFilterInfo.filterExpr
      ) {
        obj.rowFilterInfo = val.rowFilterInfo.filterExpr;
      }
      if (
        val.hasOwnProperty("dataMaskInfo") &&
        val.dataMaskInfo &&
        val.dataMaskInfo.dataMaskType
      ) {
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
      /* Policy Condition*/
      if (val?.conditions?.length > 0) {
        obj.conditions = {};
        for (let data of val.conditions) {
          obj.conditions[data.type] = data.values.join(", ");
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
    data.description = values.description;
    data.isAuditEnabled = values.isAuditEnabled;
    data.isDenyAllElse = values.isDenyAllElse;
    data.isEnabled = values.isEnabled;
    data.name = values.policyName;
    data.policyLabels = (values.policyLabel || [])?.map(({ value }) => value);
    data.policyPriority = values.policyPriority ? "1" : "0";
    data.policyType = values.policyType;
    data.service = serviceDetails.name;
    let serviceCompRes;
    if (values.policyType != null) {
      if (
        values.policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value
      )
        serviceCompRes = serviceCompDetails.dataMaskDef.resources;
      if (
        values.policyType ==
        RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value
      )
        serviceCompRes = serviceCompDetails.rowFilterDef.resources;
      if (values.policyType == RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value)
        serviceCompRes = serviceCompDetails.resources;
    }
    const grpResources = groupBy(serviceCompRes || [], "level");
    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    data.resources = {};
    for (const level of grpResourcesKeys) {
      if (
        values[`resourceName-${level}`] &&
        values[`resourceName-${level}`].value !== noneOptions.value
      ) {
        let defObj = serviceCompRes.find(function (m) {
          if (m.name == values[`resourceName-${level}`].name) {
            return m;
          }
        });
        data.resources[values[`resourceName-${level}`].name] = {
          isExcludes:
            defObj.excludesSupported &&
            values[`isExcludesSupport-${level}`] == false,
          isRecursive:
            defObj.recursiveSupported &&
            !(values[`isRecursiveSupport-${level}`] === false),
          values: values[`value-${level}`]?.map(({ value }) => value)
        };
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
      Object.entries(values.conditions).map(([key, value]) => {
        data.conditions = [];
        return data.conditions.push({
          type: key,
          values: value?.split(",")
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
        const resp = await fetchApi({
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
        const resp = await fetchApi({
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

    const decoratedOnClick = useAccordionToggle(
      eventKey,
      () => callback && callback(eventKey)
    );
    const isCurrentEventKey = currentEventKey === eventKey;

    return (
      <a className="pull-right" role="button" onClick={decoratedOnClick}>
        {!isCurrentEventKey ? (
          <span className="wrap-expand">
            show <i className="fa-fw fa fa-caret-down"></i>
          </span>
        ) : (
          <span className="wrap-collapse">
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
    let serviceCompResourcesDetails;
    if (
      RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value == values.policyType
    ) {
      serviceCompResourcesDetails = serviceCompDetails.dataMaskDef.resources;
    } else if (
      RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value == values.policyType
    ) {
      serviceCompResourcesDetails = serviceCompDetails.rowFilterDef.resources;
    } else {
      serviceCompResourcesDetails = serviceCompDetails.resources;
    }

    const grpResources = groupBy(serviceCompResourcesDetails || [], "level");
    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    for (const key of grpResourcesKeys) {
      if (errors[`value-${key}`] !== undefined) {
        return true;
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
                submitting,
                values,
                invalid,
                errors,
                dirty,
                form: {
                  mutators: { push: addPolicyItem, pop: removePolicyItem, move }
                },
                form,
                dirtyFields,
                modified,
                initialValues
              }) => (
                <>
                  <PromtDialog
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
                      <p className="formHeader">Policy Details</p>
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
                                  <span className="pull-right fnt-14">
                                    Policy Type
                                  </span>
                                </FormB.Label>
                                <Col sm={5}>
                                  <h6 className="d-inline mr-1">
                                    <Badge
                                      variant="primary"
                                      style={{ verticalAlign: "sub" }}
                                    >
                                      {
                                        getEnumElementByValue(
                                          RangerPolicyType,
                                          +input.value
                                        ).label
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
                              <span className="pull-right fnt-14">
                                Policy ID*
                              </span>
                            </FormB.Label>
                            <Col sm={5}>
                              <h6 className="d-inline mr-1">
                                <span style={{ verticalAlign: "sub" }}>
                                  <Badge variant="primary">
                                    {policyData.id}
                                  </Badge>
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
                                  <span className="pull-right fnt-14">
                                    Policy Name*
                                  </span>
                                </FormB.Label>
                                <>
                                  <Col sm={5}>
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
                                      css="info-user-role-grp-icon"
                                      position="right"
                                      message={
                                        <p
                                          className="pd-10"
                                          style={{ fontSize: "small" }}
                                        >
                                          {
                                            RegexMessage.MESSAGE
                                              .policynameinfoiconmessage
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
                                <span className="pull-right fnt-14">
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
                                />
                              </Col>
                            </FormB.Group>
                          )}
                        />
                        <ResourceComp
                          serviceDetails={serviceDetails}
                          serviceCompDetails={serviceCompDetails}
                          formValues={values}
                          policyType={
                            policyId ? policyData.policyType : policyType
                          }
                          policyItem={true}
                          policyId={policyId}
                        />
                        <Field
                          className="form-control"
                          name="description"
                          render={({ input }) => (
                            <FormB.Group as={Row} className="mb-3">
                              <FormB.Label column sm={3}>
                                <span className="pull-right fnt-14">
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
                                <span className="pull-right fnt-14">
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
                                            policyConditionDetails={
                                              serviceCompDetails.policyConditions
                                            }
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
                                      className="pull-right btn btn-mini"
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
                                      (keyName, keyIndex) => {
                                        return (
                                          <tr>
                                            <>
                                              <td>
                                                <center> {keyName} </center>
                                              </td>
                                              <td>
                                                {isObject(
                                                  values.conditions[keyName]
                                                ) ? (
                                                  <center>
                                                    {values.conditions[keyName]
                                                      .length > 1
                                                      ? values.conditions[
                                                          keyName
                                                        ].map((m) => {
                                                          return ` ${m.label} `;
                                                        })
                                                      : values.conditions[
                                                          keyName
                                                        ].label}
                                                  </center>
                                                ) : (
                                                  <center>
                                                    {values.conditions[keyName]}
                                                  </center>
                                                )}
                                              </td>
                                            </>
                                          </tr>
                                        );
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
                    {/* ------------------------------------------------- */}
                    {values.policyType == 0 ? (
                      <div>
                        <div>
                          <Accordion defaultActiveKey="0">
                            <>
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
                                          />
                                        </div>
                                      </>
                                    )}
                                  </>
                                </Accordion.Collapse>
                              </>
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
                                    document.getElementById("isError") ||
                                    document.getElementById(key) ||
                                    document.querySelector(
                                      `input[name=${key}]`
                                    ) ||
                                    document.querySelector(
                                      `input[id=${key}]`
                                    ) ||
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
