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
 * Unless required by applicable law or agreed to in writing,Row
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, {
  useState,
  useEffect,
  useCallback,
  useRef,
  useReducer
} from "react";
import withRouter from "Hooks/withRouter";
import { fetchApi } from "../../../utils/fetchAPI";
import dateFormat from "dateformat";
import {
  Button,
  Tab,
  Tabs,
  Modal,
  Accordion,
  Card,
  DropdownButton,
  Dropdown
} from "react-bootstrap";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import AccessGrantForm from "./AccessGrantForm";
import { toast } from "react-toastify";
import { Form } from "react-final-form";
import { CustomTooltip, Loader } from "../../../components/CommonComponents";
import moment from "moment-timezone";
import { useParams, useNavigate, useLocation, Link } from "react-router-dom";
import { serverError } from "../../../utils/XAUtils";
import Select from "react-select";
import userColourIcon from "../../../images/user-colour.svg";
import groupColourIcon from "../../../images/group-colour.svg";
import roleColourIcon from "../../../images/role-colour.svg";
import arrayMutators from "final-form-arrays";
import { groupBy, isEmpty, isArray } from "lodash";
import PrinciplePermissionComp from "./PrinciplePermissionComp";
import ReactPaginate from "react-paginate";

const initialState = {
  loader: false,
  preventUnBlock: false,
  blockUI: false
};

const datasetFormReducer = (state, action) => {
  switch (action.type) {
    case "SET_SELECTED_PRINCIPLE":
      return {
        ...state,
        selectedPrinciple: action.selectedPrinciple
      };
    case "SET_PREVENT_ALERT":
      return {
        ...state,
        preventUnBlock: action.preventUnBlock
      };
    case "SET_BLOCK_UI":
      return {
        ...state,
        blockUI: action.blockUI
      };
    default:
      throw new Error();
  }
};

const DatasetDetailLayout = () => {
  let { datasetId } = useParams();
  const toastId = React.useRef(null);
  const [loader, setLoader] = useState(true);
  const [datasetInfo, setDatasetInfo] = useState({});
  const [datasetDescription, setDatasetDescription] = useState();
  const [datasetTerms, setDatasetTerms] = useState();
  const [datashareSearch, setDatashareSearch] = useState();
  const [dataShareModal, setDatashareModal] = useState(false);
  const [datashareList, setDatashareList] = useState([]);
  const [selectedDatashareList, setSelectedDatashareList] = useState([]);
  const [serviceDef, setServiceDef] = useState({});
  const [dataSetDetails, dispatch] = useReducer(
    datasetFormReducer,
    initialState
  );
  const [acceptTerms, setAcceptTerms] = useState(false);
  const [dataShareRequestsList, setDatashareRequestsList] = useState([]);
  const [userSharedWithAccordion, setUserSharedWithAccordion] = useState(false);
  const [groupSharedWithAccordion, setGroupSharedWithAccordion] =
    useState(false);
  const [roleSharedWithAccordion, setRoleSharedWithAccordion] = useState(false);
  const [userList, setUserList] = useState([]);
  const [groupList, setGroupList] = useState([]);
  const [roleList, setRoleList] = useState([]);
  const [activeKey, setActiveKey] = useState("overview");
  const [userSharedWithMap, setUserSharedWithMap] = useState(new Map());
  const [groupSharedWithMap, setGroupSharedWithMap] = useState(new Map());
  const [roleSharedWithMap, setRoleSharedWithMap] = useState(new Map());
  const [filteredUserSharedWithMap, setFilteredUserSharedWithMap] = useState();
  const [filteredGroupSharedWithMap, setFilteredGroupSharedWithMap] =
    useState();
  const [filteredRoleSharedWithMap, setFilteredRoleSharedWithMap] = useState();
  const [sharedWithPrincipleName, setSharedWithPrincipleName] = useState();
  const [sharedWithAccessFilter, setSharedWithAccessFilter] = useState();
  const [saveCancelButtons, showSaveCancelButton] = useState(false);
  const [showConfirmModal, setShowConfirmModal] = useState(false);
  const navigate = useNavigate();
  const [accessGrantFormValues, setAccessGrantFormValues] = useState();
  const [blockUI, setBlockUI] = useState(false);
  const [policyData, setPolicyData] = useState();
  const [
    showDatashareRequestDeleteConfirmModal,
    setShowDatashareRequestDeleteConfirmModal
  ] = useState(false);
  const [deleteDatashareReqInfo, setDeleteDatashareReqInfo] = useState({});
  const [tabTitle] = useState({
    all: "",
    requested: "",
    granted: "",
    active: ""
  });
  const [showDeleteDatasetModal, setShowDeleteDatasetModal] = useState(false);
  const [requestCurrentPage, setRequestCurrentPage] = useState(0);
  const [requestPageCount, setRequestPageCount] = useState();
  const [requestAccordionState, setRequestAccordionState] = useState({});
  const itemsPerPage = 5;
  const [showActivateRequestModal, setShowActivateRequestModal] =
    useState(false);
  const [datashareInfo, setDatashareInfo] = useState();
  const [datashareRequestInfo, setDatashareRequestInfo] = useState();

  const toggleConfirmModalForDatasetDelete = () => {
    setShowDeleteDatasetModal(true);
  };

  const handleDatasetDeleteClick = async () => {
    toggleClose();
    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/dataset/${datasetId}`,
        method: "DELETE"
      });
      setBlockUI(false);
      toast.success(" Success! Dataset deleted successfully");
      navigate("/gds/mydatasetlisting");
    } catch (error) {
      setBlockUI(false);
      let errorMsg = "Failed to delete dataset : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error("Error occurred during deleting dataset : " + error);
    }
  };

  useEffect(() => {
    fetchDatasetInfo(datasetId);
  }, []);

  const fetchDatashareRequestList = async (datashareName, currentPage) => {
    try {
      let params = {};
      params["pageSize"] = itemsPerPage;
      params["page"] = currentPage;
      params["startIndex"] = currentPage * itemsPerPage;
      params["datasetId"] = datasetId;
      const resp = await fetchApi({
        url: `gds/datashare/dataset`,
        params: params
      });
      let accordianState = {};
      resp.data.list.map(
        (item) =>
          (accordianState = { ...accordianState, ...{ [item.id]: false } })
      );
      setRequestAccordionState(accordianState);
      setRequestPageCount(Math.ceil(resp.data.totalCount / itemsPerPage));
      setDatashareRequestsList(resp.data.list);
      tabTitle.all = "All (" + resp.data.totalCount + ")";
    } catch (error) {
      console.error(
        `Error occurred while fetching Datashare requests details ! ${error}`
      );
    }
  };

  const fetchServiceDef = async (serviceDefName) => {
    let serviceDefsResp = [];
    try {
      serviceDefsResp = await fetchApi({
        url: `plugins/definitions/name/${serviceDefName}`
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Service Definition or CSRF headers! ${error}`
      );
    }
    let modifiedServiceDef = serviceDefsResp.data;
    for (const obj of modifiedServiceDef.resources) {
      obj.recursiveSupported = false;
      obj.excludesSupported = false;
    }
    setServiceDef(modifiedServiceDef);
  };

  const setPrincipleAccordianData = (principle) => {
    let userPrinciples = principle.users;
    let groupPrinciples = principle.groups;
    let rolePrinciples = principle.roles;

    let tempUserList = [];
    let tempGroupList = [];
    let tempRoleList = [];
    let userList = [];
    let groupList = [];
    let roleList = [];
    if (userPrinciples != undefined) {
      Object.entries(userPrinciples).map(([key, value]) => {
        tempUserList.push({ name: key, type: "USER", perm: value });
      });
    }
    if (groupPrinciples != undefined) {
      Object.entries(groupPrinciples).map(([key, value]) => {
        tempGroupList.push({ name: key, type: "GROUP", perm: value });
      });
    }
    if (rolePrinciples != undefined) {
      Object.entries(rolePrinciples).map(([key, value]) => {
        tempRoleList.push({ name: key, type: "ROLE", perm: value });
      });
    }
    setUserList([...userList, ...tempUserList]);
    setGroupList([...groupList, ...tempGroupList]);
    setRoleList([...roleList, ...tempRoleList]);
  };

  const fetchAccessGrantInfo = async (datasetName) => {
    let params = {};
    let policyData = {};
    params["resource:dataset"] = datasetName;
    try {
      const resp = await fetchApi({
        url: `gds/dataset/${datasetId}/policy`,
        params: params
      });
      policyData = resp.data[0];
    } catch (error) {
      console.error(
        `Error occurred while fetching dataset access grant details ! ${error}`
      );
    }
    fetchServiceDef(policyData.serviceType);
    let grantItems = policyData.policyItems;
    const userMap = new Map();
    const groupMap = new Map();
    const roleMap = new Map();
    grantItems.forEach((item) => {
      if (item.users !== undefined) {
        item.users.forEach((user) => {
          let accessList = [];
          if (userMap.get(user) !== undefined) {
            accessList = userMap.get(user);
          }
          userMap.set(user, [...accessList, ...item.accesses]);
        });
      }

      if (item.groups !== undefined) {
        item.groups.forEach((group) => {
          let accessList = [];
          if (groupMap[group] !== undefined) {
            accessList = groupMap[group];
          }
          groupMap.set(group, [...accessList, ...item.accesses]);
        });
      }

      if (item.roles !== undefined) {
        item.roles.forEach((role) => {
          let accessList = [];
          if (roleMap[role] !== undefined) {
            accessList = roleMap[role];
          }
          roleMap.set(role, [...accessList, ...item.accesses]);
        });
      }
      setUserSharedWithMap(userMap);
      setGroupSharedWithMap(groupMap);
      setRoleSharedWithMap(roleMap);
      setFilteredUserSharedWithMap(userMap);
      setFilteredGroupSharedWithMap(groupMap);
      setFilteredRoleSharedWithMap(roleMap);
    });
  };

  const onSharedWithAccessFilterChange = (e) => {
    setSharedWithAccessFilter(e);
    filterSharedWithPrincipleList(
      undefined,
      false,
      e != undefined ? e.label : undefined,
      true
    );
  };

  const fetchDatasetInfo = async (datasetId) => {
    try {
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/dataset/${datasetId}`
      });
      setLoader(false);
      setDatasetInfo(resp.data);
      setDatasetDescription(resp.data.description);
      setDatasetTerms(resp.data.termsOfUse);
      if (resp.data.acl != undefined) setPrincipleAccordianData(resp.data.acl);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      console.error(`Error occurred while fetching dataset details ! ${error}`);
    }
  };

  const addAccessGrant = () => {
    navigate(`/gds/dataset/${datasetInfo.id}/accessGrant`);
  };

  const datasetDescriptionChange = (event) => {
    setDatasetDescription(event.target.value);
    showSaveCancelButton(true);
    console.log("DatasetDescription is:", datasetDescription);
  };

  const datasetTermsAndConditionsChange = (event) => {
    setDatasetTerms(event.target.value);
    showSaveCancelButton(true);
    console.log("datasetTermsAndConditions is:", datasetTerms);
  };

  const requestDatashare = () => {
    setSelectedDatashareList([]);
    fetchDatashareList();
  };

  const toggleClose = () => {
    setDatashareModal(false);
    setShowDeleteDatasetModal(false);
    setShowActivateRequestModal(false);
  };

  const toggleConfirmModalClose = () => {
    setShowConfirmModal(false);
  };

  const toggleDatashareRequestDelete = () => {
    setShowDatashareRequestDeleteConfirmModal(false);
  };

  const updateDatashareSearch = (event) => {
    setDatashareSearch(event.target.value);
    fetchDatashareList(event.target.value);
  };

  const submitDatashareRequest = async () => {
    console.log("Selected datasharelist");
    console.log(selectedDatashareList);

    let requestObj = {};

    requestObj["status"] = "REQUESTED";
    if (
      selectedDatashareList != undefined &&
      selectedDatashareList.length > 0
    ) {
      requestObj["dataShareId"] = selectedDatashareList[0]["id"];
    }
    requestObj["datasetId"] = datasetId;

    try {
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: true
      });
      const createDatasetResp = await fetchApi({
        url: `gds/datashare/dataset`,
        method: "post",
        data: requestObj
      });
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: false
      });
      toast.success("Request created successfully!!");
      setDatashareModal(false);
      fetchDatashareRequestList(undefined, requestCurrentPage);
    } catch (error) {
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: false
      });
      serverError(error);
      console.error(
        `Error occurred while creating Datashare request  ${error}`
      );
    }
  };

  const fetchDatashareList = useCallback(async (dataShareSearchObj) => {
    let resp = [];
    let totalCount = 0;
    let params = {};
    let dataShareName = "";
    if (dataShareSearchObj != undefined) {
      dataShareName = dataShareSearchObj;
    } else {
      dataShareName = datashareSearch;
    }
    try {
      params["dataShareName"] = dataShareName;
      resp = await fetchApi({
        url: "gds/datashare",
        params: params
      });
      setDatashareList(resp.data.list);
      totalCount = resp.data.totalCount;
      setDatashareModal(true);
    } catch (error) {
      serverError(error);
      console.error(`Error occurred while fetching Datashare list! ${error}`);
    }
  }, []);

  const checkBocChange = (event) => {
    let dataShare = [
      {
        id: event.target.value,
        name: event.target.name
      }
    ];
    if (event.target.checked == true) {
      setSelectedDatashareList([...selectedDatashareList, ...dataShare]);
    } else if (event.target.checked == false) {
      let newArray = selectedDatashareList.filter(
        (item) => item["id"] !== event.target.value
      );
      setSelectedDatashareList(newArray);
    }
  };

  const handleSubmit = async (formData) => {};

  const changeDatashareAccordion = (obj) => {
    let newArray = selectedDatashareList.filter(
      (item) => item["name"] == obj["name"]
    );
    console.log("newArray");
    console.log(newArray);
  };

  const onUserSharedWithAccordianChange = () => {
    setUserSharedWithAccordion(!userSharedWithAccordion);
  };

  const onGroupSharedWithAccordianChange = () => {
    setGroupSharedWithAccordion(!groupSharedWithAccordion);
  };

  const onRoleSharedWithAccordianChange = () => {
    setRoleSharedWithAccordion(!roleSharedWithAccordion);
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

  const customStyles = {
    control: (provided) => ({
      ...provided,
      maxHeight: "40px",
      width: "172px"
    }),
    indicatorsContainer: (provided) => ({
      ...provided
    })
  };

  const dropDownStyle = {
    control: (provided) => ({ ...provided, display: none })
  };

  const onChangeSharedWithPrincipleName = (event) => {
    setSharedWithPrincipleName(event.target.value);
    filterSharedWithPrincipleList(event.target.value, true, undefined, false);
  };

  const filterSharedWithPrincipleList = (
    name,
    nameChange,
    perm,
    permChange
  ) => {
    if (name === undefined && !nameChange) {
      name =
        sharedWithPrincipleName != undefined &&
        sharedWithPrincipleName.length > 0
          ? sharedWithPrincipleName
          : undefined;
    }

    if (perm === undefined && !permChange) {
      perm =
        sharedWithAccessFilter != undefined
          ? sharedWithAccessFilter.name
          : undefined;
    }

    let newUserMap = new Map();
    let newGroupMap = new Map();
    let newRoleMap = new Map();

    const conditionFunction = (value, key) => {
      if (name != undefined && perm != undefined) {
        let accessMatch = false;
        for (const accessObj of value) {
          if (accessObj.type == perm) {
            accessMatch = true;
          }
        }
        return key.startsWith(name, 0) && accessMatch;
      } else if (name != undefined) {
        return key.startsWith(name, 0);
      } else if (perm != undefined) {
        for (const accessObj of value) {
          if (accessObj.type == perm) return true;
        }
      } else {
        return true;
      }
    };

    userSharedWithMap.forEach((value, key) => {
      if (conditionFunction(value, key)) {
        newUserMap.set(key, value);
      }
    });

    groupSharedWithMap.forEach((value, key) => {
      if (conditionFunction(value, key)) {
        newGroupMap.set(key, value);
      }
    });

    roleSharedWithMap.forEach((value, key) => {
      if (conditionFunction(value, key)) {
        newRoleMap.set(key, value);
      }
    });

    setFilteredUserSharedWithMap(newUserMap);
    setFilteredGroupSharedWithMap(newGroupMap);
    setFilteredRoleSharedWithMap(newRoleMap);
  };

  const handleDataChange = (userList, groupList, roleList) => {
    setUserList(userList);
    setGroupList(groupList);
    setRoleList(roleList);
    showSaveCancelButton(true);
  };

  const handleTabSelect = (key) => {
    if (saveCancelButtons == true) {
      setShowConfirmModal(true);
    } else {
      setActiveKey(key);
      if (key == "sharedWith") {
        fetchAccessGrantInfo(datasetInfo.name);
      } else if (key == "dataShares") {
        fetchDatashareRequestList(undefined, 0);
      }
    }
  };

  const noneOptions = {
    label: "None",
    value: "none"
  };

  const updateDatasetDetails = async () => {
    datasetInfo.description = datasetDescription;
    datasetInfo.termsOfUse = datasetTerms;

    datasetInfo.acl = { users: {}, groups: {}, roles: {} };

    userList.forEach((user) => {
      datasetInfo.acl.users[user.name] = user.perm;
    });

    groupList.forEach((group) => {
      datasetInfo.acl.groups[group.name] = group.perm;
    });

    roleList.forEach((role) => {
      datasetInfo.acl.roles[role.name] = role.perm;
    });

    try {
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: true
      });
      await fetchApi({
        url: `gds/dataset/${datasetId}`,
        method: "put",
        data: datasetInfo
      });
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: false
      });
      toast.success("Dataset updated successfully!!");
      //self.location.hash = `#/gds/dataset/${datasetId}/detail`;
      //navigate(`/gds/dataset/${datasetId}/detail`);
      showSaveCancelButton(false);
    } catch (error) {
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: false
      });
      serverError(error);
      console.error(`Error occurred while updating dataset  ${error}`);
    }
  };

  const handleAccessGrantChange = (accessGrantData, policyData) => {
    if (accessGrantData != undefined && policyData != undefined) {
      setPolicyData(policyData);
      setAccessGrantFormValues(accessGrantData);
    }
    showSaveCancelButton(true);
  };

  const updateDatasetAccessGrant = async () => {
    let data = {};
    let values = accessGrantFormValues;
    data.policyItems = getPolicyItemsVal(values, "policyItems");
    data.description = values.description;
    data.isAuditEnabled = values.isAuditEnabled;
    data.isDenyAllElse = values.isDenyAllElse;
    data.isEnabled = values.isEnabled;
    data.name = values.policyName;
    data.policyLabels = (values.policyLabel || [])?.map(({ value }) => value);
    data.policyPriority = values.policyPriority ? "1" : "0";
    data.policyType = values.policyType;
    data.service = policyData.service;
    let serviceCompRes;
    if (values.policyType != null) {
      serviceCompRes = serviceDef.resources;
    }
    const grpResources = groupBy(serviceCompRes || [], "level");
    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    let value = { values: [datasetInfo.name] };
    data.resources = { dataset: value };

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
          values: value?.split(",")
        });
      });
    } else {
      data["conditions"] = [];
    }
    let dataVal = {
      ...policyData,
      ...data
    };
    try {
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/dataset/${datasetId}/policy/${policyData.id}`,
        method: "PUT",
        data: dataVal
      });
      setLoader(false);
      toast.dismiss(toastId.current);
      toastId.current = toast.success("Access Grant updated successfully!!");
      showSaveCancelButton(false);
    } catch (error) {
      setLoader(false);
      let errorMsg = `Failed to save policy form`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
      console.error(`Error while saving policy form! ${error}`);
    }
  };

  const getPolicyItemsVal = (formData, name) => {
    var policyResourceItem = [];
    for (let key of formData[name]) {
      if (!isEmpty(key) && Object.entries(key).length > 0) {
        let obj = {};
        if (key.delegateAdmin != "undefined" && key.delegateAdmin != null) {
          obj.delegateAdmin = key.delegateAdmin;
        }

        obj.accesses = key.accesses.map(({ value }) => ({
          type: value,
          isAllowed: true
        }));
        obj.users = [];
        obj.groups = [];
        obj.roles = [];
        if (key.principle && key.principle.length > 0) {
          for (let i = 0; i < key.principle.length; i++) {
            let principleObj = key.principle[i];
            if (principleObj.type == "USER") {
              obj.users.push(principleObj.value);
            } else if (principleObj.type == "GROUP") {
              obj.groups.push(principleObj.value);
            } else if (principleObj.type == "ROLE") {
              obj.roles.push(principleObj.value);
            }
          }
        }

        if (key?.conditions) {
          obj.conditions = [];
          viewDatashareDetail;
          Object.entries(key.conditions).map(
            ([conditionKey, conditionValue]) => {
              return obj.conditions.push({
                type: conditionKey,
                values: !isArray(conditionValue)
                  ? conditionValue?.split(",")
                  : conditionValue.map((m) => {
                      return m.value;
                    })
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

  const back = () => {
    navigate("/gds/mydatasetlisting");
  };

  const removeChanges = () => {
    fetchDatasetInfo(datasetId);
    showSaveCancelButton(false);
    toggleConfirmModalClose();
  };

  const viewDatashareDetail = (datashareId) => {
    navigate(`/gds/datashare/${datashareId}/detail`);
  };

  const toggleConfirmModalForDelete = (id, name, status) => {
    let deleteMsg = "";
    if (status == "ACTIVE") {
      deleteMsg = `Do you want to remove Datashare ${id} from ${datasetInfo.name}`;
    } else {
      deleteMsg = `Do you want to delete request of Datashare ${id}`;
    }
    let data = { id: id, name: name, status: status, msg: deleteMsg };
    setDeleteDatashareReqInfo(data);
    setShowDatashareRequestDeleteConfirmModal(true);
  };

  const deleteDatashareRequest = async () => {
    try {
      setLoader(true);
      await fetchApi({
        url: `gds/datashare/dataset/${deleteDatashareReqInfo.id}`,
        method: "DELETE"
      });
      let successMsg = "";
      if (deleteDatashareReqInfo.status == "ACTIVE") {
        successMsg = "Success! Datashare removed from dataset successfully";
      } else {
        successMsg = "Success! Datashare request deleted successfully";
      }
      setShowDatashareRequestDeleteConfirmModal(false);
      toast.success(successMsg);
      fetchDatashareRequestList(undefined, requestCurrentPage);
      //fetchSharedResourceForDatashare(datashareInfo.name);
      setLoader(false);
    } catch (error) {
      let errorMsg = "";
      if (deleteDatashareReqInfo.status == "ACTIVE") {
        errorMsg = "Failed to remove datashare from dataset ";
      } else {
        errorMsg = "Failed to delete datashare request ";
      }
      setLoader(false);
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error(
        "Error occurred during deleting Datashare request  : " + error
      );
    }
  };

  const termsCheckBocChange = () => {
    setAcceptTerms(true);
  };

  const copyURL = () => {
    navigator.clipboard.writeText(window.location.href).then(() => {
      toast.success("URL copied!!");
    });
  };

  const handleRequestPageClick = ({ selected }) => {
    setRequestCurrentPage(selected);
    fetchDatashareRequestList(undefined, selected);
  };

  const onRequestAccordionChange = (id) => {
    setRequestAccordionState({
      ...requestAccordionState,
      ...{ [id]: !requestAccordionState[id] }
    });
  };

  const showActiveateRequestModal = (requestInfo) => {
    setShowActivateRequestModal(true);
    setDatashareRequestInfo(requestInfo);
    fetchDatashareById(requestInfo.dataShareId);
  };

  const fetchDatashareById = async (datashareId) => {
    let resp = {};
    try {
      resp = await fetchApi({
        url: `gds/datashare/${datashareId}`
      });
    } catch (error) {
      let errorMsg = "Failed to delete dataset : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      console.error("Error occurred during deleting dataset : " + error);
    }
    setDatashareInfo(resp.data);
  };

  const activateDatashareRequest = async (datashareInfo) => {
    if (datashareInfo.termsOfUse != undefined && !acceptTerms) {
      toast.error("Please accept terms & conditions");
      return null;
    }
    datashareRequestInfo["status"] = "ACTIVE";
    try {
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/datashare/dataset/${datashareRequestInfo.id}`,
        method: "PUT",
        data: datashareRequestInfo
      });
      setLoader(false);
      toast.success("Request updated successfully!!");
      //showSaveCancelButton(false);
    } catch (error) {
      setLoader(false);
      let errorMsg = `Failed to update request`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
      console.error(`Error while updating request! ${error}`);
    }
    setShowActivateRequestModal(false);
  };

  return (
    <>
      <React.Fragment>
        <div className="gds-header-wrapper gap-half">
          <Button
            variant="light"
            className="border-0 bg-transparent"
            onClick={back}
            size="sm"
            data-id="back"
            data-cy="back"
          >
            <i className="fa fa-angle-left fa-lg font-weight-bold" />
          </Button>
          <h3 className="gds-header bold"> Dataset : {datasetInfo.name}</h3>
          {saveCancelButtons ? (
            <div className="gds-header-btn-grp">
              <Button
                variant="primary"
                onClick={() => removeChanges()}
                size="sm"
                data-id="cancel"
                data-cy="cancel"
              >
                Cancel
              </Button>
              <Button
                variant="primary"
                onClick={
                  activeKey != "accessGrants"
                    ? updateDatasetDetails
                    : updateDatasetAccessGrant
                }
                size="sm"
                data-id="save"
                data-cy="save"
              >
                Save
              </Button>
            </div>
          ) : (
            <div className="gds-header-btn-grp">
              <Button
                variant="primary"
                onClick={requestDatashare}
                size="sm"
                data-id="addADatashare"
                data-cy="addADatashare"
              >
                Add a Data Share
              </Button>
            </div>
          )}
          <div>
            <DropdownButton
              id="dropdown-item-button"
              title={<i className="fa fa-ellipsis-v" fontSize="36px" />}
              size="sm"
              className="hide-arrow"
            >
              <Dropdown.Item
                as="button"
                // onClick={() => {
                //   showViewModal(serviceData?.id);
                // }}
                data-name="fullView"
                data-id="fullView"
                data-cy="fullView"
              >
                Full View
              </Dropdown.Item>
              <Dropdown.Item
                as="button"
                onClick={() => {
                  copyURL();
                }}
                data-name="copyDatasetLink"
                data-id="copyDatasetLink"
                data-cy="copyDatasetLink"
              >
                Copy Dataset Link
              </Dropdown.Item>
              <Dropdown.Item
                as="button"
                // onClick={() => {
                //   showViewModal(serviceData?.id);
                // }}
                data-name="downloadJson"
                data-id="downloadJson"
                data-cy="downloadJson"
              >
                Download Json
              </Dropdown.Item>
              <hr />
              <Dropdown.Item
                as="button"
                onClick={() => {
                  toggleConfirmModalForDatasetDelete();
                }}
                data-name="deleteDataset"
                data-id="deleteDataset"
                data-cy="deleteDataset"
              >
                Delete Dataset
              </Dropdown.Item>
            </DropdownButton>
          </div>
        </div>
        {loader ? (
          <Loader />
        ) : (
          <React.Fragment>
            <div>
              <Tabs
                id="DatasetDetailLayout"
                activeKey={activeKey}
                onSelect={handleTabSelect}
              >
                <Tab eventKey="overview" title="OVERVIEW">
                  {activeKey == "overview" ? (
                    <div>
                      <Form
                        onSubmit={handleSubmit}
                        mutators={{
                          ...arrayMutators
                        }}
                        render={({}) => (
                          <div>
                            <div className="gds-tab-content gds-content-border">
                              <div className="gds-inline-field-grp">
                                <div className="wrapper">
                                  <div
                                    className="gds-left-inline-field"
                                    height="30px"
                                  >
                                    Date Updated
                                  </div>
                                  <div line-height="30px">
                                    {dateFormat(
                                      datasetInfo.updateTime,
                                      "mm/dd/yyyy hh:MM:ss TT"
                                    )}
                                  </div>
                                </div>

                                <div className="wrapper">
                                  <div
                                    className="gds-left-inline-field"
                                    line-height="30px"
                                  >
                                    Date Created
                                  </div>
                                  <div line-height="30px">
                                    {dateFormat(
                                      datasetInfo.createTime,
                                      "mm/dd/yyyy hh:MM:ss TT"
                                    )}
                                  </div>
                                </div>
                              </div>

                              <div>
                                <div>Description</div>
                              </div>
                              <div>
                                <div>
                                  <textarea
                                    placeholder="Dataset Description"
                                    className="form-control gds-description"
                                    id="description"
                                    data-cy="description"
                                    onChange={datasetDescriptionChange}
                                    value={datasetDescription}
                                    rows={5}
                                  />
                                </div>
                              </div>
                            </div>
                            <PrinciplePermissionComp
                              userList={userList}
                              groupList={groupList}
                              roleList={roleList}
                              onDataChange={handleDataChange}
                            />
                          </div>
                        )}
                      />
                    </div>
                  ) : (
                    <div></div>
                  )}
                </Tab>
                <Tab eventKey="dataShares" title="DATASHARES">
                  {activeKey == "dataShares" ? (
                    <div className="gds-tab-content">
                      <div>
                        <div className="usr-grp-role-search-width mb-4">
                          <StructuredFilter
                            key="user-listing-search-filter"
                            placeholder="Search datashares..."
                          />
                        </div>
                      </div>
                      <div>
                        <div className="usr-grp-role-search-width">
                          <Tabs id="DatashareTabs" className="mg-b-10">
                            <Tab eventKey="All" title={tabTitle.all}>
                              <Card className="border-0">
                                <div>
                                  {dataShareRequestsList.length > 0 ? (
                                    dataShareRequestsList.map((obj, index) => {
                                      return (
                                        <div>
                                          <Accordion
                                            className="mg-b-10"
                                            defaultActiveKey="0"
                                          >
                                            <div className="border-bottom">
                                              <Accordion.Toggle
                                                as={Card.Header}
                                                eventKey="1"
                                                onClick={() =>
                                                  onRequestAccordionChange(
                                                    obj.id
                                                  )
                                                }
                                                className="border-bottom-0"
                                                data-id="panel"
                                                data-cy="panel"
                                              >
                                                {obj["status"] == "GRANTED" ? (
                                                  <div>
                                                    <span>
                                                      Data access granted.
                                                    </span>
                                                    <Link
                                                      className="mb-3"
                                                      to=""
                                                      onClick={() =>
                                                        showActiveateRequestModal(
                                                          obj
                                                        )
                                                      }
                                                    >
                                                      Activate Datashare
                                                    </Link>
                                                  </div>
                                                ) : (
                                                  <div></div>
                                                )}
                                                <div className="d-flex justify-content-between align-items-center">
                                                  <div className="d-flex align-items-center gap-1">
                                                    {requestAccordionState[
                                                      obj.id
                                                    ] ? (
                                                      <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                                    ) : (
                                                      <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                                    )}
                                                    <h5 className="gds-heading-5 m-0">
                                                      {/* {obj.name} */}{" "}
                                                      Datashare{" "}
                                                      {obj.dataShareId}
                                                    </h5>
                                                  </div>
                                                  <div className="d-flex align-items-center gap-1">
                                                    <span
                                                      //className="badge badge-light gds-requested-status"
                                                      className={
                                                        obj["status"] ===
                                                        "REQUESTED"
                                                          ? "badge badge-light gds-requested-status"
                                                          : obj["status"] ===
                                                            "GRANTED"
                                                          ? "badge badge-light gds-granted-status"
                                                          : obj["status"] ===
                                                            "ACTIVE"
                                                          ? "badge badge-light gds-active-status"
                                                          : "badge badge-light gds-denied-status"
                                                      }
                                                    >
                                                      {obj["status"]}
                                                    </span>
                                                    <Button
                                                      variant="outline-dark"
                                                      size="sm"
                                                      className="mr-2"
                                                      title="View"
                                                      data-name="viewDatashare"
                                                      onClick={() =>
                                                        viewDatashareDetail(
                                                          obj.dataShareId
                                                        )
                                                      }
                                                      data-id={obj.id}
                                                    >
                                                      <i className="fa-fw fa fa-eye fa-fw fa fa-large" />
                                                    </Button>
                                                    <Button
                                                      variant="danger"
                                                      size="sm"
                                                      title="Delete"
                                                      onClick={() =>
                                                        toggleConfirmModalForDelete(
                                                          obj.id,
                                                          obj.name,
                                                          obj.status
                                                        )
                                                      }
                                                      data-name="deleteDatashareRequest"
                                                      data-id={obj["id"]}
                                                      data-cy={obj["id"]}
                                                    >
                                                      <i className="fa-fw fa fa-trash fa-fw fa fa-large" />
                                                    </Button>
                                                  </div>
                                                </div>
                                              </Accordion.Toggle>
                                              <Accordion.Collapse eventKey="1">
                                                <Card.Body>
                                                  <div className="d-flex justify-content-between">
                                                    <div className="gds-inline-field-grp">
                                                      <div className="wrapper">
                                                        <div
                                                          className="gds-left-inline-field"
                                                          height="30px"
                                                        >
                                                          Service
                                                        </div>
                                                        <div line-height="30px">
                                                          {obj["service"]}
                                                        </div>
                                                      </div>
                                                      <div className="wrapper">
                                                        <div
                                                          className="gds-left-inline-field"
                                                          height="30px"
                                                        >
                                                          Zone
                                                        </div>
                                                        <div line-height="30px">
                                                          {obj["zone"]}
                                                        </div>
                                                      </div>
                                                      <div className="wrapper">
                                                        <div
                                                          className="gds-left-inline-field"
                                                          height="30px"
                                                        >
                                                          Resource Count
                                                        </div>
                                                        <div line-height="30px">
                                                          {obj["resourceCount"]}
                                                        </div>
                                                      </div>
                                                    </div>
                                                    <div className="gds-right-inline-field-grp">
                                                      <div className="wrapper">
                                                        <div>Added</div>
                                                        <div className="gds-right-inline-field">
                                                          {dateFormat(
                                                            obj["createTime"],
                                                            "mm/dd/yyyy hh:MM:ss TT"
                                                          )}
                                                        </div>
                                                      </div>
                                                      <div className="wrapper">
                                                        <div>Updated</div>
                                                        <div className="gds-right-inline-field">
                                                          {dateFormat(
                                                            obj["updateTime"],
                                                            "mm/dd/yyyy hh:MM:ss TT"
                                                          )}
                                                        </div>
                                                      </div>
                                                      <div className="w-100 text-right">
                                                        <div>View Request</div>
                                                      </div>
                                                    </div>
                                                  </div>
                                                </Card.Body>
                                              </Accordion.Collapse>
                                            </div>
                                          </Accordion>
                                        </div>
                                      );
                                    })
                                  ) : (
                                    <div></div>
                                  )}
                                  <ReactPaginate
                                    previousLabel={"Previous"}
                                    nextLabel={"Next"}
                                    pageClassName="page-item"
                                    pageLinkClassName="page-link"
                                    previousClassName="page-item"
                                    previousLinkClassName="page-link"
                                    nextClassName="page-item"
                                    nextLinkClassName="page-link"
                                    breakLabel={"..."}
                                    pageCount={requestPageCount}
                                    onPageChange={handleRequestPageClick}
                                    breakClassName="page-item"
                                    breakLinkClassName="page-link"
                                    containerClassName="pagination"
                                    activeClassName="active"
                                  />
                                </div>
                              </Card>
                            </Tab>
                            <Tab eventKey="Active" title="Active" />
                            <Tab eventKey="Requested" title="Requested" />
                            <Tab eventKey="Granted" title="Granted" />
                          </Tabs>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div></div>
                  )}
                </Tab>
                <Tab eventKey="sharedWith" title="SHARED WITH">
                  {activeKey == "sharedWith" ? (
                    <div className="gds-tab-content gds-content-border">
                      <div>
                        <div className="usr-grp-role-search-width">
                          <p className="gds-content-header">Shared with</p>
                        </div>
                        <div className="gds-flex mg-b-10">
                          <input
                            type="search"
                            className="form-control gds-input"
                            placeholder="Search..."
                            onChange={(e) => onChangeSharedWithPrincipleName(e)}
                            value={sharedWithPrincipleName}
                          />

                          <Select
                            theme={serviceSelectTheme}
                            styles={customStyles}
                            options={serviceDef.accessTypes}
                            onChange={(e) => onSharedWithAccessFilterChange(e)}
                            value={sharedWithAccessFilter}
                            menuPlacement="auto"
                            placeholder="All Permissions"
                            isClearable
                          />
                        </div>

                        <Accordion className="mg-b-10" defaultActiveKey="0">
                          <Card>
                            <div className="border-bottom">
                              <Accordion.Toggle
                                as={Card.Header}
                                eventKey="1"
                                onClick={onUserSharedWithAccordianChange}
                                className="border-bottom-0 d-flex align-items-center justify-content-between gds-acc-card-header"
                                data-id="panel"
                                data-cy="panel"
                              >
                                <div className="d-flex align-items-center gap-half">
                                  <img
                                    src={userColourIcon}
                                    height="30px"
                                    width="30px"
                                  />
                                  Users (
                                  {filteredUserSharedWithMap == undefined
                                    ? 0
                                    : filteredUserSharedWithMap.size}
                                  )
                                </div>
                                {userSharedWithAccordion ? (
                                  <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                ) : (
                                  <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                )}
                              </Accordion.Toggle>
                            </div>
                            <Accordion.Collapse eventKey="1">
                              <Card.Body>
                                {filteredUserSharedWithMap != undefined &&
                                filteredUserSharedWithMap.size > 0 ? (
                                  Array.from(filteredUserSharedWithMap).map(
                                    ([key, value]) => (
                                      <div
                                        className="gds-principle-listing"
                                        key={key}
                                      >
                                        <span title={key}>{key}</span>
                                        <div className="gds-chips gap-one-fourth">
                                          {value.map((accessObj) => (
                                            <span
                                              className="badge badge-light badge-sm"
                                              title={accessObj.type}
                                              key={accessObj.type}
                                            >
                                              {accessObj.type}
                                            </span>
                                          ))}
                                        </div>
                                      </div>
                                    )
                                  )
                                ) : (
                                  <p className="mt-1">--</p>
                                )}
                              </Card.Body>
                            </Accordion.Collapse>
                          </Card>
                        </Accordion>

                        <Accordion className="mg-b-10" defaultActiveKey="0">
                          <Card>
                            <div className="border-bottom">
                              <Accordion.Toggle
                                as={Card.Header}
                                eventKey="1"
                                onClick={onGroupSharedWithAccordianChange}
                                className="border-bottom-0 d-flex align-items-center justify-content-between gds-acc-card-header"
                                data-id="panel"
                                data-cy="panel"
                              >
                                <div className="d-flex align-items-center gap-half">
                                  <img
                                    src={groupColourIcon}
                                    height="30px"
                                    width="30px"
                                  />
                                  Groups (
                                  {filteredGroupSharedWithMap == undefined
                                    ? 0
                                    : filteredGroupSharedWithMap.size}
                                  )
                                </div>
                                {groupSharedWithAccordion ? (
                                  <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                ) : (
                                  <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                )}
                              </Accordion.Toggle>
                            </div>
                            <Accordion.Collapse eventKey="1">
                              <Card.Body>
                                <Card.Body>
                                  {filteredGroupSharedWithMap != undefined &&
                                  filteredGroupSharedWithMap.size > 0 ? (
                                    Array.from(filteredGroupSharedWithMap).map(
                                      ([key, value]) => (
                                        <div
                                          className="gds-principle-listing"
                                          key={key}
                                        >
                                          <span title={key}>{key}</span>
                                          {value.map((accessObj) => (
                                            <span
                                              title={accessObj.type}
                                              key={accessObj.type}
                                            >
                                              {accessObj.type}
                                            </span>
                                          ))}
                                        </div>
                                      )
                                    )
                                  ) : (
                                    <p className="mt-1">--</p>
                                  )}
                                </Card.Body>
                              </Card.Body>
                            </Accordion.Collapse>
                          </Card>
                        </Accordion>

                        <Accordion className="mg-b-10" defaultActiveKey="0">
                          <Card>
                            <div className="border-bottom">
                              <Accordion.Toggle
                                as={Card.Header}
                                eventKey="1"
                                onClick={onRoleSharedWithAccordianChange}
                                className="border-bottom-0 d-flex align-items-center justify-content-between gds-acc-card-header"
                                data-id="panel"
                                data-cy="panel"
                              >
                                <div className="d-flex align-items-center gap-half">
                                  <img
                                    src={roleColourIcon}
                                    height="30px"
                                    width="30px"
                                  />
                                  Roles (
                                  {filteredRoleSharedWithMap == undefined
                                    ? 0
                                    : filteredRoleSharedWithMap.size}
                                  )
                                </div>
                                {roleSharedWithAccordion ? (
                                  <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                ) : (
                                  <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                )}
                              </Accordion.Toggle>
                            </div>
                            <Accordion.Collapse eventKey="1">
                              <Card.Body>
                                <Card.Body>
                                  {filteredRoleSharedWithMap != undefined &&
                                  filteredRoleSharedWithMap.size > 0 ? (
                                    Array.from(filteredRoleSharedWithMap).map(
                                      ([key, value]) => (
                                        <div
                                          className="gds-principle-listing"
                                          key={key}
                                        >
                                          <span title={key}>{key}</span>
                                          {value.map((accessObj) => (
                                            <span
                                              title={accessObj.type}
                                              key={accessObj.type}
                                            >
                                              {accessObj.type}
                                            </span>
                                          ))}
                                        </div>
                                      )
                                    )
                                  ) : (
                                    <p className="mt-1">--</p>
                                  )}
                                </Card.Body>
                              </Card.Body>
                            </Accordion.Collapse>
                          </Card>
                        </Accordion>
                      </div>
                    </div>
                  ) : (
                    <div></div>
                  )}
                </Tab>
                <Tab eventKey="accessGrants" title="ACCESS GRANTS">
                  <div className="wrap-gds">
                    {activeKey == "accessGrants" ? (
                      <AccessGrantForm
                        dataset={datasetInfo}
                        onDataChange={handleAccessGrantChange}
                      />
                    ) : (
                      <div></div>
                    )}
                  </div>
                </Tab>
                <Tab eventKey="history" title="HISTORY" />
                <Tab
                  eventKey="termsOfUse"
                  title="TERMS OF USE"
                  //onClick={() => handleTabClick("TERMS OF USE")}
                >
                  {activeKey == "termsOfUse" ? (
                    <div className="gds-tab-content gds-content-border">
                      <div>
                        <div className="usr-grp-role-search-width">
                          <p className="gds-content-header">
                            Terms & Conditions
                          </p>
                        </div>
                      </div>
                      <div>
                        <div>
                          <textarea
                            placeholder="Terms & Conditions"
                            className="form-control"
                            id="termsAndConditions"
                            data-cy="termsAndConditions"
                            onChange={datasetTermsAndConditionsChange}
                            value={datasetTerms}
                            rows={16}
                          />
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div></div>
                  )}
                </Tab>
              </Tabs>
            </div>

            <Modal
              show={showDatashareRequestDeleteConfirmModal}
              onHide={toggleDatashareRequestDelete}
            >
              <Modal.Header closeButton>
                <h3 className="gds-header bold">
                  {deleteDatashareReqInfo.msg}
                </h3>
              </Modal.Header>
              <Modal.Footer>
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => removeChanges()}
                >
                  No
                </Button>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={() => deleteDatashareRequest()}
                >
                  Yes
                </Button>
              </Modal.Footer>
            </Modal>

            <Modal show={showConfirmModal} onHide={toggleConfirmModalClose}>
              <Modal.Header closeButton>
                <h3 className="gds-header bold">
                  Would you like to save the changes?
                </h3>
              </Modal.Header>
              <Modal.Footer>
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => removeChanges()}
                >
                  No
                </Button>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={updateDatasetDetails}
                >
                  Yes
                </Button>
              </Modal.Footer>
            </Modal>

            <Modal show={dataShareModal} onHide={toggleClose}>
              <Modal.Header closeButton>
                <h3 className="gds-header bold">Request a Datashare</h3>
              </Modal.Header>
              <Modal.Body>
                <input
                  type="search"
                  className="form-control gds-input mb-3"
                  placeholder="Search Datashare..."
                  onChange={(e) => updateDatashareSearch(e)}
                  value={datashareSearch}
                />

                {datashareList != undefined && datashareList.length > 0 ? (
                  datashareList.map((obj, index) => {
                    return (
                      <div className="d-flex align-items-center gap-half mg-b-10 ">
                        <input
                          type="checkbox"
                          name={obj.name}
                          value={obj.id}
                          onChange={checkBocChange}
                        />
                        <span title={obj.name} className="fnt-14">
                          {obj.name}{" "}
                          <CustomTooltip
                            placement="right"
                            content={
                              <p
                                className="pd-10"
                                style={{ fontSize: "small" }}
                              >
                                {obj.description}
                              </p>
                            }
                            icon="fa-fw fa fa-info-circle gds-opacity-lowest"
                          />
                        </span>
                      </div>
                    );
                  })
                ) : (
                  <div></div>
                )}
              </Modal.Body>
              <Modal.Footer>
                <Button variant="secondary" size="sm" onClick={toggleClose}>
                  Cancel
                </Button>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={submitDatashareRequest}
                >
                  Send Request
                </Button>
              </Modal.Footer>
            </Modal>

            <Modal show={showDeleteDatasetModal} onHide={toggleClose}>
              <Modal.Header closeButton>
                <span className="text-word-break">
                  Are you sure you want to delete dataset&nbsp;"
                  <b>{datasetInfo.name}</b>" ?
                </span>
              </Modal.Header>
              <Modal.Footer>
                <Button variant="secondary" size="sm" onClick={toggleClose}>
                  No
                </Button>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={() => handleDatasetDeleteClick()}
                >
                  Yes
                </Button>
              </Modal.Footer>
            </Modal>

            <Modal show={showActivateRequestModal} onHide={toggleClose}>
              <Modal.Header closeButton>
                <span className="text-word-break">Activate Datashare</span>
              </Modal.Header>
              <Modal.Body>
                <div>
                  <p>Terms & Conditions</p>
                  <span>{datashareInfo?.termsOfUse}</span>
                </div>
                {datashareInfo?.termsOfUse != undefined ? (
                  <div className="d-flex align-items-center gap-half my-2">
                    <input
                      type="checkbox"
                      name="acceptTerms"
                      //value={obj.id}
                      onChange={termsCheckBocChange}
                    />
                    <span className="fnt-14">
                      {" "}
                      I accept the terms and conditions
                    </span>
                  </div>
                ) : (
                  <div></div>
                )}
              </Modal.Body>
              <Modal.Footer>
                <Button variant="secondary" size="sm" onClick={toggleClose}>
                  Cancel
                </Button>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={() => activateDatashareRequest(datashareInfo)}
                >
                  Activate
                </Button>
              </Modal.Footer>
            </Modal>
          </React.Fragment>
        )}
      </React.Fragment>
    </>
  );
};

export default withRouter(DatasetDetailLayout);
