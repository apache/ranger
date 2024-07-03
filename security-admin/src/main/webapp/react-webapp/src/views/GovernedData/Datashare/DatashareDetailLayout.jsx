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

import React, {
  useState,
  useEffect,
  useRef,
  useCallback,
  useReducer
} from "react";
import {
  useParams,
  useNavigate,
  Link,
  useLocation,
  useSearchParams
} from "react-router-dom";
import { fetchApi } from "../../../utils/fetchAPI";
import { Loader } from "../../../components/CommonComponents";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import {
  Button,
  Col,
  Modal,
  Accordion,
  Card,
  Tab,
  Tabs,
  DropdownButton,
  Dropdown
} from "react-bootstrap";
import dateFormat from "dateformat";
import { toast } from "react-toastify";
import { BlockUi } from "../../../components/CommonComponents";
import PrinciplePermissionComp from "../Dataset/PrinciplePermissionComp";
import { Form, Field } from "react-final-form";
import arrayMutators from "final-form-arrays";
import ReactPaginate from "react-paginate";
import AddSharedResourceComp from "./AddSharedResourceComp";
import CustomBreadcrumb from "../../CustomBreadcrumb";
import PolicyConditionsComp from "../../PolicyListing/PolicyConditionsComp";
import {
  getTableSortBy,
  getTableSortType,
  isSystemAdmin,
  parseSearchFilter,
  serverError,
  policyConditionUpdatedJSON,
  capitalizeFirstLetter
} from "../../../utils/XAUtils";
import XATableLayout from "../../../components/XATableLayout";
import moment from "moment-timezone";
import { getServiceDef } from "../../../utils/appState";
import DatashareInDatasetListComp from "../Dataset/DatashareInDatasetListComp";
import { isEmpty, isObject, isEqual } from "lodash";
import Select from "react-select";
import OperationAdminModal from "../../AuditEvent/OperationAdminModal";
import historyDetailsIcon from "../../../images/history-details.svg";
import { ClassTypes } from "../../../utils/XAEnums";

const DatashareDetailLayout = () => {
  let { datashareId } = useParams();
  const { state } = useLocation();
  const userAclPerm = state == null ? fetchDatashareSummary : state.userAclPerm;
  const [datashareName, setDatashareName] = useState(state?.datashareName);
  const [activeKey, setActiveKey] = useState("overview");
  const [datashareInfo, setDatashareInfo] = useState({});
  const [datashareDescription, setDatashareDescription] = useState();
  const [datashareConditionExpr, setDatashareConditionExpr] = useState();
  const [datashareTerms, setDatashareTerms] = useState();
  const [loader, setLoader] = useState(true);
  const [resourceContentLoader, setResourceContentLoader] = useState(false);
  const [requestContentLoader, setRequestContentLoader] = useState(true);
  const [sharedResources, setSharedResources] = useState([]);
  const [confirmDeleteModal, setConfirmDeleteModal] = useState({
    sharedResourceDetails: {}
  });
  const [blockUI, setBlockUI] = useState(false);
  const [dataShareRequestsList, setDataShareRequestsList] = useState([]);
  const [userList, setUserList] = useState([]);
  const [groupList, setGroupList] = useState([]);
  const [roleList, setRoleList] = useState([]);
  const [filteredUserList, setFilteredUserList] = useState([]);
  const [filteredGroupList, setFilteredGroupList] = useState([]);
  const [filteredRoleList, setFilteredRoleList] = useState([]);
  const navigate = useNavigate();
  const [saveCancelButtons, showSaveCancelButton] = useState(false);
  const [conditionModalData, setConditionModalData] = useState();
  const [showConditionModal, setShowConditionModal] = useState(false);
  const [resourceAccordionState, setResourceAccordionState] = useState({});
  const [requestAccordionState, setRequestAccordionState] = useState({});
  const itemsPerPage = 5;
  const [requestCurrentPage, setRequestCurrentPage] = useState(0);
  const [sharedResourcePageCount, setSharedResourcePageCount] = useState();
  const [requestPageCount, setRequestPageCount] = useState();
  const [showConfirmModal, setShowConfirmModal] = useState(false);
  const [deleteDatashareReqInfo, setDeleteDatashareReqInfo] = useState({});
  const [
    showDatashareRequestDeleteConfirmModal,
    setShowDatashareRequestDeleteConfirmModal
  ] = useState(false);
  const [showDeleteDatashareModal, setShowDeleteDatashareModal] =
    useState(false);
  const [serviceDef, setServiceDef] = useState();
  const [serviceDetails, setService] = useState({});
  const [datashareRequestTotalCount, setDatashareRequestTotalCount] =
    useState(0);
  const [resourceSearchFilterParams, setResourceSearchFilterParams] = useState(
    []
  );
  const [showModal, policyConditionState] = useState(false);
  const fetchIdRef = useRef(0);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [sharedResourceListData, setSharedResourceListData] = useState([]);
  const [entries, setEntries] = useState([]);
  const [resourceUpdateTable, setResourceUpdateTable] = useState(moment.now());
  const toggleConfirmModalForDatashareDelete = () => {
    setShowDeleteDatashareModal(true);
  };
  const [sharedResource, setSharedResource] = useState();
  const toggleConfirmModalClose = () => {
    setShowConfirmModal(false);
  };
  const [showAddResourceModal, setShowAddResourceModal] = useState(false);
  const [isEditSharedResourceModal, setIsEditSharedResourceModal] =
    useState(false);
  const [resourceModalUpdateTable, setResourceModalUpdateTable] = useState(
    moment.now()
  );
  const [datashareNameEditable, isDatashareNameEditable] = useState(false);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [requestActiveKey, setRequestActiveKey] = useState("All");
  const [requestSearchFilterParams, setRequestSearchFilterParams] = useState(
    []
  );
  const [shareStatusMetrics, setShareStatusMetrics] = useState({
    totalCount: 0,
    REQUESTED: 0,
    GRANTED: 0,
    ACTIVE: 0,
    DENIED: 0
  });
  const [accessTypeOptions, setAccessTypeOptions] = useState([]);
  const [accessType, setAccessType] = useState([]);
  const [cancel, setCancel] = useState(true);
  let cancelFlag = false;
  let saveFlag = false;
  const [showrowmodal, setShowRowModal] = useState(false);
  const [rowdata, setRowData] = useState([]);
  const handleClosed = () => setShowRowModal(false);
  const [searchHistoryFilterParams, setSearchHistoryFilterParams] = useState(
    []
  );
  const [historyListData, setHistoryListData] = useState([]);
  const [historyEntries, setHistoryEntries] = useState([]);
  const [historyLoader, setHistoryLoader] = useState(false);
  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );

  const fetchDatashareSummary = async () => {
    setLoader(false);
    let resp = [];
    let params = { ...searchFilterParams };
    params["dataShareId"] = dataShareId;
    try {
      resp = await fetchApi({
        url: "gds/datashare/summary",
        params: params
      });
      return resp.data.list[0].permissionForCaller;
    } catch (error) {
      serverError(error);
      console.error(
        `Error occurred while fetching Datashare summary! ${error}`
      );
    }
    setLoader(false);
  };

  useEffect(() => {
    fetchDatashareInfo(datashareId);
  }, []);

  const historySearchFilterOptions = [
    {
      category: "owner",
      label: "User",
      urlLabel: "owner",
      type: "text"
    },
    {
      category: "startDate",
      label: "Start Date",
      urlLabel: "startDate",
      type: "date"
    }
  ];

  const fetchShareStatusMetrics = async (requestSearchFilterOptions) => {
    try {
      setLoader(true);
      let requestList = [];
      let params =
        requestSearchFilterParams != undefined
          ? { ...requestSearchFilterOptions }
          : {};
      params["pageSize"] = 999999999;
      params["dataShareId"] = datashareId;
      try {
        let resp = await fetchApi({
          url: "gds/dataset/summary",
          params: params
        });
        if (resp.data.list.length > 0) {
          requestList = resp.data.list;
          requestList?.forEach((dataset) => {
            for (let i = 0; i < dataset.dataShares.length; i++) {
              if (dataset.dataShares[i].dataShareId == datashareId) {
                dataset.shareStatus = dataset.dataShares[i].shareStatus;
                dataset.requestId = dataset.dataShares[i].id;
                dataset.dataShareName = dataset.dataShares[i].dataShareName;
                dataset.approver = dataset.dataShares[i].approver;
                break;
              }
            }
          });
        }
      } catch (error) {
        serverError(error);
        console.error(
          `Error occurred while fetching Dataset request list! ${error}`
        );
      }

      let activeCount = 0;
      let requestedCount = 0;
      let grantedCount = 0;
      let deniedCount = 0;
      requestList.forEach((request) => {
        switch (request.shareStatus) {
          case "REQUESTED":
            requestedCount += 1;
            break;
          case "GRANTED":
            grantedCount += 1;
            break;
          case "ACTIVE":
            activeCount += 1;
            break;
          case "DENIED":
            deniedCount += 1;
            break;
        }
      });
      setShareStatusMetrics({
        totalCount: requestList.length,
        REQUESTED: requestedCount,
        GRANTED: grantedCount,
        ACTIVE: activeCount,
        DENIED: deniedCount
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching dataset request metrics ! ${error}`
      );
    }
    setLoader(false);
  };

  const onAccessTypeChange = (event, input) => {
    setAccessType(event);
    showSaveCancelButton(true);
    input.onChange(event);
  };

  const handleTabSelect = (key) => {
    if (saveCancelButtons == true) {
      setShowConfirmModal(true);
    } else {
      if (key == "sharedWith") {
        fetchShareStatusMetrics();
      }
      setActiveKey(key);
    }
  };

  const DSpolicyConditions = [
    {
      itemId: 1,
      name: "expression",
      evaluator:
        "org.apache.ranger.plugin.conditionevaluator.RangerScriptConditionEvaluator",
      evaluatorOptions: { engineName: "JavaScript", "ui.isMultiline": "true" },
      label: "Enter boolean expression",
      description: "Boolean expression"
    }
  ];

  const fetchDatashareInfo = async (datashareId) => {
    let datashareResp = {};
    let serviceResp = [];
    try {
      setLoader(true);
      datashareResp = await fetchApi({
        url: `gds/datashare/${datashareId}`
      });
      serviceResp = await fetchApi({
        url: `plugins/services/name/${datashareResp.data.service}`
      });
      const serviceDefs = getServiceDef();
      let serviceDef = serviceDefs?.allServiceDefs?.find((servicedef) => {
        return servicedef.name == serviceResp.data.type;
      });
      setServiceDef(serviceDef);
      setAccessTypeOptions(
        serviceDef.accessTypes.map(({ label, name: value }) => ({
          label,
          value
        }))
      );
    } catch (error) {
      setLoader(false);
      console.error(
        `Error occurred while fetching datashare details ! ${error}`
      );
    }
    if (datashareResp?.data?.conditionExpr !== undefined) {
      datashareResp.data.conditions = {
        expression: datashareResp.data.conditionExpr
      };
    }
    setDatashareConditionExpr(datashareResp.data.conditionExpr);
    setAccessType(
      datashareResp.data.defaultAccessTypes?.map((item) => ({
        label: capitalizeFirstLetter(item),
        value: item
      }))
    );
    setDatashareName(datashareResp.data.name);
    setService(serviceResp.data);
    setDatashareInfo(datashareResp.data);
    setDatashareDescription(datashareResp.data.description);
    setDatashareTerms(datashareResp.data.termsOfUse);
    if (datashareResp.data.acl != undefined)
      setPrincipleAccordianData(datashareResp.data.acl);
    setLoader(false);
  };

  const toggleConditionModalClose = () => {
    setShowConditionModal(false);
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
    setFilteredUserList([...filteredUserList, ...tempUserList]);
    setGroupList([...groupList, ...tempGroupList]);
    setFilteredGroupList([...filteredGroupList, ...tempGroupList]);
    setRoleList([...roleList, ...tempRoleList]);
    setFilteredRoleList([...filteredRoleList, ...tempRoleList]);
  };

  const fetchSharedResourceForDatashare = async (
    searchFilter,
    currentPage,
    getCompleteList
  ) => {
    try {
      let params = { ...searchFilter };
      let itemPerPageCount = getCompleteList ? 999999999 : itemsPerPage;
      params["pageSize"] = itemPerPageCount;
      params["page"] = currentPage;
      params["startIndex"] = currentPage * itemPerPageCount;
      params["dataShareId"] = datashareId;
      setResourceContentLoader(true);
      const resp = await fetchApi({
        url: `gds/resource`,
        params: params
      });
      setResourceContentLoader(false);
      let accordianState = {};
      resp.data.list.map(
        (item) =>
          (accordianState = { ...accordianState, ...{ [item.id]: false } })
      );
      setResourceAccordionState(accordianState);
      setSharedResourcePageCount(
        Math.ceil(resp.data.totalCount / itemPerPageCount)
      );
      if (!getCompleteList) {
        setSharedResources(resp.data.list);
      }
      return resp.data.list;
    } catch (error) {
      setResourceContentLoader(false);
      console.error(
        `Error occurred while fetching shared resource details ! ${error}`
      );
    }
  };

  const handleRequestPageClick = ({ selected }) => {
    setRequestCurrentPage(selected);
    fetchDatashareRequestList(undefined, selected, false);
  };

  const fetchDatashareRequestList = async (
    searchFilter,
    currentPage,
    getCompleteList
  ) => {
    try {
      let params = { ...searchFilter };
      let itemPerPageCount = getCompleteList ? 999999999 : itemsPerPage;
      params["pageSize"] = itemPerPageCount;
      params["page"] = currentPage;
      params["startIndex"] = currentPage * itemPerPageCount;
      params["dataShareId"] = datashareId;
      setRequestContentLoader(true);
      const resp = await fetchApi({
        url: `gds/datashare/dataset`,
        params: params
      });
      setRequestContentLoader(false);
      let accordianState = {};
      setRequestAccordionState(accordianState);
      setRequestPageCount(Math.ceil(resp.data.totalCount / itemPerPageCount));
      if (!getCompleteList) {
        setDataShareRequestsList(resp.data.list);
      }
      setDatashareRequestTotalCount(resp.data.totalCount);
      return resp.data.list;
    } catch (error) {
      setRequestContentLoader(false);
      console.error(
        `Error occurred while fetching Datashare requests details ! ${error}`
      );
    }
  };

  const datashareDescriptionChange = (event) => {
    setDatashareDescription(event.target.value);
    showSaveCancelButton(true);
  };

  const datashareTermsAndConditionsChange = (event) => {
    setDatashareTerms(event.target.value);
    showSaveCancelButton(true);
  };

  const toggleConfirmModalForDelete = (id, name) => {
    setConfirmDeleteModal({
      sharedResourceDetails: { shareId: id, shareName: name },
      showPopup: true
    });
  };

  const toggleAddResourceModalClose = () => {
    setShowAddResourceModal(false);
  };

  const toggleClose = () => {
    setConfirmDeleteModal({
      sharedResourceDetails: {},
      showPopup: false
    });
    setShowDeleteDatashareModal(false);
  };

  const handleSharedResourceDeleteClick = async (shareId) => {
    toggleClose();
    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/resource/${shareId}`,
        method: "DELETE"
      });
      setBlockUI(false);
      toast.success(" Success! Shared resource deleted successfully");
      setResourceUpdateTable(moment.now());
    } catch (error) {
      setBlockUI(false);
      let errorMsg = "Failed to delete Shared resource  : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error(
        "Error occurred during deleting Shared resource  : " + error
      );
    }
  };

  const handleDataChange = (userList, groupList, roleList) => {
    setUserList(userList);
    setGroupList(groupList);
    setRoleList(roleList);
    showSaveCancelButton(true);
  };

  const redirectToDatasetDetailView = (datasetId) => {
    navigate(`/gds/dataset/${datasetId}/detail`);
  };

  const handleSubmit = () => {};

  const onRequestAccordionChange = (id) => {
    setRequestAccordionState({
      ...requestAccordionState,
      ...{ [id]: !requestAccordionState[id] }
    });
  };

  const updateDatashareDetails = async () => {
    datashareInfo.name = datashareName;
    datashareInfo.description = datashareDescription;
    datashareInfo.termsOfUse = datashareTerms;
    datashareInfo.conditionExpr = datashareConditionExpr;
    datashareInfo.defaultAccessTypes = [];

    if (datashareName.length > 512) {
      toast.error("Datashare name must be 512 characters or less!");
      return;
    }

    accessType?.forEach((access) =>
      datashareInfo.defaultAccessTypes.push(access.value)
    );

    datashareInfo.acl = { users: {}, groups: {}, roles: {} };

    userList.forEach((user) => {
      datashareInfo.acl.users[user.name] = user.perm;
    });

    groupList.forEach((group) => {
      datashareInfo.acl.groups[group.name] = group.perm;
    });

    roleList.forEach((role) => {
      datashareInfo.acl.roles[role.name] = role.perm;
    });

    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/datashare/${datashareId}`,
        method: "put",
        data: datashareInfo,
        skipNavigate: true
      });
      toast.success("Datashare updated successfully!!");
    } catch (error) {
      serverError(error);
      console.error(`Error occurred while updating datashare  ${error}`);
      setBlockUI(false);
      return;
    }
    isDatashareNameEditable(false);
    showSaveCancelButton(false);
    saveFlag = true;
    setBlockUI(false);
    setShowConfirmModal(false);
  };

  const removeChanges = () => {
    fetchDatashareInfo(datashareId);
    cancelFlag = true;
    setCancel(true);
    showSaveCancelButton(false);
    setShowConfirmModal(false);
    isDatashareNameEditable(false);
    setShowDatashareRequestDeleteConfirmModal(false);
  };

  const toggleRequestDeleteModal = (id, datashareId, name, status) => {
    let deleteMsg = "";
    if (status == "ACTIVE") {
      deleteMsg = `Do you want to remove Dataset ${datashareId} from ${datashareInfo?.name}`;
    } else {
      deleteMsg = `Do you want to delete request of Dataset ${datashareId}`;
    }
    let data = { id: id, name: name, status: status, msg: deleteMsg };
    setDeleteDatashareReqInfo(data);
    setShowDatashareRequestDeleteConfirmModal(true);
  };

  const toggleDatashareRequestDelete = () => {
    setShowDatashareRequestDeleteConfirmModal(false);
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
      fetchDatashareRequestList(undefined, requestCurrentPage, false);
    } catch (error) {
      let errorMsg = "";
      if (deleteDatashareReqInfo.status == "ACTIVE") {
        errorMsg = "Failed to remove datashare from dataset ";
      } else {
        errorMsg = "Failed to delete datashare request ";
      }
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error(
        "Error occurred during deleting Datashare request  : " + error
      );
    }
    setLoader(false);
  };

  const handleDatashareDeleteClick = async () => {
    toggleClose();
    try {
      let params = {};
      params["forceDelete"] = true;
      setBlockUI(true);
      await fetchApi({
        url: `gds/datashare/${datashareId}`,
        method: "DELETE",
        params: params
      });
      setBlockUI(false);
      toast.success(" Success! Datashare deleted successfully");
      navigate("/gds/mydatasharelisting");
    } catch (error) {
      setBlockUI(false);
      let errorMsg = "Failed to delete datashare : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error("Error occurred during deleting datashare : " + error);
    }
  };

  const copyURL = () => {
    navigator.clipboard.writeText(window.location.href).then(() => {
      toast.success("URL copied!!");
    });
  };

  const navigateToFullView = () => {
    navigate(`/gds/datashare/${datashareId}/fullview`, {
      userAclPerm: userAclPerm,
      datashareNamee: datashareName
    });
  };

  const downloadJsonFile = async () => {
    let jsonData = datashareInfo;
    jsonData.resources = await fetchSharedResourceForDatashare(
      undefined,
      0,
      true
    );
    jsonData.datasets = await fetchDatashareRequestList(undefined, 0, true);
    const jsonContent = JSON.stringify(jsonData);
    const blob = new Blob([jsonContent], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = datashareInfo.name + ".json";
    a.click();
    URL.revokeObjectURL(url);
  };

  const resourceSearchFilterOptions = [
    {
      category: "resourceContains",
      label: "Resource",
      urlLabel: "resourceContains",
      type: "text"
    }
  ];

  const updateResourceSearchFilter = (filter) => {
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      resourceSearchFilterOptions
    );
    setResourceSearchFilterParams(searchFilterParam);
    setSearchFilterParams(searchFilterParam);
  };

  const requestSearchFilterOptions = [
    {
      category: "datasetNamePartial",
      label: "Name",
      urlLabel: "datasetNamePartial",
      type: "text"
    }
  ];

  const updateRequestSearchFilter = (filter) => {
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      requestSearchFilterOptions
    );
    setRequestSearchFilterParams(searchFilterParam);
    fetchShareStatusMetrics(searchFilterParam);
  };

  const updateHistorySearchFilter = (filter) => {
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      historySearchFilterOptions
    );
    setSearchHistoryFilterParams(searchFilterParam);
  };

  const openOperationalModal = async (row) => {
    setShowRowModal(true);
    setRowData(row);
  };

  const fetchHistoryList = useCallback(
    async ({ pageSize, pageIndex, sortBy }) => {
      setHistoryLoader(true);
      let resp = [];
      let historyList = [];
      let totalCount = 0;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchHistoryFilterParams };
      if (fetchId === fetchIdRef.current) {
        params["pageSize"] = pageSize;
        params["startIndex"] =
          state && state.showLastPage
            ? (state.addPageData.totalPage - 1) * pageSize
            : pageIndex * pageSize;
        if (sortBy.length > 0) {
          params["sortBy"] = getTableSortBy(sortBy);
          params["sortType"] = getTableSortType(sortBy);
        }
        params["objectClassType"] =
          ClassTypes.CLASS_TYPE_RANGER_DATA_SHARE.value;
        params["objectId"] = datashareId;
        try {
          resp = await fetchApi({
            url: "assets/report",
            params: params
          });
          historyList = resp.data.vXTrxLogs;
          totalCount = resp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(
            `Error occurred while fetching Datashare History! ${error}`
          );
        }
        setHistoryListData(historyList);
        setHistoryEntries(resp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setHistoryLoader(false);
      }
    },
    [searchHistoryFilterParams]
  );

  const fetchSharedResourcetList = useCallback(
    async ({ pageSize, pageIndex }) => {
      setResourceContentLoader(true);
      let resp = [];
      let page =
        state && state.showLastPage
          ? state.addPageData.totalPage - 1
          : pageIndex;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchFilterParams };
      if (fetchId === fetchIdRef.current) {
        params["pageSize"] = pageSize;
        params["startIndex"] =
          state && state.showLastPage
            ? (state.addPageData.totalPage - 1) * pageSize
            : pageIndex * pageSize;
        params["dataShareId"] = datashareId;
        try {
          resp = await fetchApi({
            url: "gds/resource",
            params: params
          });
          setSharedResourceListData(resp.data?.list);
          setEntries(resp.data);
          setSharedResourcePageCount(
            Math.ceil(resp.data.totalCount / pageSize)
          );
        } catch (error) {
          console.error(
            `Error occurred while fetching Datashare list! ${error}`
          );
        }
      }
      setResourceContentLoader(false);
    },
    [resourceUpdateTable, searchFilterParams]
  );

  const editSharedResourceModal = (sharedResource) => {
    setSharedResource(sharedResource);
    setResourceModalUpdateTable(moment.now());
    setIsEditSharedResourceModal(true);
    setShowAddResourceModal(true);
  };

  const openAddResourceModal = () => {
    setSharedResource();
    setResourceModalUpdateTable(moment.now());
    setIsEditSharedResourceModal(false);
    setShowAddResourceModal(true);
  };

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "updateTime",
        desc: true
      }
    ],
    []
  );

  const sharedResourceCols = React.useMemo(
    () => [
      {
        Header: "Resource",
        accessor: "resource",
        width: 450,
        disableSortBy: true,
        Cell: ({ row: { original } }) => {
          return (
            <div className="gds-shared-resource">
              {Object.entries(original.resource).map(([key, value]) => {
                console.log(key);
                console.log(value);
                return (
                  <div className="mb-1 form-group row">
                    <Col sm={4}>
                      <span
                        className="form-label fnt-14 text-muted"
                        style={{ textTransform: "capitalize" }}
                      >
                        {key}
                      </span>
                    </Col>
                    <Col sm={8}>
                      <span>{value.values.toString()}</span>
                    </Col>
                  </div>
                );
              })}
            </div>
          );
        }
      },
      {
        Header: "Access Type",
        accessor: "access_conditions",
        disableSortBy: true,
        Cell: ({ row: { original } }) => {
          return (
            <div>
              <div className="gds-chips gap-one-fourth">
                {original.accessTypes?.map((accessObj) => (
                  <span
                    className="badge text-bg-light badge-sm"
                    title={accessObj}
                    key={accessObj}
                  >
                    {capitalizeFirstLetter(accessObj)}
                  </span>
                ))}
              </div>
            </div>
          );
        }
      },
      {
        Header: "",
        accessor: "actions",
        width: 120,
        Cell: ({ row: { original } }) => {
          return (
            <div>
              {(isSystemAdmin() || userAclPerm == "ADMIN") && (
                <div className="d-flex gap-half align-items-start">
                  {(isSystemAdmin() || userAclPerm == "ADMIN") && (
                    <div className="d-flex gap-half align-items-start">
                      <Button
                        variant="outline-dark"
                        size="sm"
                        title="Edit"
                        onClick={() => editSharedResourceModal(original)}
                        data-name="editSharedResource"
                        data-id="editSharedResource"
                      >
                        <i className="fa-fw fa fa-edit"></i>
                      </Button>
                      <Button
                        variant="danger"
                        size="sm"
                        title="Delete"
                        onClick={() =>
                          toggleConfirmModalForDelete(
                            original.id,
                            original.name
                          )
                        }
                        data-name="deletSharedResources"
                        data-id={original.id}
                        data-cy={original.id}
                      >
                        <i className="fa-fw fa fa-trash fa-fw fa fa-large" />
                      </Button>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        },
        disableSortBy: true
      }
    ],
    []
  );

  const onDatashareNameChange = (event) => {
    setDatashareName(event.target.value);
    showSaveCancelButton(true);
  };

  const handleEditClick = () => {
    if (isSystemAdmin() || userAclPerm == "ADMIN") {
      isDatashareNameEditable(true);
      showSaveCancelButton(true);
    }
  };

  const handleRequestTabSelect = (key) => {
    setRequestActiveKey(key);
  };

  const FormChange = (props) => {
    const { isDirtyField, formValues } = props;
    if (isDirtyField && (!cancelFlag || !saveFlag)) {
      setDatashareConditionExpr(props.formValues.conditions?.expression);
      showSaveCancelButton(true);
    }
    return null;
  };

  const isDirtyFieldCheck = (values, initialValues) => {
    let modifiedVal = false;
    if (
      values?.conditions?.expression &&
      !isEqual(values?.conditions?.expression, datashareConditionExpr)
    ) {
      modifiedVal = true;
      saveFlag = false;
      cancelFlag = false;
    }
    return modifiedVal;
  };

  const historyColumns = React.useMemo(
    () => [
      {
        Header: "Time",
        accessor: "createDate",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy h:MM:ss TT");
        },
        width: 170,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "User",
        accessor: "owner",
        width: 650,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "",
        accessor: "actions",
        width: 30,
        Cell: ({ row: { original } }) => {
          return (
            <div className="d-flex gap-half align-items-start">
              <div className="d-flex gap-half align-items-start">
                <Button
                  variant="outline-dark"
                  size="sm"
                  title="showDetails"
                  onClick={() => openOperationalModal(original)}
                  data-name="showDetails"
                  data-id="showDetails"
                >
                  <img src={historyDetailsIcon} height="30px" width="30px" />
                </Button>
              </div>
            </div>
          );
        },
        disableSortBy: true
      }
    ],
    []
  );

  return (
    <>
      <Form
        onSubmit={handleSubmit}
        mutators={{
          ...arrayMutators
        }}
        initialValues={datashareInfo}
        render={({ values, dirty, initialValues }) => (
          <React.Fragment>
            <div
              className={
                saveCancelButtons
                  ? "gds-header-wrapper gap-half pt-2 pb-2"
                  : "gds-header-wrapper gap-half"
              }
            >
              <Button
                variant="light"
                className="border-0 bg-transparent"
                onClick={() => window.history.back()}
                size="sm"
                data-id="back"
                data-cy="back"
              >
                <i className="fa fa-angle-left fa-lg font-weight-bold" />
              </Button>
              <h3 className="gds-header bold">
                <div className="d-flex align-items-center">
                  <span className="me-1">Datashare: </span>
                  {!datashareNameEditable ? (
                    <div>
                      <span
                        title={datashareName}
                        className="text-truncate"
                        style={{ maxWidth: "300px", display: "inline-block" }}
                        onClick={() => handleEditClick()}
                      >
                        {datashareName}
                      </span>
                    </div>
                  ) : (
                    <input
                      type="text"
                      name="shareName"
                      style={{ height: "39px" }}
                      className="form-control"
                      data-cy="shareName"
                      value={datashareName}
                      onChange={onDatashareNameChange}
                    />
                  )}
                </div>
              </h3>
              <h3 className="gds-header bold">
                <span
                  title={datashareInfo?.service}
                  className="text-truncate"
                  style={{ maxWidth: "300px", display: "inline-block" }}
                >
                  Service: {datashareInfo?.service}
                </span>
              </h3>
              {datashareInfo?.zone?.length > 0 && (
                <h3 className="gds-header bold">
                  <span
                    title={datashareInfo?.zone}
                    className="text-truncate"
                    style={{ maxWidth: "300px", display: "inline-block" }}
                  >
                    Zone: {datashareInfo?.zone}
                  </span>
                </h3>
              )}

              {!datashareNameEditable && !saveCancelButtons && (
                <>
                  <CustomBreadcrumb />
                  <span className="pipe" />
                </>
              )}
              {(isSystemAdmin() ||
                userAclPerm == "ADMIN" ||
                userAclPerm == "POLICY_ADMIN") && (
                <div>
                  {saveCancelButtons ? (
                    <div className="gds-header-btn-grp">
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() => removeChanges()}
                        data-id="cancel"
                        data-cy="cancel"
                      >
                        Cancel
                      </Button>
                      <Button
                        variant="primary"
                        onClick={updateDatashareDetails}
                        size="sm"
                        data-id="save"
                        data-cy="save"
                      >
                        Save
                      </Button>
                    </div>
                  ) : (
                    <p></p>
                  )}
                </div>
              )}

              {!datashareNameEditable && !saveCancelButtons && (
                <div>
                  <DropdownButton
                    id="dropdown-item-button"
                    title={<i className="fa fa-ellipsis-v" fontSize="36px" />}
                    size="sm"
                    className="hide-arrow"
                  >
                    <Dropdown.Item
                      as="button"
                      onClick={() => navigateToFullView()}
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
                      data-name="copyDatashareLink"
                      data-id="copyDatashareLink"
                      data-cy="copyDatashareLink"
                    >
                      Copy Datashare Link
                    </Dropdown.Item>
                    <Dropdown.Item
                      as="button"
                      onClick={() => downloadJsonFile()}
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
                        toggleConfirmModalForDatashareDelete();
                      }}
                      data-name="deleteDatashare"
                      data-id="deleteDatashare"
                      data-cy="deleteDatashare"
                    >
                      Delete Datashare
                    </Dropdown.Item>
                  </DropdownButton>
                </div>
              )}
            </div>
            {loader ? (
              <Loader />
            ) : (
              <React.Fragment>
                <div>
                  <Tabs
                    id="DatashareDetailLayout"
                    activeKey={activeKey}
                    onSelect={handleTabSelect}
                  >
                    <Tab eventKey="overview" title="OVERVIEW">
                      {activeKey == "overview" ? (
                        <div>
                          {!cancelFlag && !saveFlag && (
                            <FormChange
                              isDirtyField={
                                dirty == !isEqual(initialValues, values)
                                  ? isDirtyFieldCheck(values, initialValues)
                                  : false
                              }
                              formValues={values}
                            />
                          )}

                          <div className="gds-tab-content gds-content-border px-3">
                            <div className="gds-inline-field-grp">
                              <div className="wrapper">
                                <div
                                  className="gds-left-inline-field"
                                  height="30px"
                                >
                                  <span className="gds-label-color">ID</span>
                                </div>
                                <div line-height="30px">{datashareInfo.id}</div>
                              </div>
                              <div className="wrapper">
                                <div
                                  className="gds-left-inline-field pl-1 fnt-14"
                                  height="30px"
                                >
                                  <span className="gds-label-color">
                                    Date Updated
                                  </span>
                                </div>
                                <div className="fnt-14" line-height="30px">
                                  {dateFormat(
                                    datashareInfo?.updateTime,
                                    "mm/dd/yyyy hh:MM:ss TT"
                                  )}
                                </div>
                              </div>

                              <div className="wrapper">
                                <div
                                  className="gds-left-inline-field pl-1 fnt-14"
                                  line-height="30px"
                                >
                                  <span className="gds-label-color">
                                    Date Created
                                  </span>
                                </div>
                                <div className="fnt-14" line-height="30px">
                                  {dateFormat(
                                    datashareInfo?.createTime,
                                    "mm/dd/yyyy hh:MM:ss TT"
                                  )}
                                </div>
                              </div>
                            </div>
                            <div>
                              <div className="fnt-14 pl-1">
                                <span className="gds-label-color">
                                  Description
                                </span>
                              </div>
                            </div>
                            <div>
                              <div>
                                <textarea
                                  placeholder="Datashare Description"
                                  className="form-control gds-description pl-1"
                                  id="description"
                                  data-cy="description"
                                  onChange={datashareDescriptionChange}
                                  value={datashareDescription}
                                  readOnly={
                                    !isSystemAdmin() && userAclPerm != "ADMIN"
                                  }
                                  rows={5}
                                />
                              </div>
                            </div>
                          </div>

                          <div className="gds-action-card mb-5 gds-tab-content gds-content-border px-3">
                            <div
                              className={
                                values.conditions == undefined
                                  ? "gds-section-title border-0 p-0"
                                  : "gds-section-title"
                              }
                            >
                              <p className="gds-card-heading">
                                Default Condition
                              </p>
                              {isSystemAdmin() || userAclPerm == "ADMIN" ? (
                                <Button
                                  className="btn btn-sm"
                                  onClick={() => {
                                    policyConditionState(true);
                                  }}
                                  data-js="customPolicyConditions"
                                  data-cy="customPolicyConditions"
                                  variant="secondary"
                                >
                                  {values.conditions !== undefined
                                    ? "Modify Condition"
                                    : "Set Condition"}
                                </Button>
                              ) : (
                                <></>
                              )}
                            </div>
                            {values.conditions !== undefined &&
                              Object.keys(values.conditions).map((keyName) => {
                                if (
                                  values.conditions[keyName] != "" &&
                                  values.conditions[keyName] != null
                                ) {
                                  let conditionObj = find(
                                    DSpolicyConditions,
                                    function (m) {
                                      if (m.name == keyName) {
                                        return m;
                                      }
                                    }
                                  );
                                  return (
                                    <div className="pt-3">
                                      {isObject(values.conditions[keyName]) ? (
                                        <div>
                                          <span className="fnt-14">
                                            {values.conditions[keyName].length >
                                            1
                                              ? values.conditions[keyName].map(
                                                  (m) => {
                                                    return ` ${m.label} `;
                                                  }
                                                )
                                              : values.conditions[keyName]
                                                  .label}
                                          </span>
                                        </div>
                                      ) : (
                                        <div>
                                          <span className="fnt-14">
                                            {values.conditions[keyName]}
                                          </span>
                                        </div>
                                      )}
                                    </div>
                                  );
                                }
                              })}
                          </div>

                          {showModal && (
                            <Field
                              className="form-control"
                              name="conditions"
                              render={({ input }) => (
                                <PolicyConditionsComp
                                  policyConditionDetails={policyConditionUpdatedJSON(
                                    DSpolicyConditions
                                  )}
                                  inputVal={input}
                                  showModal={showModal}
                                  handleCloseModal={policyConditionState}
                                />
                              )}
                            />
                          )}

                          <div className="gds-action-card mb-5 gds-tab-content gds-content-border px-3">
                            <div className="gds-section-title">
                              <p className="gds-card-heading">
                                Default access types
                              </p>
                            </div>
                            <div className="gds-flex mg-b-10 mg-t-20">
                              <div className="w-100">
                                <Field
                                  name={`defaultAccessTypes`}
                                  render={({ input, meta }) => (
                                    <div>
                                      <Select
                                        {...input}
                                        className="w-100"
                                        options={accessTypeOptions}
                                        onChange={(e) =>
                                          onAccessTypeChange(e, input)
                                        }
                                        menuPortalTarget={document.body}
                                        value={accessType}
                                        isDisabled={
                                          !isSystemAdmin() &&
                                          userAclPerm != "ADMIN"
                                        }
                                        menuPlacement="auto"
                                        placeholder="All Permissions"
                                        isClearable
                                        isMulti
                                      />
                                    </div>
                                  )}
                                />
                              </div>
                            </div>
                          </div>

                          {(isSystemAdmin() || userAclPerm != "VIEW") && (
                            <PrinciplePermissionComp
                              userList={userList}
                              groupList={groupList}
                              roleList={roleList}
                              isAdmin={
                                isSystemAdmin() || userAclPerm == "ADMIN"
                                  ? true
                                  : false
                              }
                              isDetailView={true}
                              onDataChange={handleDataChange}
                              type="datashare"
                            />
                          )}
                        </div>
                      ) : (
                        <div></div>
                      )}
                    </Tab>
                    <Tab eventKey="resources" title="RESOURCES">
                      {activeKey == "resources" ? (
                        <div className="gds-content-border gds-request-content">
                          <div className="mb-3">
                            <div className="w-100 d-flex gap-1 mb-3 mg-t-20">
                              <StructuredFilter
                                key="shared-reource-search-filter"
                                placeholder="Search resources..."
                                options={resourceSearchFilterOptions}
                                onChange={updateResourceSearchFilter}
                                defaultSelected={[]}
                              />
                              {(isSystemAdmin() || userAclPerm == "ADMIN") && (
                                <>
                                  <Button
                                    variant="primary"
                                    size="sm"
                                    onClick={() => openAddResourceModal()}
                                  >
                                    Add Resource
                                  </Button>
                                </>
                              )}
                            </div>
                            <XATableLayout
                              data={sharedResourceListData}
                              columns={sharedResourceCols}
                              fetchData={fetchSharedResourcetList}
                              totalCount={entries && entries.totalCount}
                              loading={resourceContentLoader}
                              pageCount={sharedResourcePageCount}
                              getRowProps={(row) => ({
                                onClick: (e) => {
                                  e.stopPropagation();
                                }
                              })}
                              columnHide={false}
                              columnResizable={false}
                              columnSort={true}
                              defaultSort={getDefaultSort}
                            />
                          </div>
                        </div>
                      ) : (
                        <div></div>
                      )}
                    </Tab>
                    <Tab eventKey="sharedWith" title="SHARED WITH">
                      {activeKey == "sharedWith" ? (
                        <div className="gds-tab-content">
                          <div>
                            <div className="usr-grp-role-search-width mb-4">
                              <StructuredFilter
                                key="request-listing-search-filter"
                                placeholder="Search dataset..."
                                options={requestSearchFilterOptions}
                                onChange={updateRequestSearchFilter}
                              />
                            </div>
                          </div>
                          <div>
                            <div className="usr-grp-role-search-width">
                              <Tabs
                                id="datashareRequestTab"
                                className="mg-b-10"
                                activeKey={requestActiveKey}
                                onSelect={handleRequestTabSelect}
                              >
                                <Tab
                                  eventKey="All"
                                  title={
                                    "All (" +
                                    shareStatusMetrics.totalCount +
                                    ")"
                                  }
                                >
                                  {requestActiveKey == "All" && (
                                    <DatashareInDatasetListComp
                                      id={Number(datashareId)}
                                      type="datashare"
                                      setUpdateTable={setUpdateTable}
                                      updateTable={updateTable}
                                      userAclPerm={userAclPerm}
                                      searchFilter={requestSearchFilterParams}
                                      fetchShareStatusMetrics={
                                        fetchShareStatusMetrics
                                      }
                                    />
                                  )}
                                </Tab>
                                <Tab
                                  eventKey="Active"
                                  title={
                                    "Active (" + shareStatusMetrics.ACTIVE + ")"
                                  }
                                >
                                  {requestActiveKey == "Active" && (
                                    <DatashareInDatasetListComp
                                      id={Number(datashareId)}
                                      type="datashare"
                                      shareStatus="ACTIVE"
                                      setUpdateTable={setUpdateTable}
                                      updateTable={updateTable}
                                      userAclPerm={userAclPerm}
                                      searchFilter={requestSearchFilterParams}
                                      fetchShareStatusMetrics={
                                        fetchShareStatusMetrics
                                      }
                                    />
                                  )}
                                </Tab>
                                <Tab
                                  eventKey="Requested"
                                  title={
                                    "Requested (" +
                                    shareStatusMetrics.REQUESTED +
                                    ")"
                                  }
                                >
                                  {requestActiveKey == "Requested" && (
                                    <DatashareInDatasetListComp
                                      id={Number(datashareId)}
                                      type="datashare"
                                      shareStatus="REQUESTED"
                                      setUpdateTable={setUpdateTable}
                                      updateTable={updateTable}
                                      userAclPerm={userAclPerm}
                                      searchFilter={requestSearchFilterParams}
                                      fetchShareStatusMetrics={
                                        fetchShareStatusMetrics
                                      }
                                    />
                                  )}
                                </Tab>
                                <Tab
                                  eventKey="Granted"
                                  title={
                                    "Granted (" +
                                    shareStatusMetrics.GRANTED +
                                    ")"
                                  }
                                >
                                  {requestActiveKey == "Granted" && (
                                    <DatashareInDatasetListComp
                                      id={Number(datashareId)}
                                      type="datashare"
                                      shareStatus="GRANTED"
                                      setUpdateTable={setUpdateTable}
                                      updateTable={updateTable}
                                      userAclPerm={userAclPerm}
                                      searchFilter={requestSearchFilterParams}
                                      fetchShareStatusMetrics={
                                        fetchShareStatusMetrics
                                      }
                                    />
                                  )}
                                </Tab>
                                <Tab
                                  eventKey="Denied"
                                  title={
                                    "Denied (" + shareStatusMetrics.DENIED + ")"
                                  }
                                >
                                  {requestActiveKey == "Denied" && (
                                    <DatashareInDatasetListComp
                                      id={Number(datashareId)}
                                      type="datashare"
                                      shareStatus="DENIED"
                                      setUpdateTable={setUpdateTable}
                                      updateTable={updateTable}
                                      userAclPerm={userAclPerm}
                                      searchFilter={requestSearchFilterParams}
                                      fetchShareStatusMetrics={
                                        fetchShareStatusMetrics
                                      }
                                    />
                                  )}
                                </Tab>
                              </Tabs>
                            </div>
                          </div>
                        </div>
                      ) : (
                        <div></div>
                      )}
                    </Tab>

                    {(isSystemAdmin() ||
                      userAclPerm == "ADMIN" ||
                      userAclPerm == "AUDIT") && (
                      <Tab eventKey="history" title="HISTORY">
                        {activeKey == "history" && (
                          <div className="gds-content-border gds-request-content">
                            <div className="mb-3">
                              <div className="usr-grp-role-search-width mb-3 mg-t-20">
                                <StructuredFilter
                                  key="dataset-history-search-filter"
                                  placeholder="Search..."
                                  onChange={updateHistorySearchFilter}
                                  options={historySearchFilterOptions}
                                />
                              </div>
                              <div className="gds-header-btn-grp"></div>
                            </div>
                            <XATableLayout
                              data={historyListData}
                              columns={historyColumns}
                              fetchData={fetchHistoryList}
                              totalCount={
                                historyEntries && historyEntries.totalCount
                              }
                              loading={historyLoader}
                              pageCount={pageCount}
                              columnHide={false}
                              columnResizable={false}
                              columnSort={true}
                              defaultSort={getDefaultSort}
                            />
                          </div>
                        )}
                      </Tab>
                    )}

                    <Tab eventKey="termsOfUse" title="TERMS OF USE">
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
                              onChange={datashareTermsAndConditionsChange}
                              value={datashareTerms}
                              readOnly={
                                !isSystemAdmin() && userAclPerm != "ADMIN"
                              }
                              rows={16}
                            />
                          </div>
                        </div>
                      </div>
                    </Tab>
                  </Tabs>
                </div>

                <Modal show={confirmDeleteModal.showPopup} onHide={toggleClose}>
                  <Modal.Header closeButton>
                    <span className="text-word-break">
                      Are you sure you want to delete shared resource &nbsp;"
                      <b>
                        {confirmDeleteModal?.sharedResourceDetails?.shareName}
                      </b>
                      " ?
                    </span>
                  </Modal.Header>
                  <Modal.Footer>
                    <Button variant="secondary" size="sm" onClick={toggleClose}>
                      Cancel
                    </Button>
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={() =>
                        handleSharedResourceDeleteClick(
                          confirmDeleteModal.sharedResourceDetails.shareId
                        )
                      }
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal>

                <Modal
                  show={showConditionModal}
                  onHide={toggleConditionModalClose}
                >
                  <Modal.Header closeButton>
                    <h3 className="gds-header bold">Conditions</h3>
                  </Modal.Header>
                  <Modal.Body>
                    <div className="p-1">
                      <div className="gds-inline-field-grp">
                        <div className="wrapper">
                          <div className="gds-left-inline-field" height="30px">
                            Boolean Expression :
                          </div>
                          <div line-height="30px">
                            {conditionModalData?.conditionExpr != undefined
                              ? conditionModalData.conditionExpr
                              : ""}
                          </div>
                        </div>
                        <div className="wrapper">
                          <div className="gds-left-inline-field" height="30px">
                            Access Type :
                          </div>
                          <div line-height="30px">
                            {conditionModalData?.accessTypes != undefined
                              ? conditionModalData.accessTypes.toString()
                              : ""}
                          </div>
                        </div>
                        {false && (
                          <div className="wrapper">
                            <div
                              className="gds-left-inline-field"
                              height="30px"
                            >
                              Row Filter :
                            </div>
                            <div line-height="30px">
                              {conditionModalData?.rowFilter != undefined
                                ? conditionModalData.rowFilter.filterExpr
                                : ""}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </Modal.Body>
                  <Modal.Footer>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={toggleConditionModalClose}
                    >
                      Close
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
                      onClick={updateDatashareDetails}
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal>

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

                <Modal show={showDeleteDatashareModal} onHide={toggleClose}>
                  <Modal.Header closeButton>
                    <span className="text-word-break">
                      Are you sure you want to delete datashare&nbsp;"
                      <b>{datashareInfo?.name}</b>" ?
                    </span>
                  </Modal.Header>
                  <Modal.Footer>
                    <Button variant="secondary" size="sm" onClick={toggleClose}>
                      No
                    </Button>
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={() => handleDatashareDeleteClick()}
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal>

                <AddSharedResourceComp
                  datashareId={datashareId}
                  onToggleAddResourceClose={toggleAddResourceModalClose}
                  sharedResource={sharedResource}
                  datashareInfo={datashareInfo}
                  serviceDef={serviceDef}
                  showModal={showAddResourceModal}
                  setShowModal={setShowAddResourceModal}
                  isEdit={isEditSharedResourceModal}
                  serviceDetails={serviceDetails}
                  setResourceUpdateTable={setResourceUpdateTable}
                  resourceModalUpdateTable={resourceModalUpdateTable}
                />

                <OperationAdminModal
                  show={showrowmodal}
                  data={rowdata}
                  onHide={handleClosed}
                ></OperationAdminModal>
              </React.Fragment>
            )}
            <BlockUi isUiBlock={blockUI} />
          </React.Fragment>
        )}
      />
    </>
  );
};

export default DatashareDetailLayout;
