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
  useReducer,
  useRef
} from "react";
import withRouter from "Hooks/withRouter";
import { fetchApi } from "../../../utils/fetchAPI";
import dateFormat from "dateformat";
import XATableLayout from "../../../components/XATableLayout";
import { ClassTypes } from "../../../utils/XAEnums";
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
import {
  useParams,
  useNavigate,
  useLocation,
  useSearchParams
} from "react-router-dom";
import {
  getTableSortBy,
  getTableSortType,
  serverError,
  isSystemAdmin,
  parseSearchFilter
} from "../../../utils/XAUtils";
import Select from "react-select";
import userColourIcon from "../../../images/user-colour.svg";
import groupColourIcon from "../../../images/group-colour.svg";
import roleColourIcon from "../../../images/role-colour.svg";
import historyDetailsIcon from "../../../images/history-details.svg";
import arrayMutators from "final-form-arrays";
import { groupBy, isEmpty, isArray } from "lodash";
import PrinciplePermissionComp from "./PrinciplePermissionComp";
import ReactPaginate from "react-paginate";
import CustomBreadcrumb from "../../CustomBreadcrumb";
import ErrorPage from "../../../views/ErrorPage";
import DatashareInDatasetListComp from "./DatashareInDatasetListComp";
import OperationAdminModal from "../../AuditEvent/OperationAdminModal";

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
  const { state } = useLocation();
  const [userAclPerm, setUserAclPerm] = useState(state?.userAclPerm);
  const toastId = React.useRef(null);
  const [loader, setLoader] = useState(true);
  const [datasetInfo, setDatasetInfo] = useState({});
  const [datasetName, setDatasetName] = useState();
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
  const [userList, setUserList] = useState([]);
  const [groupList, setGroupList] = useState([]);
  const [roleList, setRoleList] = useState([]);
  const [activeKey, setActiveKey] = useState("overview");
  const [requestActiveKey, setRequestActiveKey] = useState("All");
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
  const [datashareCurrentPage, setDatashareCurrentPage] = useState(0);
  const [datasharePageCount, setDatasharePageCount] = useState();
  const [requestAccordionState, setRequestAccordionState] = useState({});
  const itemsPerPage = 5;
  const datashareItemsPerPage = 10;
  const [showActivateRequestModal, setShowActivateRequestModal] =
    useState(false);
  const [datashareInfo, setDatashareInfo] = useState();
  const [datashareRequestInfo, setDatashareRequestInfo] = useState();
  const [completeDatashareRequestsList, setCompleteDatashareRequestsList] =
    useState([]);
  const [datashareTotalCount, setDatashareTotalCount] = useState(0);
  const [datashareRequestTotalCount, setDatashareRequestTotalCount] =
    useState(0);
  const toggleConfirmModalForDatasetDelete = () => {
    setShowDeleteDatasetModal(true);
  };
  const [loadRequestListDataCounter, setLoadRequestListDataCounter] = useState(
    Math.random()
  );
  const [updateTable, setUpdateTable] = useState(moment.now());
  const gdsServiceDefName = "gds";
  const [datasetNameEditable, isDatasetNameEditable] = useState(false);
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
  const [searchHistoryFilterParams, setSearchHistoryFilterParams] = useState(
    []
  );
  const fetchIdRef = useRef(0);
  const [historyListData, setHistoryListData] = useState([]);
  const [historyLoader, setHistoryLoader] = useState(false);
  const [entries, setEntries] = useState([]);
  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );
  const [showrowmodal, setShowRowModal] = useState(false);
  const [rowdata, setRowData] = useState([]);
  const handleClosed = () => setShowRowModal(false);

  const fetchShareStatusMetrics = async (requestSearchFilterParams) => {
    try {
      setLoader(true);
      let requestList = [];
      let params =
        requestSearchFilterParams != undefined
          ? { ...requestSearchFilterParams }
          : {};
      params["pageSize"] = 999999999;
      params["datasetId"] = datasetId;
      try {
        let resp = await fetchApi({
          url: "gds/datashare/summary",
          params: params
        });
        if (resp.data.list.length > 0) {
          requestList = resp.data.list;
          requestList?.forEach((datashare) => {
            for (let i = 0; i < datashare.datasets.length; i++) {
              if (datashare.datasets[i].datasetId == datasetId) {
                datashare.shareStatus = datashare.datasets[i].shareStatus;
                datashare.requestId = datashare.datasets[i].id;
                datashare.datasetName = datashare.datasets[i].datasetName;
                break;
              }
            }
          });
        }
      } catch (error) {
        serverError(error);
        console.error(
          `Error occurred while fetching Datashare request metrics! ${error}`
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
        `Error occurred while fetching dataset summary details ! ${error}`
      );
    }
    setLoader(false);
  };

  const handleDatasetDeleteClick = async () => {
    toggleClose();
    try {
      let params = {};
      params["forceDelete"] = true;
      setBlockUI(true);
      await fetchApi({
        url: `gds/dataset/${datasetId}`,
        method: "DELETE",
        params: params
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

  const getServiceDefData = async () => {
    let data = null;
    let resp = {};
    try {
      resp = await fetchApi({
        url: `plugins/definitions/name/${gdsServiceDefName}`
      });
      setServiceDef(resp.data);
    } catch (error) {
      console.error(
        `Error occurred while fetching Service Definition or CSRF headers! ${error}`
      );
    }
    return data;
  };

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

  const updateHistorySearchFilter = (filter) => {
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      historySearchFilterOptions
    );
    setSearchHistoryFilterParams(searchFilterParam);
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
        params["objectClassType"] = ClassTypes.CLASS_TYPE_RANGER_DATASET.value;
        params["objectId"] = datasetId;
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
            `Error occurred while fetching Dataset History! ${error}`
          );
        }
        setHistoryListData(historyList);
        setEntries(resp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setHistoryLoader(false);
      }
    },
    [searchHistoryFilterParams]
  );

  const openOperationalModal = async (row) => {
    setShowRowModal(true);
    setRowData(row);
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

  const requestSearchFilterOptions = [
    {
      category: "dataShareNamePartial",
      label: "Name",
      urlLabel: "dataShareNamePartial",
      type: "text"
    },
    {
      category: "serviceNamePartial",
      label: "Service",
      urlLabel: "serviceNamePartial",
      type: "text"
    },
    {
      category: "zoneNamePartial",
      label: "Zone",
      urlLabel: "zoneNamePartial",
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

  useEffect(() => {
    fetchDatasetInfo(datasetId);
    getServiceDefData();
    if (userAclPerm == undefined) fetchDatasetSummary(datasetId);
    if (activeKey == "sharedWith") {
      fetchAccessGrantInfo();
    }
  }, []);

  const fetchDatasetSummary = async (datasetId) => {
    try {
      let params = {};
      params["datasetId"] = datasetId;
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/dataset/summary`,
        params: params
      });
      setLoader(false);
      setUserAclPerm(resp.data.list[0].permissionForCaller);
    } catch (error) {
      setLoader(false);
      if (error.response.status == 401 || error.response.status == 400) {
        <ErrorPage errorCode="401" />;
      }
      console.error(
        `Error occurred while fetching dataset summary details ! ${error}`
      );
    }
  };

  const fetchDatashareRequestList = async (
    datashareName,
    currentPage,
    getCompleteList
  ) => {
    try {
      let params = {};
      let itemPerPageCount = getCompleteList ? 999999999 : itemsPerPage;
      params["pageSize"] = itemPerPageCount;
      params["page"] = currentPage;
      params["startIndex"] = currentPage * itemPerPageCount;
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
      if (!getCompleteList) {
        setDatashareRequestsList(resp.data.list);
        tabTitle.all = "All (" + resp.data.totalCount + ")";
      } else {
        setCompleteDatashareRequestsList(resp.data.list);
      }
      setDatashareRequestTotalCount(resp.data.totalCount);
      return resp.data.list;
    } catch (error) {
      console.error(
        `Error occurred while fetching Datashare requests details ! ${error}`
      );
    }
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

  const fetchAccessGrantInfo = async () => {
    let policyData = {};
    try {
      const resp = await fetchApi({
        url: `gds/dataset/${datasetId}/policy`
      });
      policyData = resp.data[0];
    } catch (error) {
      console.error(
        `Error occurred while fetching dataset access grant details ! ${error}`
      );
    }
    let grantItems = undefined;
    if (policyData != undefined) {
      grantItems = policyData.policyItems;
      const userMap = new Map();
      const groupMap = new Map();
      const roleMap = new Map();
      grantItems?.forEach((item) => {
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
    }

    return grantItems;
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
      setDatasetName(resp.data.name);
      setDatasetDescription(resp.data.description);
      setDatasetTerms(resp.data.termsOfUse);
      if (resp.data.acl != undefined) setPrincipleAccordianData(resp.data.acl);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      if (error.response.status == 401 || error.response.status == 400) {
        <ErrorPage errorCode="401" />;
      }
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
    fetchDatashareList(undefined, 0);
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
    fetchDatashareList(event.target.value, datashareCurrentPage);
  };

  const submitDatashareRequest = async () => {
    console.log("Selected datasharelist");
    console.log(selectedDatashareList);

    let payloadObj = [];

    for (let i = 0; i < selectedDatashareList.length; i++) {
      let data = {};
      data["datasetId"] = datasetId;
      data["dataShareId"] = selectedDatashareList[i];
      data["status"] = "REQUESTED";
      payloadObj.push(data);
    }

    if (payloadObj.length > 0) {
      try {
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: true
        });
        const createDatasetResp = await fetchApi({
          url: `gds/dataset/${datasetId}/datashare`,
          method: "post",
          data: payloadObj
        });
        dispatch({
          type: "SET_BLOCK_UI",
          blockUI: false
        });
        toast.success("Request created successfully!!");
        setDatashareModal(false);
        fetchShareStatusMetrics();
        setUpdateTable(moment.now());
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
    } else {
      toast.error("Please select a datashare");
    }
  };

  const fetchDatashareList = useCallback(
    async (dataShareSearchObj, datashareCurrentPage) => {
      let resp = [];
      let totalCount = 0;
      let params = {};
      let dataShareName = "";
      params["pageSize"] = datashareItemsPerPage;
      params["page"] = datashareCurrentPage;
      params["startIndex"] = datashareCurrentPage * datashareItemsPerPage;
      //params["dataShareName"] = datasetId;
      if (dataShareSearchObj != undefined) {
        dataShareName = dataShareSearchObj;
      } else {
        dataShareName = datashareSearch;
      }
      try {
        params["dataShareNamePartial"] = dataShareName;
        resp = await fetchApi({
          url: "gds/datashare",
          params: params
        });
        setDatashareList(resp.data.list);
        setDatasharePageCount(
          Math.ceil(resp.data.totalCount / datashareItemsPerPage)
        );
        //totalCount = resp.data.totalCount;
        setDatashareTotalCount(resp.data.totalCount);
        setDatashareModal(true);
      } catch (error) {
        serverError(error);
        console.error(`Error occurred while fetching Datashare list! ${error}`);
      }
    },
    []
  );

  const checkBocChange = (event) => {
    if (
      event.target.checked == true &&
      !selectedDatashareList.includes(event.target.value)
    ) {
      setSelectedDatashareList([...selectedDatashareList, event.target.value]);
    } else if (
      event.target.checked == false &&
      selectedDatashareList.includes(event.target.value)
    ) {
      setSelectedDatashareList(
        selectedDatashareList.filter((item) => item !== event.target.value)
      );
    }
  };

  const handleSubmit = async (formData) => {};

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
    if (saveCancelButtons == true && userAclPerm != "AUDIT") {
      setShowConfirmModal(true);
    } else {
      setActiveKey(key);
      if (key == "sharedWith") {
        fetchAccessGrantInfo();
      } else if (key == "datashares") {
        fetchShareStatusMetrics();
      }
    }
  };

  const handleRequestTabSelect = (key) => {
    setRequestActiveKey(key);
  };

  const updateDatasetAndAccessGrant = async () => {
    updateDatasetDetails();
    updateDatasetAccessGrant();
  };

  const updateDatasetDetails = async () => {
    datasetInfo.name = datasetName;
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
        data: datasetInfo,
        skipNavigate: true
      });
      dispatch({
        type: "SET_BLOCK_UI",
        blockUI: false
      });
      toast.success("Dataset updated successfully!!");
      isDatasetNameEditable(false);
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
    if (userAclPerm != "AUDIT") {
      if (accessGrantData != undefined) {
        setAccessGrantFormValues(accessGrantData);
      }
      if (policyData != undefined) {
        setPolicyData(policyData);
      }
      showSaveCancelButton(true);
    }
  };

  const updateDatasetAccessGrant = async () => {
    let data = {};
    let values = accessGrantFormValues;
    let errorList = [];
    data.policyItems = getPolicyItemsVal(values, "policyItems", errorList);
    data.description = values.description;
    data.isAuditEnabled = values.isAuditEnabled;
    data.isDenyAllElse = values.isDenyAllElse;
    data.isEnabled = values.isEnabled;
    data.name = values.policyName;
    data.policyLabels = (values.policyLabel || [])?.map(({ value }) => value);
    data.policyPriority = values.policyPriority ? "1" : "0";
    data.policyType = values.policyType;
    //data.service = policyData?.service;
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

    if (errorList.length == 0) {
      if (policyData != undefined) {
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
          toastId.current = toast.success(
            "Access Grant updated successfully!!"
          );
          showSaveCancelButton(false);
          setAccessGrantFormValues();
        } catch (error) {
          setLoader(false);
          let errorMsg = `Failed to update Access Grant`;
          if (error?.response?.data?.msgDesc) {
            errorMsg = `Error! ${error.response.data.msgDesc}`;
          }
          toast.error(errorMsg);
          console.error(`Error while updating Access Grant! ${error}`);
        }
      } else {
        try {
          setLoader(true);
          const resp = await fetchApi({
            url: `gds/dataset/${datasetId}/policy`,
            method: "POST",
            data: data
          });
          setLoader(false);
          toast.dismiss(toastId.current);
          toastId.current = toast.success(
            "Access Grant created successfully!!"
          );
          showSaveCancelButton(false);
          setAccessGrantFormValues();
        } catch (error) {
          setLoader(false);
          let errorMsg = `Failed to create Access Grant`;
          if (error?.response?.data?.msgDesc) {
            errorMsg = `Error! ${error.response.data.msgDesc}`;
          }
          toast.error(errorMsg);
          console.error(`Error while creating Access Grant! ${error}`);
        }
      }
    }
  };

  const getPolicyItemsVal = (formData, name, errorList) => {
    var policyResourceItem = [];
    let errorMsg = "";
    if (formData == undefined || formData == null) {
      errorMsg = "Please add access grant details";
      errorList.push(errorMsg);
      toast.error(errorMsg);
      return null;
    }
    if (formData[name] != undefined) {
      for (let key of formData[name]) {
        if (!isEmpty(key) && Object.entries(key).length > 0) {
          let obj = {};
          if (key.delegateAdmin != "undefined" && key.delegateAdmin != null) {
            obj.delegateAdmin = key.delegateAdmin;
          }
          if (key.accesses != undefined && key.accesses.length > 0) {
            obj.accesses = key.accesses.map(({ value }) => ({
              type: value,
              isAllowed: true
            }));
          } else {
            errorMsg = "Please select Permission";
            errorList.push(errorMsg);
            toast.error(errorMsg);
            return null;
          }

          if (key.principle != undefined && key.principle.length > 0) {
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
          } else {
            errorMsg = "Please select Principal";
            errorList.push(errorMsg);
            toast.error(errorMsg);
            return null;
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
    }

    return policyResourceItem;
  };

  const removeChanges = () => {
    fetchDatasetInfo(datasetId);
    showSaveCancelButton(false);
    isDatasetNameEditable(false);
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

  const downloadJsonFile = async () => {
    let jsonData = datasetInfo;
    jsonData.datashares = await fetchDatashareRequestList(undefined, 0, true);
    if (
      userAclPerm == "ADMIN" ||
      userAclPerm == "AUDIT" ||
      userAclPerm == "POLICY_ADMIN"
    ) {
      jsonData.sharedWith = { users: {}, groups: {}, roles: {} };
      let policyItems = await fetchAccessGrantInfo();
      for (const item of policyItems) {
        let accessList = [];
        item.accesses.forEach((item) => {
          accessList.push(item.type);
        });
        item.users?.forEach((user) => {
          if (jsonData.sharedWith.users[user] != undefined) {
            let newAccessTypeList = [
              ...jsonData.sharedWith.users[user],
              ...accessList
            ];
            jsonData.sharedWith.users[user] = [...new Set(newAccessTypeList)];
          } else {
            jsonData.sharedWith.users[user] = accessList;
          }
        });
        item.groups?.forEach((group) => {
          if (jsonData.sharedWith.groups[group] != undefined) {
            let newAccessTypeList = [
              ...jsonData.sharedWith.groups[group],
              ...accessList
            ];
            jsonData.sharedWith.groups[group] = [...new Set(newAccessTypeList)];
          } else {
            jsonData.sharedWith.groups[group] = accessList;
          }
        });
        item.roles?.forEach((role) => {
          if (jsonData.sharedWith.roles[role] != undefined) {
            let newAccessTypeList = [
              ...jsonData.sharedWith.roles[role],
              ...accessList
            ];
            jsonData.sharedWith.roles[role] = [...new Set(newAccessTypeList)];
          } else {
            jsonData.sharedWith.roles[role] = accessList;
          }
        });
      }
    }
    const jsonContent = JSON.stringify(jsonData);
    const blob = new Blob([jsonContent], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = datasetInfo.name + ".json";
    a.click();
    URL.revokeObjectURL(url);
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
    //fetchDatashareRequestList(undefined, selected, false);
  };

  const handleDatasharePageClick = ({ selected }) => {
    setDatashareCurrentPage(selected);
    fetchDatashareList(undefined, selected);
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

  const navidateToFullViewPage = () => {
    navigate(`/gds/dataset/${datasetId}/fullview`, {
      state: {
        userAclPerm: userAclPerm
      }
    });
  };

  const onDatasetNameChange = (event) => {
    setDatasetName(event.target.value);
    showSaveCancelButton(true);
  };

  const handleEditClick = () => {
    if (isSystemAdmin() || userAclPerm == "ADMIN") {
      isDatasetNameEditable(true);
      showSaveCancelButton(true);
    }
  };

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "createdDate",
        desc: true
      }
    ],
    []
  );

  return (
    <>
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
              <span className="me-1">Dataset: </span>
              {!datasetNameEditable ? (
                <span
                  title={datasetName}
                  className="text-truncate"
                  style={{ maxWidth: "700px", display: "inline-block" }}
                  onClick={() => handleEditClick()}
                >
                  {datasetName}
                </span>
              ) : (
                <input
                  type="text"
                  name="datasetName"
                  style={{ height: "39px" }}
                  className="form-control"
                  data-cy="datasetName"
                  value={datasetName}
                  onChange={onDatasetNameChange}
                />
              )}
            </div>
          </h3>
          {!datasetNameEditable && !saveCancelButtons && (
            <>
              <CustomBreadcrumb />
              <span className="pipe" />
            </>
          )}

          {datasetNameEditable ||
          ((isSystemAdmin() ||
            userAclPerm == "ADMIN" ||
            userAclPerm == "POLICY_ADMIN") &&
            saveCancelButtons) ? (
            <div className="gds-header-btn-grp">
              <Button
                variant="secondary"
                onClick={() => removeChanges()}
                size="sm"
                data-id="cancel"
                data-cy="cancel"
              >
                Cancel
              </Button>
              <Button
                variant="primary"
                onClick={() => {
                  if (datasetName.length > 512) {
                    toast.error("Dataset name must be 512 characters or less");
                  } else {
                    if (
                      activeKey === "accessGrants" &&
                      accessGrantFormValues !== undefined &&
                      datasetNameEditable
                    ) {
                      updateDatasetAndAccessGrant();
                    } else if (
                      activeKey !== "accessGrants" ||
                      (activeKey === "accessGrants" && datasetNameEditable)
                    ) {
                      updateDatasetDetails();
                    } else {
                      updateDatasetAccessGrant();
                    }
                  }
                }}
                size="sm"
                data-id="save"
                data-cy="save"
              >
                Save
              </Button>
            </div>
          ) : (
            <div></div>
          )}
          {!datasetNameEditable && !saveCancelButtons && (
            <div>
              <DropdownButton
                id="dropdown-item-button"
                title={<i className="fa fa-ellipsis-v" fontSize="36px" />}
                size="sm"
                className="hide-arrow"
              >
                <Dropdown.Item
                  as="button"
                  onClick={() => {
                    navidateToFullViewPage();
                  }}
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
          )}
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
                                    <span className="gds-label-color">ID</span>
                                  </div>
                                  <div line-height="30px">{datasetInfo.id}</div>
                                </div>
                                <div className="wrapper">
                                  <div
                                    className="gds-left-inline-field"
                                    height="30px"
                                  >
                                    <span className="gds-label-color">
                                      Date Updated
                                    </span>
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
                                    <span className="gds-label-color">
                                      Date Created
                                    </span>
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
                                <div>
                                  <span className="gds-label-color">
                                    Description
                                  </span>
                                </div>
                              </div>
                              <div>
                                <div>
                                  <textarea
                                    placeholder="Dataset Description"
                                    className="form-control gds-description pl-1"
                                    id="description"
                                    data-cy="description"
                                    readOnly={
                                      !isSystemAdmin() && userAclPerm != "ADMIN"
                                    }
                                    onChange={datasetDescriptionChange}
                                    value={datasetDescription}
                                    rows={5}
                                  />
                                </div>
                              </div>
                            </div>
                            {(isSystemAdmin() || userAclPerm != "VIEW") && (
                              <PrinciplePermissionComp
                                userList={userList}
                                groupList={groupList}
                                roleList={roleList}
                                type="dataset"
                                isAdmin={
                                  isSystemAdmin() || userAclPerm == "ADMIN"
                                    ? true
                                    : false
                                }
                                isDetailView={true}
                                onDataChange={handleDataChange}
                              />
                            )}
                          </div>
                        )}
                      />
                    </div>
                  ) : (
                    <div></div>
                  )}
                </Tab>
                <Tab eventKey="datashares" title="DATASHARES">
                  {activeKey == "datashares" ? (
                    <div className="gds-content-border gds-request-content">
                      <div className="mb-3">
                        <div className="usr-grp-role-search-width mb-3">
                          <StructuredFilter
                            key="user-listing-search-filter"
                            placeholder="Search datashares..."
                            onChange={updateRequestSearchFilter}
                            options={requestSearchFilterOptions}
                          />
                        </div>
                        {(isSystemAdmin() || userAclPerm == "ADMIN") && (
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
                      </div>
                      <div>
                        <div className="usr-grp-role-search-width">
                          <Tabs
                            id="DatashareTabs"
                            className="mg-b-10"
                            activeKey={requestActiveKey}
                            onSelect={handleRequestTabSelect}
                          >
                            <Tab
                              eventKey="All"
                              title={
                                "All (" + shareStatusMetrics.totalCount + ")"
                              }
                            >
                              {requestActiveKey == "All" && (
                                <DatashareInDatasetListComp
                                  id={Number(datasetId)}
                                  type="dataset"
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
                                  id={Number(datasetId)}
                                  type="dataset"
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
                                  id={Number(datasetId)}
                                  type="dataset"
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
                                "Granted (" + shareStatusMetrics.GRANTED + ")"
                              }
                            >
                              {requestActiveKey == "Granted" && (
                                <DatashareInDatasetListComp
                                  id={Number(datasetId)}
                                  type="dataset"
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
                                  id={Number(datasetId)}
                                  type="dataset"
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
                  userAclPerm === "ADMIN" ||
                  userAclPerm === "AUDIT" ||
                  userAclPerm === "POLICY_ADMIN") && (
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
                              placeholder="Search Users, Groups and Roles..."
                              onChange={(e) =>
                                onChangeSharedWithPrincipleName(e)
                              }
                              value={sharedWithPrincipleName}
                            />

                            <Select
                              theme={serviceSelectTheme}
                              styles={customStyles}
                              options={serviceDef.accessTypes}
                              onChange={(e) =>
                                onSharedWithAccessFilterChange(e)
                              }
                              value={sharedWithAccessFilter}
                              menuPlacement="auto"
                              placeholder="Select Permission"
                              isClearable
                            />
                          </div>

                          <Accordion className="mg-b-10" defaultActiveKey="0">
                            <Accordion.Item>
                              <Accordion.Header
                                eventKey="1"
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
                              </Accordion.Header>
                              <Accordion.Body eventKey="1">
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
                                              className="badge text-bg-light badge-sm"
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
                              </Accordion.Body>
                            </Accordion.Item>
                          </Accordion>
                          <Accordion className="mg-b-10" defaultActiveKey="0">
                            <Accordion.Item>
                              <Accordion.Header
                                eventKey="1"
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
                              </Accordion.Header>
                              <Accordion.Body eventKey="1">
                                {filteredGroupSharedWithMap != undefined &&
                                filteredGroupSharedWithMap.size > 0 ? (
                                  Array.from(filteredGroupSharedWithMap).map(
                                    ([key, value]) => (
                                      <div
                                        className="gds-principle-listing"
                                        key={key}
                                      >
                                        <span title={key}>{key}</span>
                                        <div className="gds-chips gap-one-fourth">
                                          {value.map((accessObj) => (
                                            <span
                                              className="badge text-bg-light badge-sm"
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
                              </Accordion.Body>
                            </Accordion.Item>
                          </Accordion>
                          <Accordion className="mg-b-10" defaultActiveKey="0">
                            <Accordion.Item>
                              <Accordion.Header
                                eventKey="1"
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
                              </Accordion.Header>
                              <Accordion.Body eventKey="1">
                                {filteredRoleSharedWithMap != undefined &&
                                filteredRoleSharedWithMap.size > 0 ? (
                                  Array.from(filteredRoleSharedWithMap).map(
                                    ([key, value]) => (
                                      <div
                                        className="gds-principle-listing"
                                        key={key}
                                      >
                                        <span title={key}>{key}</span>
                                        <div className="gds-chips gap-one-fourth">
                                          {value.map((accessObj) => (
                                            <span
                                              className="badge text-bg-light badge-sm"
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
                              </Accordion.Body>
                            </Accordion.Item>
                          </Accordion>
                        </div>
                      </div>
                    ) : (
                      <div></div>
                    )}
                  </Tab>
                )}
                {(isSystemAdmin() ||
                  userAclPerm === "ADMIN" ||
                  userAclPerm === "AUDIT" ||
                  userAclPerm === "POLICY_ADMIN") && (
                  <Tab eventKey="accessGrants" title="ACCESS GRANTS">
                    <div className="wrap-gds">
                      {activeKey == "accessGrants" ? (
                        <AccessGrantForm
                          dataset={datasetInfo}
                          onDataChange={handleAccessGrantChange}
                          serviceCompDetails={serviceDef}
                          isAdmin={
                            isSystemAdmin() ||
                            userAclPerm == "ADMIN" ||
                            userAclPerm == "POLICY_ADMIN"
                          }
                        />
                      ) : (
                        <div></div>
                      )}
                    </div>
                  </Tab>
                )}

                {(isSystemAdmin() ||
                  userAclPerm === "ADMIN" ||
                  userAclPerm === "AUDIT") && (
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
                              defaultSelected={[]}
                            />
                          </div>
                          <div className="gds-header-btn-grp"></div>
                        </div>
                        <XATableLayout
                          data={historyListData}
                          columns={historyColumns}
                          fetchData={fetchHistoryList}
                          totalCount={entries && entries.totalCount}
                          loading={historyLoader}
                          pageCount={pageCount}
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
                    )}
                  </Tab>
                )}

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
                            readOnly={
                              !isSystemAdmin() && userAclPerm != "ADMIN"
                            }
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
                          id={obj.id}
                          checked={selectedDatashareList.includes(
                            obj.id.toString()
                          )}
                          onChange={checkBocChange}
                        />
                        <span
                          title={obj.name}
                          className="fnt-14 text-truncate"
                          style={{ maxWidth: "300px", display: "inline-block" }}
                        >
                          {obj.name}
                        </span>
                        <CustomTooltip
                          placement="right"
                          content={
                            <p className="pd-10" style={{ fontSize: "small" }}>
                              {obj.description}
                            </p>
                          }
                          icon="fa-fw fa fa-info-circle gds-opacity-lowest"
                        />
                      </div>
                    );
                  })
                ) : (
                  <div></div>
                )}
                {datashareTotalCount > itemsPerPage && (
                  <div className="d-flex">
                    <ReactPaginate
                      previousLabel={"<"}
                      nextLabel={">"}
                      pageClassName="page-item"
                      pageLinkClassName="page-link"
                      previousClassName="page-item"
                      previousLinkClassName="page-link"
                      nextClassName="page-item"
                      nextLinkClassName="page-link"
                      breakLabel={"..."}
                      pageCount={datasharePageCount}
                      onPageChange={handleDatasharePageClick}
                      breakClassName="page-item"
                      breakLinkClassName="page-link"
                      containerClassName="pagination"
                      activeClassName="active"
                    />
                  </div>
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
                <h3 className="gds-header bold">Activate Datashare</h3>
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

            <OperationAdminModal
              show={showrowmodal}
              data={rowdata}
              onHide={handleClosed}
            ></OperationAdminModal>
          </React.Fragment>
        )}
      </React.Fragment>
    </>
  );
};

export default withRouter(DatasetDetailLayout);
