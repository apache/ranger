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

import React, { useState, useCallback, useEffect, useReducer } from "react";
import {
  useSearchParams,
  useOutletContext,
  useLocation
} from "react-router-dom";
import { Badge, Row, Col, Table } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import dateFormat from "dateformat";
import { fetchApi } from "Utils/fetchAPI";
import {
  AccessMoreLess,
  AuditFilterEntries,
  CustomTooltip,
  CustomPopoverOnClick,
  CustomPopoverTagOnClick,
  Loader
} from "Components/CommonComponents";
import moment from "moment-timezone";
import AccessLogModal from "./AccessLogModal";
import AccessLogPolicyModal from "./AccessLogPolicyModal";
import {
  isEmpty,
  isUndefined,
  pick,
  map,
  sortBy,
  toString,
  toUpper,
  has,
  filter,
  find,
  isArray,
  cloneDeep
} from "lodash";
import { toast } from "react-toastify";
import qs from "qs";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import {
  isKeyAdmin,
  isKMSAuditor,
  getTableSortBy,
  getTableSortType,
  serverError,
  requestDataTitle,
  fetchSearchFilterParams,
  parseSearchFilter
} from "Utils/XAUtils";
import {
  ServiceRequestDataRangerAcl,
  ServiceRequestDataHadoopAcl
} from "Utils/XAEnums";
import { getServiceDef } from "Utils/appState";
import { ACTIONS } from "Views/AuditEvent/action";
import { reducer, ACCESS_INITIAL_STATE } from "Views/AuditEvent/reducer";

function AccessLogs() {
  const [state, dispatch] = useReducer(reducer, ACCESS_INITIAL_STATE);

  const location = useLocation();

  const { services, servicesAvailable } = useOutletContext();

  const { allServiceDefs } = cloneDeep(getServiceDef());
  const isKMSRole = isKeyAdmin() || isKMSAuditor();

  const [searchParams, setSearchParams] = useSearchParams();

  const [excludeServiceUser, setExcludeServiceUser] = useState(() => {
    return localStorage?.excludeServiceUser === "true" ? true : false;
  });

  useEffect(() => {
    if (!isKMSRole) {
      fetchSecurityZones();
    }

    const currentDate = moment(moment()).format("MM/DD/YYYY");
    const { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("bigData", searchParams, searchFilterOptions);

    if (
      !has(searchFilterParam, "startDate") &&
      !has(searchFilterParam, "endDate")
    ) {
      searchParam.startDate = currentDate;
      searchFilterParam.startDate = currentDate;
      defaultSearchFilterParam.push({
        category: "startDate",
        value: currentDate
      });
    }

    // Updating the states for search params, search filter, default search filter and localStorage
    setSearchParams(searchParam, { replace: true });
    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: searchFilterParam,
      refreshTableData: moment.now()
    });
    dispatch({
      type: ACTIONS.SET_DEFAULT_SEARCH_FILTER_PARAMS,
      defaultSearchFilterParams: defaultSearchFilterParam
    });
    localStorage.setItem("bigData", JSON.stringify(searchParam));
  }, []);

  useEffect(() => {
    if (servicesAvailable !== null) {
      const { searchFilterParam, defaultSearchFilterParam, searchParam } =
        fetchSearchFilterParams("bigData", searchParams, searchFilterOptions);

      // Updating the states for search params, search filter, default search filter and localStorage
      setSearchParams(searchParam, { replace: true });
      if (
        JSON.stringify(state.searchFilterParams) !==
        JSON.stringify(searchFilterParam)
      ) {
        dispatch({
          type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
          searchFilterParams: searchFilterParam,
          refreshTableData: moment.now()
        });
      }
      dispatch({
        type: ACTIONS.SET_DEFAULT_SEARCH_FILTER_PARAMS,
        defaultSearchFilterParams: defaultSearchFilterParam
      });
      localStorage.setItem("bigData", JSON.stringify(searchParam));
      dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
    }
  }, [location.search, servicesAvailable]);

  const fetchAccessLogsInfo = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });
      if (servicesAvailable !== null) {
        const params = {
          ...state.searchFilterParams,
          pageSize,
          startIndex: pageIndex * pageSize,
          excludeServiceUser: excludeServiceUser,
          ...(sortBy.length > 0 && {
            sortBy: getTableSortBy(sortBy),
            sortType: getTableSortType(sortBy)
          })
        };

        try {
          const response = await fetchApi({
            url: "assets/accessAudit",
            params: params,
            skipNavigate: true,
            paramsSerializer: function (params) {
              return qs.stringify(params, { arrayFormat: "repeat" });
            }
          });

          const logsEntries = pick(response.data, [
            "startIndex",
            "pageSize",
            "totalCount",
            "resultSize"
          ]);
          const logsResp = response.data?.vXAccessAudits || [];
          const totalCount = response.data?.totalCount || 0;

          dispatch({
            type: ACTIONS.SET_TABLE_DATA,
            tableListingData: logsResp,
            entries: logsEntries,
            pageCount: Math.ceil(totalCount / pageSize),
            resetPage: { page: gotoPage }
          });
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching access logs : ${error}`);
        }

        dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: false });
      }
    },
    [state.refreshTableData, excludeServiceUser, servicesAvailable]
  );

  const fetchSecurityZones = async () => {
    let zonesResp = [];
    try {
      const response = await fetchApi({
        url: "public/v2/api/zone-headers"
      });

      zonesResp = response?.data || [];
    } catch (error) {
      console.error(`Error occurred while fetching security zones : ${error}`);
    }

    dispatch({
      type: ACTIONS.SET_SECURITY_ZONES,
      securityZones: sortBy(zonesResp, ["name"])
    });
  };

  const toggleChange = (e) => {
    let checkboxValue = e?.target?.checked;
    let searchParam = {};

    for (const [key, value] of searchParams.entries()) {
      let searchFilterObj = find(searchFilterOptions, {
        urlLabel: key
      });

      if (!isUndefined(searchFilterObj)) {
        if (searchFilterObj?.addMultiple) {
          let oldValue = searchParam[key];
          let newValue = value;
          if (oldValue) {
            if (isArray(oldValue)) {
              searchParam[key].push(newValue);
            } else {
              searchParam[key] = [oldValue, newValue];
            }
          } else {
            searchParam[key] = newValue;
          }
        } else {
          searchParam[key] = value;
        }
      } else {
        searchParam[key] = value;
      }
    }

    localStorage.setItem("excludeServiceUser", checkboxValue);

    setExcludeServiceUser(checkboxValue);
    dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });
  };

  const hidePolicyModal = () =>
    dispatch({
      type: ACTIONS.SHOW_POLICY_MODAL,
      showPolicyModal: false,
      policyData: null
    });

  const hideRowModal = () =>
    dispatch({
      type: ACTIONS.SHOW_ROW_MODAL,
      showRowModal: false,
      rowData: {}
    });

  const openRowModal = (row) => {
    dispatch({
      type: ACTIONS.SHOW_ROW_MODAL,
      showRowModal: true,
      rowData: row.original
    });
  };

  const openPolicyModal = (policy) => {
    let policyParams = pick(policy, ["eventTime", "policyId", "policyVersion"]);
    dispatch({
      type: ACTIONS.SHOW_POLICY_MODAL,
      showPolicyModal: true,
      policyData: policyParams
    });
  };

  const refreshTable = () => {
    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: state.searchFilterParams,
      refreshTableData: moment.now()
    });
  };

  const requestDataContent = (requestData) => {
    const copyText = (val) => {
      !isEmpty(val) && toast.success("Copied successfully !!");
      return val;
    };
    return (
      <Row>
        <Col sm={9} className="popover-span">
          <span>{requestData}</span>
        </Col>
        <Col sm={3} className="float-end">
          <button
            className="float-end link-tag query-icon btn btn-sm"
            size="sm"
            title="Copy"
            onClick={(e) => {
              e.stopPropagation();
              navigator.clipboard.writeText(copyText(requestData));
            }}
          >
            <i className="fa-fw fa fa-copy"> </i>
          </button>
        </Col>
      </Row>
    );
  };

  const requestTagContent = (requestData) => {
    return (
      <>
        <Table bordered hover>
          <thead>
            <tr>
              <th>Name</th>
              <th>Value</th>
            </tr>
          </thead>
          <tbody>
            {Object.keys(requestData).map((obj, idx) => {
              return (
                <tr key={idx}>
                  <td>{obj}</td>
                  <td>{requestData[obj]}</td>
                </tr>
              );
            })}
          </tbody>
        </Table>
      </>
    );
  };

  const showQueryPopup = (rowId, aclEnforcer, serviceType, requestData) => {
    if (!isEmpty(requestData)) {
      if (
        aclEnforcer === "ranger-acl" &&
        ServiceRequestDataRangerAcl.includes(serviceType)
      ) {
        return queryPopupContent(rowId, serviceType, requestData);
      } else if (
        aclEnforcer === "hadoop-acl" &&
        ServiceRequestDataHadoopAcl.includes(serviceType)
      ) {
        return queryPopupContent(rowId, serviceType, requestData);
      } else {
        return "";
      }
    }
  };

  const queryPopupContent = (rowId, serviceType, requestData) => {
    return (
      <div className="float-end">
        <div className="queryInfo btn btn-sm link-tag query-icon">
          <CustomPopoverOnClick
            icon="fa-fw fa fa-table"
            title={requestDataTitle(serviceType)}
            content={requestDataContent(requestData)}
            placement="left"
            trigger={["click", "focus"]}
            id={rowId}
          ></CustomPopoverOnClick>
        </div>
      </div>
    );
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Policy ID",
        accessor: "policyId",
        Cell: (rawValue) => {
          return rawValue.value == -1 ? (
            <div className="text-center">--</div>
          ) : (
            <div className="text-center">
              <a
                role="button"
                title={rawValue.value}
                className="text-primary"
                onClick={(e) => {
                  e.stopPropagation();
                  openPolicyModal(rawValue.row.original);
                }}
              >
                {rawValue.value}
              </a>
            </div>
          );
        },
        width: 90,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Policy Version",
        accessor: "policyVersion",
        Cell: (rawValue) => {
          return rawValue.value !== undefined ? (
            <div className="text-center">{rawValue.value}</div>
          ) : (
            <div className="text-center">--</div>
          );
        },
        width: 110,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Event Time",
        accessor: "eventTime",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy h:MM:ss TT");
        },
        width: 170,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Application",
        accessor: "agentId",
        Cell: (rawValue) => {
          if (!isEmpty(rawValue?.value)) {
            return (
              <div className="text-truncate" title={rawValue.value}>
                {rawValue.value}
              </div>
            );
          } else return "--";
        },
        width: 100,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "User",
        accessor: "requestUser",
        Cell: (rawValue) => {
          if (!isEmpty(rawValue?.value)) {
            return (
              <div className="text-truncate" title={rawValue.value}>
                {rawValue.value}
              </div>
            );
          } else return "--";
        },
        width: 120,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Service (Name / Type)",
        accessor: (s) => (
          <div>
            <div
              className="text-start lht-2 mb-1 text-truncate"
              title={s.repoDisplayName}
            >
              {s.repoDisplayName}
            </div>
            <div
              className="bt-1 text-start lht-2 mb-0"
              title={s.serviceTypeDisplayName}
            >
              {s.serviceTypeDisplayName}
            </div>
          </div>
        ),
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Resource (Name / Type)",
        accessor: "resourceType",
        Cell: (r) => {
          let rowId = r.row.original.id;
          let resourcePath = r.row.original.resourcePath;
          let resourceType = r.row.original.resourceType;
          let serviceType = r.row.original.serviceType;
          let aclEnforcer = r.row.original.aclEnforcer;
          let requestData = r.row.original.requestData;

          if (!isUndefined(resourcePath) || !isUndefined(requestData)) {
            let resourcePathText = isEmpty(resourcePath) ? "--" : resourcePath;
            let resourceTypeText =
              isEmpty(resourceType) || resourceType == "@null"
                ? "--"
                : resourceType;
            return (
              <React.Fragment>
                <div className="clearfix d-flex flex-nowrap m-0">
                  <div
                    className="float-start resource-text lht-2 mb-1"
                    title={resourcePathText}
                  >
                    {resourcePathText}
                  </div>
                  {showQueryPopup(rowId, aclEnforcer, serviceType, requestData)}
                </div>
                <div className="bt-1 lht-2 mb-0" title={resourceTypeText}>
                  {resourceTypeText}
                </div>
              </React.Fragment>
            );
          } else {
            return <div className="text-center">--</div>;
          }
        },
        minWidth: 180
      },
      {
        Header: "Access Type",
        accessor: "accessType",
        Cell: (rawValue) => {
          if (!isEmpty(rawValue?.value)) {
            return (
              <div className="text-truncate" title={rawValue.value}>
                {rawValue.value}
              </div>
            );
          } else return "--";
        },
        width: 130,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Permission",
        accessor: "action",
        Cell: (rawValue) => {
          return (
            <h6>
              <Badge bg="info" title={rawValue.value} className="text-truncate">
                {rawValue.value}
              </Badge>
            </h6>
          );
        },
        width: 100,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Result",
        accessor: "accessResult",
        Cell: (rawValue) => {
          if (rawValue.value == 1) {
            return (
              <h6>
                <Badge bg="success">Allowed</Badge>
              </h6>
            );
          } else
            return (
              <h6>
                <Badge bg="danger">Denied</Badge>
              </h6>
            );
        },
        width: 100,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Access Enforcer",
        accessor: "aclEnforcer",
        Cell: (rawValue) => {
          if (!isEmpty(rawValue?.value)) {
            return (
              <div className="text-truncate" title={rawValue.value}>
                {rawValue.value}
              </div>
            );
          } else return "--";
        },
        width: 120,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Agent Host Name",
        accessor: "agentHost",
        Cell: (rawValue) => {
          if (!isEmpty(rawValue?.value)) {
            return (
              <div className="text-truncate" title={rawValue.value}>
                {rawValue.value}
              </div>
            );
          } else return <div className="text-center">--</div>;
        },
        width: 150,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Client IP",
        accessor: "clientIP",
        Cell: (rawValue) => {
          if (!isEmpty(rawValue?.value)) {
            return (
              <div className="text-truncate" title={rawValue.value}>
                {rawValue.value}
              </div>
            );
          } else return "--";
        },
        width: 110,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Cluster Name",
        accessor: "clusterName",
        width: 100,
        disableResizing: true,
        disableSortBy: true,
        Cell: (rawValue) => {
          if (!isEmpty(rawValue?.value)) {
            return (
              <div className="text-truncate" title={rawValue.value}>
                {rawValue.value}
              </div>
            );
          } else {
            return <div className="text-center">--</div>;
          }
        },
        getResizerProps: () => {}
      },
      {
        Header: "Zone Name",
        accessor: "zoneName",
        Cell: (rawValue) => {
          if (!isEmpty(rawValue?.value)) {
            return (
              <h6>
                <Badge
                  bg="dark"
                  className="text-truncate"
                  title={rawValue.value}
                >
                  {rawValue.value}
                </Badge>
              </h6>
            );
          } else return <div className="text-center">--</div>;
        },
        width: 100,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Event Count",
        accessor: "eventCount",
        width: 100,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Tags",
        accessor: "tags",
        Cell: (rawValue) => {
          let tags = [];
          if (!isEmpty(rawValue.value)) {
            tags = sortBy(JSON.parse(rawValue.value), "type")?.map((tag) => {
              if (tag.attributes && !isEmpty(tag.attributes)) {
                return (
                  <CustomPopoverTagOnClick
                    icon="text-info"
                    data={tag.type}
                    title={"Atrribute Details"}
                    content={requestTagContent(tag.attributes)}
                    placement="left"
                    trigger={["click", "focus"]}
                  ></CustomPopoverTagOnClick>
                );
              }

              return tag.type;
            });
          } else {
            return <div className="text-center">--</div>;
          }
          return <AccessMoreLess Data={tags} />;
        },
        width: 140,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Datasets",
        accessor: "datasets",
        Cell: (rawValue) => {
          let Datasets = [];
          if (!isEmpty(rawValue.value)) {
            Datasets = JSON.parse(rawValue.value).sort();
          } else {
            return <div className="text-center">--</div>;
          }
          return <AccessMoreLess Data={Datasets} />;
        },
        width: 140,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Projects",
        accessor: "projects",
        Cell: (rawValue) => {
          let Projects = [];
          if (!isEmpty(rawValue.value)) {
            Projects = JSON.parse(rawValue.value).sort();
          } else {
            return <div className="text-center">--</div>;
          }
          return <AccessMoreLess Data={Projects} />;
        },
        width: 140,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      }
    ],
    []
  );

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "eventTime",
        desc: true
      }
    ],
    []
  );

  const getServiceDefType = () => {
    let serviceDefType = [];
    serviceDefType = filter(allServiceDefs, function (serviceDef) {
      return serviceDef.name !== "tag";
    });

    return serviceDefType.map((serviceDef) => ({
      label: toUpper(serviceDef.displayName),
      value: toString(serviceDef.id)
    }));
  };

  const getServices = () => {
    let servicesName = [];
    servicesName = filter(services, function (service) {
      return !isKMSRole
        ? service.type !== "tag" && service.type !== "kms"
        : service.type !== "tag";
    });

    return sortBy(servicesName, "name")?.map((service) => ({
      label: service.displayName,
      value: service.name
    }));
  };

  const getSecurityZones = () => {
    let zonesName = [];

    zonesName = map(state.securityZones, function (zone) {
      return { label: zone.name, value: zone.name };
    });

    return zonesName;
  };

  const updateSearchFilter = (filter) => {
    const { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: searchFilterParam,
      refreshTableData: moment.now()
    });

    setSearchParams(searchParam, { replace: true });
    localStorage.setItem("bigData", JSON.stringify(searchParam));

    if (typeof state.resetPage?.page === "function") {
      state.resetPage.page(0);
    }
  };

  const searchFilterOptions = [
    {
      category: "aclEnforcer",
      label: "Access Enforcer",
      urlLabel: "accessEnforcer",
      type: "text"
    },
    {
      category: "accessType",
      label: "Access Type",
      urlLabel: "accessType",
      type: "text"
    },
    {
      category: "agentHost",
      label: "Agent Host Name",
      urlLabel: "agentHost",
      type: "text"
    },
    {
      category: "agentId",
      label: "Application",
      urlLabel: "application",
      type: "text"
    },
    {
      category: "eventId",
      label: "Audit ID",
      urlLabel: "eventId",
      type: "text"
    },
    {
      category: "clientIP",
      label: "Client IP",
      urlLabel: "clientIP",
      type: "text"
    },
    {
      category: "cluster",
      label: "Cluster Name",
      urlLabel: "clusterName",
      type: "text"
    },
    {
      category: "endDate",
      label: "End Date",
      urlLabel: "endDate",
      type: "date"
    },
    {
      category: "excludeUser",
      label: "Exclude User",
      urlLabel: "excludeUser",
      addMultiple: true,
      type: "text"
    },
    {
      category: "policyId",
      label: "Policy ID",
      urlLabel: "policyID",
      type: "text"
    },
    {
      category: "resourcePath",
      label: "Resource Name",
      urlLabel: "resourceName",
      type: "text"
    },
    {
      category: "resourceType",
      label: "Resource Type",
      urlLabel: "resourceType",
      type: "text"
    },
    {
      category: "accessResult",
      label: "Result",
      urlLabel: "result",
      type: "textoptions",
      options: () => {
        return [
          { value: "0", label: "Denied" },
          { value: "1", label: "Allowed" },
          { value: "2", label: "Not Determined" }
        ];
      }
    },
    {
      category: "repoName",
      label: "Service Name",
      urlLabel: "serviceName",
      type: "textoptions",
      options: getServices
    },
    {
      category: "repoType",
      label: "Service Type",
      urlLabel: "serviceType",
      type: "textoptions",
      options: getServiceDefType
    },
    {
      category: "startDate",
      label: "Start Date",
      urlLabel: "startDate",
      type: "date"
    },
    {
      category: "tags",
      label: "Tags",
      urlLabel: "tags",
      type: "text"
    },
    {
      category: "datasets",
      label: "Datasets",
      urlLabel: "datasets",
      type: "text"
    },
    {
      category: "projects",
      label: "Projects",
      urlLabel: "projects",
      type: "text"
    },
    {
      category: "requestUser",
      label: "User",
      urlLabel: "user",
      addMultiple: true,
      type: "text"
    },
    {
      category: "zoneName",
      label: "Zone Name",
      urlLabel: "zoneName",
      type: "textoptions",
      options: getSecurityZones
    },
    {
      category: "excludeResourceName",
      label: "Exclude Resource Name",
      urlLabel: "excludeResourceName",
      type: "text"
    }
  ];

  return state.contentLoader ? (
    <Loader />
  ) : (
    <div className="wrap">
      <React.Fragment>
        <Row className="mb-2">
          <Col sm={12}>
            <div className="searchbox-border">
              <StructuredFilter
                key="access-log-search-filter"
                placeholder="Search for your access audits..."
                options={sortBy(searchFilterOptions, ["label"])}
                onChange={updateSearchFilter}
                defaultSelected={state.defaultSearchFilterParams}
              />

              <span className="info-icon">
                <CustomTooltip
                  placement="left"
                  content={
                    <p className="pd-10" style={{ fontSize: "small" }}>
                      Wildcard searches( for example using * or ? ) are not
                      currently supported.
                      <br /> <b>Access Enforcer :</b> Search by access enforcer
                      name.
                      <br />
                      <b> Access Type :</b> Search by access Type like
                      READ_EXECUTE, WRITE_EXECUTE.
                      <br />
                      <b>Client IP :</b> Search by IP address from where
                      resource was accessed.
                      <br />
                      <b>Cluster Name : </b> Name of cluster <br />
                      <b>Zone Name :</b> Name of Zone. <br />
                      <b>End Date :</b> Set end date. <br />
                      <b>Resource Name :</b> Resource name.
                      <br /> <b>Resource Type :</b> Search by resource type
                      based on component. eg. path in HDFS, database ,table in
                      Hive.
                      <br />
                      <b> Result :</b> Search by access result i.e
                      Allowed/Denied logs.
                      <br /> <b> Service Name :</b> Name of service.
                      <br /> <b> Service Type :</b> Select type of service.
                      <br /> <b> Start Date :</b> Set start date.
                      <br /> <b> User :</b> Name of User.
                      <br /> <b> Exclude User :</b> Name of User.
                      <br /> <b> Application :</b> Application.
                      <br /> <b> Tags :</b> Tag Name.
                      <br /> <b> Datasets:</b> Dataset Name.
                      <br /> <b> Projects:</b> Project Name.
                      <br /> <b> Permission :</b> Permission
                    </p>
                  }
                  icon="fa-fw fa fa-info-circle"
                />
              </span>
            </div>
          </Col>
        </Row>

        <div className="position-relative">
          <Row className="mb-2">
            <Col sm={2}>
              <span>Exclude Service Users: </span>
              <input
                type="checkbox"
                className="align-middle"
                checked={excludeServiceUser}
                onChange={toggleChange}
                data-id="serviceUsersExclude"
                data-cy="serviceUsersExclude"
              />
            </Col>
            <Col sm={9}>
              <AuditFilterEntries
                entries={state.entries}
                refreshTable={refreshTable}
              />
            </Col>
          </Row>

          <XATableLayout
            data={state.tableListingData}
            columns={columns}
            fetchData={fetchAccessLogsInfo}
            totalCount={state.entries && state.entries.totalCount}
            loading={state.loader}
            pageCount={state.pageCount}
            getRowProps={(row) => ({
              onClick: (e) => {
                e.stopPropagation();
                openRowModal(row);
              }
            })}
            columnHide={{ tableName: "bigData", isVisible: true }}
            columnResizable={true}
            columnSort={true}
            defaultSort={getDefaultSort}
          />
        </div>

        <AccessLogModal
          rowData={state.rowData}
          showRowModal={state.showRowModal}
          hideRowModal={hideRowModal}
        ></AccessLogModal>

        <AccessLogPolicyModal
          policyData={state.policyData}
          policyView={false}
          policyRevert={false}
          showPolicyModal={state.showPolicyModal}
          hidePolicyModal={hidePolicyModal}
        ></AccessLogPolicyModal>
      </React.Fragment>
    </div>
  );
}

export default AccessLogs;
