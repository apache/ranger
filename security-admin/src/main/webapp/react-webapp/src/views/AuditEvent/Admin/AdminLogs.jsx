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

import React, { useCallback, useEffect, useReducer } from "react";
import { useSearchParams, useNavigate, useLocation } from "react-router-dom";
import { Badge, Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { fetchApi } from "Utils/fetchAPI";
import { ClassTypes, enumValueToLabel } from "Utils/XAEnums";
import dateFormat from "dateformat";
import AdminModal from "./AdminModal";
import OperationAdminModal from "./OperationAdminModal";
import { AuditFilterEntries, Loader } from "Components/CommonComponents";
import moment from "moment-timezone";
import { capitalize, startCase, sortBy, toLower, pick } from "lodash";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import {
  getTableSortBy,
  getTableSortType,
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError,
  currentTimeZone
} from "Utils/XAUtils";
import { ACTIONS } from "Views/AuditEvent/action";
import { reducer, ADMIN_INITIAL_STATE } from "Views/AuditEvent/reducer";

function AdminLogs() {
  const [state, dispatch] = useReducer(reducer, ADMIN_INITIAL_STATE);

  const location = useLocation();
  const navigate = useNavigate();

  const [searchParams, setSearchParams] = useSearchParams();

  const hideRowModal = () =>
    dispatch({
      type: ACTIONS.SHOW_ROW_MODAL,
      showRowModal: false,
      rowData: {}
    });

  const hideSessionModal = () =>
    dispatch({
      type: ACTIONS.SHOW_SESSION_MODAL,
      showSessionModal: false,
      sessionId: undefined
    });

  const openRowModal = async (row) => {
    const { original = {} } = row;

    dispatch({
      type: ACTIONS.SHOW_ROW_MODAL,
      showRowModal: true,
      rowData: original
    });
  };

  const openSessionModal = (sessionId) => {
    dispatch({
      type: ACTIONS.SHOW_SESSION_MODAL,
      showSessionModal: true,
      sessionId: sessionId
    });
  };

  const updateSessionId = (id) => {
    navigate(`/reports/audit/admin?sessionId=${id}`);
    setSearchParams({ sessionId: id }, { replace: true });
  };

  useEffect(() => {
    const { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("admin", searchParams, searchFilterOptions);

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
    localStorage.setItem("admin", JSON.stringify(searchParam));
    dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
  }, [location.search]);

  const fetchAdminLogsInfo = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });

      const params = {
        ...state.searchFilterParams,
        pageSize,
        startIndex: pageIndex * pageSize,
        ...(sortBy.length > 0 && {
          sortBy: getTableSortBy(sortBy),
          sortType: getTableSortType(sortBy)
        })
      };

      try {
        const response = await fetchApi({
          url: "assets/report",
          params: params
        });

        const logsEntries = pick(response.data, [
          "startIndex",
          "pageSize",
          "totalCount",
          "resultSize"
        ]);
        const logsResp = response.data?.vXTrxLogs || [];
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
        console.error(`Error occurred while fetching Admin logs! ${error}`);
      }

      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: false });
    },
    [state.refreshTableData]
  );

  const refreshTable = () => {
    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: state.searchFilterParams,
      refreshTableData: moment.now()
    });
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Operation",
        accessor: "operation",
        Cell: (rawValue) => {
          let classtype = rawValue.row.original.objectClassType;
          let action = rawValue.row.original.action;
          let objectname = rawValue.row.original.objectName;

          let operation = "";
          let hasAction = [
            "EXPORT JSON",
            "EXPORT EXCEL",
            "EXPORT CSV",
            "IMPORT START",
            "IMPORT END"
          ];
          if (hasAction.includes(action)) {
            if (
              action == "EXPORT JSON" ||
              action == "EXPORT EXCEL" ||
              action == "EXPORT CSV"
            )
              return "Exported policies";
            else return action;
          } else {
            if (
              classtype == ClassTypes.CLASS_TYPE_XA_ASSET.value ||
              classtype == ClassTypes.CLASS_TYPE_RANGER_SERVICE.value
            )
              operation = (
                <span>
                  Service {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (
              classtype == ClassTypes.CLASS_TYPE_XA_RESOURCE.value ||
              classtype == ClassTypes.CLASS_TYPE_RANGER_POLICY.value
            )
              operation = (
                <span>
                  Policy {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_XA_USER.value)
              operation = (
                <span>
                  User {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_XA_GROUP.value)
              operation = (
                <span>
                  Group {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_USER_PROFILE.value)
              operation = (
                <span>
                  User profile {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_PASSWORD_CHANGE.value)
              operation = (
                <span>
                  User profile {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (
              classtype == ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value
            )
              operation = (
                <span>
                  Security Zone {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_RANGER_ROLE.value)
              operation = (
                <span>
                  Role {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_XA_GROUP_USER.value)
              operation =
                action == "create" ? (
                  <span>
                    User <strong> {objectname} </strong>added to group{" "}
                    <strong>
                      {rawValue?.row?.original?.parentObjectName || " "}
                    </strong>
                  </span>
                ) : (
                  <span>
                    User <strong> {objectname} </strong>removed from group{" "}
                    <strong>
                      {rawValue?.row?.original?.parentObjectName || " "}
                    </strong>
                  </span>
                );
            else if (classtype == ClassTypes.CLASS_TYPE_RANGER_DATASET.value)
              operation = (
                <span>
                  Dataset {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_RANGER_PROJECT.value)
              operation = (
                <span>
                  Project {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_RANGER_DATA_SHARE.value)
              operation = (
                <span>
                  Data Share {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (
              classtype == ClassTypes.CLASS_TYPE_RANGER_SHARED_RESOURCE.value
            )
              operation = (
                <span>
                  Shared Resource {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (
              classtype ==
              ClassTypes.CLASS_TYPE_RANGER_DATA_SHARE_IN_DATASET.value
            )
              operation = (
                <span>
                  DataShare in Dataset {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (
              classtype == ClassTypes.CLASS_TYPE_RANGER_DATASET_IN_PROJECT.value
            )
              operation = (
                <span>
                  Dataset in Project {action}d <strong>{objectname}</strong>
                </span>
              );
            return <div className="text-truncate">{operation}</div>;
          }
        },
        disableSortBy: true
      },
      {
        Header: "Audit Type",
        accessor: "objectClassType",
        Cell: (rawValue) => {
          let classtype = rawValue.row.original.objectClassType;
          var audittype = enumValueToLabel(ClassTypes, classtype);
          return (
            <div className="text-center">{Object.values(audittype.label)}</div>
          );
        },
        disableSortBy: true
      },
      {
        Header: "User",
        accessor: "owner",
        Cell: (rawValue) => {
          return rawValue.value !== undefined ? (
            <span className="text-center d-block">{rawValue.value}</span>
          ) : (
            <span className="text-center d-block">--</span>
          );
        },
        disableSortBy: true
      },
      {
        Header: `Date ( ${currentTimeZone()} )`,
        accessor: "createDate",
        Cell: (rawValue) => {
          const date = rawValue.value;
          const newdate = dateFormat(date, "mm/dd/yyyy hh:MM:ss TT");
          return <span className="text-center d-block">{newdate}</span>;
        }
      },
      {
        Header: "Actions",
        accessor: "action",
        Cell: (rawValue) => {
          var operation = "";
          if (rawValue.value == "create") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="success">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else if (rawValue.value == "update") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="warning">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else if (rawValue.value == "delete") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="danger">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else if (rawValue.value == "IMPORT START") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="info">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else if (rawValue.value == "IMPORT END") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="info">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else {
            operation = (
              <span className="text-center d-block">
                <Badge bg="secondary">
                  {startCase(toLower(rawValue.value))}
                </Badge>{" "}
              </span>
            );
          }
          return operation;
        },
        disableSortBy: true
      },
      {
        Header: "Session ID",
        accessor: "sessionId",
        Cell: (rawValue) => {
          var sessionId = rawValue.value;
          if (sessionId != undefined) {
            return (
              <span className="text-center d-block">
                <a
                  role="button"
                  className="text-primary"
                  onClick={(e) => {
                    e.stopPropagation();
                    openSessionModal(sessionId);
                  }}
                  data-id={sessionId}
                  data-cy={sessionId}
                  title={sessionId}
                >
                  {sessionId}
                </a>
              </span>
            );
          } else {
            return <span className="text-center d-block">--</span>;
          }
        },
        disableSortBy: true
      }
    ],
    []
  );

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "id",
        desc: true
      }
    ],
    []
  );

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
    localStorage.setItem("admin", JSON.stringify(searchParam));

    if (typeof state.resetPage?.page === "function") {
      state.resetPage.page(0);
    }
  };

  const searchFilterOptions = [
    {
      category: "action",
      label: "Actions",
      urlLabel: "actions",
      type: "textoptions",
      options: () => {
        return [
          { value: "create", label: "Create" },
          { value: "update", label: "Update" },
          { value: "delete", label: "Delete" },
          { value: "password change", label: "Password Change" },
          { value: "EXPORT JSON", label: "Export Json" },
          { value: "EXPORT CSV", label: "Export Csv" },
          { value: "EXPORT EXCEL", label: "Export Excel" },
          { value: "IMPORT END", label: "Import End" },
          { value: "IMPORT START", label: "Import Start" },
          { value: "Import Create", label: "Import Create" },
          { value: "Import Delete", label: "Import Delete" }
        ];
      }
    },
    {
      category: "objectClassType",
      label: "Audit Type",
      urlLabel: "auditType",
      type: "textoptions",
      options: () => {
        return [
          { value: "1020", label: "Ranger Policy" },
          { value: "1002", label: "Ranger Group" },
          { value: "1056", label: "Ranger Security Zone" },
          { value: "1030", label: "Ranger Service" },
          { value: "1003", label: "Ranger User" },
          { value: "1004", label: "XA Group of Users" },
          { value: "1062", label: "Ranger Dataset" },
          { value: "1063", label: "Ranger Project" },
          { value: "1064", label: "Ranger Data Share" },
          { value: "1065", label: "Ranger Shared Resource" },
          { value: "1066", label: "Ranger Data Share in Dataset" },
          { value: "1067", label: "Ranger Dataset in Project" },
          { value: "2", label: "User Profile" }
        ];
      }
    },
    {
      category: "endDate",
      label: "End Date",
      urlLabel: "endDate",
      type: "date"
    },
    {
      category: "sessionId",
      label: "Session ID",
      urlLabel: "sessionId",
      type: "text"
    },
    {
      category: "startDate",
      label: "Start Date",
      urlLabel: "startDate",
      type: "date"
    },
    {
      category: "owner",
      label: "User",
      urlLabel: "user",
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
                key="admin-log-search-filter"
                placeholder="Search for your access logs..."
                options={sortBy(searchFilterOptions, ["label"])}
                onChange={updateSearchFilter}
                defaultSelected={state.defaultSearchFilterParams}
              />
            </div>
          </Col>
        </Row>

        <AuditFilterEntries
          entries={state.entries}
          refreshTable={refreshTable}
        />

        <XATableLayout
          data={state.tableListingData}
          columns={columns}
          fetchData={fetchAdminLogsInfo}
          totalCount={state.entries && state.entries.totalCount}
          pageCount={state.pageCount}
          loading={state.loader}
          columnSort={true}
          defaultSort={getDefaultSort}
          getRowProps={(row) => ({
            onClick: (e) => {
              e.stopPropagation();
              openRowModal(row);
            }
          })}
        />

        <AdminModal
          show={state.showSessionModal}
          data={state.sessionId}
          onHide={hideSessionModal}
          updateSessionId={updateSessionId}
        ></AdminModal>

        <OperationAdminModal
          data={state.rowData}
          show={state.showRowModal}
          onHide={hideRowModal}
        ></OperationAdminModal>
      </React.Fragment>
    </div>
  );
}

export default AdminLogs;
