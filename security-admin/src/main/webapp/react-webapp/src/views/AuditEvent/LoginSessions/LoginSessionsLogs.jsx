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
import { AuthStatus, AuthType } from "Utils/XAEnums";
import AdminModal from "Views/AuditEvent/Admin/AdminModal";
import dateFormat from "dateformat";
import { AuditFilterEntries, Loader } from "Components/CommonComponents";
import moment from "moment-timezone";
import { pick, sortBy } from "lodash";
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
import {
  reducer,
  LOGIN_SESSIONS_INITIAL_STATE
} from "Views/AuditEvent/reducer";

function LoginSessionsLogs() {
  const [state, dispatch] = useReducer(reducer, LOGIN_SESSIONS_INITIAL_STATE);

  const location = useLocation();
  const navigate = useNavigate();

  const [searchParams, setSearchParams] = useSearchParams();

  const hideSessionModal = () =>
    dispatch({
      type: ACTIONS.SHOW_SESSION_MODAL,
      showSessionModal: false,
      sessionId: undefined
    });

  const openSessionModal = (id) => {
    dispatch({
      type: ACTIONS.SHOW_SESSION_MODAL,
      showSessionModal: true,
      sessionId: id
    });
  };

  const updateSessionId = (id) => {
    navigate(`/reports/audit/admin?sessionId=${id}`);
  };

  useEffect(() => {
    const { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams(
        "loginSession",
        searchParams,
        searchFilterOptions
      );

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
    localStorage.setItem("loginSession", JSON.stringify(searchParam));
    dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
  }, [location.search]);

  const fetchLoginSessionLogsInfo = useCallback(
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
          url: "xusers/authSessions",
          params: params,
          skipNavigate: true
        });

        const logsEntries = pick(response.data, [
          "startIndex",
          "pageSize",
          "totalCount",
          "resultSize"
        ]);
        const logsResp = response.data?.vXAuthSessions || [];
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
        console.error(
          `Error occurred while fetching Login Session logs! ${error}`
        );
      }

      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: false });
    },
    [state.refreshTableData]
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
        Header: "Session ID",
        accessor: "id",
        Cell: (rawValue) => {
          var id = rawValue.value;
          if (id != undefined) {
            return (
              <span className="text-center d-block">
                <a
                  role="button"
                  className="text-primary"
                  onClick={() => {
                    openSessionModal(id);
                  }}
                  data-id={id}
                  data-cy={id}
                  title={id}
                >
                  {id}
                </a>
              </span>
            );
          } else {
            return <span className="text-center d-block">--</span>;
          }
        },
        width: 90
      },
      {
        Header: "Login ID",
        accessor: "loginId",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <span className="text-center d-block text-truncate">
                {rawValue.value}
              </span>
            );
          } else {
            return <span className="text-center d-block">--</span>;
          }
        },
        width: 100,
        disableSortBy: true
      },
      {
        Header: "Result",
        accessor: "authStatus",
        Cell: (rawValue) => {
          var label = "";
          var html = "";
          Object.keys(AuthStatus).map((item) => {
            if (rawValue.value == AuthStatus[item].value) {
              label = AuthStatus[item].label;
              if (AuthStatus[item].value == 1) {
                html = (
                  <span className="text-center d-block">
                    <Badge bg="success">{label}</Badge>
                  </span>
                );
              } else if (AuthStatus[item].value == 2) {
                html = (
                  <span className="text-center d-block">
                    <Badge bg="danger">{label}</Badge>
                  </span>
                );
              } else {
                html = (
                  <span className="text-center d-block">
                    <Badge bg="secondary">{label}</Badge>
                  </span>
                );
              }
            }
          });
          return html;
        },
        width: 100,
        disableSortBy: true
      },
      {
        Header: "Login Type",
        accessor: "authType",
        Cell: (rawValue) => {
          var label = "";
          Object.keys(AuthType).map((item) => {
            if (rawValue.value == AuthType[item].value) {
              label = AuthType[item].label;
            }
          });
          return <span className="text-center d-block">{label}</span>;
        },
        disableSortBy: true
      },
      {
        Header: "IP",
        accessor: "requestIP",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <span
                className="text-truncate text-center d-block"
                title={rawValue.value}
              >
                {rawValue.value}
              </span>
            );
          } else {
            return <span className="text-center d-block">--</span>;
          }
        },
        disableSortBy: true
      },
      {
        Header: "User Agent",
        accessor: "requestUserAgent",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <div className="text-truncate" title={rawValue.value}>
                {rawValue.value}
              </div>
            );
          } else {
            return <div className="text-center">--</div>;
          }
        },
        disableSortBy: true
      },
      {
        Header: `Login Time ( ${currentTimeZone()} )`,
        accessor: "authTime",
        Cell: (rawValue) => {
          let formatDateTime = dateFormat(
            rawValue.value,
            "mm/dd/yyyy hh:MM:ss TT"
          );
          return <span className="text-center d-block">{formatDateTime}</span>;
        },
        width: 180,
        disableSortBy: true
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
    localStorage.setItem("loginSession", JSON.stringify(searchParam));

    if (typeof state.resetPage?.page === "function") {
      state.resetPage.page(0);
    }
  };

  const searchFilterOptions = [
    {
      category: "endDate",
      label: "End Date",
      urlLabel: "endDate",
      type: "date"
    },
    {
      category: "requestIP",
      label: "IP",
      urlLabel: "requestIP",
      type: "text"
    },
    {
      category: "loginId",
      label: "Login ID  ",
      urlLabel: "loginID",
      type: "text"
    },
    {
      category: "authType",
      label: "Login Type",
      urlLabel: "loginType",
      type: "textoptions",
      options: () => {
        return [
          { value: "1", label: "Username/Password" },
          { value: "2", label: "Kerberos" },
          { value: "3", label: "SingleSignOn" },
          { value: "4", label: "Trusted Proxy" }
        ];
      }
    },
    {
      category: "authStatus",
      label: "Result",
      urlLabel: "result",
      type: "textoptions",
      options: () => {
        return [
          { value: "1", label: "Success" },
          { value: "2", label: "Wrong Password" },
          { value: "3", label: "Account Disabled" },
          { value: "4", label: "Locked" },
          { value: "5", label: "Password Expired" },
          { value: "6", label: "User not found" }
        ];
      }
    },
    {
      category: "id",
      label: "Session ID",
      urlLabel: "sessionID",
      type: "text"
    },
    {
      category: "startDate",
      label: "Start Date",
      urlLabel: "startDate",
      type: "date"
    },
    {
      category: "requestUserAgent",
      label: "User Agent",
      urlLabel: "userAgent",
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
                key="login-session-search-filter"
                placeholder="Search for your login sessions..."
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
          fetchData={fetchLoginSessionLogsInfo}
          totalCount={state.entries && state.entries.totalCount}
          loading={state.loader}
          pageCount={state.pageCount}
          columnSort={true}
          defaultSort={getDefaultSort}
        />

        <AdminModal
          show={state.showSessionModal}
          data={state.sessionId}
          onHide={hideSessionModal}
          updateSessionId={updateSessionId}
        ></AdminModal>
      </React.Fragment>
    </div>
  );
}

export default LoginSessionsLogs;
