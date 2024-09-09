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

import React, { useState, useCallback, useRef, useEffect } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import { Badge, Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { fetchApi } from "Utils/fetchAPI";
import { AuthStatus, AuthType } from "../../utils/XAEnums";
import AdminModal from "./AdminModal";
import dateFormat from "dateformat";
import { AuditFilterEntries } from "Components/CommonComponents";
import moment from "moment-timezone";
import { sortBy } from "lodash";
import StructuredFilter from "../../components/structured-filter/react-typeahead/tokenizer";
import {
  getTableSortBy,
  getTableSortType,
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError,
  currentTimeZone
} from "../../utils/XAUtils";
import { Loader } from "../../components/CommonComponents";

function LoginSessions() {
  const [loginSessionListingData, setLoginSessionLogs] = useState([]);
  const [loader, setLoader] = useState(true);
  const [sessionId, setSessionId] = useState([]);
  const [showmodal, setShowModal] = useState(false);
  const [pageCount, setPageCount] = React.useState(0);
  const [entries, setEntries] = useState([]);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const fetchIdRef = useRef(0);
  const [contentLoader, setContentLoader] = useState(true);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [resetPage, setResetpage] = useState({ page: null });
  const handleClose = () => setShowModal(false);
  const navigate = useNavigate();

  const updateSessionId = (id) => {
    navigate(`/reports/audit/admin?sessionId=${id}`);
  };

  useEffect(() => {
    let { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams(
        "loginSession",
        searchParams,
        searchFilterOptions
      );

    // Updating the states for search params, search filter, default search filter and localStorage
    setSearchParams(searchParam, { replace: true });
    if (
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    localStorage.setItem("loginSession", JSON.stringify(searchParam));
    setContentLoader(false);
  }, [searchParams]);

  const fetchLoginSessionLogsInfo = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      setLoader(true);
      let logsResp = [];
      let logs = [];
      let totalCount = 0;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchFilterParams };
      if (fetchId === fetchIdRef.current) {
        params["pageSize"] = pageSize;
        params["startIndex"] = pageIndex * pageSize;
        if (sortBy.length > 0) {
          params["sortBy"] = getTableSortBy(sortBy);
          params["sortType"] = getTableSortType(sortBy);
        }
        try {
          logsResp = await fetchApi({
            url: "xusers/authSessions",
            params: params,
            skipNavigate: true
          });
          logs = logsResp.data.vXAuthSessions;
          totalCount = logsResp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(
            `Error occurred while fetching Login Session logs! ${error}`
          );
        }
        setLoginSessionLogs(logs);
        setEntries(logsResp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [updateTable, searchFilterParams]
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
    setLoginSessionLogs([]);
    setLoader(true);
    setUpdateTable(moment.now());
  };

  const openModal = (id) => {
    setShowModal(true);
    setSessionId(id);
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
                    openModal(id);
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
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam, { replace: true });
    localStorage.setItem("loginSession", JSON.stringify(searchParam));

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
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

  return contentLoader ? (
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
                defaultSelected={defaultSearchFilterParams}
              />
            </div>
          </Col>
        </Row>
        <AuditFilterEntries entries={entries} refreshTable={refreshTable} />
        <XATableLayout
          data={loginSessionListingData}
          columns={columns}
          fetchData={fetchLoginSessionLogsInfo}
          totalCount={entries && entries.totalCount}
          loading={loader}
          pageCount={pageCount}
          columnSort={true}
          defaultSort={getDefaultSort}
        />
        <AdminModal
          show={showmodal}
          data={sessionId}
          onHide={handleClose}
          updateSessionId={updateSessionId}
        ></AdminModal>
      </React.Fragment>
    </div>
  );
}

export default LoginSessions;
