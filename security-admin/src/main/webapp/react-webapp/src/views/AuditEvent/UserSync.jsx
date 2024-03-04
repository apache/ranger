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
import { useSearchParams } from "react-router-dom";
import { Badge, Modal, Button, Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { AuditFilterEntries, Loader } from "Components/CommonComponents";
import { SyncSourceDetails } from "../UserGroupRoleListing/SyncSourceDetails";
import dateFormat from "dateformat";
import moment from "moment-timezone";
import StructuredFilter from "../../components/structured-filter/react-typeahead/tokenizer";
import { sortBy, has } from "lodash";
import {
  getTableSortBy,
  getTableSortType,
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError
} from "../../utils/XAUtils";

function User_Sync() {
  const [userSyncListingData, setUserSyncLogs] = useState([]);
  const [loader, setLoader] = useState(true);
  const [pageCount, setPageCount] = React.useState(0);
  const fetchIdRef = useRef(0);
  const [entries, setEntries] = useState([]);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [showTableSyncDetails, setTableSyncdetails] = useState({
    syncDteails: {},
    showSyncDetails: false
  });
  const [contentLoader, setContentLoader] = useState(true);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [resetPage, setResetpage] = useState({ page: null });

  useEffect(() => {
    let currentDate = moment(moment()).format("MM/DD/YYYY");
    let { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("userSync", searchParams, searchFilterOptions);

    if (
      !has(searchFilterParam, "startDate") &&
      !has(searchFilterParam, "endDate")
    ) {
      searchParam["startDate"] = currentDate;
      searchFilterParam["startDate"] = currentDate;
      defaultSearchFilterParam.push({
        category: "startDate",
        value: currentDate
      });
    }

    // Updating the states for search params, search filter, default search filter and localStorage
    setSearchParams(searchParam, { replace: true });
    setSearchFilterParams(searchFilterParam);
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    localStorage.setItem("userSync", JSON.stringify(searchParam));
    setContentLoader(false);
  }, []);

  useEffect(() => {
    let { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("userSync", searchParams, searchFilterOptions);

    // Updating the states for search params, search filter, default search filter and localStorage
    setSearchParams(searchParam, { replace: true });
    if (
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    localStorage.setItem("userSync", JSON.stringify(searchParam));
    setContentLoader(false);
  }, [searchParams]);

  const fetchUserSyncInfo = useCallback(
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
          const { fetchApi } = await import("Utils/fetchAPI");
          logsResp = await fetchApi({
            url: "assets/ugsyncAudits",
            params: params
          });
          logs = logsResp.data.vxUgsyncAuditInfoList;
          totalCount = logsResp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(
            `Error occurred while fetching User Sync logs! ${error}`
          );
        }
        setUserSyncLogs(logs);
        setEntries(logsResp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [updateTable, searchFilterParams]
  );

  const refreshTable = () => {
    setUserSyncLogs([]);
    setLoader(true);
    setUpdateTable(moment.now());
  };

  const toggleTableSyncModal = (raw) => {
    setTableSyncdetails({
      syncDteails: raw,
      showSyncDetails: true
    });
  };

  const toggleTableSyncModalClose = () => {
    setTableSyncdetails({
      syncDteails: {},
      showSyncDetails: false
    });
  };

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "eventTime",
        desc: true
      }
    ],
    []
  );

  const columns = React.useMemo(
    () => [
      {
        Header: "User Name",
        accessor: "userName",
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
        Header: "Sync Source",
        accessor: "syncSource",
        Cell: (rawValue) => {
          return (
            <span className="text-center d-block">
              <Badge bg="success">{rawValue.value}</Badge>
            </span>
          );
        },
        disableSortBy: true
      },
      {
        Header: "Number Of New",
        id: "new",
        columns: [
          {
            Header: "Users",
            accessor: "noOfNewUsers",
            Cell: (rawValue) => {
              return (
                <span className="text-center d-block">{rawValue.value}</span>
              );
            },
            width: 100,
            disableSortBy: true
          },
          {
            Header: "Groups",
            accessor: "noOfNewGroups",
            Cell: (rawValue) => {
              return (
                <span className="text-center d-block">{rawValue.value}</span>
              );
            },
            width: 100,
            disableSortBy: true
          }
        ]
      },
      {
        Header: "Number Of Modified",
        id: "modified",
        columns: [
          {
            Header: "Users",
            accessor: "noOfModifiedUsers",
            Cell: (rawValue) => {
              return (
                <span className="text-center d-block">{rawValue.value}</span>
              );
            },
            width: 100,
            disableSortBy: true
          },
          {
            Header: "Groups",
            accessor: "noOfModifiedGroups",
            Cell: (rawValue) => {
              return (
                <span className="text-center d-block">{rawValue.value}</span>
              );
            },
            width: 100,
            disableSortBy: true
          }
        ]
      },

      {
        Header: "Event Time",
        accessor: "eventTime",
        Cell: (rawValue) => {
          const formatDateTime = dateFormat(
            rawValue.value,
            "mm/dd/yyyy hh:MM:ss TT"
          );
          return <span className="text-center d-block">{formatDateTime}</span>;
        },
        minWidth: 170,
        sortable: true
      },
      {
        Header: "Sync Details",
        accessor: "syncSourceInfo",
        Cell: (rawValue, model) => {
          if (rawValue.value) {
            return (
              <div className="text-center">
                <button
                  className="btn btn-outline-dark btn-sm"
                  data-id="syncDetailes"
                  data-cy="syncDetailes"
                  title="Sync Details"
                  id={model.id}
                  onClick={() => {
                    toggleTableSyncModal(rawValue.value);
                  }}
                >
                  <i className="fa-fw fa fa-eye"> </i>
                </button>
              </div>
            );
          } else {
            return " -- ";
          }
        },
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
    localStorage.setItem("userSync", JSON.stringify(searchParam));

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
      category: "startDate",
      label: "Start Date",
      urlLabel: "startDate",
      type: "date"
    },
    {
      category: "syncSource",
      label: "Sync Source",
      urlLabel: "syncSource",
      type: "textoptions",
      options: () => {
        return [
          { value: "File", label: "File" },
          { value: "LDAP/AD", label: "LDAP/AD" },
          { value: "Unix", label: "Unix" }
        ];
      }
    },
    {
      category: "userName",
      label: "User Name",
      urlLabel: "userName",
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
                key="usersync-audit-search-filter"
                placeholder="Search for your user sync audits..."
                options={sortBy(searchFilterOptions, ["label"])}
                onChange={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
              />
            </div>
          </Col>
        </Row>
        <AuditFilterEntries entries={entries} refreshTable={refreshTable} />
        <XATableLayout
          data={userSyncListingData}
          columns={columns}
          loading={loader}
          totalCount={entries && entries.totalCount}
          fetchData={fetchUserSyncInfo}
          pageCount={pageCount}
          columnSort={true}
          defaultSort={getDefaultSort}
        />
        <Modal
          show={showTableSyncDetails && showTableSyncDetails.showSyncDetails}
          onHide={toggleTableSyncModalClose}
          size="xl"
        >
          <Modal.Header>
            <Modal.Title>Sync Source Details</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <SyncSourceDetails
              syncDetails={showTableSyncDetails.syncDteails}
            ></SyncSourceDetails>
          </Modal.Body>
          <Modal.Footer>
            <Button
              variant="primary"
              size="sm"
              onClick={toggleTableSyncModalClose}
            >
              OK
            </Button>
          </Modal.Footer>
        </Modal>
      </React.Fragment>
    </div>
  );
}

export default User_Sync;
