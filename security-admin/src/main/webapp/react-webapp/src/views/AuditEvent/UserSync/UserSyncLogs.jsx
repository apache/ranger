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
import { useSearchParams, useLocation } from "react-router-dom";
import { Badge, Modal, Button, Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { AuditFilterEntries, Loader } from "Components/CommonComponents";
import { SyncSourceDetails } from "Views/UserGroupRoleListing/SyncSourceDetails";
import dateFormat from "dateformat";
import moment from "moment-timezone";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import { fetchApi } from "Utils/fetchAPI";
import { sortBy, has, pick } from "lodash";
import {
  getTableSortBy,
  getTableSortType,
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError
} from "Utils/XAUtils";
import { ACTIONS } from "Views/AuditEvent/action";
import { reducer, USERSYNC_INITIAL_STATE } from "Views/AuditEvent/reducer";

function UserSync() {
  const [state, dispatch] = useReducer(reducer, USERSYNC_INITIAL_STATE);

  const location = useLocation();

  const [searchParams, setSearchParams] = useSearchParams();

  const openSyncTableModal = (data) => {
    dispatch({
      type: ACTIONS.SHOW_SYNC_TABLE_MODAL,
      showSyncTableModal: true,
      syncTableData: data
    });
  };

  const closeSyncTableModal = () => {
    dispatch({
      type: ACTIONS.SHOW_SYNC_TABLE_MODAL,
      showSyncTableModal: false,
      syncTableData: {}
    });
  };

  useEffect(() => {
    const currentDate = moment(moment()).format("MM/DD/YYYY");
    const { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("userSync", searchParams, searchFilterOptions);

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
    localStorage.setItem("userSync", JSON.stringify(searchParam));
  }, []);

  useEffect(() => {
    const { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("userSync", searchParams, searchFilterOptions);

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
    localStorage.setItem("userSync", JSON.stringify(searchParam));
    dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
  }, [location.search]);

  const fetchUserSyncInfo = useCallback(
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
          url: "assets/ugsyncAudits",
          params: params
        });

        const logsEntries = pick(response.data, [
          "startIndex",
          "pageSize",
          "totalCount",
          "resultSize"
        ]);
        const logsResp = response.data?.vxUgsyncAuditInfoList || [];
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
        console.error(`Error occurred while fetching User Sync logs! ${error}`);
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
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <div className="text-center">
                <button
                  className="btn btn-outline-dark btn-sm"
                  data-id="syncDetailes"
                  data-cy="syncDetailes"
                  title="Sync Details"
                  onClick={() => {
                    openSyncTableModal(rawValue.value);
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
    localStorage.setItem("userSync", JSON.stringify(searchParam));

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

  return state.contentLoader ? (
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
          loading={state.loader}
          totalCount={state.entries && state.entries.totalCount}
          fetchData={fetchUserSyncInfo}
          pageCount={state.pageCount}
          columnSort={true}
          defaultSort={getDefaultSort}
        />

        <Modal
          show={state.showSyncTableModal}
          onHide={closeSyncTableModal}
          size="xl"
        >
          <Modal.Header closeButton>
            <Modal.Title>Sync Source Details</Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <SyncSourceDetails
              syncDetails={state.syncTableData}
            ></SyncSourceDetails>
          </Modal.Body>
          <Modal.Footer>
            <Button variant="primary" size="sm" onClick={closeSyncTableModal}>
              OK
            </Button>
          </Modal.Footer>
        </Modal>
      </React.Fragment>
    </div>
  );
}

export default UserSync;
