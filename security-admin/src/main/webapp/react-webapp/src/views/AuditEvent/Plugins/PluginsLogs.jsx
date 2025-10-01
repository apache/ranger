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
import {
  useSearchParams,
  useOutletContext,
  useLocation
} from "react-router-dom";
import { Badge, Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { AuditFilterEntries, Loader } from "Components/CommonComponents";
import moment from "moment-timezone";
import dateFormat from "dateformat";
import { sortBy, filter, isEmpty, pick } from "lodash";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import { fetchApi } from "Utils/fetchAPI";
import {
  getTableSortBy,
  getTableSortType,
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError,
  isKeyAdmin,
  isKMSAuditor,
  currentTimeZone
} from "Utils/XAUtils";
import { ACTIONS } from "Views/AuditEvent/action";
import { reducer, PLUGINS_INITIAL_STATE } from "Views/AuditEvent/reducer";

function PluginsLogs() {
  const [state, dispatch] = useReducer(reducer, PLUGINS_INITIAL_STATE);

  const location = useLocation();

  const { services } = useOutletContext();

  const isKMSRole = isKeyAdmin() || isKMSAuditor();

  const [searchParams, setSearchParams] = useSearchParams();

  useEffect(() => {
    const { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("agent", searchParams, searchFilterOptions);

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
    localStorage.setItem("agent", JSON.stringify(searchParam));
    dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
  }, [location.search]);

  const fetchPluginsInfo = useCallback(
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
          url: "assets/exportAudit",
          params: params,
          skipNavigate: true
        });

        const logsEntries = pick(response.data, [
          "startIndex",
          "pageSize",
          "totalCount",
          "resultSize"
        ]);
        const logsResp = response.data?.vXPolicyExportAudits || [];
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
        console.error(`Error occurred while fetching Plugins logs! ${error}`);
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
        Header: `Export Date  ( ${currentTimeZone()} )`,
        accessor: "createDate",
        Cell: (rawValue) => {
          const formatDateTime = dateFormat(
            rawValue.value,
            "mm/dd/yyyy hh:MM:ss TT"
          );
          return <span className="text-center d-block">{formatDateTime}</span>;
        },
        width: 240
      },
      {
        Header: "Service Name",
        accessor: "repositoryName",
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
        Header: "Plugin ID",
        accessor: "agentId",
        Cell: (rawValue) => {
          return !isEmpty(rawValue.value) ? (
            <span
              className="text-truncate text-center d-block"
              title={rawValue.value}
            >
              {rawValue.value}
            </span>
          ) : (
            <span className="text-center d-block">--</span>
          );
        },
        disableSortBy: true
      },
      {
        Header: "Plugin IP",
        accessor: "clientIP",
        Cell: (rawValue) => {
          return (
            <span
              className="text-truncate text-center d-block"
              title={rawValue.value}
            >
              {rawValue.value}
            </span>
          );
        },
        disableSortBy: true
      },
      {
        Header: "Cluster Name",
        accessor: "clusterName",
        Cell: (rawValue) => {
          return !isEmpty(rawValue.value) ? (
            <span
              className="text-truncate text-center d-block"
              title={rawValue.value}
            >
              {rawValue.value}
            </span>
          ) : (
            <span className="text-center d-block">--</span>
          );
        },
        width: 100,
        disableSortBy: true
      },
      {
        Header: "Http Response Code",
        accessor: "httpRetCode",
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
        Header: "Status",
        accessor: "syncStatus",
        disableSortBy: true
      }
    ],
    []
  );

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "createDate",
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
    localStorage.setItem("agent", JSON.stringify(searchParam));

    if (typeof state.resetPage?.page === "function") {
      state.resetPage.page(0);
    }
  };

  const getServices = () => {
    let servicesName = [];
    servicesName = filter(services, function (service) {
      return !isKMSRole
        ? service.type !== "tag" && service.type !== "kms"
        : service.type !== "tag";
    });

    return sortBy(servicesName, "name")?.map((service) => ({
      label: service.name,
      value: service.name
    }));
  };

  const searchFilterOptions = [
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
      category: "httpRetCode",
      label: "Http Response Code",
      urlLabel: "httpResponseCode",
      type: "text"
    },
    {
      category: "agentId",
      label: "Plugin ID",
      urlLabel: "pluginID",
      type: "text"
    },
    {
      category: "clientIP",
      label: "Plugin IP",
      urlLabel: "pluginIP",
      type: "text"
    },
    {
      category: "repositoryName",
      label: "Service Name",
      urlLabel: "serviceName",
      type: "textoptions",
      options: getServices
    },
    {
      category: "startDate",
      label: "Start Date",
      urlLabel: "startDate",
      type: "date"
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
                key="plugin-log-search-filter"
                placeholder="Search for your plugins..."
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
          fetchData={fetchPluginsInfo}
          pageCount={state.pageCount}
          columnSort={true}
          defaultSort={getDefaultSort}
        />
      </React.Fragment>
    </div>
  );
}

export default PluginsLogs;
