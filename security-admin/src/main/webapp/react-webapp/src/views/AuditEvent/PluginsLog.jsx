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
import { useSearchParams, useOutletContext } from "react-router-dom";
import { Badge, Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { AuditFilterEntries } from "Components/CommonComponents";
import moment from "moment-timezone";
import dateFormat from "dateformat";
import { sortBy, filter, isEmpty } from "lodash";
import StructuredFilter from "../../components/structured-filter/react-typeahead/tokenizer";
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
} from "../../utils/XAUtils";
import { Loader } from "../../components/CommonComponents";

function Plugins() {
  const context = useOutletContext();
  const services = context.services;
  const [pluginsListingData, setPluginsLogs] = useState([]);
  const [loader, setLoader] = useState(true);
  const [pageCount, setPageCount] = React.useState(0);
  const [entries, setEntries] = useState([]);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const fetchIdRef = useRef(0);
  //const [services, setServices] = useState([]);
  const [contentLoader, setContentLoader] = useState(true);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [resetPage, setResetpage] = useState({ page: null });
  const isKMSRole = isKeyAdmin() || isKMSAuditor();

  useEffect(() => {
    let { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("agent", searchParams, searchFilterOptions);

    // Updating the states for search params, search filter, default search filter and localStorage
    setSearchParams(searchParam, { replace: true });
    if (
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    localStorage.setItem("agent", JSON.stringify(searchParam));
    setContentLoader(false);
  }, [searchParams]);

  /* const fetchServices = async () => {
    let servicesResp = [];
    try {
      servicesResp = await fetchApi({
        url: "plugins/services"
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Services or CSRF headers! ${error}`
      );
    }

    setServices(servicesResp.data.services);
    setLoader(false);
  }; */

  const fetchPluginsInfo = useCallback(
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
            url: "assets/exportAudit",
            params: params,
            skipNavigate: true
          });
          logs = logsResp.data.vXPolicyExportAudits;
          totalCount = logsResp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching Plugins logs! ${error}`);
        }
        setPluginsLogs(logs);
        setEntries(logsResp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [updateTable, searchFilterParams]
  );

  const refreshTable = () => {
    setPluginsLogs([]);
    setLoader(true);
    setUpdateTable(moment.now());
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
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam, { replace: true });
    localStorage.setItem("agent", JSON.stringify(searchParam));

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
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

  return contentLoader ? (
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
                defaultSelected={defaultSearchFilterParams}
              />
            </div>
          </Col>
        </Row>
        <AuditFilterEntries entries={entries} refreshTable={refreshTable} />
        <XATableLayout
          data={pluginsListingData}
          columns={columns}
          loading={loader}
          totalCount={entries && entries.totalCount}
          fetchData={fetchPluginsInfo}
          pageCount={pageCount}
          columnSort={true}
          defaultSort={getDefaultSort}
        />
      </React.Fragment>
    </div>
  );
}

export default Plugins;
