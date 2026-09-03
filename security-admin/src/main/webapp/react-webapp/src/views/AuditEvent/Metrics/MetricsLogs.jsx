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
import { Table, Modal, Button, Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { fetchApi } from "Utils/fetchAPI";
import moment from "moment-timezone";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import {
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError,
  isKeyAdmin,
  isKMSAuditor
} from "Utils/XAUtils";
import { AuditFilterEntries, Loader } from "Components/CommonComponents";
import { toUpper, sortBy, filter, map, cloneDeep, pick } from "lodash";
import MetricGraphs from "./MetricGraphs";
import { getServiceDef } from "Utils/appState";
import { ACTIONS } from "Views/AuditEvent/action";
import { reducer, METRICS_INITIAL_STATE } from "Views/AuditEvent/reducer";

function MetricsLogs() {
  const [state, dispatch] = useReducer(reducer, METRICS_INITIAL_STATE);

  const location = useLocation();

  const { services, servicesAvailable } = useOutletContext();

  const { allServiceDefs } = cloneDeep(getServiceDef());
  const isKMSRole = isKeyAdmin() || isKMSAuditor();

  const [searchParams, setSearchParams] = useSearchParams();

  useEffect(() => {
    if (servicesAvailable !== null) {
      const { searchFilterParam, defaultSearchFilterParam, searchParam } =
        fetchSearchFilterParams("metrics", searchParams, searchFilterOptions);

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
      localStorage.setItem("metrics", JSON.stringify(searchParam));
      dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
    }
  }, [location.search, servicesAvailable]);

  const fetchMetricInfo = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });
      if (servicesAvailable !== null) {
        const params = {
          ...state.searchFilterParams,
          pageSize,
          startIndex: pageIndex * pageSize
        };

        try {
          const response = await fetchApi({
            url: "audit/metrics",
            params: params
          });

          const logsEntries = pick(response.data, [
            "startIndex",
            "pageSize",
            "totalCount",
            "resultSize"
          ]);
          const logsResp = response.data?.rangerAuditMetricsList || [];
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
          console.error(`Error occurred while fetching Metric logs! ${error}`);
        }

        dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: false });
      }
    },
    [state.refreshTableData, servicesAvailable]
  );

  const refreshTable = () => {
    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: state.searchFilterParams,
      refreshTableData: moment.now()
    });
  };

  const closeMetricModal = () => {
    dispatch({
      type: ACTIONS.SHOW_METRIC_MODAL,
      showMetricModal: false,
      metricData: null
    });
  };

  const openMetricGraphModal = (metricData) => {
    dispatch({
      type: ACTIONS.SHOW_METRIC_GRAPH_MODAL,
      showMetricGraphModal: true,
      metricGraphData: metricData
    });
  };

  const closeMetricGraphModal = () => {
    dispatch({
      type: ACTIONS.SHOW_METRIC_GRAPH_MODAL,
      showMetricGraphModal: false,
      metricGraphData: state.metricGraphData
    });
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Service Name",
        accessor: "serviceName",
        Cell: (rawValue) => {
          return rawValue?.value ? rawValue.value : <center>--</center>;
        },
        disableSortBy: true
      },
      {
        Header: "Service Type",
        accessor: "serviceType",
        Cell: (rawValue) => {
          return rawValue?.value ? rawValue.value : <center>--</center>;
        },
        disableSortBy: true
      },
      {
        Header: "Application Type",
        accessor: "appId",
        Cell: (rawValue) => {
          return rawValue?.value ? (
            <span>{rawValue.value}</span>
          ) : (
            <center>&quot;--&quot;</center>
          );
        },
        disableSortBy: true
      },
      {
        Header: "Cluster Name",
        accessor: "clusterName",
        Cell: (rawValue) => {
          return rawValue?.value ? (
            <span>{rawValue.value}</span>
          ) : (
            <center>--</center>
          );
        },
        disableSortBy: true
      },
      {
        Header: "Client IP",
        accessor: "clientIP",
        Cell: (rawValue) => {
          return rawValue?.value ? (
            <span>{rawValue.value}</span>
          ) : (
            <center>&quot;--&quot;</center>
          );
        },
        disableSortBy: true
      },
      {
        Header: "Metrics Graph",
        accessor: "metricsGraph",
        Cell: (rawValue) => {
          return (
            <div>
              <button
                className="btn btn-outline-dark btn-sm"
                data-id="metricGraph"
                data-cy="metricGraph"
                onClick={() => {
                  openMetricGraphModal(rawValue.row.original);
                }}
              >
                Metrics Graph
              </button>
            </div>
          );
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
    localStorage.setItem("metrics", JSON.stringify(searchParam));

    if (typeof state.resetPage?.page === "function") {
      state.resetPage.page(0);
    }
  };

  const getServiceDefType = () => {
    let serviceDefType = [];

    serviceDefType = map(allServiceDefs, function (serviceDef) {
      return {
        label: toUpper(serviceDef.displayName),
        value: serviceDef.name
      };
    });

    return serviceDefType;
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

  const searchFilterOptions = [
    {
      category: "serviceName",
      label: "Service Name",
      urlLabel: "serviceName",
      type: "textoptions",
      options: getServices
    },
    {
      category: "serviceType",
      label: "Service Type",
      urlLabel: "serviceType",
      type: "textoptions",
      options: getServiceDefType
    }
  ];

  const showMetricDetails = (metrics) => {
    let metricText = metrics?.metricsText;
    if (metricText !== undefined) {
      for (let val in metricText) {
        return (
          <>
            <td>{val}</td>
            <td>{JSON.stringify(metricText[val])}</td>
          </>
        );
      }
    } else {
      return (
        <td>
          <div>
            <center>No Data Founds</center>
          </div>
        </td>
      );
    }
  };

  return state.contentLoader ? (
    <Loader />
  ) : (
    <div className="wrap">
      <React.Fragment>
        <Row className="mb-2">
          <Col sm={12}>
            <div className="searchbox-border">
              <StructuredFilter
                key="metrics-audit-search-filter"
                placeholder="Search for your user metric audits..."
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
          fetchData={fetchMetricInfo}
          pageCount={state.pageCount}
          columnSort={true}
        />

        <Modal show={state.showMetricModal} onHide={closeMetricModal} size="lg">
          <Modal.Header closeButton>
            <Modal.Title>
              <h4>Metrics Details</h4>
            </Modal.Title>
          </Modal.Header>
          <Modal.Body>
            <Table bordered hover>
              <thead className="thead-light">
                <tr>
                  <th>Name</th>
                  <th>Value</th>
                </tr>
              </thead>
              <tbody>
                <tr>{showMetricDetails(state.metricData)}</tr>
              </tbody>
            </Table>
          </Modal.Body>
          <Modal.Footer>
            <Button variant="primary" size="sm" onClick={closeMetricModal}>
              OK
            </Button>
          </Modal.Footer>
        </Modal>

        <Modal
          show={state.showMetricGraphModal}
          size="lg"
          onHide={closeMetricGraphModal}
        >
          <Modal.Header closeButton>
            <Modal.Title>
              <h4>Metric Graph</h4>
            </Modal.Title>
          </Modal.Header>
          <Modal.Body className="overflow-auto p-3 mb-3 mb-md-0 me-md-3">
            <MetricGraphs metricData={state.metricGraphData} />
          </Modal.Body>
          <Modal.Footer>
            <Button variant="primary" size="sm" onClick={closeMetricGraphModal}>
              OK
            </Button>
          </Modal.Footer>
        </Modal>
      </React.Fragment>
    </div>
  );
}

export default MetricsLogs;
