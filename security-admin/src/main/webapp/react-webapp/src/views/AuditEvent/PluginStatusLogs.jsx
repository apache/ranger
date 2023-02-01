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
import { Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { AuditFilterEntries } from "Components/CommonComponents";
import moment from "moment-timezone";
import dateFormat from "dateformat";
import {
  setTimeStamp,
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError,
  isKeyAdmin,
  isKMSAuditor
} from "Utils/XAUtils";
import {
  CustomPopover,
  CustomTooltip,
  Loader
} from "../../components/CommonComponents";
import StructuredFilter from "../../components/structured-filter/react-typeahead/tokenizer";
import { fetchApi } from "Utils/fetchAPI";
import { isEmpty, isUndefined, map, sortBy, toUpper, filter } from "lodash";

function Plugin_Status() {
  const [pluginStatusListingData, setPluginStatusLogs] = useState([]);
  const [loader, setLoader] = useState(true);
  const [pageCount, setPageCount] = React.useState(0);
  const [entries, setEntries] = useState([]);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const fetchIdRef = useRef(0);
  const [serviceDefs, setServiceDefs] = useState([]);
  const [services, setServices] = useState([]);
  const [contentLoader, setContentLoader] = useState(true);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [resetPage, setResetpage] = useState({ page: null });
  const isKMSRole = isKeyAdmin() || isKMSAuditor();

  useEffect(() => {
    if (isEmpty(serviceDefs)) {
      fetchServiceDefs(), fetchServices();
    }

    if (!isEmpty(serviceDefs)) {
      let { searchFilterParam, defaultSearchFilterParam, searchParam } =
        fetchSearchFilterParams(
          "pluginStatus",
          searchParams,
          searchFilterOptions
        );

      // Updating the states for search params, search filter, default search filter and localStorage
      setSearchParams(searchParam);
      setSearchFilterParams(searchFilterParam);
      setDefaultSearchFilterParams(defaultSearchFilterParam);
      localStorage.setItem("pluginStatus", JSON.stringify(searchParam));
      setContentLoader(false);
    }
  }, [serviceDefs]);

  useEffect(() => {
    let { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams(
        "pluginStatus",
        searchParams,
        searchFilterOptions
      );

    // Updating the states for search params, search filter, default search filter and localStorage
    setSearchParams(searchParam);
    if (
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    localStorage.setItem("pluginStatus", JSON.stringify(searchParam));
    setContentLoader(false);
  }, [searchParams]);

  const fetchServiceDefs = async () => {
    setLoader(true);
    let serviceDefsResp = [];
    try {
      serviceDefsResp = await fetchApi({
        url: "plugins/definitions"
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Service Definitions or CSRF headers! ${error}`
      );
    }

    setServiceDefs(serviceDefsResp.data.serviceDefs);
    setLoader(false);
  };

  const fetchServices = async () => {
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
    setContentLoader(false);
  };

  const fetchPluginStatusInfo = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      setLoader(true);
      if (!isEmpty(serviceDefs)) {
        let logsResp = [];
        let logs = [];
        let totalCount = 0;
        const fetchId = ++fetchIdRef.current;
        let params = { ...searchFilterParams };
        if (fetchId === fetchIdRef.current) {
          params["pageSize"] = pageSize;
          params["startIndex"] = pageIndex * pageSize;
          try {
            logsResp = await fetchApi({
              url: "plugins/plugins/info",
              params: params
            });
            logs = logsResp.data.pluginInfoList;
            totalCount = logsResp.data.totalCount;
          } catch (error) {
            serverError(error);
            console.error(
              `Error occurred while fetching Plugin Status logs! ${error}`
            );
          }
          setPluginStatusLogs(logs);
          setEntries(logsResp.data);
          setPageCount(Math.ceil(totalCount / pageSize));
          setResetpage({ page: gotoPage });
          setLoader(false);
        }
      }
    },
    [updateTable, searchFilterParams]
  );

  const isDateDifferenceMoreThanHr = (date1, date2) => {
    let diff = (date1 - date2) / 36e5;
    return diff < 0 ? true : false;
  };

  const refreshTable = () => {
    setPluginStatusLogs([]);
    setLoader(true);
    setUpdateTable(moment.now());
  };

  const contents = (val) => {
    return (
      <>
        <ul className="list-inline">
          <li className="list-inline-item">
            <strong>Last Update: </strong> Last updated time of{" "}
            {val == "Tag" ? "Tag-service" : "policy"}.
          </li>
          <li className="list-inline-item">
            <strong>Download: </strong>
            {val == "Tag"
              ? "Time when tag-based policies sync-up with Ranger."
              : "Time when policy actually downloaded(sync-up with Ranger)."}
          </li>
          <li className="list-inline-item">
            <strong>Active: </strong> Time when{" "}
            {val == "Tag" ? "tag-based" : "policy"} actually in use for
            enforcement.
          </li>
        </ul>
      </>
    );
  };

  const infoIcon = (val) => {
    return (
      <>
        <b>{val} ( Time )</b>

        <CustomPopover
          icon="fa-fw fa fa-info-circle info-icon"
          title={
            val == "Policy"
              ? "Policy (Time details)"
              : "Tag Policy (Time details)"
          }
          content={contents(val)}
          placement="left"
          trigger={["hover", "focus"]}
        />
      </>
    );
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Service Name",
        accessor: "serviceDisplayName",
        sortMethod: (a, b) => {
          if (a.length === b.length) {
            return a > b ? 1 : -1;
          }
          return a.length > b.length ? 1 : -1;
        }
      },
      {
        Header: "Service Type",
        accessor: "serviceType",
        Cell: (rawValue) => {
          return (
            <div className="text-truncate">
              <span title={rawValue.value}>{rawValue.value}</span>
            </div>
          );
        }
      },
      {
        Header: "Application",
        accessor: "appType"
      },
      {
        Header: "Host Name",
        accessor: "hostName",
        Cell: (rawValue) => {
          return (
            <div className="text-truncate">
              <span title={rawValue.value}>{rawValue.value}</span>
            </div>
          );
        }
      },
      {
        Header: "Plugin IP",
        accessor: "ipAddress"
      },
      {
        Header: "Cluster Name",
        accessor: "clusterName",
        Cell: ({ row: { original } }) => {
          let clusterName = original?.info?.clusterName;
          return isUndefined(clusterName) ? "--" : clusterName;
        }
      },
      {
        Header: infoIcon("Policy"),
        id: "policyinfo",
        columns: [
          {
            Header: "Last Update",
            accessor: "lastPolicyUpdateTime",
            Cell: ({ row: { original } }) => {
              return setTimeStamp(original.info.lastPolicyUpdateTime);
            },
            minWidth: 190
          },
          {
            Header: "Download",
            accessor: "policyDownloadTime",
            Cell: ({ row: { original } }) => {
              var downloadDate = new Date(
                parseInt(original.info.policyDownloadTime)
              );

              dateFormat(
                parseInt(original.info.policyDownloadTime),
                "mm/dd/yyyy hh:MM:ss TT"
              );
              if (!isUndefined(original.info.lastPolicyUpdateTime)) {
                var lastUpdateDate = new Date(
                  parseInt(original.info.lastPolicyUpdateTime)
                );
                if (isDateDifferenceMoreThanHr(downloadDate, lastUpdateDate)) {
                  if (
                    moment(downloadDate).diff(
                      moment(lastUpdateDate),
                      "minutes"
                    ) >= -2
                  ) {
                    return (
                      <span className="text-warning">
                        <CustomTooltip
                          placement="bottom"
                          content={
                            "Policy is updated but not yet downloaded(sync-upwith Ranger)"
                          }
                          icon="fa-fw fa fa-exclamation-circle active-policy-alert"
                        />
                        {setTimeStamp(original.info.policyDownloadTime)}
                      </span>
                    );
                  } else {
                    return (
                      <span className="text-error">
                        <CustomTooltip
                          placement="bottom"
                          content={
                            "Policy is updated but not yet downloaded(sync-upwith Ranger)"
                          }
                          icon="fa-fw fa fa-exclamation-circle active-policy-alert"
                        />
                        {setTimeStamp(original.info.policyDownloadTime)}{" "}
                      </span>
                    );
                  }
                }
              }
              return setTimeStamp(original.info.policyDownloadTime);
            },
            minWidth: 190
          },
          {
            Header: "Active",
            accessor: "policyActivationTime",
            Cell: ({ row: { original } }) => {
              let activeDate = new Date(
                parseInt(original.info.policyActivationTime)
              );

              if (!isUndefined(original.info.lastPolicyUpdateTime)) {
                let lastUpdateDate = new Date(
                  parseInt(original.info.lastPolicyUpdateTime)
                );

                if (isDateDifferenceMoreThanHr(activeDate, lastUpdateDate)) {
                  if (
                    moment(activeDate).diff(
                      moment(lastUpdateDate),
                      "minutes"
                    ) >= -2
                  ) {
                    return (
                      <span className="text-warning">
                        <CustomTooltip
                          placement="bottom"
                          content={
                            " Policy is updated but not yet used for any enforcement."
                          }
                          icon="fa-fw fa fa-exclamation-circle active-policy-alert"
                        />
                        {setTimeStamp(original.info.policyActivationTime)}
                      </span>
                    );
                  } else {
                    return (
                      <span className="text-error">
                        <CustomTooltip
                          placement="bottom"
                          content={
                            " Policy is updated but not yet used for any enforcement."
                          }
                          icon="fa-fw fa fa-exclamation-circle active-policy-alert"
                        />
                        {setTimeStamp(original.info.policyActivationTime)}
                      </span>
                    );
                  }
                }
              }
              return setTimeStamp(original.info.policyActivationTime);
            },
            minWidth: 190
          }
        ]
      },
      {
        Header: infoIcon("Tag"),
        id: "taginfo",
        columns: [
          {
            Header: "Last Update",
            accessor: "lastTagUpdateTime",
            Cell: ({ row: { original } }) => {
              return setTimeStamp(original.info.lastTagUpdateTime);
            },
            minWidth: 190
          },
          {
            Header: "Download",
            accessor: "tagDownloadTime",

            Cell: ({ row: { original } }) => {
              var downloadDate = new Date(
                parseInt(original.info.tagDownloadTime)
              );

              if (!isUndefined(original.info.lastTagUpdateTime)) {
                var lastUpdateDate = new Date(
                  parseInt(original.info.lastTagUpdateTime)
                );
                if (isDateDifferenceMoreThanHr(downloadDate, lastUpdateDate)) {
                  if (
                    moment(downloadDate).diff(
                      moment(lastUpdateDate),
                      "minutes"
                    ) >= -2
                  ) {
                    return (
                      <span className="text-warning">
                        <CustomTooltip
                          placement="bottom"
                          content={
                            "Policy is updated but not yet downloaded(sync-upwith Ranger)"
                          }
                          icon="fa-fw fa fa-exclamation-circle active-policy-alert"
                        />
                        {setTimeStamp(original.info.tagDownloadTime)}
                      </span>
                    );
                  } else {
                    return (
                      <span className="text-error">
                        <CustomTooltip
                          placement="bottom"
                          content={
                            "Policy is updated but not yet downloaded(sync-upwith Ranger)"
                          }
                          icon="fa-fw fa fa-exclamation-circle active-policy-alert"
                        />
                        {setTimeStamp(original.info.tagDownloadTime)}{" "}
                      </span>
                    );
                  }
                }
              }
              return setTimeStamp(original.info.tagDownloadTime);
            },
            minWidth: 190
          },
          {
            Header: "Active",
            accessor: "tagActivationTime",

            Cell: ({ row: { original } }) => {
              var downloadDate = new Date(
                parseInt(original.info.tagActivationTime)
              );

              if (!isUndefined(original.info.lastTagUpdateTime)) {
                var lastUpdateDate = new Date(
                  parseInt(original.info.lastTagUpdateTime)
                );
                if (isDateDifferenceMoreThanHr(downloadDate, lastUpdateDate)) {
                  if (
                    moment(downloadDate).diff(
                      moment(lastUpdateDate),
                      "minutes"
                    ) >= -2
                  ) {
                    return (
                      <span className="text-warning">
                        <CustomTooltip
                          placement="bottom"
                          content={
                            "Policy is updated but not yet used for anyenforcement."
                          }
                          icon="fa-fw fa fa-exclamation-circle active-policy-alert"
                        />
                        {setTimeStamp(original.info.tagActivationTime)}
                      </span>
                    );
                  } else {
                    return (
                      <span className="text-error">
                        <CustomTooltip
                          placement="bottom"
                          content={
                            "Policy is updated but not yet used for anyenforcement."
                          }
                          icon="fa-fw fa fa-exclamation-circle active-policy-alert"
                        />
                        {setTimeStamp(original.info.tagActivationTime)}{" "}
                      </span>
                    );
                  }
                }
              }
              return setTimeStamp(original.info.tagActivationTime);
            },
            minWidth: 190
          }
        ]
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
    setSearchParams(searchParam);
    localStorage.setItem("pluginStatus", JSON.stringify(searchParam));

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
    }
  };

  const getServiceDefType = () => {
    let serviceDefType = [];

    serviceDefType = map(serviceDefs, function (serviceDef) {
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
      category: "pluginAppType",
      label: "Application",
      urlLabel: "applicationType",
      type: "text"
    },
    {
      category: "clusterName",
      label: "Cluster Name",
      urlLabel: "clusterName",
      type: "text"
    },
    {
      category: "pluginHostName",
      label: "Host Name",
      urlLabel: "hostName",
      type: "text"
    },
    {
      category: "pluginIpAddress",
      label: "Plugin IP",
      urlLabel: "agentIp",
      type: "text"
    },
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

  return contentLoader ? (
    <Loader />
  ) : (
    <div className="wrap">
      <React.Fragment>
        <Row className="mb-2">
          <Col sm={12}>
            <div className="searchbox-border">
              <StructuredFilter
                key="plugin-status-log-search-filter"
                placeholder="Search for your plugin status..."
                options={sortBy(searchFilterOptions, ["label"])}
                onChange={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
              />
            </div>
          </Col>
        </Row>
        <AuditFilterEntries entries={entries} refreshTable={refreshTable} />
        <XATableLayout
          data={pluginStatusListingData}
          columns={columns}
          loading={loader}
          totalCount={entries && entries.totalCount}
          fetchData={fetchPluginStatusInfo}
          pageCount={pageCount}
          columnSort={true}
          clientSideSorting={true}
        />
      </React.Fragment>
    </div>
  );
}

export default Plugin_Status;
