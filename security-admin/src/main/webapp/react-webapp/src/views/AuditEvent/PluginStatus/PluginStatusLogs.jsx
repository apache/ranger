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
import { Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import moment from "moment-timezone";
import {
  setTimeStamp,
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError,
  isKeyAdmin,
  isKMSAuditor
} from "Utils/XAUtils";
import {
  AuditFilterEntries,
  CustomPopover,
  CustomTooltip,
  Loader
} from "Components/CommonComponents";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import { fetchApi } from "Utils/fetchAPI";
import {
  isUndefined,
  map,
  sortBy,
  toUpper,
  filter,
  isNull,
  isEmpty,
  cloneDeep,
  get,
  pick
} from "lodash";
import { getServiceDef } from "Utils/appState";
import { pluginStatusColumnInfo } from "Utils/XAMessages";
import { pluginStatusColumnInfoMsg } from "Utils/XAEnums";
import { ACTIONS } from "Views/AuditEvent/action";
import { reducer, PLUGIN_STATUS_INITIAL_STATE } from "Views/AuditEvent/reducer";

function PluginStatusLogs() {
  const [state, dispatch] = useReducer(reducer, PLUGIN_STATUS_INITIAL_STATE);

  const location = useLocation();

  const { services, servicesAvailable } = useOutletContext();

  const { allServiceDefs } = cloneDeep(getServiceDef());
  const isKMSRole = isKeyAdmin() || isKMSAuditor();

  const [searchParams, setSearchParams] = useSearchParams();

  useEffect(() => {
    if (servicesAvailable !== null) {
      const { searchFilterParam, defaultSearchFilterParam, searchParam } =
        fetchSearchFilterParams(
          "pluginStatus",
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
      localStorage.setItem("pluginStatus", JSON.stringify(searchParam));
      dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
    }
  }, [location.search, servicesAvailable]);

  const fetchPluginStatusInfo = useCallback(
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
            url: "plugins/plugins/info",
            params: params
          });

          const logsEntries = pick(response.data, [
            "startIndex",
            "pageSize",
            "totalCount",
            "resultSize"
          ]);
          const logsResp = response.data?.pluginInfoList || [];
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
            `Error occurred while fetching Plugin Status logs! ${error}`
          );
        }

        dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: false });
      }
    },
    [state.refreshTableData, servicesAvailable]
  );

  const isDateDifferenceMoreThanHr = (date1, date2) => {
    let diff = (date1 - date2) / 36e5;
    return diff < 0 ? true : false;
  };

  const getLastUpdateTime = (lastUpdateTime) => {
    if (isUndefined(lastUpdateTime) || isNull(lastUpdateTime)) {
      return <center>--</center>;
    }
    return setTimeStamp(lastUpdateTime);
  };

  const getDownloadTime = (downloadTime, lastUpdateTime, columnsName) => {
    if (isUndefined(downloadTime) || isNull(downloadTime)) {
      return <center>--</center>;
    }

    if (!isUndefined(lastUpdateTime)) {
      let downloadDate = new Date(parseInt(downloadTime));
      let lastUpdateDate = new Date(parseInt(lastUpdateTime));
      if (isDateDifferenceMoreThanHr(downloadDate, lastUpdateDate)) {
        if (
          moment(downloadDate).diff(moment(lastUpdateDate), "minutes") >= -2
        ) {
          return (
            <span className="text-warning">
              <CustomTooltip
                placement="bottom"
                content={
                  pluginStatusColumnInfoMsg[columnsName].downloadTimeDelayMsg
                }
                icon="fa-fw fa fa-exclamation-circle active-policy-alert"
              />
              {setTimeStamp(downloadTime)}
            </span>
          );
        } else {
          return (
            <span className="text-error">
              <CustomTooltip
                placement="bottom"
                content={
                  pluginStatusColumnInfoMsg[columnsName].downloadTimeDelayMsg
                }
                icon="fa-fw fa fa-exclamation-circle active-policy-alert"
              />
              {setTimeStamp(downloadTime)}{" "}
            </span>
          );
        }
      }
    }
    return setTimeStamp(downloadTime);
  };

  const getActivationTime = (activeTime, lastUpdateTime, columnsName) => {
    if (isUndefined(activeTime) || isNull(activeTime) || activeTime == 0) {
      return <center>--</center>;
    }
    if (!isUndefined(lastUpdateTime)) {
      let activeDate = new Date(parseInt(activeTime));
      let lastUpdateDate = new Date(parseInt(lastUpdateTime));
      if (isDateDifferenceMoreThanHr(activeDate, lastUpdateDate)) {
        if (moment(activeDate).diff(moment(lastUpdateDate), "minutes") >= -2) {
          return (
            <span className="text-warning">
              <CustomTooltip
                placement="bottom"
                content={
                  pluginStatusColumnInfoMsg[columnsName].activationTimeDelayMsg
                }
                icon="fa-fw fa fa-exclamation-circle active-policy-alert"
              />
              {setTimeStamp(activeTime)}
            </span>
          );
        } else {
          return (
            <span className="text-error">
              <CustomTooltip
                placement="bottom"
                content={
                  pluginStatusColumnInfoMsg[columnsName].activationTimeDelayMsg
                }
                icon="fa-fw fa fa-exclamation-circle active-policy-alert"
              />
              {setTimeStamp(activeTime)}
            </span>
          );
        }
      }
    }
    return setTimeStamp(activeTime);
  };

  const refreshTable = () => {
    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: state.searchFilterParams,
      refreshTableData: moment.now()
    });
  };

  const infoIcon = (columnsName) => {
    return (
      <>
        <b>{columnsName} ( Time )</b>

        <CustomPopover
          icon="fa-fw fa fa-info-circle info-icon"
          title={pluginStatusColumnInfoMsg[columnsName].title}
          content={pluginStatusColumnInfo(columnsName)}
          placement="left"
          trigger={["hover", "focus"]}
        />
      </>
    );
  };

  const dateTimeSorting = (rowA, rowB, colAccessor) => {
    // Custom comparator function for sorting date-time values
    const dateA = new Date(parseInt(rowA.original?.info[colAccessor] ?? 0));
    const dateB = new Date(parseInt(rowB.original?.info[colAccessor] ?? 0));

    // Compare dates
    if (dateA < dateB) return -1;
    if (dateA > dateB) return 1;
    return;
  };

  const dataSorting = (rowA, rowB, colAccessor) => {
    const allSame =
      (get(rowA.original, colAccessor) ?? "") ===
      (get(rowB.original, colAccessor) ?? "");
    if (allSame == true) {
      // If all values are the same, return to disable sorting
      return;
    } else {
      // Otherwise, call the generic sorting function for sorting values
      return (get(rowA.original, colAccessor) ?? "").localeCompare(
        get(rowB.original, colAccessor) ?? ""
      );
    }
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Service Name",
        accessor: "serviceDisplayName",
        sortType: (rowA, rowB, colAccessor) => {
          return dataSorting(rowA, rowB, colAccessor);
        },
        Cell: (rawValue) => {
          return (
            <div className="text-truncate">
              <span title={rawValue.value}>
                {!isEmpty(rawValue.value) ? (
                  rawValue.value
                ) : (
                  <center>--</center>
                )}
              </span>
            </div>
          );
        }
      },
      {
        Header: "Service Type",
        accessor: "serviceType",
        sortType: (rowA, rowB, colAccessor) => {
          return dataSorting(rowA, rowB, colAccessor);
        },
        Cell: (rawValue) => {
          return (
            <div className="text-truncate">
              <span title={rawValue.value}>
                {!isEmpty(rawValue.value) ? (
                  rawValue.value
                ) : (
                  <center>--</center>
                )}
              </span>
            </div>
          );
        }
      },
      {
        Header: "Application",
        accessor: "appType",
        sortType: (rowA, rowB, colAccessor) => {
          return dataSorting(rowA, rowB, colAccessor);
        },
        Cell: (rawValue) => {
          return (
            <div className="text-truncate">
              <span title={rawValue.value}>
                {!isEmpty(rawValue.value) ? (
                  rawValue.value
                ) : (
                  <center>--</center>
                )}
              </span>
            </div>
          );
        }
      },
      {
        Header: "Host Name",
        accessor: "hostName",
        sortType: (rowA, rowB, colAccessor) => {
          return dataSorting(rowA, rowB, colAccessor);
        },
        Cell: (rawValue) => {
          return (
            <div className="text-truncate">
              <span title={rawValue.value}>
                {!isEmpty(rawValue.value) ? (
                  rawValue.value
                ) : (
                  <center>--</center>
                )}
              </span>
            </div>
          );
        }
      },
      {
        Header: "Plugin IP",
        accessor: "ipAddress",
        sortType: (rowA, rowB, colAccessor) => {
          return dataSorting(rowA, rowB, colAccessor);
        },
        Cell: (rawValue) => {
          return (
            <div className="text-truncate">
              <span title={rawValue.value}>
                {!isEmpty(rawValue.value) ? (
                  rawValue.value
                ) : (
                  <center>--</center>
                )}
              </span>
            </div>
          );
        }
      },
      {
        Header: "Cluster Name",
        accessor: "info.clusterName",
        sortType: (rowA, rowB, colAccessor) => {
          return dataSorting(rowA, rowB, colAccessor);
        },
        Cell: (rawValue) => {
          return (
            <div className="text-truncate">
              <span title={rawValue.value}>
                {!isEmpty(rawValue.value) ? (
                  rawValue.value
                ) : (
                  <center>--</center>
                )}
              </span>
            </div>
          );
        }
      },
      {
        Header: infoIcon("Policy"),
        id: "Policy (Time)",
        columns: [
          {
            Header: "Last Update",
            accessor: "lastPolicyUpdateTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getLastUpdateTime(original.info.lastPolicyUpdateTime);
            },
            minWidth: 190
          },
          {
            Header: "Download",
            accessor: "policyDownloadTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getDownloadTime(
                original.info.policyDownloadTime,
                original.info.lastPolicyUpdateTime,
                "Policy"
              );
            },
            minWidth: 190
          },
          {
            Header: "Active",
            accessor: "policyActivationTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getActivationTime(
                original.info.policyActivationTime,
                original.info.lastPolicyUpdateTime,
                "Policy"
              );
            },
            minWidth: 190
          }
        ]
      },
      {
        Header: infoIcon("Tag"),
        id: "Tag (Time)",
        columns: [
          {
            Header: "Last Update",
            accessor: "lastTagUpdateTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getLastUpdateTime(original.info.lastTagUpdateTime);
            },
            minWidth: 190
          },
          {
            Header: "Download",
            accessor: "tagDownloadTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getDownloadTime(
                original.info.tagDownloadTime,
                original.info.lastTagUpdateTime,
                "Tag"
              );
            },
            minWidth: 190
          },
          {
            Header: "Active",
            accessor: "tagActivationTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getActivationTime(
                original.info.tagActivationTime,
                original.info.lastTagUpdateTime,
                "Tag"
              );
            },
            minWidth: 190
          }
        ]
      },
      {
        Header: infoIcon("GDS"),
        id: "GDS (Time)",
        columns: [
          {
            Header: "Last Update",
            accessor: "lastGdsUpdateTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getLastUpdateTime(original.info.lastGdsUpdateTime);
            },
            minWidth: 190
          },
          {
            Header: "Download",
            accessor: "gdsDownloadTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getDownloadTime(
                original.info.gdsDownloadTime,
                original.info.lastGdsUpdateTime,
                "GDS"
              );
            },
            minWidth: 190
          },
          {
            Header: "Active",
            accessor: "gdsActivationTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getActivationTime(
                original.info.gdsActivationTime,
                original.info.lastGdsUpdateTime,
                "GDS"
              );
            },
            minWidth: 190
          }
        ]
      },
      {
        Header: infoIcon("Role"),
        id: "Role (Time)",
        columns: [
          {
            Header: "Last Update",
            accessor: "lastRoleUpdateTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getLastUpdateTime(original.info.lastRoleUpdateTime);
            },
            minWidth: 190
          },
          {
            Header: "Download",
            accessor: "roleDownloadTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getDownloadTime(
                original.info.roleDownloadTime,
                original.info.lastRoleUpdateTime,
                "Role"
              );
            },
            minWidth: 190
          },
          {
            Header: "Active",
            accessor: "roleActivationTime",
            sortType: (rowA, rowB, colAccessor) => {
              return dateTimeSorting(rowA, rowB, colAccessor);
            },
            Cell: ({ row: { original } }) => {
              return getActivationTime(
                original.info.roleActivationTime,
                original.info.lastRoleUpdateTime,
                "Role"
              );
            },
            minWidth: 190
          }
        ]
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
    localStorage.setItem("pluginStatus", JSON.stringify(searchParam));

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

  return state.contentLoader ? (
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
                defaultSelected={state.defaultSearchFilterParams}
              />
            </div>
          </Col>
        </Row>

        <div className="position-relative">
          <Row>
            <Col sm={11}>
              <AuditFilterEntries
                entries={state.entries}
                refreshTable={refreshTable}
              />
            </Col>
          </Row>

          <XATableLayout
            data={state.tableListingData}
            columns={columns}
            loading={state.loader}
            totalCount={state.entries && state.entries.totalCount}
            fetchData={fetchPluginStatusInfo}
            pageCount={state.pageCount}
            columnSort={true}
            clientSideSorting={true}
            columnHide={{ tableName: "pluginStatus", isVisible: true }}
          />
        </div>
      </React.Fragment>
    </div>
  );
}

export default PluginStatusLogs;
