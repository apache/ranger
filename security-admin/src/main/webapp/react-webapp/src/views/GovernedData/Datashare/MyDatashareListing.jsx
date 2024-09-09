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
import XATableLayout from "../../../components/XATableLayout";
import { Loader } from "../../../components/CommonComponents";
import { Button, Row, Col } from "react-bootstrap";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import { useSearchParams, useNavigate, useLocation } from "react-router-dom";
import { fetchApi } from "../../../utils/fetchAPI";
import dateFormat from "dateformat";
import {
  getTableSortBy,
  getTableSortType,
  serverError,
  parseSearchFilter
} from "../../../utils/XAUtils";
import moment from "moment-timezone";
import { sortBy } from "lodash";
import CustomBreadcrumb from "../../CustomBreadcrumb";

const MyDatashareListing = () => {
  const navigate = useNavigate();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [searchParams, setSearchParams] = useSearchParams();
  const [datashareListData, setDatashareListData] = useState([]);
  const [loader, setLoader] = useState(false);
  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );
  const { state } = useLocation();
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [contentLoader, setContentLoader] = useState(false);
  const [entries, setEntries] = useState([]);
  const fetchIdRef = useRef(0);
  const [resetPage, setResetpage] = useState({ page: 0 });
  const [showrowmodal, setShowRowModal] = useState(false);
  const [rowdata, setRowData] = useState([]);
  const isMyDatasharePage = window.location.href.includes("mydatasharelisting");

  const searchFilterOptions = [
    {
      category: "dataShareNamePartial",
      label: "Name",
      urlLabel: "dataShareNamePartial",
      type: "text"
    },
    {
      category: "serviceNamePartial",
      label: "Service",
      urlLabel: "serviceNamePartial",
      type: "text"
    },
    {
      category: "zoneNamePartial",
      label: "Zone",
      urlLabel: "zoneNamePartial",
      type: "text"
    }
  ];

  useEffect(() => {
    let searchFilterParam = {};
    let searchParam = {};
    let defaultSearchFilterParam = [];

    // Get Search Filter Params from current search params
    const currentParams = Object.fromEntries([...searchParams]);
    for (const param in currentParams) {
      let category = param;
      let value = currentParams[param];
      searchFilterParam[category] = value;
      defaultSearchFilterParam.push({
        category: category,
        value: value
      });
    }
    setSearchParams({ ...currentParams, ...searchParam });
    if (
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    setContentLoader(false);
    localStorage.setItem("newDataAdded", state && state.showLastPage);
  }, [searchParams]);

  const rowModal = (row) => {
    setShowRowModal(true);
    setRowData(row.original);
  };

  const updateSearchFilter = (filter) => {
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );
    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam);
    localStorage.setItem("datashare", JSON.stringify(searchParam));

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
    }
  };

  const addDatashare = () => {
    navigate("/gds/datashare/create");
  };

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "updateTime",
        desc: true
      }
    ],
    []
  );

  const navigateToDetailPage = (datashareId, perm, name) => {
    if (perm != "LIST") {
      navigate(`/gds/datashare/${datashareId}/detail`, {
        state: {
          userAclPerm: perm,
          datashareName: name
        }
      });
    }
  };

  const myDatashareColumns = React.useMemo(
    () => [
      {
        Header: "ID",
        accessor: "id",
        width: 80,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {},
        Cell: ({ row }) => {
          return (
            <div className="position-relative text-center">
              <Button
                data-id="datashareId"
                data-cy="datashareId"
                onClick={() =>
                  navigateToDetailPage(
                    row.original.id,
                    row.original.permissionForCaller,
                    row.original.name
                  )
                }
                style={{
                  lineHeight: 1,
                  padding: 0,
                  backgroundColor: "transparent",
                  color: "#0b7fad",
                  border: 0,
                  outline: "none",
                  fontSize: 13,
                  cursor: "pointer"
                }}
              >
                {row.original.id}
              </Button>
            </div>
          );
        }
      },
      {
        Header: "Name",
        accessor: "name",
        width: 250,
        Cell: (val) => {
          return (
            <span
              className="text-truncate"
              title={val.value}
              style={{ maxWidth: "240px", display: "inline-block" }}
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Service",
        accessor: "serviceName",
        width: 250,
        Cell: (val) => {
          return (
            <span
              className="text-truncate"
              title={val.value}
              style={{ maxWidth: "240px", display: "inline-block" }}
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Zone",
        accessor: "zoneName",
        width: 250,
        Cell: (val) => {
          return (
            <span
              className="text-truncate"
              title={val.value}
              style={{ maxWidth: "240px", display: "inline-block" }}
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Created",
        accessor: "createTime",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy h:MM:ss TT");
        },
        width: 170,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Last Updated",
        accessor: "updateTime",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy h:MM:ss TT");
        },
        width: 170,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Resources",
        accessor: "resourceCount",
        width: 80,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "DATASET",
        id: "datasetInfo",
        disableResizing: true,
        columns: [
          {
            Header: "Active",
            accessor: "datasetActiveCount",
            width: 80,
            disableResizing: true,
            disableSortBy: true,
            getResizerProps: () => {}
          },
          {
            Header: "Pending",
            accessor: "datasetPendingCount",
            width: 80,
            disableResizing: true,
            disableSortBy: true,
            getResizerProps: () => {}
          }
        ]
      }
    ],
    []
  );

  const datashareColumns = React.useMemo(
    () => [
      {
        Header: "ID",
        accessor: "id",
        width: 25,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {},
        Cell: ({ row }) => {
          return (
            <div className="position-relative text-center">
              <Button
                data-id="datashareId"
                data-cy="datashareId"
                disabled={row.original.permissionForCaller == "LIST"}
                onClick={() =>
                  navigateToDetailPage(
                    row.original.id,
                    row.original.permissionForCaller
                  )
                }
                style={{
                  lineHeight: 1,
                  padding: 0,
                  backgroundColor: "transparent",
                  color: "#0b7fad",
                  border: 0,
                  outline: "none",
                  fontSize: 13,
                  cursor: "pointer"
                }}
              >
                {row.original.id}
              </Button>
            </div>
          );
        }
      },
      {
        Header: "Name",
        accessor: "name",
        width: 250,
        Cell: (val) => {
          return (
            <span
              className="text-truncate"
              title={val.value}
              style={{ maxWidth: "240px", display: "inline-block" }}
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Service",
        accessor: "serviceName",
        width: 250,
        Cell: (val) => {
          return (
            <span
              className="text-truncate"
              title={val.value}
              style={{ maxWidth: "240px", display: "inline-block" }}
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Zone",
        accessor: "zoneName",
        width: 250,
        Cell: (val) => {
          return (
            <span
              className="text-truncate"
              title={val.value}
              style={{ maxWidth: "240px", display: "inline-block" }}
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Permission",
        accessor: "permissionForCaller",
        width: 120,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {},
        Cell: (rawValue) => {
          return (
            <div className="position-relative text-center">
              <span>{rawValue.value}</span>
            </div>
          );
        }
      },
      {
        Header: "Created",
        accessor: "createTime",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy h:MM:ss TT");
        },
        width: 170,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "Last Updated",
        accessor: "updateTime",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy h:MM:ss TT");
        },
        width: 170,
        disableResizing: true,
        getResizerProps: () => {}
      }
    ],
    []
  );

  const fetchDataharetList = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      setLoader(false);
      let resp = [];
      let datashareList = [];
      let totalCount = 0;
      let page =
        state && state.showLastPage
          ? state.addPageData.totalPage - 1
          : pageIndex;
      let totalPageCount = 0;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchFilterParams };
      if (isMyDatasharePage) {
        params["gdsPermission"] = "ADMIN";
      } else {
        params["gdsPermission"] = "LIST";
      }
      if (fetchId === fetchIdRef.current) {
        params["pageSize"] = pageSize;
        params["startIndex"] =
          state && state.showLastPage
            ? (state.addPageData.totalPage - 1) * pageSize
            : pageIndex * pageSize;
        if (sortBy.length > 0) {
          if (getTableSortBy(sortBy) == "name")
            params["sortBy"] = "datashareName";
          else params["sortBy"] = getTableSortBy(sortBy);
          params["sortType"] = getTableSortType(sortBy);
        }
        try {
          resp = await fetchApi({
            url: "gds/datashare/summary",
            params: params
          });
          datashareList = resp.data.list;
          totalCount = resp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(
            `Error occurred while fetching Datashare list! ${error}`
          );
        }
        for (let i = 0; i < datashareList.length; i++) {
          let datasetActiveCount = 0;
          let datasetPendingCount = 0;

          if (datashareList[i].datasets != undefined) {
            for (let j = 0; j < datashareList[i].datasets.length; j++) {
              if (datashareList[i].datasets[j].shareStatus === "ACTIVE") {
                datasetActiveCount++;
              } else if (
                datashareList[i].datasets[j].shareStatus !== "ACTIVE" &&
                datashareList[i].datasets[j].shareStatus !== "DENIED"
              ) {
                datasetPendingCount++;
              }
            }
          }
          datashareList[i]["datasetActiveCount"] = datasetActiveCount;
          datashareList[i]["datasetPendingCount"] = datasetPendingCount;
        }
        setDatashareListData(datashareList);
        setEntries(resp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [searchFilterParams, isMyDatasharePage]
  );

  return contentLoader ? (
    <Loader />
  ) : (
    <>
      <div className="gds-header-wrapper">
        <h3 className="gds-header bold">
          {isMyDatasharePage ? "My" : ""} Datashares
        </h3>
        <CustomBreadcrumb />
      </div>
      <div className="wrap">
        <React.Fragment>
          {/* <BlockUi isUiBlock={blockUI} /> */}
          <Row className="mb-4">
            <Col sm={10} className="usr-grp-role-search-width gds-input">
              <StructuredFilter
                key="user-listing-search-filter"
                placeholder="Search for your datashares..."
                options={sortBy(searchFilterOptions, ["label"])}
                onChange={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
              />
            </Col>
            {isMyDatasharePage && (
              <Col sm={2} className="gds-button">
                <Button variant="primary" size="md" onClick={addDatashare}>
                  Create Datashare
                </Button>
              </Col>
            )}
          </Row>
          {isMyDatasharePage ? (
            <XATableLayout
              data={datashareListData}
              columns={myDatashareColumns}
              fetchData={fetchDataharetList}
              totalCount={entries && entries.totalCount}
              loading={loader}
              pageCount={pageCount}
              getRowProps={(row) => ({
                onClick: (e) => {
                  e.stopPropagation();
                  rowModal(row);
                }
              })}
              columnHide={false}
              columnResizable={false}
              columnSort={true}
              defaultSort={getDefaultSort}
            />
          ) : (
            <XATableLayout
              data={datashareListData}
              columns={datashareColumns}
              fetchData={fetchDataharetList}
              totalCount={entries && entries.totalCount}
              loading={loader}
              pageCount={pageCount}
              getRowProps={(row) => ({
                onClick: (e) => {
                  e.stopPropagation();
                  rowModal(row);
                }
              })}
              columnHide={false}
              columnResizable={false}
              columnSort={true}
              defaultSort={getDefaultSort}
            />
          )}
        </React.Fragment>
      </div>
    </>
  );
};

export default MyDatashareListing;
