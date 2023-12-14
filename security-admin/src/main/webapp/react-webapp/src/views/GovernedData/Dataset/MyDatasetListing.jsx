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

import React, { useState, useCallback, useEffect, useRef } from "react";
import { useSearchParams, useNavigate, useLocation } from "react-router-dom";
import { Button, Row, Col } from "react-bootstrap";
import XATableLayout from "../../../components/XATableLayout";
import dateFormat from "dateformat";
import { fetchApi } from "../../../utils/fetchAPI";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import { Loader, BlockUi } from "../../../components/CommonComponents";
import {
  getTableSortBy,
  getTableSortType,
  serverError,
  parseSearchFilter
} from "../../../utils/XAUtils";
import CustomBreadcrumb from "../../CustomBreadcrumb";

const MyDatasetListing = () => {
  const navigate = useNavigate();
  const { state } = useLocation();
  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );
  const [currentpageIndex, setCurrentPageIndex] = useState(
    state && state.showLastPage ? state.addPageData.totalPage - 1 : 0
  );
  const [currentpageSize, setCurrentPageSize] = useState(
    state && state.showLastPage ? state.addPageData.pageSize : 25
  );
  const [datasetListData, setDatasetListData] = useState([]);
  const [loader, setLoader] = useState(true);
  const [entries, setEntries] = useState([]);
  const fetchIdRef = useRef(0);
  const [totalCount, setTotalCount] = useState(0);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [pageLoader, setPageLoader] = useState(true);
  const [resetPage, setResetpage] = useState({ page: 0 });
  const [blockUI, setBlockUI] = useState(false);
  const isMyDatasetPage = window.location.href.includes("mydatasetlisting");

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

    // Updating the states for search params, search filter and default search filter
    setSearchParams({ ...currentParams, ...searchParam });
    if (
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    setPageLoader(false);
    localStorage.setItem("newDataAdded", state && state.showLastPage);
  }, [searchParams]);

  useEffect(() => {
    if (localStorage.getItem("newDataAdded") == "true") {
      scrollToNewData(datasetListData);
    }
  }, [totalCount]);

  const toggleClose = () => {
    setConfirmModal({
      datasetDetails: {},
      showPopup: false
    });
  };

  const fetchDatasetList = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      setLoader(true);
      let resp = [];
      let datasetList = [];
      let totalCount = 0;
      let page =
        state && state.showLastPage
          ? state.addPageData.totalPage - 1
          : pageIndex;
      let totalPageCount = 0;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchFilterParams };
      if (fetchId === fetchIdRef.current) {
        params["pageSize"] = pageSize;
        params["startIndex"] =
          state && state.showLastPage
            ? (state.addPageData.totalPage - 1) * pageSize
            : pageIndex * pageSize;
        if (sortBy.length > 0) {
          if (getTableSortBy(sortBy) == "name")
            params["sortBy"] = "datasetName";
          else params["sortBy"] = getTableSortBy(sortBy);

          params["sortType"] = getTableSortType(sortBy);
        }
        if (isMyDatasetPage) {
          params["gdsPermission"] = "ADMIN";
        } else {
          params["gdsPermission"] = "LIST";
        }

        try {
          resp = await fetchApi({
            url: "gds/dataset/summary",
            params: params
          });
          datasetList = resp.data.list;
          totalCount = resp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching Dataset list! ${error}`);
        }
        for (let i = 0; i < datasetList.length; i++) {
          let datashareActiveCount = 0;
          let datasharePendingCount = 0;

          if (datasetList[i].dataShares != undefined) {
            for (let j = 0; j < datasetList[i].dataShares.length; j++) {
              if (datasetList[i].dataShares[j].shareStatus === "ACTIVE") {
                datashareActiveCount++;
              } else if (
                datasetList[i].dataShares[j].shareStatus !== "ACTIVE" &&
                datasetList[i].dataShares[j].shareStatus !== "DENIED"
              ) {
                datasharePendingCount++;
              }
            }
          }
          datasetList[i]["datashareActiveCount"] = datashareActiveCount;
          datasetList[i]["datasharePendingCount"] = datasharePendingCount;
        }

        setTotalCount(totalCount);
        setDatasetListData(datasetList);
        setEntries(resp.data);
        setCurrentPageIndex(page);
        setCurrentPageSize(pageSize);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [searchFilterParams, isMyDatasetPage]
  );

  const addDataset = () => {
    navigate("/gds/create");
  };

  const navigateToDetailPage = (datasetId, perm) => {
    navigate(`/gds/dataset/${datasetId}/detail`, {
      state: {
        userAclPerm: perm
      }
    });
  };

  const myDatasetColumns = React.useMemo(
    () => [
      {
        Header: "ID",
        accessor: "id",
        width: 80,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {},
        Cell: ({ row }) => {
          const hiddenValue = row.original.permissionForCaller;
          return (
            <div className="position-relative text-center">
              <Button
                data-id="datasetId"
                data-cy="datasetId"
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
        width: 470,
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
        Header: "DATASHARE",
        id: "datashareInfo",
        disableResizing: true,
        columns: [
          {
            Header: "Active",
            accessor: "datashareActiveCount",
            width: 80,
            disableResizing: true,
            disableSortBy: true,
            getResizerProps: () => {}
          },
          {
            Header: "Pending",
            accessor: "datasharePendingCount",
            width: 80,
            disableResizing: true,
            disableSortBy: true,
            getResizerProps: () => {}
          }
        ]
      },
      {
        Header: "SHARED WITH",
        id: "sharedWithInfo",
        disableResizing: true,
        columns: [
          {
            Header: "Users",
            accessor: "principalsCount",
            accessor: (raw) => {
              let userCount = raw.principalsCount?.USER;

              return userCount != undefined ? (
                <span>{userCount}</span>
              ) : (
                <span>0</span>
              );
            },
            width: 60,
            disableResizing: true,
            getResizerProps: () => {}
          },
          {
            Header: "Groups",
            accessor: "principalsCount",
            accessor: (raw) => {
              let groupCount = raw.principalsCount?.GROUP;

              return groupCount != undefined ? (
                <span>{groupCount}</span>
              ) : (
                <span>0</span>
              );
            },
            width: 60,
            disableResizing: true,
            getResizerProps: () => {}
          },
          {
            Header: "Roles",
            accessor: "principalsCount",
            accessor: (raw) => {
              let roleCount = raw.principalsCount?.ROLE;

              return roleCount != undefined ? (
                <span>{roleCount}</span>
              ) : (
                <span>0</span>
              );
            },
            width: 60,
            disableResizing: true,
            getResizerProps: () => {}
          }
        ]
      }
    ],
    []
  );

  const datasetColumns = React.useMemo(
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
                data-id="datasetId"
                data-cy="datasetId"
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
        width: 600,
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

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "updateTime",
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
    setSearchParams(searchParam);

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
    }
  };

  const searchFilterOptions = [
    {
      category: "datasetNamePartial",
      label: "Name",
      urlLabel: "datasetNamePartial",
      type: "text"
    }
  ];

  return (
    <div>
      {pageLoader ? (
        <Loader />
      ) : (
        <>
          <div className="gds-header-wrapper">
            <h3 className="gds-header bold">
              {isMyDatasetPage ? "My" : ""} Datasets
            </h3>
            <CustomBreadcrumb />
          </div>
          <div className="wrap">
            <React.Fragment>
              <BlockUi isUiBlock={blockUI} />
              <Row className="mb-4">
                <Col sm={10} className="usr-grp-role-search-width gds-input">
                  <StructuredFilter
                    key="dataset-listing-search-filter"
                    placeholder="Search for your datasets..."
                    options={searchFilterOptions}
                    onChange={updateSearchFilter}
                    defaultSelected={defaultSearchFilterParams}
                  />
                </Col>
                {isMyDatasetPage && (
                  <Col sm={2} className="gds-button">
                    <Button variant="primary" size="md" onClick={addDataset}>
                      Create Dataset
                    </Button>
                  </Col>
                )}
              </Row>
              {isMyDatasetPage ? (
                <XATableLayout
                  data={datasetListData}
                  columns={myDatasetColumns}
                  fetchData={fetchDatasetList}
                  totalCount={entries && entries.totalCount}
                  loading={loader}
                  pageCount={pageCount}
                  getRowProps={(row) => ({
                    onClick: (e) => {
                      e.stopPropagation();
                      //rowModal(row);
                    }
                  })}
                  currentpageIndex={currentpageIndex}
                  currentpageSize={currentpageSize}
                  columnHide={false}
                  columnResizable={false}
                  columnSort={true}
                  defaultSort={getDefaultSort}
                />
              ) : (
                <XATableLayout
                  data={datasetListData}
                  columns={datasetColumns}
                  fetchData={fetchDatasetList}
                  totalCount={entries && entries.totalCount}
                  loading={loader}
                  pageCount={pageCount}
                  getRowProps={(row) => ({
                    onClick: (e) => {
                      e.stopPropagation();
                      //rowModal(row);
                    }
                  })}
                  currentpageIndex={currentpageIndex}
                  currentpageSize={currentpageSize}
                  columnHide={false}
                  columnResizable={false}
                  columnSort={true}
                  defaultSort={getDefaultSort}
                />
              )}
            </React.Fragment>
          </div>
        </>
      )}
    </div>
  );
};

export default MyDatasetListing;
