/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *onDatashareSelect
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,Row
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useState, useEffect, useCallback, useRef } from "react";
import { useNavigate, useLocation, useSearchParams } from "react-router-dom";
import { Row, Col, Button } from "react-bootstrap";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import XATableLayout from "../../../components/XATableLayout";
import { fetchApi } from "../../../utils/fetchAPI";
import dateFormat from "dateformat";
import { Loader, BlockUi } from "../../../components/CommonComponents";
import CustomBreadcrumb from "../../CustomBreadcrumb";
import {
  getTableSortBy,
  getTableSortType,
  serverError,
  parseSearchFilter
} from "../../../utils/XAUtils";

const RequestListing = () => {
  const [contentLoader, setContentLoader] = useState(false);
  const [blockUI, setBlockUI] = useState(false);
  const [requestListData, setRequestListData] = useState([]);
  const [loader, setLoader] = useState(true);
  const fetchIdRef = useRef(0);
  const [resetPage, setResetpage] = useState({ page: 0 });
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const navigate = useNavigate();
  const [entries, setEntries] = useState([]);
  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );
  const { state } = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );

  const searchFilterOptions = [
    {
      category: "dataShareNamePartial",
      label: "Datashare Name",
      urlLabel: "dataShareNamePartial",
      type: "text"
    },
    {
      category: "datasetNamePartial",
      label: "Dataset Name",
      urlLabel: "datasetNamePartial",
      type: "text"
    },
    {
      category: "shareStatus",
      label: "Status",
      urlLabel: "shareStatus",
      type: "textoptions",
      options: () => {
        return [
          { value: "REQUESTED", label: "REQUESTED" },
          { value: "GRANTED", label: "GRANTED" },
          { value: "ACTIVE", label: "ACTIVE" },
          { value: "DENIED", label: "DENIED" }
        ];
      }
    },
    {
      category: "createdBy",
      label: "Requested By",
      urlLabel: "createdBy",
      type: "text"
    },
    {
      category: "approver",
      label: "Approved By",
      urlLabel: "approver",
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

  const fetchRequestList = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      setLoader(true);
      let resp = [];
      let requestList = [];
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
          params["sortBy"] = getTableSortBy(sortBy);
          params["sortType"] = getTableSortType(sortBy);
        }
        try {
          resp = await fetchApi({
            url: "gds/datashare/dataset/summary",
            params: params
          });
          requestList = resp.data.list;
          totalCount = resp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching Request list! ${error}`);
        }
        setRequestListData(requestList);
        setEntries(resp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [searchFilterParams]
  );

  const navigateToRequestDetail = (requestId) => {
    navigate(`/gds/request/detail/${requestId}`);
  };

  const columns = React.useMemo(
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
                onClick={() => navigateToRequestDetail(row.original.id)}
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
        Header: "For Datashare",
        accessor: "dataShareName",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Into Dataset",
        accessor: "datasetName",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Status",
        accessor: "shareStatus",
        width: 108,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Requested By",
        accessor: "createdBy",
        width: 100,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Approved By",
        accessor: "approver",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
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

  return contentLoader ? (
    <Loader />
  ) : (
    <>
      <div className="header-wraper">
        <h3 className="wrap-header bold">My Requests</h3>
        <CustomBreadcrumb />
      </div>
      <div className="wrap">
        <React.Fragment>
          <BlockUi isUiBlock={blockUI} />
          <Row className="mb-4">
            <Col sm={10} className="usr-grp-role-search-width">
              <StructuredFilter
                key="user-listing-search-filter"
                placeholder="Search..."
                options={searchFilterOptions}
                onChange={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
              />
            </Col>
          </Row>
          <XATableLayout
            data={requestListData}
            columns={columns}
            fetchData={fetchRequestList}
            totalCount={entries && entries.totalCount}
            loading={loader}
            pageCount={pageCount}
            getRowProps={(row) => ({
              onClick: (e) => {
                e.stopPropagation();
                // rowModal(row);
              }
            })}
            columnHide={false}
            columnResizable={false}
            columnSort={true}
            defaultSort={getDefaultSort}
          />
        </React.Fragment>
      </div>
    </>
  );
};

export default RequestListing;
