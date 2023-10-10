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
import {
  useSearchParams,
  useNavigate,
  useLocation,
  Link
} from "react-router-dom";
import { Row, Col } from "react-bootstrap";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import XATableLayout from "../../../components/XATableLayout";
import { fetchApi } from "../../../utils/fetchAPI";
import dateFormat from "dateformat";
import {
  CustomTooltip,
  Loader,
  BlockUi
} from "../../../components/CommonComponents";
import moment from "moment-timezone";
import {
  isKeyAdmin,
  isKMSAuditor,
  getTableSortBy,
  getTableSortType,
  serverError,
  isSystemAdmin,
  requestDataTitle,
  fetchSearchFilterParams,
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
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [entries, setEntries] = useState([]);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );
  const { state } = useLocation();

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
            url: "gds/datashare/dataset",
            params: params
          });
          requestList = resp.data.list;
          totalCount = resp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching Dataset list! ${error}`);
        }
        setRequestListData(requestList);
        setEntries(resp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [updateTable, searchFilterParams]
  );

  const columns = React.useMemo(
    () => [
      {
        Header: "Id",
        accessor: "id",
        width: 25,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {},
        Cell: (rawValue) => {
          return (
            <div className="position-relative text-center">
              <Link title="Edit" to={`/gds/request/detail/${rawValue.value}`}>
                {rawValue.value}
              </Link>
            </div>
          );
        }
      },
      {
        Header: "Name",
        accessor: "name",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Type",
        accessor: "type",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Status",
        accessor: "status",
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
      },
      {
        Header: "Created By",
        accessor: "createdBy",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Approver",
        accessor: "approvedBy",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      }
    ],
    []
  );

  return contentLoader ? (
    <Loader />
  ) : (
    <>
      <div className="gds-header-wrapper">
        <h3 className="gds-header bold">My Requests</h3>
      </div>
      <div className="wrap">
        <React.Fragment>
          <BlockUi isUiBlock={blockUI} />
          <Row className="mb-4">
            <Col sm={10} className="usr-grp-role-search-width">
              <StructuredFilter
                key="user-listing-search-filter"
                placeholder="Search..."
                //options={sortBy(searchFilterOptions, ["label"])}
                //onChange={updateSearchFilter}
                //defaultSelected={defaultSearchFilterParams}
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
            //defaultSort={getDefaultSort}
          />
        </React.Fragment>
      </div>
    </>
  );
};

export default RequestListing;
