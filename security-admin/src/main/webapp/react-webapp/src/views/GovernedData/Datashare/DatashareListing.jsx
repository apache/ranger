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

import React, { useState, useCallback, useRef } from "react";
import XATableLayout from "../../../components/XATableLayout";
import { Loader } from "../../../components/CommonComponents";
import { Button, Row, Col } from "react-bootstrap";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import {
  useSearchParams,
  useNavigate,
  useLocation,
  Link
} from "react-router-dom";
import { fetchApi } from "../../../utils/fetchAPI";
import dateFormat from "dateformat";
import {
  getTableSortBy,
  getTableSortType,
  serverError,
  isSystemAdmin,
  parseSearchFilter
} from "../../../utils/XAUtils";
import moment from "moment-timezone";
import { sortBy } from "lodash";

const DatashareListing = () => {
  const navigate = useNavigate();
  const [blockUI, setBlockUI] = useState(false);
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

  const searchFilterOptions = [
    {
      category: "datashareName",
      label: "Datashare Name",
      urlLabel: "datashareName",
      type: "text"
    }
  ];

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
        id: "eventTime",
        desc: true
      }
    ],
    []
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
              <Link title="Edit" to={`/gds/datashare/${rawValue.value}/detail`}>
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
        Header: "Service",
        accessor: "service",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {}
      },
      {
        Header: "Zone",
        accessor: "zone",
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
            accessor: "active",
            width: 80,
            disableResizing: true,
            disableSortBy: true,
            getResizerProps: () => {}
          },
          {
            Header: "Pending",
            accessor: "pending",
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
            url: "gds/datashare",
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
        setDatashareListData(datashareList);
        setEntries(resp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [updateTable, searchFilterParams]
  );

  return contentLoader ? (
    <Loader />
  ) : (
    <>
      <div className="gds-header-wrapper">
        <h3 className="gds-header bold">My Datashares</h3>
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
            {isSystemAdmin() && (
              <Col sm={2} className="gds-button">
                <Button variant="primary" size="md" onClick={addDatashare}>
                  Create Datashare
                </Button>
              </Col>
            )}
          </Row>
          <XATableLayout
            data={datashareListData}
            columns={columns}
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
        </React.Fragment>
      </div>
    </>
  );
};

export default DatashareListing;
