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
import { Button, Row, Col, Modal } from "react-bootstrap";
import XATableLayout from "../../../components/XATableLayout";
import dateFormat from "dateformat";
import { fetchApi } from "../../../utils/fetchAPI";
import moment from "moment-timezone";
import { toast } from "react-toastify";
import { Link } from "react-router-dom";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import { Loader, BlockUi } from "../../../components/CommonComponents";
import {
  isKeyAdmin,
  isKMSAuditor,
  getTableSortBy,
  getTableSortType,
  serverError,
  parseSearchFilter,
} from "../../../utils/XAUtils";

const DatasetListing = () => {
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
  const [updateTable, setUpdateTable] = useState(moment.now());
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

  const [deleteDatasetModal, setConfirmModal] = useState({
    datasetDetails: {},
  });

  const toggleConfirmModalForDelete = (datasetID, datasetName) => {
    setConfirmModal({
      datasetDetails: { datasetID: datasetID, datasetName: datasetName },
      showPopup: true,
    });
  };

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
        value: value,
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

  const handleDeleteClick = async (datasetID) => {
    toggleClose();
    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/dataset/${datasetID}`,
        method: "DELETE",
      });
      setBlockUI(false);
      toast.success(" Success! Dataset deleted successfully");
    } catch (error) {
      setBlockUI(false);
      let errorMsg = "Failed to delete dataset : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error("Error occurred during deleting dataset : " + error);
    }
    if (datasetListData.length == 1 && currentpageIndex > 0) {
      let page = currentpageIndex - currentpageIndex;
      if (typeof resetPage?.page === "function") {
        resetPage.page(page);
      }
    } else {
      setUpdateTable(moment.now());
    }
  };

  const toggleClose = () => {
    setConfirmModal({
      datasetDetails: {},
      showPopup: false,
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
          params["sortBy"] = getTableSortBy(sortBy);
          params["sortType"] = getTableSortType(sortBy);
        }
        try {
          resp = await fetchApi({
            url: "gds/dataset",
            params: params,
          });
          datasetList = resp.data.list;
          totalCount = resp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching Dataset list! ${error}`);
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
    [updateTable, searchFilterParams]
  );

  const addDataset = () => {
    navigate("/gds/create");
  };

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
              <Link title="Edit" to={`/gds/dataset/${rawValue.value}/detail`}>
                {rawValue.value}
              </Link>
            </div>
          );
        },
      },
      {
        Header: "Name",
        accessor: "name",
        width: 250,
        disableResizing: true,
        disableSortBy: true,
        getResizerProps: () => {},
      },
      {
        Header: "Created",
        accessor: "createTime",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy h:MM:ss TT");
        },
        width: 170,
        disableResizing: true,
        getResizerProps: () => {},
      },
      {
        Header: "Last Updated",
        accessor: "updateTime",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy h:MM:ss TT");
        },
        width: 170,
        disableResizing: true,
        getResizerProps: () => {},
      },
      {
        Header: "DATASHARE",
        id: "datashareInfo",
        disableResizing: true,
        columns: [
          {
            Header: "Active",
            accessor: "active",
            width: 80,
            disableResizing: true,
            disableSortBy: true,
            getResizerProps: () => {},
          },
          {
            Header: "Pending",
            accessor: "pending",
            width: 80,
            disableResizing: true,
            disableSortBy: true,
            getResizerProps: () => {},
          },
        ],
      },
      {
        Header: "SHARED WITH",
        id: "sharedWithInfo",
        disableResizing: true,
        columns: [
          {
            Header: "Users",
            //accessor: "users",
            width: 60,
            disableResizing: true,
            getResizerProps: () => {},
          },
          {
            Header: "Groups",
            //accessor: "groups",
            width: 60,
            disableResizing: true,
            getResizerProps: () => {},
          },
          {
            Header: "Roles",
            //accessor: "roles",
            width: 60,
            disableResizing: true,
            getResizerProps: () => {},
          },
          {
            Header: "Projects",
            accessor: "projects",
            width: 70,
            disableResizing: true,
            getResizerProps: () => {},
          },
        ],
      },
      {
        Header: "Actions",
        accessor: "actions",
        Cell: ({ row: { original } }) => {
          return (
            <div>
              <>
                <Button
                  variant="danger"
                  size="sm"
                  title="Delete"
                  onClick={() =>
                    toggleConfirmModalForDelete(original.id, original.name)
                  }
                  data-name="deletePolicy"
                  data-id={original.id}
                  data-cy={original.id}
                >
                  <i className="fa-fw fa fa-trash fa-fw fa fa-large"></i>
                </Button>
              </>
            </div>
          );
        },
        width: 60,
        disableSortBy: true,
      },
    ],
    []
  );

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "eventTime",
        desc: true,
      },
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
    //localStorage.setItem("bigData", JSON.stringify(searchParams));

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
    }
  };

  const searchFilterOptions = [
    {
      category: "datasetName",
      label: "Dataset Name",
      urlLabel: "datasetName",
      type: "text",
    },
  ];

  return (
    <div>
      {pageLoader ? (
        <Loader />
      ) : (
        <>
          <div className="gds-header-wrapper">
            <h3 className="gds-header bold">My Datasets</h3>
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
                <Col sm={2} className="gds-button">
                  <Button variant="primary" size="md" onClick={addDataset}>
                    Create Dataset
                  </Button>
                </Col>
              </Row>
              <XATableLayout
                data={datasetListData}
                columns={columns}
                fetchData={fetchDatasetList}
                totalCount={entries && entries.totalCount}
                loading={loader}
                pageCount={pageCount}
                getRowProps={(row) => ({
                  onClick: (e) => {
                    e.stopPropagation();
                    //rowModal(row);
                  },
                })}
                currentpageIndex={currentpageIndex}
                currentpageSize={currentpageSize}
                columnHide={false}
                columnResizable={false}
                columnSort={true}
                defaultSort={getDefaultSort}
              />
            </React.Fragment>

            <Modal show={deleteDatasetModal.showPopup} onHide={toggleClose}>
              <Modal.Header closeButton>
                <span className="text-word-break">
                  Are you sure you want to delete dataset&nbsp;"
                  <b>{deleteDatasetModal?.datasetDetails?.datasetName}</b>" ?
                </span>
              </Modal.Header>
              <Modal.Footer>
                <Button variant="secondary" size="sm" onClick={toggleClose}>
                  Close
                </Button>
                <Button
                  variant="primary"
                  size="sm"
                  onClick={() =>
                    handleDeleteClick(
                      deleteDatasetModal.datasetDetails.datasetID
                    )
                  }
                >
                  OK
                </Button>
              </Modal.Footer>
            </Modal>
          </div>
        </>
      )}
    </div>
  );
};

export default DatasetListing;
