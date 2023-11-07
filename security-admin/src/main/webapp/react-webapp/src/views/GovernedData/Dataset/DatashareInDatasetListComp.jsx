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

import React, { useRef, useCallback, useState, useEffect } from "react";
import { useSearchParams, useNavigate, useLocation } from "react-router-dom";
import XATableLayout from "../../../components/XATableLayout";
import dateFormat from "dateformat";
import { fetchApi } from "../../../utils/fetchAPI";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import { Loader, BlockUi } from "../../../components/CommonComponents";
import { toast } from "react-toastify";
import moment from "moment-timezone";

import {
  getTableSortBy,
  getTableSortType,
  serverError,
  parseSearchFilter,
  isSystemAdmin
} from "../../../utils/XAUtils";
import { Button, Row, Col, Modal } from "react-bootstrap";

const DatashareInDatasetListComp = ({
  datasetId,
  setUpdateTable,
  updateTable,
  userAclPerm
}) => {
  const { state } = useLocation();
  const [requestListData, setRequestListData] = useState([]);
  const [loader, setLoader] = useState(true);
  const [entries, setEntries] = useState([]);
  const fetchIdRef = useRef(0);
  const navigate = useNavigate();

  const [totalCount, setTotalCount] = useState(0);
  const [resetPage, setResetpage] = useState({ page: 0 });

  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );
  const [currentpageIndex, setCurrentPageIndex] = useState(
    state && state.showLastPage ? state.addPageData.totalPage - 1 : 0
  );
  const [currentpageSize, setCurrentPageSize] = useState(
    state && state.showLastPage ? state.addPageData.pageSize : 25
  );
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [deleteDatashareReqInfo, setDeleteDatashareReqInfo] = useState({});
  const [
    showDatashareRequestDeleteConfirmModal,
    setShowDatashareRequestDeleteConfirmModal
  ] = useState(false);
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [pageLoader, setPageLoader] = useState(true);

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
        params["datasetId"] = datasetId;
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
          console.error(
            `Error occurred while fetching Datashare list! ${error}`
          );
        }
        //setUpdateTable(moment.now());
        setTotalCount(totalCount);
        setRequestListData(requestList);
        setEntries(resp.data);
        setCurrentPageIndex(page);
        setCurrentPageSize(pageSize);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [searchFilterParams, updateTable]
  );

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

  const searchFilterOptions = [
    {
      category: "dataShareNamePartial",
      label: "Datashare Name",
      urlLabel: "dataShareNamePartial",
      type: "text"
    }
  ];

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

  const requestColumns = React.useMemo(
    () => [
      {
        Header: "Name",
        accessor: "dataShareId",
        //width: 200,
        Cell: (val) => {
          return (
            <span
              className="text-truncate"
              title={val.value}
              style={{ maxWidth: "240px", display: "inline-block" }}
            >
              Datashare {val.value}
            </span>
          );
        }
      },
      {
        Header: "Service",
        accessor: "service",
        //width: 200,
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
        accessor: "zone",
        // width: 200,
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
        Header: "Resources",
        accessor: "resourceCount",
        width: 100,
        Cell: (val) => {
          return (
            <span
              title={val.value}
              style={{ maxWidth: "240px", display: "inline-block" }}
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Status",
        accessor: "status",
        width: 108,
        Cell: (val) => {
          return (
            <span
              className={
                val.value === "REQUESTED"
                  ? "badge badge-light gds-requested-status"
                  : val.value === "GRANTED"
                  ? "badge badge-light gds-granted-status"
                  : val.value === "ACTIVE"
                  ? "badge badge-light gds-active-status"
                  : "badge badge-light gds-denied-status"
              }
            >
              {val.value}
            </span>
          );
        }
      },
      {
        Header: "Last Updated",
        accessor: "updateTime",
        Cell: (rawValue) => {
          return dateFormat(rawValue.value, "mm/dd/yyyy");
        },
        width: 108,
        disableResizing: true,
        getResizerProps: () => {}
      },
      {
        Header: "",
        accessor: "actions",
        width: 100,
        Cell: ({ row: { original } }) => {
          return (
            <div>
              <Button
                variant="outline-dark"
                size="sm"
                className="mr-2"
                title="View"
                onClick={() =>
                  navigate(`/gds/datashare/${original.dataShareId}/detail`)
                }
                data-name="viewDatashare"
                data-id={original.dataShareId}
              >
                <i className="fa-fw fa fa-eye fa-fw fa fa-large"></i>
              </Button>

              <>
                {(isSystemAdmin() || userAclPerm == "ADMIN") && (
                  <Button
                    variant="danger"
                    size="sm"
                    title="Delete"
                    onClick={() =>
                      toggleConfirmModalForDelete(
                        original.id,
                        "Dummy",
                        original.status
                      )
                    }
                    data-name="deleteDatashareRequest"
                    data-id={original.id}
                    data-cy={original.id}
                  >
                    <i className="fa-fw fa fa-trash fa-fw fa fa-large" />
                  </Button>
                )}
              </>
            </div>
          );
        },
        disableSortBy: true
      }
    ],
    []
  );

  const toggleDatashareRequestDelete = () => {
    setShowDatashareRequestDeleteConfirmModal(false);
  };

  const toggleConfirmModalForDelete = (id, name, status) => {
    let deleteMsg = "";
    if (status == "ACTIVE") {
      deleteMsg = `Do you want to remove Datashare ${id} from ${datasetInfo.name}`;
    } else {
      deleteMsg = `Do you want to delete request of Datashare ${id}`;
    }
    let data = { id: id, name: name, status: status, msg: deleteMsg };
    setDeleteDatashareReqInfo(data);
    setShowDatashareRequestDeleteConfirmModal(true);
  };

  const deleteDatashareRequest = async () => {
    try {
      setLoader(true);
      await fetchApi({
        url: `gds/datashare/dataset/${deleteDatashareReqInfo.id}`,
        method: "DELETE"
      });
      let successMsg = "";
      if (deleteDatashareReqInfo.status == "ACTIVE") {
        successMsg = "Success! Datashare removed from dataset successfully";
      } else {
        successMsg = "Success! Datashare request deleted successfully";
      }
      setShowDatashareRequestDeleteConfirmModal(false);
      toast.success(successMsg);
      setUpdateTable(moment.now());

      //   fetchDatashareRequestList(
      //     undefined,
      //     dataShareRequestsList.length == 1
      //       ? requestCurrentPage - 1
      //       : requestCurrentPage,
      //     false
      //   );
      setLoader(false);
    } catch (error) {
      let errorMsg = "";
      if (deleteDatashareReqInfo.status == "ACTIVE") {
        errorMsg = "Failed to remove datashare from dataset ";
      } else {
        errorMsg = "Failed to delete datashare request ";
      }
      setLoader(false);
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error(
        "Error occurred during deleting Datashare request  : " + error
      );
    }
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

  return (
    <>
      <XATableLayout
        data={requestListData}
        columns={requestColumns}
        fetchData={fetchRequestList}
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

      <Modal
        show={showDatashareRequestDeleteConfirmModal}
        onHide={toggleDatashareRequestDelete}
      >
        <Modal.Header closeButton>
          <h3 className="gds-header bold">{deleteDatashareReqInfo.msg}</h3>
        </Modal.Header>
        <Modal.Footer>
          <Button
            variant="secondary"
            size="sm"
            onClick={() => toggleDatashareRequestDelete()}
          >
            No
          </Button>
          <Button
            variant="primary"
            size="sm"
            onClick={() => deleteDatashareRequest()}
          >
            Yes
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
};

export default DatashareInDatasetListComp;
