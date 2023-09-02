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
import { Badge, Button, Row, Col, Table, Modal } from "react-bootstrap";
import XATableLayout from "../../../components/XATableLayout";
import dateFormat from "dateformat";
import { fetchApi } from "../../../utils/fetchAPI";
import {
  AuditFilterEntries,
  CustomPopoverOnClick,
  CustomPopoverTagOnClick
} from "Components/CommonComponents";
import moment from "moment-timezone";
import {
  isEmpty,
  isUndefined,
  pick,
  indexOf,
  map,
  sortBy,
  toString,
  toUpper,
  has,
  filter
} from "lodash";
import CustomBreadcrumb from "../../CustomBreadcrumb";
import { toast } from "react-toastify";
import { Link } from "react-router-dom";
import { AccessMoreLess } from "Components/CommonComponents";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
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
import { CustomTooltip, Loader, BlockUi } from "../../../components/CommonComponents";
import {
  ServiceRequestDataRangerAcl,
  ServiceRequestDataHadoopAcl
} from "../../../utils/XAEnums";

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
    state && state.showLastPage ? state.addPageData.pageSize : 5
  );
  const isKMSRole = isKeyAdmin() || isKMSAuditor();
  const [datasetListData, setDatasetListData] = useState([]);
  const [serviceDefs, setServiceDefs] = useState([]);
  const [services, setServices] = useState([]);
  const [zones, setZones] = useState([]);
  const [loader, setLoader] = useState(true);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [entries, setEntries] = useState([]);
  const [showrowmodal, setShowRowModal] = useState(false);
  const [policyviewmodal, setPolicyViewModal] = useState(false);
  const [policyParamsData, setPolicyParamsData] = useState(null);
  const [rowdata, setRowData] = useState([]);
  const [checked, setChecked] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const fetchIdRef = useRef(0);
  const [contentLoader, setContentLoader] = useState(true);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [resetPage, setResetpage] = useState({ page: 0 });
    const [policyDetails, setPolicyDetails] = useState({});
    const [blockUI, setBlockUI] = useState(false);

  
const [deleteDatasetModal, setConfirmModal] = useState({
  datasetDetails: {}
});
  
const toggleConfirmModalForDelete = (datasetID, datasetName) => {
    setConfirmModal({
      datasetDetails: { datasetID: datasetID, datasetName: datasetName },
      showPopup: true
    });
};
 
  
    const handleDeleteClick = async (datasetID) => {
    toggleClose();
    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/dataset/${datasetID}`,
        method: "DELETE"
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
      showPopup: false
    });
  };
  
  useEffect(() => {
    if (isEmpty(serviceDefs)) {
      fetchServiceDefs(), fetchServices();

      if (!isKMSRole) {
        fetchZones();
      }
    }
  }, [serviceDefs]);

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
              params: params
            });
            datasetList = resp.data.list;
            totalCount = resp.data.totalCount;
          } catch (error) {
            serverError(error);
            console.error(
              `Error occurred while fetching Dataset list! ${error}`
            );
          }
            let modifiedDatasetList = [];
            if (datasetList !== undefined) {
                for (let i = 0; i < datasetList.length; i++) {
                    let dataset = datasetList[i];
                    if (dataset.acl !== undefined) {
                        if (dataset.acl.users !== undefined) {
                            const map = new Map(Object.entries(dataset.acl.users));
                            dataset.users = map.size;
                        } else { 
                            dataset.users = 0;
                        }

                        if (dataset.acl.groups !== undefined) {
                            const map = new Map(Object.entries(dataset.acl.groups));
                            dataset.groups = map.size;
                        } else { 
                            dataset.groups = 0;
                        }

                        if (dataset.acl.roles !== undefined) {
                            const map = new Map(Object.entries(dataset.acl.roles));
                            dataset.roles = map.size;
                        } else { 
                            dataset.roles = 0;
                        }
                    } else { 
                        dataset.users = dataset.groups = dataset.roles = 0;
                    }
                    modifiedDatasetList[i] = dataset;
                }
             }
          setDatasetListData(modifiedDatasetList);
          setEntries(resp.data);
          setPageCount(Math.ceil(totalCount / pageSize));
          setResetpage({ page: gotoPage });
          setLoader(false);
        }
    },
    [updateTable, searchFilterParams]
  );

  const fetchServiceDefs = async () => {
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
    setContentLoader(false);
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

  const fetchZones = async () => {
    let zonesResp;
    try {
      zonesResp = await fetchApi({
        url: "zones/zones"
      });
    } catch (error) {
      console.error(`Error occurred while fetching Zones! ${error}`);
    }

    setZones(sortBy(zonesResp.data.securityZones, ["name"]));
    setContentLoader(false);
  };

  const toggleChange = () => {
    let currentParams = Object.fromEntries([...searchParams]);
    currentParams["excludeServiceUser"] = !checked;
    localStorage.setItem("excludeServiceUser", JSON.stringify(!checked));
    setSearchParams(currentParams);
    setAccessLogs([]);
    setChecked(!checked);
    setLoader(true);
    setUpdateTable(moment.now());
  };

  const handleClosePolicyId = () => setPolicyViewModal(false);
  const handleClose = () => setShowRowModal(false);
  const rowModal = (row) => {
    setShowRowModal(true);
    setRowData(row.original);
  };

  const openModal = (policyDetails) => {
    let policyParams = pick(policyDetails, [
      "eventTime",
      "policyId",
      "policyVersion"
    ]);
    setPolicyViewModal(true);
    setPolicyDetails(policyDetails);
    setPolicyParamsData(policyParams);
    fetchVersions(policyDetails.policyId);
  };
    
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
        getResizerProps: () => { },
        Cell: (rawValue) => { 
          return (
                <div className="position-relative text-center">
                  <Link 
                    title="Edit"
                    to={`/gds/dataset/${rawValue.value}/detail`}
                  >
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
            getResizerProps: () => {}
          },
          {
            Header: "Groups",
            //accessor: "groups",
            width: 60,
            disableResizing: true,
            getResizerProps: () => {}
          },
          {
            Header: "Roles",
            //accessor: "roles",
            width: 60,
            disableResizing: true,
            getResizerProps: () => {}
          },
          {
            Header: "Projects",
            accessor: "projects",
            width: 70,
            disableResizing: true,
            getResizerProps: () => {}
          }
        ]
      },
      {
        Header: "Actions",
        accessor: "actions",
        Cell: ({ row: { original } }) => {
          return (
            <div>
              {(isSystemAdmin() || isKeyAdmin() || isUser()) && (
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
              )}
            </div>
          );
        },
        width: 60,
        disableSortBy: true
      }
    ],
    []
  );

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "eventTime",
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

    searchParam["excludeServiceUser"] = checked;

    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam);
    localStorage.setItem("bigData", JSON.stringify(searchParam));

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
    }
  };

  const searchFilterOptions = [
    {
      category: "datasetName",
      label: "Dataset Name",
      urlLabel: "datasetName",
      type: "text"
    }
  ];

  return contentLoader ? (
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
            <Col sm={10} className="usr-grp-role-search-width">
              <StructuredFilter
                key="user-listing-search-filter"
                placeholder="Search for your users..."
                options={sortBy(searchFilterOptions, ["label"])}
                onChange={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
              />
            </Col>
            {isSystemAdmin() && (
              <Col sm={2} className="gds-button">
                <Button
                  variant="primary"
                  size="sm"
                  className="btn-sm"
                  onClick={addDataset}
                >
                  Create Dataset
                </Button>
              </Col>
                          )}
            
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
                rowModal(row);
                }
            })}
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
                  handleDeleteClick(deleteDatasetModal.datasetDetails.datasetID)
                }
              >
                OK
              </Button>
            </Modal.Footer>
          </Modal>
        </div>
    </>
  );
}

export default DatasetListing;
