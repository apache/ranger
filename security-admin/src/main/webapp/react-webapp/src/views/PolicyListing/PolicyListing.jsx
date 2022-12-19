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
import {
  Link,
  useParams,
  useNavigate,
  useLocation,
  useSearchParams
} from "react-router-dom";
import { Badge, Button, Col, Row, Modal, Alert } from "react-bootstrap";
import moment from "moment-timezone";
import { toast } from "react-toastify";
import {
  pick,
  indexOf,
  isUndefined,
  isEmpty,
  map,
  sortBy,
  find,
  concat,
  camelCase
} from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import XATableLayout from "Components/XATableLayout";
import {
  showGroupsOrUsersOrRolesForPolicy,
  QueryParamsName
} from "Utils/XAUtils";
import { MoreLess, scrollToNewData } from "Components/CommonComponents";
import {} from "Utils/XAUtils";
import PolicyViewDetails from "../AuditEvent/AdminLogs/PolicyViewDetails";
import StructuredFilter from "../../components/structured-filter/react-typeahead/tokenizer";
import {
  isAuditor,
  isKMSAuditor,
  isPolicyExpired,
  isSystemAdmin,
  isKeyAdmin,
  isUser
} from "../../utils/XAUtils";
import { alertMessage } from "../../utils/XAEnums";
import { ContentLoader } from "../../components/CommonComponents";

function PolicyListing(props) {
  const { serviceDef } = props;
  const { state } = useLocation();
  const [policyListingData, setPolicyData] = useState([]);
  const [loader, setLoader] = useState(true);
  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );
  const [currentpageIndex, setCurrentPageIndex] = useState(
    state && state.showLastPage ? state.addPageData.totalPage - 1 : 0
  );
  const [currentpageSize, setCurrentPageSize] = useState(
    state && state.showLastPage ? state.addPageData.pageSize : 25
  );
  const [totalCount, setTotalCount] = useState(0);
  const [tblpageData, setTblPageData] = useState({
    totalPage: 0,
    pageRecords: 0,
    pageSize: 25
  });
  const fetchIdRef = useRef(0);
  const [deletePolicyModal, setConfirmModal] = useState({
    policyDetails: {},
    showSyncDetails: false
  });
  const [policyviewmodal, setPolicyViewModal] = useState(false);
  const [policyParamsData, setPolicyParamsData] = useState(null);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [currentPage, setCurrentPage] = useState(1);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [pageLoader, setPageLoader] = useState(true);
  const [resetPage, setResetpage] = useState({ page: 0 });
  const [show, setShow] = useState(true);
  let navigate = useNavigate();
  let { serviceId, policyType } = useParams();

  useEffect(() => {
    let searchFilterParam = {};
    let searchParam = {};
    let defaultSearchFilterParam = [];

    // Get Search Filter Params from current search params
    const currentParams = Object.fromEntries([...searchParams]);
    console.log("PRINT search params : ", currentParams);

    for (const param in currentParams) {
      let searchFilterObj = find(getSearchFilterOption(), {
        urlLabel: param
      });

      if (!isUndefined(searchFilterObj)) {
        let category = searchFilterObj.category;
        let value = currentParams[param];

        if (searchFilterObj.type == "textoptions") {
          let textOptionObj = find(searchFilterObj.options(), {
            label: value
          });
          value = textOptionObj !== undefined ? textOptionObj.value : value;
        }

        searchFilterParam[category] = value;
        defaultSearchFilterParam.push({
          category: category,
          value: value
        });
      }
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

    console.log(
      "PRINT Final searchFilterParam to server : ",
      searchFilterParam
    );
    console.log(
      "PRINT Final defaultSearchFilterParam to tokenzier : ",
      defaultSearchFilterParam
    );
    localStorage.setItem("newDataAdded", state && state.showLastPage);
  }, [searchParams]);

  const getTableSortBy = (sortArr = []) => {
    return sortArr
      .map(({ id }) => {
        return QueryParamsName(id);
      })
      .join(",");
  };

  const getTableSortType = (sortArr = []) => {
    return sortArr.map(({ desc }) => (desc ? "desc" : "asc")).join(",");
  };

  const fetchPolicyInfo = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      setLoader(true);
      let policyData = [];
      let policyResp = [];
      let totalCount = 0;
      let page =
        state && state.showLastPage
          ? state.addPageData.totalPage - 1
          : pageIndex;
      let totalPageCount = 0;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchFilterParams };
      if (fetchId === fetchIdRef.current) {
        params["page"] = page;
        params["startIndex"] =
          state && state.showLastPage
            ? (state.addPageData.totalPage - 1) * pageSize
            : pageIndex * pageSize;
        params["pageSize"] = pageSize;
        params["policyType"] = policyType;
        if (sortBy.length > 0) {
          params["sortBy"] = getTableSortBy(sortBy);
          params["sortType"] = getTableSortType(sortBy);
        }
        if (localStorage.getItem("zoneDetails") != null) {
          params["zoneName"] = JSON.parse(
            localStorage.getItem("zoneDetails")
          ).label;
        }
        try {
          policyResp = await fetchApi({
            url: `plugins/policies/service/${serviceId}`,
            params: params
          });
          policyData = policyResp.data.policies;
          totalCount = policyResp.data.totalCount;
          totalPageCount = Math.ceil(totalCount / pageSize);
        } catch (error) {
          console.error(`Error occurred while fetching Policies ! ${error}`);
        }
        if (state) {
          state["showLastPage"] = false;
        }
        setPolicyData(policyData);
        setTblPageData({
          totalPage: totalPageCount,
          pageRecords: policyResp.data.totalCount,
          pageSize: 25
        });
        setTotalCount(totalCount);
        setPageCount(totalPageCount);
        setCurrentPageIndex(page);
        setCurrentPageSize(pageSize);
        setResetpage({ page: gotoPage });
        setLoader(false);
        if (
          page == totalPageCount - 1 &&
          localStorage.getItem("newDataAdded") == "true"
        ) {
          scrollToNewData(policyData, policyResp.data.resultSize);
        }
      }
      localStorage.removeItem("newDataAdded");
    },
    [updateTable, searchFilterParams]
  );

  const toggleConfirmModalForDelete = (policyID, policyName) => {
    setConfirmModal({
      policyDetails: { policyID: policyID, policyName: policyName },
      showPopup: true
    });
  };

  const toggleClose = () => {
    setConfirmModal({
      policyDetails: {},
      showPopup: false
    });
  };

  const handleClosePolicyId = () => setPolicyViewModal(false);

  const openModal = (policyDetails) => {
    let policyId = pick(policyDetails, ["id"]);
    setPolicyViewModal(true);
    setPolicyParamsData(policyDetails);
    fetchVersions(policyId.id);
  };

  const fetchVersions = async (policyId) => {
    let versionsResp = {};
    try {
      versionsResp = await fetchApi({
        url: `plugins/policy/${policyId}/versionList`
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Policy Version or CSRF headers! ${error}`
      );
    }
    setCurrentPage(
      versionsResp.data.value
        .split(",")
        .map(Number)
        .sort(function (a, b) {
          return a - b;
        })
    );
    setLoader(false);
  };

  const handleDeleteClick = async (policyID) => {
    try {
      await fetchApi({
        url: `plugins/policies/${policyID}`,
        method: "DELETE"
      });
      toast.success(" Success! Policy deleted successfully");
    } catch (error) {
      console.log(error.response);
      if (error.response.data.msgDesc) {
        errorMsg += error.response.data.msgDesc + "\n";
      } else {
        errorMsg += `Error occurred during deleting policy`;
      }
    }
    if (policyListingData.length == 1 && currentpageIndex > 1) {
      let page = currentpageIndex - currentpageIndex;
      resetPage.page(page);
    } else {
      setUpdateTable(moment.now());
    }
    toggleClose();
  };

  const previousVersion = (e) => {
    if (e.currentTarget.classList.contains("active")) {
      let curr = policyParamsData && policyParamsData.version;
      let policyVersionList = currentPage;
      var previousVal =
        policyVersionList[
          (indexOf(policyVersionList, curr) - 1) % policyVersionList.length
        ];
    }
    let prevVal = {};
    prevVal.version = previousVal;
    prevVal.id = policyParamsData.id;
    prevVal.isChangeVersion = true;
    setPolicyParamsData(prevVal);
  };

  const nextVersion = (e) => {
    if (e.currentTarget.classList.contains("active")) {
      let curr = policyParamsData && policyParamsData.version;
      let policyVersionList = currentPage;
      var nextValue =
        policyVersionList[
          (indexOf(policyVersionList, curr) + 1) % policyVersionList.length
        ];
    }
    let nextVal = {};
    nextVal.version = nextValue;
    nextVal.id = policyParamsData.id;
    nextVal.isChangeVersion = true;
    setPolicyParamsData(nextVal);
  };

  const revert = (e) => {
    e.preventDefault();
    let version = policyParamsData && policyParamsData.version;
    let revertVal = {};
    revertVal.version = version;
    revertVal.id = policyParamsData.id;
    revertVal.isRevert = true;
    setPolicyParamsData(revertVal);
    setPolicyViewModal(false);
  };

  const updateServices = () => {
    setUpdateTable(moment.now());
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Policy ID",
        accessor: "id",
        Cell: (rawValue) => {
          if (isAuditor() || isKMSAuditor()) {
            if (
              !isEmpty(rawValue.row.original.validitySchedules) &&
              isPolicyExpired(rawValue.row.original)
            ) {
              return (
                <div className="position-relative text-center">
                  <i
                    className="fa-fw fa fa-exclamation-circle policy-expire-icon"
                    title="Policy expired"
                  ></i>
                  {rawValue.value}
                </div>
              );
            } else {
              return (
                <div className="position-relative text-center">
                  {rawValue.value}
                </div>
              );
            }
          } else {
            if (
              !isEmpty(rawValue.row.original.validitySchedules) &&
              isPolicyExpired(rawValue.row.original)
            ) {
              return (
                <div className="position-relative text-center">
                  <i
                    className="fa-fw fa fa-exclamation-circle policy-expire-icon"
                    title="Policy expired"
                  ></i>
                  <Link
                    title="Edit"
                    to={`/service/${serviceId}/policies/${rawValue.value}/edit`}
                  >
                    {rawValue.value}
                  </Link>
                </div>
              );
            } else {
              return (
                <div className="position-relative text-center">
                  <Link
                    title="Edit"
                    to={`/service/${serviceId}/policies/${rawValue.value}/edit`}
                  >
                    {rawValue.value}
                  </Link>
                </div>
              );
            }
          }
        },
        width: 90
      },
      {
        Header: "Policy Name",
        accessor: "name",
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
        },
        width: 250
      },
      {
        Header: "Policy Label",
        accessor: "policyLabels",
        Cell: (rawValue) => {
          return !isEmpty(rawValue.value) ? (
            <MoreLess data={rawValue.value} />
          ) : (
            <div>--</div>
          );
        },
        width: 130,
        disableSortBy: true
      },
      {
        Header: "Status",
        accessor: "isEnabled",
        Cell: (rawValue) => {
          if (rawValue.value)
            return (
              <h6>
                <Badge variant="success">Enabled</Badge>
              </h6>
            );
          else
            return (
              <h6>
                <Badge variant="danger">Disabled</Badge>
              </h6>
            );
        },
        width: 100,
        disableSortBy: true
      },
      {
        Header: "Audit Logging",
        accessor: "isAuditEnabled",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <h6>
                <Badge variant="success">Enabled</Badge>
              </h6>
            );
          } else
            return (
              <h6>
                <Badge variant="danger">Disabled</Badge>
              </h6>
            );
        },
        width: 110,
        disableSortBy: true
      },
      {
        Header: "Roles",
        accessor: "roles",
        Cell: (rawValue) => {
          let rolesData = showGroupsOrUsersOrRolesForPolicy(
            "roles",
            rawValue.row.original,
            policyType
          );
          return !isEmpty(rolesData) ? (
            <MoreLess data={rolesData} key={rawValue.row.original.id} />
          ) : (
            <div className="text-center">--</div>
          );
        },
        minWidth: 190,
        disableSortBy: true
      },
      {
        Header: "Groups",
        accessor: "groups",
        Cell: (rawValue) => {
          let groupsData = showGroupsOrUsersOrRolesForPolicy(
            "groups",
            rawValue.row.original,
            policyType
          );
          return !isEmpty(groupsData) ? (
            <MoreLess data={groupsData} key={rawValue.row.original.id} />
          ) : (
            <div className="text-center">--</div>
          );
        },
        minWidth: 190,
        disableSortBy: true
      },
      {
        Header: "Users",
        accessor: "users",
        Cell: (rawValue) => {
          let usersData = showGroupsOrUsersOrRolesForPolicy(
            "users",
            rawValue.row.original,
            policyType
          );
          return !isEmpty(usersData) ? (
            <MoreLess data={usersData} key={rawValue.row.original.id} />
          ) : (
            <div className="text-center">--</div>
          );
        },
        minWidth: 190,
        disableSortBy: true
      },
      {
        Header: "Actions",
        accessor: "actions",
        Cell: ({ row: { original } }) => {
          return (
            <div>
              <Button
                variant="outline-dark"
                size="sm"
                className="mr-2"
                title="View"
                onClick={(e) => {
                  e.stopPropagation();
                  openModal(original);
                }}
                data-name="viewPolicy"
                data-id={original.id}
              >
                <i className="fa-fw fa fa-eye fa-fw fa fa-large"></i>
              </Button>
              {(isSystemAdmin() || isKeyAdmin() || isUser()) && (
                <>
                  <Link
                    className="btn btn-outline-dark btn-sm mr-2"
                    title="Edit"
                    to={`/service/${serviceId}/policies/${original.id}/edit`}
                  >
                    <i className="fa-fw fa fa-edit"></i>
                  </Link>
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
        disableSortBy: true
      }
    ],
    []
  );

  const addPolicy = () => {
    navigate(`/service/${serviceId}/policies/create/${policyType}`, {
      state: { tblpageData: tblpageData }
    });
  };

  const searchFilterOption = [
    {
      category: "group",
      label: "Group Name",
      urlLabel: "groupName",
      type: "text"
    },
    {
      category: "policyLabelsPartial",
      label: "Policy Label",
      urlLabel: "policyLabel",
      type: "text"
    },
    {
      category: "policyNamePartial",
      label: "Policy Name",
      urlLabel: "policyName",
      type: "text"
    },
    {
      category: "role",
      label: "Role Name",
      urlLabel: "roleName",
      type: "text"
    },
    {
      category: "isEnabled",
      label: "Status",
      urlLabel: "status",
      type: "textoptions",
      options: () => {
        return [
          { value: "true", label: "Enabled" },
          { value: "false", label: "Disabled" }
        ];
      }
    },
    {
      category: "user",
      label: "User Name",
      urlLabel: "userName",
      type: "text"
    }
  ];

  const getSearchFilterOption = () => {
    let currentServiceDef = serviceDef;

    if (currentServiceDef !== undefined) {
      let serviceDefResource = currentServiceDef.resources;

      let serviceDefResourceOption = serviceDefResource.map((obj) => ({
        category: "resource:" + obj.name,
        label: obj.label,
        urlLabel: camelCase(obj.label),
        type: "text"
      }));

      return sortBy(concat(searchFilterOption, serviceDefResourceOption), [
        "label"
      ]);
    }

    return sortBy(searchFilterOption, ["label"]);
  };

  const updateSearchFilter = (filter) => {
    console.log("PRINT Filter from tokenizer : ", filter);

    let searchFilterParam = {};
    let searchParam = {};

    map(filter, function (obj) {
      searchFilterParam[obj.category] = obj.value;

      let searchFilterObj = find(getSearchFilterOption(), {
        category: obj.category
      });

      let urlLabelParam = searchFilterObj.urlLabel;

      if (searchFilterObj.type == "textoptions") {
        let textOptionObj = find(searchFilterObj.options(), {
          value: obj.value
        });
        searchParam[urlLabelParam] = textOptionObj.label;
      } else {
        searchParam[urlLabelParam] = obj.value;
      }
    });
    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam);
    resetPage.page(0);
  };

  return (
    <div className="wrap">
      {(props.serviceData.type == "hdfs" || props.serviceData.type == "yarn") &&
        show && (
          <Alert variant="warning" onClose={() => setShow(false)} dismissible>
            <i className="fa-fw fa fa-info-circle d-inline text-dark"></i>
            <p className="pd-l-10 d-inline">
              {`By default, fallback to ${
                alertMessage[props.serviceData.type].label
              } ACLs are enabled. If access cannot be
              determined by Ranger policies, authorization will fall back to
              ${
                alertMessage[props.serviceData.type].label
              } ACLs. If this behavior needs to be changed, modify ${
                alertMessage[props.serviceData.type].label
              }
              plugin config - ${
                alertMessage[props.serviceData.type].configs
              }-authorization.`}
            </p>
          </Alert>
        )}
      {pageLoader ? (
        <ContentLoader size="50px" />
      ) : (
        <React.Fragment>
          <div className="policy-listing">
            <Row className="mb-3">
              <Col sm={10}>
                <StructuredFilter
                  key="policy-listing-search-filter"
                  placeholder="Search for your policy..."
                  options={getSearchFilterOption()}
                  onTokenAdd={updateSearchFilter}
                  onTokenRemove={updateSearchFilter}
                  defaultSelected={defaultSearchFilterParams}
                />
              </Col>
              <Col sm={2}>
                <div className="pull-right mb-1">
                  {(isSystemAdmin() || isKeyAdmin() || isUser()) && (
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={addPolicy}
                      data-js="addNewPolicy"
                      data-cy="addNewPolicy"
                    >
                      Add New Policy
                    </Button>
                  )}
                </div>
              </Col>
            </Row>

            <XATableLayout
              data={policyListingData}
              columns={columns}
              fetchData={fetchPolicyInfo}
              totalCount={totalCount}
              pagination
              pageCount={pageCount}
              currentpageIndex={currentpageIndex}
              currentpageSize={currentpageSize}
              loading={loader}
              columnSort={true}
            />
          </div>

          <Modal show={deletePolicyModal.showPopup} onHide={toggleClose}>
            <Modal.Body>Are you sure you want to delete</Modal.Body>
            <Modal.Footer>
              <Button variant="secondary" size="sm" onClick={toggleClose}>
                Close
              </Button>
              <Button
                variant="primary"
                size="sm"
                onClick={() =>
                  handleDeleteClick(deletePolicyModal.policyDetails.policyID)
                }
              >
                OK
              </Button>
            </Modal.Footer>
          </Modal>

          <Modal show={policyviewmodal} onHide={handleClosePolicyId} size="xl">
            <Modal.Header closeButton>
              <Modal.Title>Policy Details</Modal.Title>
            </Modal.Header>
            <Modal.Body>
              <PolicyViewDetails
                paramsData={policyParamsData}
                serviceDef={serviceDef}
                policyInfo={fetchPolicyInfo}
                totalCount={totalCount}
                policyView={true}
                updateServices={updateServices}
              />
            </Modal.Body>
            <Modal.Footer>
              <div className="policy-version text-left">
                <span>
                  <i
                    className={
                      policyParamsData && policyParamsData.version > 1
                        ? "fa-fw fa fa-chevron-left active"
                        : "fa-fw fa fa-chevron-left"
                    }
                    onClick={(e) =>
                      e.currentTarget.classList.contains("active") &&
                      previousVersion(e)
                    }
                  ></i>
                  <span>{`Version ${
                    policyParamsData && policyParamsData.version
                  }`}</span>
                  <i
                    className={
                      !isUndefined(
                        currentPage[
                          indexOf(
                            currentPage,
                            policyParamsData && policyParamsData.version
                          ) + 1
                        ]
                      )
                        ? "fa-fw fa fa-chevron-right active"
                        : "fa-fw fa fa-chevron-right"
                    }
                    onClick={(e) =>
                      e.currentTarget.classList.contains("active") &&
                      nextVersion(e)
                    }
                  ></i>
                </span>
                {!isUndefined(
                  currentPage[
                    indexOf(
                      currentPage,
                      policyParamsData && policyParamsData.version
                    ) + 1
                  ]
                ) && (
                  <Button
                    variant="primary"
                    size="sm"
                    onClick={(e) => revert(e)}
                  >
                    Revert
                  </Button>
                )}
              </div>
              <Button variant="primary" size="sm" onClick={handleClosePolicyId}>
                OK
              </Button>
            </Modal.Footer>
          </Modal>
        </React.Fragment>
      )}
    </div>
  );
}

export default PolicyListing;
