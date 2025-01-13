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
import { omit, has } from "lodash";
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
  camelCase,
  union
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
  isUser,
  parseSearchFilter,
  getResourcesDefVal
} from "../../utils/XAUtils";
import {
  alertMessage,
  ResourcesOverrideInfoMsg,
  ServerAttrName
} from "../../utils/XAEnums";
import {
  BlockUi,
  CustomPopover,
  Loader
} from "../../components/CommonComponents";

function PolicyListing(props) {
  const { serviceDef, serviceData, serviceZone } = props;
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
  const [blockUI, setBlockUI] = useState(false);
  let navigate = useNavigate();
  let { serviceId, policyType } = useParams();

  const [showModal, setShowModal] = useState(false);
  const [selectedFile, setSelectedFile] = useState(null);
  const [statusMessage, setStatusMessage] = useState("");
  const [loading, setLoading] = useState(false);

  const handleFileSelection = (event) => {
    const file = event.target.files[0];
    setSelectedFile(file); // Store selected file
    setStatusMessage("");
  };

  const importNewPolicy = () => {
    setShowModal(true); // Show the modal when button is clicked
  };

  const handleCloseModal = () => {
    setSelectedFile(null); // Reset the selected file
    setStatusMessage("");
    setShowModal(false);
  };

  const handleUpload = () => {
    if (!selectedFile) {
      setStatusMessage("No file selected !");
      return;
    }

    const reader = new FileReader();
    reader.onload = async (e) => {
      try {
        const json = JSON.parse(e.target.result);

        if (has("service")) {
          json["service"] = serviceData.name;
        }

        // Make API call with the processed JSON
        setLoading(true); // Show loading spinner
        await fetchApi({
          url: "plugins/policies",
          method: "POST",
          headers: {
            "Content-Type": "application/json" // Set Content-Type header
          },
          data: JSON.stringify(json) // Serialize the JSON
        });

        setLoading(false); // Hide loading spinner
        setStatusMessage("");
        setShowModal(false);
        setUpdateTable(moment.now());
        toast.success("Successfully imported policy json file !");
      } catch (error) {
        setLoading(false);
        setStatusMessage(
          `Error parsing or processing JSON file: ${error.message}`
        );
        console.log(error);
      }
    };

    reader.readAsText(selectedFile);
  };

  useEffect(() => {
    let searchFilterParam = {};
    let searchParam = {};
    let defaultSearchFilterParam = [];

    // Get Search Filter Params from current search params
    const currentParams = Object.fromEntries([...searchParams]);
    for (const param in currentParams) {
      let searchFilterObj = find(getSearchFilterOptions(), {
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
    setSearchParams({ ...currentParams, ...searchParam }, { replace: true });
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
      scrollToNewData(policyListingData);
    }
  }, [totalCount]);
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

  const downloadPolicy = async (id) => {
    try {
      const response = await fetchApi({
        url: `plugins/policies/${id}`
      });

      if (response.status !== 200) {
        toast.error("Error downloading the policy !");
        return;
      }

      let data = response.data || null;

      data = JSON.parse(JSON.stringify(data));

      const fieldsToRemove = [
        "createdBy",
        "createTime",
        "guid",
        "id",
        "resourceSignature",
        "updatedBy",
        "updateTime",
        "version"
      ];

      data = omit(data, fieldsToRemove);

      // Create a blob with the JSON data
      const blob = new Blob([JSON.stringify(data, null, 2)], {
        type: "application/json"
      });
      const url = URL.createObjectURL(blob);

      // Create a link element and set its href to the blob URL
      const a = document.createElement("a");
      a.href = url;
      a.download =
        data["serviceType"] +
          "_" +
          data["service"] +
          "_" +
          data["name"] +
          "-" +
          "policy_" +
          id +
          ".json" || "policy-data.json"; // Set the default filename for the download
      document.body.appendChild(a);
      a.click();

      // Clean up
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (error) {
      toast.error("Error downloading the policy !");
      console.error("Error fetching data: ", error);
    }
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

        if (serviceZone !== null) {
          params["zoneName"] = serviceZone.label;
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
          pageRecords: policyResp?.data?.totalCount,
          pageSize: 25
        });
        setTotalCount(totalCount);
        setPageCount(totalPageCount);
        setCurrentPageIndex(page);
        setCurrentPageSize(pageSize);
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
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
    toggleClose();
    try {
      setBlockUI(true);
      await fetchApi({
        url: `plugins/policies/${policyID}`,
        method: "DELETE"
      });
      setBlockUI(false);
      toast.success(" Success! Policy deleted successfully");
    } catch (error) {
      setBlockUI(false);
      let errorMsg = "Failed to delete policy : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error("Error occurred during deleting policy : " + error);
    }
    if (policyListingData.length == 1 && currentpageIndex > 0) {
      let page = currentpageIndex - currentpageIndex;
      if (typeof resetPage?.page === "function") {
        resetPage.page(page);
      }
    } else {
      setUpdateTable(moment.now());
    }
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
            <div className="text-center">--</div>
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
                <Badge bg="success">Enabled</Badge>
              </h6>
            );
          else
            return (
              <h6>
                <Badge bg="danger">Disabled</Badge>
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
                <Badge bg="success">Enabled</Badge>
              </h6>
            );
          } else
            return (
              <h6>
                <Badge bg="danger">Disabled</Badge>
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
                className="me-2"
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
                  <Button
                    className="me-2"
                    variant="outline-dark"
                    size="sm"
                    title="Download"
                    onClick={() => downloadPolicy(original.id)}
                    data-name="downloadPolicy"
                    data-id={original.id}
                    data-cy={original.id}
                  >
                    <i className="fa-fw fa fa-download fa-fw fa fa-large"></i>
                  </Button>
                  <Link
                    className="btn btn-outline-dark btn-sm me-2"
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
        disableSortBy: true,
        minWidth: 190
      }
    ],
    []
  );

  const addPolicy = () => {
    navigate(`/service/${serviceId}/policies/create/${policyType}`, {
      state: { tblpageData: tblpageData }
    });
  };

  const searchFilterOptions = [
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

  const getSearchFilterOptions = () => {
    let currentServiceDef = serviceDef;

    if (currentServiceDef !== undefined) {
      let serviceDefResource = getResourcesDefVal(
        currentServiceDef,
        policyType
      );

      let serviceDefResourceOption = serviceDefResource?.map((obj) => ({
        category: "resource:" + obj.name,
        label: obj.label,
        urlLabel: camelCase(obj.label),
        type: "text"
      }));

      return sortBy(concat(searchFilterOptions, serviceDefResourceOption), [
        "label"
      ]);
    }

    return sortBy(searchFilterOptions, ["label"]);
  };

  const getSearchInfoContent = () => {
    let resources = [];
    let resourceSearchOpt = [];
    let serverRsrcAttrName = [];
    let policySearchInfoMsg = [];

    resources = getResourcesDefVal(serviceDef, policyType);

    resourceSearchOpt = map(resources, function (resource) {
      return {
        name: resource.name,
        label: resource.label,
        description: resource.description
      };
    });

    serverRsrcAttrName = map(resourceSearchOpt, function (opt) {
      return {
        text: opt.label,
        info: !isUndefined(opt?.description)
          ? opt.description
          : ResourcesOverrideInfoMsg[opt.name]
      };
    });

    policySearchInfoMsg = union(ServerAttrName, serverRsrcAttrName);

    return (
      <div className="policy-search-info">
        <p className="m-0">
          Wildcard searches ( for example using * or ? ) are not currently
          supported.
        </p>
        {policySearchInfoMsg?.map((m, index) => {
          return (
            <p className="m-0" key={index}>
              <span className="fw-bold">{m.text}: </span>
              <span>{m.info}</span>
            </p>
          );
        })}
      </div>
    );
  };

  const updateSearchFilter = (filter) => {
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      getSearchFilterOptions()
    );

    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam, { replace: true });

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
    }
  };

  return (
    <div className="wrap">
      {(serviceData.type == "hdfs" || serviceData.type == "yarn") && show && (
        <Alert variant="warning" onClose={() => setShow(false)} dismissible>
          <i className="fa-fw fa fa-info-circle d-inline text-dark"></i>
          <p className="pd-l-10 d-inline">
            {`By default, fallback to ${
              alertMessage[serviceData.type].label
            } ACLs are enabled. If access cannot be
              determined by Ranger policies, authorization will fall back to
              ${
                alertMessage[serviceData.type].label
              } ACLs. If this behavior needs to be changed, modify ${
              alertMessage[serviceData.type].label
            }
              plugin config - ${
                alertMessage[serviceData.type].configs
              }-authorization.`}
          </p>
        </Alert>
      )}
      {pageLoader ? (
        <Loader />
      ) : (
        <React.Fragment>
          <BlockUi isUiBlock={blockUI} />
          <div className="policy-listing">
            <Row className="mb-3">
              <Col sm={9}>
                <div className="filter-icon-wrap">
                  <StructuredFilter
                    key="policy-listing-search-filter"
                    placeholder="Search for your policy..."
                    options={getSearchFilterOptions()}
                    onChange={updateSearchFilter}
                    defaultSelected={defaultSearchFilterParams}
                  />
                  <CustomPopover
                    icon="fa-fw fa fa-info-circle info-icon"
                    title={
                      <span style={{ fontSize: "14px" }}>
                        Search Filter Hints
                      </span>
                    }
                    content={getSearchInfoContent()}
                    placement="bottom"
                    trigger={["hover", "focus"]}
                  />
                </div>
              </Col>
              <Col sm={3}>
                <div className="float-end mb-1">
                  {(isSystemAdmin() || isKeyAdmin() || isUser()) && (
                    <div>
                      <Button
                        variant="primary"
                        size="sm"
                        className="ms-1"
                        onClick={addPolicy}
                        data-js="addNewPolicy"
                        data-cy="addNewPolicy"
                      >
                        Add New Policy
                      </Button>
                      <Button
                        variant="primary"
                        size="sm"
                        className="ms-1"
                        onClick={importNewPolicy}
                        data-js="importNewPolicy"
                        data-cy="importNewPolicy"
                      >
                        Import New Policy
                      </Button>
                    </div>
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

          {/* Modal for file upload */}
          {showModal && (
            <Modal show={showModal} onHide={handleCloseModal}>
              <Modal.Header closeButton>
                <Modal.Title>Upload JSON Policy</Modal.Title>
              </Modal.Header>
              <Modal.Body>
                <input
                  type="file"
                  accept="application/json"
                  onChange={handleFileSelection}
                />
                {loading && <p>Uploading...</p>}
                {!loading && statusMessage && <p>{statusMessage}</p>}
              </Modal.Body>
              <Modal.Footer>
                <Button
                  size="sm"
                  variant="secondary"
                  onClick={handleCloseModal}
                >
                  Close
                </Button>
                <Button size="sm" variant="primary" onClick={handleUpload}>
                  Upload
                </Button>
              </Modal.Footer>
            </Modal>
          )}

          <Modal show={deletePolicyModal.showPopup} onHide={toggleClose}>
            <Modal.Header closeButton>
              <span className="text-word-break">
                Are you sure you want to delete policy&nbsp;&quot;
                <b>{deletePolicyModal?.policyDetails?.policyName}</b>&quot; ?
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
                policyInfo={fetchPolicyInfo}
                totalCount={totalCount}
                policyView={true}
                updateServices={updateServices}
              />
            </Modal.Body>
            <Modal.Footer>
              <div className="policy-version text-start">
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
