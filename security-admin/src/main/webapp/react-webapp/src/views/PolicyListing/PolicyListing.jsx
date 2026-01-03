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

import React, { useCallback, useEffect, useReducer } from "react";
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
  isUndefined,
  isEmpty,
  map,
  sortBy,
  find,
  concat,
  camelCase,
  union,
  omit,
  has
} from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import XATableLayout from "Components/XATableLayout";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import {
  isAuditor,
  isKMSAuditor,
  isPolicyExpired,
  isSystemAdmin,
  isKeyAdmin,
  isUser,
  parseSearchFilter,
  getResourcesDefVal,
  showGroupsOrUsersOrRolesForPolicy,
  QueryParamsName
} from "Utils/XAUtils";
import {
  alertMessage,
  ResourcesOverrideInfoMsg,
  ServerAttrName
} from "Utils/XAEnums";
import {
  BlockUi,
  CustomPopover,
  Loader,
  MoreLess,
  scrollToNewData
} from "Components/CommonComponents";
import AccessLogPolicyModal from "Views/AuditEvent/Access/AccessLogPolicyModal";
import { ACTIONS } from "./action";
import { reducer, INITIAL_STATE } from "./reducer";

function PolicyListing(props) {
  const { serviceDef, serviceData, serviceZone } = props;
  const navigate = useNavigate();
  const { state: navigateState, search } = useLocation();
  const { serviceId, policyType } = useParams();

  let initialArg = { navigateState: navigateState };

  const [state, dispatch] = useReducer(reducer, initialArg, INITIAL_STATE);

  const [searchParams, setSearchParams] = useSearchParams();

  const handlePolicyImportFile = (event) => {
    const file = event.target.files[0];
    dispatch({
      type: ACTIONS.IMPORT_POLICY,
      showImportPolicyModal: state.showImportPolicyModal,
      importPolicyFile: file,
      importPolicyStatusMsg: [],
      importPolicyLoading: false
    });
  };

  const importNewPolicy = () => {
    dispatch({
      type: ACTIONS.IMPORT_POLICY,
      showImportPolicyModal: true,
      importPolicyFile: null,
      importPolicyStatusMsg: [],
      importPolicyLoading: false
    });
  };

  const closePolicyImportModal = () => {
    dispatch({
      type: ACTIONS.IMPORT_POLICY,
      showImportPolicyModal: false,
      importPolicyFile: null,
      importPolicyStatusMsg: [],
      importPolicyLoading: false
    });
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
      JSON.stringify(state.searchFilterParams) !==
      JSON.stringify(searchFilterParam)
    ) {
      dispatch({
        type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
        searchFilterParams: searchFilterParam,
        refreshTableData: moment.now()
      });
    }
    dispatch({
      type: ACTIONS.SET_DEFAULT_SEARCH_FILTER_PARAMS,
      defaultSearchFilterParams: defaultSearchFilterParam
    });
    dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
    localStorage.setItem(
      "newDataAdded",
      navigateState && navigateState.showLastPage
    );
  }, [search]);

  useEffect(() => {
    if (localStorage.getItem("newDataAdded") == "true") {
      scrollToNewData(state.tableListingData);
    }
  }, [state.totalCount]);

  const handlePolicyUpload = () => {
    if (!state.importPolicyFile) {
      dispatch({
        type: ACTIONS.IMPORT_POLICY,
        showImportPolicyModal: state.showImportPolicyModal,
        importPolicyFile: state.importPolicyFile,
        importPolicyStatusMsg: [
          <p className="fw-bold" key="upload-no-file">
            No file selected !
          </p>
        ],
        importPolicyLoading: false
      });
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
        dispatch({
          type: ACTIONS.IMPORT_POLICY,
          showImportPolicyModal: state.showImportPolicyModal,
          importPolicyFile: state.importPolicyFile,
          importPolicyStatusMsg: state.importPolicyStatusMsg,
          importPolicyLoading: true
        });
        await fetchApi({
          url: "plugins/policies",
          method: "POST",
          headers: {
            "Content-Type": "application/json" // Set Content-Type header
          },
          data: JSON.stringify(json) // Serialize the JSON
        });

        dispatch({
          type: ACTIONS.IMPORT_POLICY,
          showImportPolicyModal: false,
          importPolicyFile: state.importPolicyFile,
          importPolicyStatusMsg: [],
          importPolicyLoading: false
        });
        refreshTable();
        toast.success("Successfully imported policy json file !");
      } catch (error) {
        let errorMsg = [
          <p className="fw-bold" key="upload-error">
            Error parsing or processing JSON file: {error.message}
          </p>
        ];
        if (error?.response?.data?.msgDesc) {
          errorMsg.push([
            <p className="connection-error" key="upload-error-message">
              {error.response.data.msgDesc}
            </p>
          ]);
        }
        dispatch({
          type: ACTIONS.IMPORT_POLICY,
          showImportPolicyModal: state.showImportPolicyModal,
          importPolicyFile: state.importPolicyFile,
          importPolicyStatusMsg: errorMsg,
          importPolicyLoading: false
        });
        console.log(error);
      }
    };

    reader.readAsText(state.importPolicyFile);
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

  const fetchPolicies = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });
      let policyData = [];
      let totalCount = 0;
      let page =
        navigateState && navigateState.showLastPage
          ? navigateState.addPageData.totalPage - 1
          : pageIndex;
      let totalPageCount = 0;
      let params = { ...state.searchFilterParams };

      params["page"] = page;
      params["startIndex"] =
        navigateState && navigateState.showLastPage
          ? (navigateState.addPageData.totalPage - 1) * pageSize
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
        const response = await fetchApi({
          url: `plugins/policies/service/${serviceId}`,
          params: params
        });
        policyData = response.data?.policies || [];
        totalCount = response.data?.totalCount || 0;
        totalPageCount = Math.ceil(totalCount / pageSize);
      } catch (error) {
        console.error(`Error occurred while fetching policies : ${error}`);
      }

      if (navigateState) {
        navigateState["showLastPage"] = false;
      }

      dispatch({
        type: ACTIONS.SET_TABLE_DATA,
        tableListingData: policyData,
        totalCount: totalCount,
        pageCount: totalPageCount,
        currentPageIndex: page,
        currentPageSize: pageSize,
        resetPage: { page: gotoPage },
        tablePageData: {
          totalPage: totalPageCount,
          pageRecords: totalCount,
          pageSize: pageSize
        }
      });
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: false });
    },
    [state.refreshTableData]
  );

  const policyDelete = (policyId, policyName) => {
    dispatch({
      type: ACTIONS.SHOW_DELETE_MODAL,
      showDeleteModal: true,
      policyDetail: { policyId: policyId, policyName: policyName }
    });
  };

  const togglePolicyDeleteModal = () => {
    dispatch({
      type: ACTIONS.SHOW_DELETE_MODAL,
      showDeleteModal: false,
      policyDetail: { policyId: "", policyName: "" }
    });
  };

  const handlePolicyDelete = async (policyId) => {
    togglePolicyDeleteModal();
    try {
      dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: true });
      await fetchApi({
        url: `plugins/policies/${policyId}`,
        method: "DELETE"
      });
      dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: false });
      toast.success("Policy deleted successfully !");
    } catch (error) {
      dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: false });
      let errorMsg = "Failed to delete policy : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error("Error occurred during deleting policy : " + error);
    }
    if (state.tableListingData.length == 1 && state.currentPageIndex > 0) {
      if (typeof state.resetPage?.page === "function") {
        state.resetPage.page(0);
      }
    } else {
      dispatch({
        type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
        searchFilterParams: state.searchFilterParams,
        refreshTableData: moment.now()
      });
    }
  };

  const hidePolicyModal = () =>
    dispatch({
      type: ACTIONS.SHOW_POLICY_MODAL,
      showPolicyModal: false,
      policyData: null
    });

  const openPolicyModal = (policy) => {
    let policyParams = {
      policyId: policy.id,
      policyVersion: policy.version
    };
    dispatch({
      type: ACTIONS.SHOW_POLICY_MODAL,
      showPolicyModal: true,
      policyData: policyParams
    });
  };

  const refreshTable = () => {
    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: state.searchFilterParams,
      refreshTableData: moment.now()
    });
  };

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

  const addPolicy = () => {
    navigate(`/service/${serviceId}/policies/create/${policyType}`, {
      state: { tablePageData: state.tablePageData }
    });
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
                  openPolicyModal(original);
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
                    onClick={() => policyDelete(original.id, original.name)}
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
    const { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      getSearchFilterOptions()
    );

    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: searchFilterParam,
      refreshTableData: moment.now()
    });

    setSearchParams(searchParam, { replace: true });

    if (typeof state.resetPage?.page === "function") {
      state.resetPage.page(0);
    }
  };

  return (
    <div className="wrap">
      {(serviceData.type == "hdfs" || serviceData.type == "yarn") &&
        state.showAlert && (
          <Alert
            variant="warning"
            onClose={() =>
              dispatch({ type: ACTIONS.SHOW_ALERT, showAlert: false })
            }
            dismissible
          >
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
      {state.contentLoader ? (
        <Loader />
      ) : (
        <React.Fragment>
          <BlockUi isUiBlock={state.blockUi} />
          <div className="policy-listing">
            <Row className="mb-3">
              <Col sm={9}>
                <div className="filter-icon-wrap">
                  <StructuredFilter
                    key="policy-listing-search-filter"
                    placeholder="Search for your policy..."
                    options={getSearchFilterOptions()}
                    onChange={updateSearchFilter}
                    defaultSelected={state.defaultSearchFilterParams}
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
              data={state.tableListingData}
              columns={columns}
              fetchData={fetchPolicies}
              totalCount={state.totalCount}
              pagination
              pageCount={state.pageCount}
              currentpageIndex={state.currentPageIndex}
              currentpageSize={state.currentPageSize}
              loading={state.loader}
              columnSort={true}
            />
          </div>

          <Modal
            show={state.showImportPolicyModal}
            onHide={closePolicyImportModal}
          >
            <Modal.Header closeButton>
              <Modal.Title>Upload JSON Policy</Modal.Title>
            </Modal.Header>
            <Modal.Body>
              <input
                type="file"
                accept="application/json"
                onChange={handlePolicyImportFile}
              />
              {state.importPolicyLoading && (
                <p className="mt-2 fw-bold">Uploading...</p>
              )}
              {!state.importPolicyLoading &&
                state.importPolicyStatusMsg.length > 0 && (
                  <div className="mt-2">{state.importPolicyStatusMsg}</div>
                )}
            </Modal.Body>
            <Modal.Footer>
              <Button
                size="sm"
                variant="secondary"
                onClick={closePolicyImportModal}
              >
                Close
              </Button>
              <Button size="sm" variant="primary" onClick={handlePolicyUpload}>
                Upload
              </Button>
            </Modal.Footer>
          </Modal>

          <Modal show={state.showDeleteModal} onHide={togglePolicyDeleteModal}>
            <Modal.Header closeButton>
              <span className="text-word-break">
                Are you sure you want to delete policy&nbsp;&quot;
                <b>{state.policyDetail.policyName}</b>&quot; ?
              </span>
            </Modal.Header>
            <Modal.Footer>
              <Button
                variant="secondary"
                size="sm"
                onClick={togglePolicyDeleteModal}
              >
                Close
              </Button>
              <Button
                variant="primary"
                size="sm"
                onClick={() => handlePolicyDelete(state.policyDetail.policyId)}
              >
                OK
              </Button>
            </Modal.Footer>
          </Modal>

          <AccessLogPolicyModal
            policyData={state.policyData}
            policyView={true}
            policyRevert={true}
            refreshTable={refreshTable}
            showPolicyModal={state.showPolicyModal}
            hidePolicyModal={hidePolicyModal}
          ></AccessLogPolicyModal>
        </React.Fragment>
      )}
    </div>
  );
}

export default PolicyListing;
