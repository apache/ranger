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

import React, { useCallback, useRef, useEffect, useReducer } from "react";
import {
  Badge,
  Button,
  Row,
  Col,
  Modal,
  DropdownButton,
  Dropdown
} from "react-bootstrap";
import moment from "moment-timezone";
import XATableLayout from "Components/XATableLayout";
import { UserRoles, UserTypes, VisibilityStatus } from "Utils/XAEnums";
import { MoreLess, scrollToNewData } from "Components/CommonComponents";
import {
  useNavigate,
  Link,
  useLocation,
  useSearchParams
} from "react-router-dom";
import qs from "qs";
import { fetchApi } from "Utils/fetchAPI";
import { toast } from "react-toastify";
import { SyncSourceDetails } from "../SyncSourceDetails";
import {
  isSystemAdmin,
  isKeyAdmin,
  isAuditor,
  isKMSAuditor,
  serverError,
  parseSearchFilter
} from "Utils/XAUtils";
import { find, isEmpty, isUndefined, sortBy } from "lodash";
import { getUserAccessRoleList } from "Utils/XAUtils";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import { BlockUi, Loader } from "Components/CommonComponents";
import { ACTIONS } from "./action";
import { reducer, INITIAL_STATE } from "./reducer";

function UserListing() {
  const navigate = useNavigate();
  const { state: navigateState, search } = useLocation();

  const isKMSRole = isKeyAdmin() || isKMSAuditor();

  let initialArg = { navigateState: navigateState };

  const [state, dispatch] = useReducer(reducer, initialArg, INITIAL_STATE);

  const [searchParams, setSearchParams] = useSearchParams();

  const selectedRows = useRef([]);
  const toastId = useRef(null);

  useEffect(() => {
    let searchFilterParam = {};
    let searchParam = {};
    let defaultSearchFilterParam = [];

    // Get Search Filter Params from current search params
    const currentParams = Object.fromEntries([...searchParams]);
    for (const param in currentParams) {
      let searchFilterObj = find(searchFilterOptions, {
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

  const fetchUsers = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });
      let userData = [];
      let totalCount = 0;
      let page =
        navigateState && navigateState.showLastPage
          ? navigateState.addPageData.totalPage - 1
          : pageIndex;

      let totalPageCount = 0;
      let params = { ...state.searchFilterParams };

      const userRoleListData = getUserAccessRoleList().map((m) => {
        return m.value;
      });

      params["page"] = page;
      params["startIndex"] =
        navigateState && navigateState.showLastPage
          ? (navigateState.addPageData.totalPage - 1) * pageSize
          : page * pageSize;
      params["pageSize"] = pageSize;
      params["userRoleList"] = userRoleListData;

      try {
        const response = await fetchApi({
          url: "xusers/users",
          params: params,
          paramsSerializer: function (params) {
            return qs.stringify(params, { arrayFormat: "repeat" });
          }
        });
        userData = response.data.vXUsers || [];
        totalCount = response.data.totalCount || 0;
        totalPageCount = Math.ceil(totalCount / pageSize);
      } catch (error) {
        serverError(error);
        console.error(`Error occurred while fetching users ! ${error}`);
      }

      if (navigateState) {
        navigateState["showLastPage"] = false;
      }

      dispatch({
        type: ACTIONS.SET_TABLE_DATA,
        tableListingData: userData,
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

  const userDelete = () => {
    if (selectedRows.current.length > 0) {
      toggleUserDeleteModal();
    } else {
      toast.warning("Please select atleast one user !!");
    }
  };

  const toggleUserDeleteModal = () => {
    dispatch({
      type: ACTIONS.SHOW_DELETE_MODAL,
      showDeleteModal: !state.showDeleteModal
    });
  };

  const confirmUserDelete = () => {
    handleUserDelete();
  };

  const handleUserDelete = async () => {
    const selectedData = selectedRows.current;
    let errorMsg = "";
    if (selectedData.length > 0) {
      toggleUserDeleteModal();
      for (const { original } of selectedData) {
        try {
          dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: true });
          await fetchApi({
            url: `xusers/secure/users/id/${original.id}`,
            method: "DELETE",
            params: {
              forceDelete: true
            }
          });
          dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: false });
        } catch (error) {
          dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: false });
          if (error?.response?.data?.msgDesc) {
            errorMsg += error.response.data.msgDesc + "\n";
          } else {
            errorMsg +=
              `Error occurred during deleting user: ${original.name}` + "\n";
          }
          console.error(errorMsg);
        }
      }
      if (errorMsg) {
        toast.dismiss(toastId.current);
        toastId.current = toast.error(errorMsg);
      } else {
        toast.dismiss(toastId.current);
        toastId.current = toast.success("User deleted successfully !");
        if (
          (state.tableListingData.length == 1 ||
            state.tableListingData.length == selectedRows.current.length) &&
          state.currentPageIndex > 0
        ) {
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
      }
    }
  };

  const handleSetVisibility = async (e) => {
    if (selectedRows.current.length > 0) {
      let selectedRowData = selectedRows.current;
      let obj = {};
      for (const { original } of selectedRowData) {
        if (original.isVisible != e) {
          obj[original.id] = e;
        }
      }
      if (isEmpty(obj)) {
        toast.dismiss(toastId.current);
        toastId.current = toast.warning(
          e == VisibilityStatus.STATUS_VISIBLE.value
            ? `Selected ${
                selectedRows.current.length === 1 ? "user is" : "users are"
              } already visible`
            : `Selected ${
                selectedRows.current.length === 1 ? "user is " : "users are"
              } already hidden`
        );
        return;
      }
      try {
        await fetchApi({
          url: "xusers/secure/users/visibility",
          method: "PUT",
          data: obj
        });
        toast.dismiss(toastId.current);
        toastId.current = toast.success(
          `Sucessfully updated ${
            selectedRows.current.length === 1 ? "user" : "users"
          } visibility !!`
        );
        dispatch({
          type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
          searchFilterParams: state.searchFilterParams,
          refreshTableData: moment.now()
        });
      } catch (error) {
        serverError(error);
        console.error(`Error occurred during set user visibility ! ${error}`);
      }
    } else {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please select atleast one user !!");
    }
  };

  const addUser = () => {
    navigate("/user/create", { state: { tablePageData: state.tablePageData } });
  };

  const openSyncDetailsModal = (value) => {
    dispatch({
      type: ACTIONS.SHOW_SYNC_DETAILS_MODAL,
      showSyncDetailsModal: true,
      syncDetailsData: !isEmpty(value) && JSON.parse(value)
    });
  };

  const hideSyncDetailsModal = () => {
    dispatch({
      type: ACTIONS.SHOW_SYNC_DETAILS_MODAL,
      showSyncDetailsModal: false,
      syncDetailsData: {}
    });
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "User Name",
        accessor: "name",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <Link
                style={{ maxWidth: "100%", display: "inline-block" }}
                className={`text-truncate ${
                  isAuditor() || isKMSAuditor()
                    ? "disabled-link text-secondary"
                    : "text-info"
                }`}
                title={rawValue.value}
                to={"/user/" + rawValue.row.original.id}
              >
                {rawValue.value}
              </Link>
            );
          }
          return "--";
        }
      },
      {
        Header: "Email Address",
        accessor: "emailAddress",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <div
                style={{ maxWidth: "100%", display: "inline-block" }}
                className="text-truncate"
                title={rawValue.value}
              >
                {rawValue.value}
              </div>
            );
          } else return <div className="text-center">--</div>;
        }
      },
      {
        Header: "Role",
        accessor: "userRoleList",
        Cell: (rawValue) => {
          if (rawValue.value && rawValue.value.length > 0) {
            let role = rawValue.value[0];
            return (
              <h6 className="text-center">
                <Badge bg="info">{UserRoles[role].label} </Badge>
              </h6>
            );
          }
          return <div className="textt-center">--</div>;
        },
        width: 70
      },
      {
        Header: "User Source",
        accessor: "userSource",
        Cell: (rawValue) => {
          if (rawValue?.value != null) {
            const userSourceVal = find(UserTypes, { value: rawValue.value });
            return (
              <h6 className="text-center">
                <Badge bg={userSourceVal.variant}>{userSourceVal.label}</Badge>
              </h6>
            );
          } else return "--";
        },
        width: 70
      },
      {
        Header: "Sync Source",
        accessor: "syncSource",
        Cell: (rawValue) => {
          return rawValue.value ? (
            <h6 className="text-center">
              <Badge bg="success">{rawValue.value} </Badge>
            </h6>
          ) : (
            <div className="text-center">--</div>
          );
        },
        width: 100
      },
      {
        Header: "Groups",
        accessor: "groupNameList",
        Cell: (rawValue) => {
          if (rawValue.row.values.groupNameList !== undefined) {
            return (
              <div className="overflow-auto">
                {!isEmpty(rawValue.row.values.groupNameList) ? (
                  <h6>
                    <MoreLess
                      data={rawValue.row.values.groupNameList}
                      key={rawValue.row.original.id}
                    />
                  </h6>
                ) : (
                  <div className="text-center">--</div>
                )}
              </div>
            );
          } else {
            return "--";
          }
        }
      },
      {
        Header: "Visibility",
        accessor: "isVisible",
        Cell: (rawValue) => {
          if (rawValue) {
            if (rawValue.value == VisibilityStatus.STATUS_VISIBLE.value)
              return (
                <h6 className="text-center">
                  <Badge bg="success">
                    {VisibilityStatus.STATUS_VISIBLE.label}
                  </Badge>
                </h6>
              );
            else
              return (
                <h6 className="text-center">
                  <Badge bg="info">
                    {VisibilityStatus.STATUS_HIDDEN.label}
                  </Badge>
                </h6>
              );
          } else return <div className="text-center">--</div>;
        },
        width: 70
      },
      {
        Header: "Sync Details",
        accessor: "otherAttributes",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <div className="text-center">
                <button
                  className="btn btn-outline-dark btn-sm "
                  data-id="syncDetailes"
                  data-cy="syncDetailes"
                  data-for="users"
                  title="Sync Details"
                  onClick={() => {
                    openSyncDetailsModal(rawValue.value);
                  }}
                >
                  <i className="fa-fw fa fa-eye"> </i>
                </button>
              </div>
            );
          } else {
            return <div className="text-center">--</div>;
          }
        },
        width: 80
      }
    ],
    []
  );

  const searchFilterOptions = [
    {
      category: "emailAddress",
      label: "Email Address",
      urlLabel: "emailAddress",
      type: "text"
    },
    {
      category: "userRole",
      label: "Role",
      urlLabel: "role",
      type: "textoptions",
      options: () => {
        if (isKMSRole) {
          return [
            { value: "ROLE_USER", label: "User" },
            { value: "ROLE_KEY_ADMIN", label: "KeyAdmin" },
            { value: "ROLE_KEY_ADMIN_AUDITOR", label: "KMSAuditor" }
          ];
        } else {
          return [
            { value: "ROLE_USER", label: "User" },
            { value: "ROLE_SYS_ADMIN", label: "Admin" },
            { value: "ROLE_ADMIN_AUDITOR", label: "Auditor" }
          ];
        }
      }
    },
    {
      category: "syncSource",
      label: "Sync Source",
      urlLabel: "syncSource",
      type: "textoptions",
      options: () => {
        return [
          { value: "File", label: "File" },
          { value: "LDAP/AD", label: "LDAP/AD" },
          { value: "Unix", label: "Unix" }
        ];
      }
    },
    {
      category: "name",
      label: "User Name",
      urlLabel: "userName",
      type: "text"
    },
    {
      category: "userSource",
      label: "User Source",
      urlLabel: "userSource",
      type: "textoptions",
      options: () => {
        return [
          { value: "0", label: "Internal" },
          { value: "1", label: "External" },
          { value: "6", label: "Federated" }
        ];
      }
    },
    {
      category: "status",
      label: "User Status",
      urlLabel: "userStatus",
      type: "textoptions",
      options: () => {
        return [
          { value: "0", label: "Disabled" },
          { value: "1", label: "Enabled" }
        ];
      }
    },
    {
      category: "isVisible",
      label: "Visibility",
      urlLabel: "visibility",
      type: "textoptions",
      options: () => {
        return [
          { value: "0", label: "Hidden" },
          { value: "1", label: "Visible" }
        ];
      }
    }
  ];

  const updateSearchFilter = (filter) => {
    const { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
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
      {state.contentLoader ? (
        <Loader />
      ) : (
        <React.Fragment>
          <BlockUi isUiBlock={state.blockUi} />
          <Row className="mb-4">
            <Col sm={8} className="usr-grp-role-search-width">
              <StructuredFilter
                key="user-listing-search-filter"
                placeholder="Search for your users..."
                options={sortBy(searchFilterOptions, ["label"])}
                onChange={updateSearchFilter}
                defaultSelected={state.defaultSearchFilterParams}
              />
            </Col>
            {isSystemAdmin() && (
              <Col sm={4} className="text-end">
                <Button
                  variant="primary"
                  size="sm"
                  className="btn-sm"
                  onClick={addUser}
                  data-id="addNewUser"
                  data-cy="addNewUser"
                >
                  Add New User
                </Button>
                <DropdownButton
                  title="Set Visibility"
                  size="sm"
                  className="ms-1 d-inline-block manage-visibility"
                  onSelect={handleSetVisibility}
                  data-id="hideShowVisibility"
                  data-cy="hideShowVisibility"
                >
                  <Dropdown.Item eventKey="1">Visible</Dropdown.Item>
                  <Dropdown.Item eventKey="0">Hidden</Dropdown.Item>
                </DropdownButton>
                <Button
                  variant="danger"
                  size="sm"
                  title="Delete"
                  onClick={userDelete}
                  className="ms-1 btn-sm"
                  data-id="deleteUserGroup"
                  data-cy="deleteUserGroup"
                >
                  <i className="fa-fw fa fa-trash"></i>
                </Button>
              </Col>
            )}
          </Row>

          <XATableLayout
            data={state.tableListingData}
            columns={columns}
            fetchData={fetchUsers}
            totalCount={state.totalCount}
            pageCount={state.pageCount}
            currentpageIndex={state.currentPageIndex}
            currentpageSize={state.currentPageSize}
            pagination
            loading={state.loader}
            rowSelectOp={
              (isSystemAdmin() || isKeyAdmin()) && {
                position: "first",
                selectedRows
              }
            }
            getRowProps={(row) => ({
              className: row.values.isVisible == 0 && "row-inactive"
            })}
          />

          <Modal show={state.showDeleteModal} onHide={toggleUserDeleteModal}>
            <Modal.Header closeButton>
              <span className="text-word-break">
                Are you sure you want to delete the&nbsp;
                {selectedRows.current.length === 1 ? (
                  <>
                    <b>&quot;{selectedRows.current[0].original.name}&quot;</b>
                    &nbsp;user ?
                  </>
                ) : (
                  <>
                    selected<b> {selectedRows.current.length}</b> users ?
                  </>
                )}
              </span>
            </Modal.Header>
            <Modal.Footer>
              <Button
                variant="secondary"
                size="sm"
                onClick={toggleUserDeleteModal}
              >
                Close
              </Button>
              <Button variant="primary" size="sm" onClick={confirmUserDelete}>
                OK
              </Button>
            </Modal.Footer>
          </Modal>
          <Modal
            show={state.showSyncDetailsModal}
            onHide={hideSyncDetailsModal}
            size="xl"
          >
            <Modal.Header>
              <Modal.Title>Sync Source Details</Modal.Title>
            </Modal.Header>
            <Modal.Body>
              <SyncSourceDetails
                syncDetails={state.syncDetailsData}
              ></SyncSourceDetails>
            </Modal.Body>
            <Modal.Footer>
              <Button
                variant="primary"
                size="sm"
                onClick={hideSyncDetailsModal}
              >
                OK
              </Button>
            </Modal.Footer>
          </Modal>
        </React.Fragment>
      )}
    </div>
  );
}

export default UserListing;
