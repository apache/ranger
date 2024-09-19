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
import { Button, Row, Col, Modal } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import {
  BlockUi,
  Loader,
  MoreLess,
  scrollToNewData
} from "Components/CommonComponents";
import {
  useNavigate,
  Link,
  useLocation,
  useSearchParams
} from "react-router-dom";
import moment from "moment-timezone";
import { find, isEmpty, isUndefined, map } from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import { toast } from "react-toastify";
import {
  isSystemAdmin,
  isKeyAdmin,
  isAuditor,
  isKMSAuditor,
  serverError,
  parseSearchFilter
} from "Utils/XAUtils";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import { ACTIONS } from "./action";
import { reducer, INITIAL_STATE } from "./reducer";

function RoleListing() {
  const navigate = useNavigate();
  const { state: navigateState, search } = useLocation();

  let initialArg = { navigateState: navigateState };

  const [state, dispatch] = useReducer(reducer, initialArg, INITIAL_STATE);

  const [searchParams, setSearchParams] = useSearchParams();

  const selectedRows = useRef([]);

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

  const fetchRoles = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });
      let roleData = [];
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

      try {
        const response = await fetchApi({
          url: "roles/lookup/roles",
          params: params
        });
        roleData = response.data.roles || [];
        totalCount = response.data.totalCount || 0;
        totalPageCount = Math.ceil(totalCount / pageSize);
      } catch (error) {
        serverError(error);
        console.error(`Error occurred while fetching roles ! ${error}`);
      }

      if (navigateState) {
        navigateState["showLastPage"] = false;
      }

      dispatch({
        type: ACTIONS.SET_TABLE_DATA,
        tableListingData: roleData,
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

  const roleDelete = () => {
    if (selectedRows.current.length > 0) {
      toggleRoleDeleteModal();
    } else {
      toast.warning("Please select atleast one role !!");
    }
  };

  const toggleRoleDeleteModal = () => {
    dispatch({
      type: ACTIONS.SHOW_DELETE_MODAL,
      showDeleteModal: !state.showDeleteModal
    });
  };

  const confirmRoleDelete = () => {
    handleRoleDelete();
  };

  const handleRoleDelete = async () => {
    const selectedData = selectedRows.current;
    let errorMsg = "";
    if (selectedData.length > 0) {
      toggleRoleDeleteModal();
      for (const { original } of selectedData) {
        try {
          dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: true });
          await fetchApi({
            url: `roles/roles/${original.id}`,
            method: "DELETE"
          });
          dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: false });
        } catch (error) {
          dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: false });
          if (error?.response?.data?.msgDesc) {
            errorMsg += error.response.data.msgDesc + "\n";
          } else {
            errorMsg +=
              `Error occurred during deleting role: ${original.name}` + "\n";
          }
          console.error(errorMsg);
        }
      }
      if (errorMsg) {
        toast.error(errorMsg);
      } else {
        toast.success("Role deleted successfully !");
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

  const addRole = () => {
    navigate("/roles/create", {
      state: { tablePageData: state.tablePageData }
    });
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Role Name",
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
                to={"/roles/" + rawValue.row.original.id}
                title={rawValue.value}
              >
                {rawValue.value}
              </Link>
            );
          }
          return "--";
        }
      },
      {
        Header: "Users",
        accessor: (raw) => {
          let usersList = map(raw.users, "name");
          return !isEmpty(usersList) ? (
            <MoreLess data={usersList} key={raw.id} />
          ) : (
            <div className="text-center">--</div>
          );
        }
      },
      {
        Header: "Groups",
        accessor: (raw) => {
          let groupsList = map(raw.groups, "name");
          return !isEmpty(groupsList) ? (
            <MoreLess data={groupsList} key={raw.id} />
          ) : (
            <div className="text-center">--</div>
          );
        }
      },
      {
        Header: "Roles",
        accessor: (raw) => {
          let rolesList = map(raw.roles, "name");

          return !isEmpty(rolesList) ? (
            <MoreLess data={rolesList} key={raw.id} />
          ) : (
            <div className="text-center">--</div>
          );
        }
      }
    ],
    []
  );

  const searchFilterOptions = [
    {
      category: "groupNamePartial",
      label: "Group Name",
      urlLabel: "groupName",
      type: "text"
    },
    {
      category: "roleNamePartial",
      label: "Role Name",
      urlLabel: "roleName",
      type: "text"
    },
    {
      category: "userNamePartial",
      label: "User Name",
      urlLabel: "userName",
      type: "text"
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
            <Col md={8} className="usr-grp-role-search-width">
              <StructuredFilter
                key="role-listing-search-filter"
                placeholder="Search for your roles..."
                options={searchFilterOptions}
                onChange={updateSearchFilter}
                defaultSelected={state.defaultSearchFilterParams}
              />
            </Col>
            {isSystemAdmin() && (
              <Col md={4} className="text-end">
                <Button
                  variant="primary"
                  size="sm"
                  onClick={addRole}
                  data-id="addNewRoles"
                  data-cy="addNewRoles"
                >
                  Add New Role
                </Button>
                <Button
                  className="ms-2"
                  variant="danger"
                  size="sm"
                  title="Delete"
                  onClick={roleDelete}
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
            fetchData={fetchRoles}
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
          />

          <Modal show={state.showDeleteModal} onHide={toggleRoleDeleteModal}>
            <Modal.Header closeButton>
              <span className="text-word-break">
                Are you sure you want to delete the&nbsp;
                {selectedRows.current.length === 1 ? (
                  <>
                    <b>&quot;{selectedRows.current[0].original.name}&quot;</b>
                    &nbsp;role ?
                  </>
                ) : (
                  <>
                    selected<b> {selectedRows.current.length}</b> roles?
                  </>
                )}
              </span>
            </Modal.Header>
            <Modal.Footer>
              <Button
                variant="secondary"
                size="sm"
                onClick={toggleRoleDeleteModal}
              >
                Close
              </Button>
              <Button variant="primary" size="sm" onClick={confirmRoleDelete}>
                OK
              </Button>
            </Modal.Footer>
          </Modal>
        </React.Fragment>
      )}
    </div>
  );
}

export default RoleListing;
