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
import XATableLayout from "Components/XATableLayout";
import { GroupSource } from "Utils/XAEnums";
import { GroupTypes } from "Utils/XAEnums";
import { VisibilityStatus } from "Utils/XAEnums";
import {
  useNavigate,
  Link,
  useLocation,
  useSearchParams
} from "react-router-dom";
import moment from "moment-timezone";
import { fetchApi } from "Utils/fetchAPI";
import { toast } from "react-toastify";
import { SyncSourceDetails } from "../SyncSourceDetails";
import GroupAssociateUserDetails from "../GroupAssociateUserDetails";
import {
  isSystemAdmin,
  isKeyAdmin,
  isAuditor,
  isKMSAuditor,
  serverError,
  parseSearchFilter
} from "Utils/XAUtils";
import { find, isUndefined, isEmpty, pick } from "lodash";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import { BlockUi, Loader, scrollToNewData } from "Components/CommonComponents";
import { ACTIONS } from "./action";
import { reducer, INITIAL_STATE } from "./reducer";

function GroupListing() {
  const navigate = useNavigate();
  const { state: navigateState, search } = useLocation();

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

  const fetchGroups = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });
      let groupData = [];
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
          url: "xusers/groups",
          params: params
        });
        groupData = response.data?.vXGroups || [];
        totalCount = response.data?.totalCount || 0;
        totalPageCount = Math.ceil(totalCount / pageSize);
      } catch (error) {
        serverError(error);
        console.error(`Error occurred while fetching groups ! ${error}`);
      }

      if (navigateState) {
        navigateState["showLastPage"] = false;
      }

      dispatch({
        type: ACTIONS.SET_TABLE_DATA,
        tableListingData: groupData,
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

  const groupDelete = () => {
    if (selectedRows.current.length > 0) {
      toggleGroupDeleteModal();
    } else {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please select atleast one group !!");
    }
  };

  const toggleGroupDeleteModal = () => {
    dispatch({
      type: ACTIONS.SHOW_DELETE_MODAL,
      showDeleteModal: !state.showDeleteModal
    });
  };

  const confirmGroupDelete = () => {
    handleGroupDelete();
  };

  const handleGroupDelete = async () => {
    const selectedData = selectedRows.current;
    let errorMsg = "";
    if (selectedData.length > 0) {
      toggleGroupDeleteModal();
      for (const { original } of selectedData) {
        try {
          dispatch({ type: ACTIONS.SET_BLOCK_UI, blockUi: true });
          await fetchApi({
            url: `xusers/secure/groups/id/${original.id}`,
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
              `Error occurred during deleting group: ${original.name}` + "\n";
          }
          console.error(errorMsg);
        }
      }
      if (errorMsg) {
        toast.dismiss(toastId.current);
        toastId.current = toast.error(errorMsg);
      } else {
        toast.dismiss(toastId.current);
        toastId.current = toast.success("Group deleted successfully !");
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
                selectedRows.current.length === 1 ? "group is" : "groups are"
              } already visible`
            : `Selected ${
                selectedRows.current.length === 1 ? "group is " : "groups are"
              } already hidden`
        );
        return;
      }
      try {
        await fetchApi({
          url: "xusers/secure/groups/visibility",
          method: "PUT",
          data: obj
        });
        toast.dismiss(toastId.current);
        toastId.current = toast.success(
          `Sucessfully updated ${
            selectedRows.current.length === 1 ? "group" : "groups"
          } visibility !!`
        );
        dispatch({
          type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
          searchFilterParams: state.searchFilterParams,
          refreshTableData: moment.now()
        });
      } catch (error) {
        serverError(error);
        console.error(`Error occurred during set group visibility ! ${error}`);
      }
    } else {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please select atleast one group !!");
    }
  };

  const addGroup = () => {
    navigate("/group/create", {
      state: { tablePageData: state.tablePageData }
    });
  };

  const openSyncDetailsModal = (value) => {
    dispatch({
      type: ACTIONS.SHOW_SYNC_DETAILS_MODAL,
      showSyncDetailsModal: true,
      syncDetailsData: JSON.parse(value)
    });
  };

  const hideSyncDetailsModal = () => {
    dispatch({
      type: ACTIONS.SHOW_SYNC_DETAILS_MODAL,
      showSyncDetailsModal: false,
      syncDetailsData: {}
    });
  };

  const openGroupUsersModal = (group) => {
    let groupParams = pick(group, ["id", "name"]);

    dispatch({
      type: ACTIONS.SHOW_GROUP_USERS_MODAL,
      showGroupUsersModal: true,
      groupData: groupParams
    });
  };

  const hideGroupUsersModal = () => {
    dispatch({
      type: ACTIONS.SHOW_GROUP_USERS_MODAL,
      showGroupUsersModal: false,
      groupData: { id: "", name: "" }
    });
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Group Name",
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
                to={"/group/" + rawValue.row.original.id}
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
        Header: "Group Source",
        accessor: "groupSource",
        Cell: (rawValue) => {
          if (rawValue.value !== null && rawValue.value !== undefined) {
            if (rawValue.value == GroupSource.XA_PORTAL_GROUP.value)
              return (
                <div className="text-center">
                  <h6>
                    <Badge bg="success">
                      {GroupTypes.GROUP_INTERNAL.label}
                    </Badge>
                  </h6>
                </div>
              );
            else
              return (
                <div className="text-center">
                  <h6>
                    <Badge bg="warning">
                      {GroupTypes.GROUP_EXTERNAL.label}
                    </Badge>
                  </h6>
                </div>
              );
          } else return <div className="text-center">--</div>;
        },
        width: 100
      },
      {
        Header: "Sync Source",
        accessor: "syncSource",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <div className="text-center">
                <h6>
                  <Badge bg="success">{rawValue.value} </Badge>
                </h6>
              </div>
            );
          } else return <div className="text-center">--</div>;
        },
        width: 100
      },
      {
        Header: "Visibility",
        accessor: "isVisible",
        Cell: (rawValue) => {
          if (rawValue) {
            if (rawValue.value == VisibilityStatus.STATUS_VISIBLE.value)
              return (
                <div className="text-center">
                  <h6>
                    <Badge bg="success">
                      {VisibilityStatus.STATUS_VISIBLE.label}
                    </Badge>
                  </h6>
                </div>
              );
            else
              return (
                <div className="text-center">
                  <h6>
                    <Badge bg="info">
                      {VisibilityStatus.STATUS_HIDDEN.label}
                    </Badge>
                  </h6>
                </div>
              );
          } else return <div className="text-center">--</div>;
        },
        width: 100
      },
      {
        Header: "Users",
        accessor: "member",
        Cell: (rawValue) => {
          return (
            <div className="text-center">
              <button
                className="btn btn-outline-dark btn-sm"
                title="View Users"
                data-js="showUserList"
                onClick={() => {
                  openGroupUsersModal(rawValue.row.original);
                }}
                data-cy="showUserList"
              >
                <i className="fa-fw fa fa-group"> </i>
              </button>
            </div>
          );
        },
        width: 80
      },
      {
        Header: "Sync Details",
        accessor: "otherAttributes",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return (
              <div className="text-center">
                <button
                  className="btn btn-outline-dark btn-sm"
                  data-id="syncDetailes"
                  data-cy="syncDetailes"
                  data-for="group"
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
      category: "name",
      label: "Group Name",
      urlLabel: "groupName",
      type: "text"
    },
    {
      category: "groupSource",
      label: "Group Source",
      urlLabel: "groupSource",
      type: "textoptions",
      options: () => {
        return [
          { value: "0", label: "Internal" },
          { value: "1", label: "External" }
        ];
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
            <Col md={8} className="usr-grp-role-search-width">
              <StructuredFilter
                key="user-listing-search-filter"
                placeholder="Search for your groups..."
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
                  onClick={addGroup}
                  data-id="addNewGroup"
                  data-cy="addNewGroup"
                >
                  Add New Group
                </Button>
                <DropdownButton
                  title="Set Visibility"
                  size="sm"
                  className="ms-2 d-inline-block manage-visibility"
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
                  className="ms-2"
                  onClick={groupDelete}
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
            fetchData={fetchGroups}
            totalCount={state.totalCount}
            pageCount={state.pageCount}
            currentpageIndex={state.currentPageIndex}
            currentpageSize={state.currentPageSize}
            loading={state.loader}
            pagination
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

          <Modal show={state.showDeleteModal} onHide={toggleGroupDeleteModal}>
            <Modal.Header closeButton>
              <span className="text-word-break">
                Are you sure you want to delete the&nbsp;
                {selectedRows.current.length === 1 ? (
                  <>
                    <b>&quot;{selectedRows.current[0].original.name}&quot;</b>
                    &nbsp;group ?
                  </>
                ) : (
                  <>
                    selected<b> {selectedRows.current.length}</b> groups ?
                  </>
                )}
              </span>
            </Modal.Header>
            <Modal.Footer>
              <Button
                variant="secondary"
                size="sm"
                onClick={toggleGroupDeleteModal}
              >
                Close
              </Button>
              <Button variant="primary" size="sm" onClick={confirmGroupDelete}>
                OK
              </Button>
            </Modal.Footer>
          </Modal>

          <Modal
            show={state.showSyncDetailsModal}
            onHide={hideSyncDetailsModal}
            size="xl"
          >
            <Modal.Header closeButton>
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

          <Modal
            show={state.showGroupUsersModal}
            onHide={hideGroupUsersModal}
            size="lg"
          >
            <Modal.Header closeButton>
              <Modal.Title>
                <div className="d-flex">
                  User&apos;s List :
                  <div
                    className="ps-2 more-less-width text-truncate"
                    title={state.groupData.name}
                  >
                    {state.groupData.name}
                  </div>
                </div>
              </Modal.Title>
            </Modal.Header>
            <Modal.Body>
              <GroupAssociateUserDetails
                groupId={state.groupData.id}
              ></GroupAssociateUserDetails>
            </Modal.Body>
            <Modal.Footer>
              <Button variant="primary" size="sm" onClick={hideGroupUsersModal}>
                OK
              </Button>
            </Modal.Footer>
          </Modal>
        </React.Fragment>
      )}
    </div>
  );
}

export default GroupListing;
