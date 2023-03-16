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
import {
  UserRoles,
  UserSource,
  UserTypes,
  VisibilityStatus
} from "Utils/XAEnums";
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
  serverError
} from "Utils/XAUtils";
import { find, isEmpty, isUndefined, map, sortBy } from "lodash";
import { getUserAccessRoleList } from "Utils/XAUtils";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import { BlockUi, Loader } from "../../../components/CommonComponents";

function Users() {
  const navigate = useNavigate();
  const { state } = useLocation();
  const [loader, setLoader] = useState(true);
  const [userListingData, setUserData] = useState([]);
  const fetchIdRef = useRef(0);
  const selectedRows = useRef([]);
  const [showModal, setConfirmModal] = useState(false);
  const [showUserSyncDetails, setUserSyncdetails] = useState({
    syncDteails: {},
    showSyncDetails: false
  });
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [totalCount, setTotalCount] = useState(0);
  const [pageCount, setPageCount] = useState(
    state && state.showLastPage ? state.addPageData.totalPage : 0
  );
  const [currentpageIndex, setCurrentPageIndex] = useState(
    state && state.showLastPage ? state.addPageData.totalPage - 1 : 0
  );
  const [currentpageSize, setCurrentPageSize] = useState(
    state && state.showLastPage ? state.addPageData.pageSize : 25
  );
  const [resetPage, setResetPage] = useState({ page: 0 });
  const [tblpageData, setTblPageData] = useState({
    totalPage: 0,
    pageRecords: 0,
    pageSize: 0
  });
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );

  const [pageLoader, setPageLoader] = useState(true);
  const [blockUI, setBlockUI] = useState(false);

  useEffect(() => {
    let searchFilterParam = {};
    let searchParam = {};
    let defaultSearchFilterParam = [];

    // Get Search Filter Params from current search params
    const currentParams = Object.fromEntries([...searchParams]);
    console.log("PRINT search params : ", currentParams);

    for (const param in currentParams) {
      let searchFilterObj = find(searchFilterOption, {
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

  const fetchUserInfo = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      setLoader(true);
      let userData = [],
        userResp = [];
      let totalCount = 0;
      let page =
        state && state.showLastPage
          ? state.addPageData.totalPage - 1
          : pageIndex;

      let totalPageCount = 0;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchFilterParams };
      const userRoleListData = getUserAccessRoleList().map((m) => {
        return m.value;
      });

      if (fetchId === fetchIdRef.current) {
        params["page"] = page;

        params["startIndex"] =
          state && state.showLastPage
            ? (state.addPageData.totalPage - 1) * pageSize
            : page * pageSize;
        params["pageSize"] = pageSize;
        params["userRoleList"] = userRoleListData;
        try {
          userResp = await fetchApi({
            url: "xusers/users",
            params: params,
            paramsSerializer: function (params) {
              return qs.stringify(params, { arrayFormat: "repeat" });
            }
          });
          userData = userResp.data.vXUsers;
          totalCount = userResp.data.totalCount;
          totalPageCount = Math.ceil(totalCount / pageSize);
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching User list! ${error}`);
        }
        if (state) {
          state["showLastPage"] = false;
        }

        setUserData(userData);
        setTblPageData({
          totalPage: totalPageCount,
          pageRecords: userResp && userResp.data && userResp.data.totalCount,
          pageSize: pageSize
        });
        setTotalCount(totalCount);
        setPageCount(totalPageCount);
        setCurrentPageIndex(page);
        setCurrentPageSize(pageSize);
        setResetPage({ page: gotoPage });
        setLoader(false);
        if (
          page == totalPageCount - 1 &&
          localStorage.getItem("newDataAdded") == "true"
        ) {
          scrollToNewData(userData, userResp.data.resultSize);
        }
      }
      localStorage.removeItem("newDataAdded");
    },
    [updateTable, searchFilterParams]
  );

  const handleDeleteBtnClick = () => {
    if (selectedRows.current.length > 0) {
      toggleConfirmModal();
    } else {
      toast.warning("Please select atleast one user!!");
    }
  };

  const handleSetVisibility = async (e) => {
    if (selectedRows.current.length > 0) {
      let selectedRowData = selectedRows.current;
      for (const { original } of selectedRowData) {
        if (original.isVisible == e) {
          toast.warning(
            e == VisibilityStatus.STATUS_VISIBLE.value
              ? "Selected user is already visible"
              : "Selected user is already hidden"
          );
        } else {
          let obj = {};
          obj[original.id] = e;
          try {
            await fetchApi({
              url: "xusers/secure/users/visibility",
              method: "PUT",
              data: obj
            });
            toast.success("Sucessfully updated Users visibility!!");
            setUpdateTable(moment.now());
          } catch (error) {
            serverError(error);
            console.log(`Error occurred during set Users visibility! ${error}`);
          }
        }
      }
    } else {
      toast.warning("Please select atleast one user!!");
    }
  };

  const handleDeleteClick = async () => {
    const selectedData = selectedRows.current;
    let errorMsg = "";
    if (selectedData.length > 0) {
      toggleConfirmModal();
      for (const { original } of selectedData) {
        try {
          setBlockUI(true);
          await fetchApi({
            url: `xusers/secure/users/id/${original.id}`,
            method: "DELETE",
            params: {
              forceDelete: true
            }
          });
          setBlockUI(false);
        } catch (error) {
          setBlockUI(false);
          if (error.response.data.msgDesc) {
            errorMsg += error.response.data.msgDesc + "\n";
          } else {
            errorMsg +=
              `Error occurred during deleting Users: ${original.name}` + "\n";
          }
          console.log(errorMsg);
        }
      }
      if (errorMsg) {
        toast.error(errorMsg);
      } else {
        toast.success("User deleted successfully!");
        if (
          (userListingData.length == 1 ||
            userListingData.length == selectedRows.current.length) &&
          currentpageIndex > 1
        ) {
          resetPage.page(0);
        } else {
          setUpdateTable(moment.now());
        }
      }
    }
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
                style={{maxWidth:"100%", display: "inline-block" }}
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
        },
      },
      {
        Header: "Email Address",
        accessor: "emailAddress", // accessor is the "key" in the data
        Cell: (rawValue) => {
          if (rawValue.value) {
            return <div style={{maxWidth:"100%", display: "inline-block" }}
                         className="text-truncate"  title={rawValue.value}>
                          {rawValue.value}
                   </div>;
          } else return <div className="text-center">--</div>;
        },
      },
      {
        Header: "Role",
        accessor: "userRoleList",
        Cell: (rawValue) => {
          if (rawValue.value && rawValue.value.length > 0) {
            let role = rawValue.value[0];
            return (
              <h6 className="text-center">
                <Badge variant="info">{UserRoles[role].label} </Badge>
              </h6>
            );
          }
          return <div className="textt-center">--</div>;
        },
        width:70
      },
      {
        Header: "User Source",
        accessor: "userSource", // accessor is the "key" in the data
        Cell: (rawValue) => {
          if (rawValue.value !== null && rawValue.value !== undefined) {
            if (rawValue.value == UserSource.XA_PORTAL_USER.value)
              return (
                <h6 className="text-center">
                  <Badge variant="success">
                    {UserTypes.USER_INTERNAL.label}
                  </Badge>
                </h6>
              );
            else
              return (
                <h6 className="text-center">
                  <Badge variant="warning">
                    {UserTypes.USER_EXTERNAL.label}
                  </Badge>
                </h6>
              );
          } else return "--";
        },
        width:70
      },
      {
        Header: "Sync Source",
        accessor: "syncSource",
        Cell: (rawValue) => {
          return rawValue.value ? (
            <h6 className="text-center">
              <Badge variant="success">{rawValue.value} </Badge>
            </h6>
          ) : (
            <div className="text-center">--</div>
          );
        },
        width:100
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
        },
      },
      {
        Header: "Visibility",
        accessor: "isVisible",
        Cell: (rawValue) => {
          if (rawValue) {
            if (rawValue.value == VisibilityStatus.STATUS_VISIBLE.value)
              return (
                <h6 className="text-center">
                  <Badge variant="success">
                    {VisibilityStatus.STATUS_VISIBLE.label}
                  </Badge>
                </h6>
              );
            else
              return (
                <h6 className="text-center">
                  <Badge variant="info">
                    {VisibilityStatus.STATUS_HIDDEN.label}
                  </Badge>
                </h6>
              );
          } else return <div className="text-center">--</div>;
        },
        width:70
      },
      {
        Header: "Sync Details",
        accessor: "otherAttributes",
        Cell: (rawValue, model) => {
          if (rawValue.value) {
            return (
              <div className="text-center">
                <button
                  className="btn btn-outline-dark btn-sm "
                  data-id="syncDetailes"
                  data-cy="syncDetailes"
                  data-for="users"
                  title="Sync Details"
                  id={model.id}
                  onClick={() => {
                    toggleUserSyncModal(rawValue.value);
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
      width:80
      }
    ],
    []
  );

  const addUser = () => {
    navigate("/user/create", { state: { tblpageData: tblpageData } });
  };

  const toggleConfirmModal = () => {
    setConfirmModal((state) => !state);
  };

  const toggleUserSyncModal = (raw) => {
    setUserSyncdetails({
      syncDteails: JSON.parse(raw),
      showSyncDetails: true
    });
  };

  const toggleUserSyncModalClose = () => {
    setUserSyncdetails({
      syncDteails: {},
      showSyncDetails: false
    });
  };

  const handleConfirmClick = () => {
    handleDeleteClick();
  };

  const searchFilterOption = [
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
        return [
          { value: "ROLE_USER", label: "User" },
          { value: "ROLE_SYS_ADMIN", label: "Admin" },
          { value: "ROLE_ADMIN_AUDITOR", label: "Auditor" }
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
          { value: "1", label: "External" }
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
    console.log("PRINT Filter from tokenizer : ", filter);

    let searchFilterParam = {};
    let searchParam = {};

    map(filter, function (obj) {
      searchFilterParam[obj.category] = obj.value;

      let searchFilterObj = find(searchFilterOption, {
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
      <h4 className="wrap-header font-weight-bold">User List</h4>
      {pageLoader ? (
        <Loader />
      ) : (
        <React.Fragment>
          <BlockUi isUiBlock={blockUI} />
          <Row className="mb-4">
            <Col sm={8} className="usr-grp-role-search-width">
              <StructuredFilter
                key="user-listing-search-filter"
                placeholder="Search for your users..."
                options={sortBy(searchFilterOption, ["label"])}
                onTokenAdd={updateSearchFilter}
                onTokenRemove={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
              />
            </Col>
            {isSystemAdmin() && (
              <Col sm={4} className="text-right">
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
                  style={{ display: "inline-block" }}
                  className="ml-1 btn-sm"
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
                  onClick={handleDeleteBtnClick}
                  className="ml-1 btn-sm"
                  data-id="deleteUserGroup"
                  data-cy="deleteUserGroup"
                >
                  <i className="fa-fw fa fa-trash"></i>
                </Button>
              </Col>
            )}
          </Row>

          <XATableLayout
            data={userListingData}
            columns={columns}
            fetchData={fetchUserInfo}
            totalCount={totalCount}
            pageCount={pageCount}
            currentpageIndex={currentpageIndex}
            currentpageSize={currentpageSize}
            pagination
            loading={loader}
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

          <Modal show={showModal} onHide={toggleConfirmModal}>
            <Modal.Body>
              Are you sure you want to delete&nbsp;
              {selectedRows.current.length === 1 ? (
                <span>
                  <b>"{selectedRows.current[0].original.name}"</b> user ?
                </span>
              ) : (
                <span>
                  <b>"{selectedRows.current.length}"</b> users ?
                </span>
              )}
            </Modal.Body>
            <Modal.Footer>
              <Button
                variant="secondary"
                size="sm"
                onClick={toggleConfirmModal}
              >
                Close
              </Button>
              <Button variant="primary" size="sm" onClick={handleConfirmClick}>
                OK
              </Button>
            </Modal.Footer>
          </Modal>
          <Modal
            show={showUserSyncDetails && showUserSyncDetails.showSyncDetails}
            onHide={toggleUserSyncModalClose}
            size="xl"
          >
            <Modal.Header>
              <Modal.Title>Sync Source Details</Modal.Title>
            </Modal.Header>
            <Modal.Body>
              <SyncSourceDetails
                syncDetails={showUserSyncDetails.syncDteails}
              ></SyncSourceDetails>
            </Modal.Body>
            <Modal.Footer>
              <Button
                variant="primary"
                size="sm"
                onClick={toggleUserSyncModalClose}
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

export default Users;
