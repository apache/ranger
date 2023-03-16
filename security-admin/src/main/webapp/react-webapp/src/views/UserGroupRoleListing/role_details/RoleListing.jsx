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
import { Button, Row, Col, Modal } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { MoreLess, scrollToNewData } from "Components/CommonComponents";
import {
  useNavigate,
  Link,
  useLocation,
  useSearchParams
} from "react-router-dom";
import moment from "moment-timezone";
import { find, isEmpty } from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import { toast } from "react-toastify";
import {
  isSystemAdmin,
  isKeyAdmin,
  isAuditor,
  isKMSAuditor,
  serverError
} from "Utils/XAUtils";
import { isUndefined, map } from "lodash";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import { Loader } from "../../../components/CommonComponents";
import { BlockUi } from "../../../components/CommonComponents";

function Roles() {
  const navigate = useNavigate();
  const { state } = useLocation();
  const [roleListingData, setRoleData] = useState([]);
  const [loader, setLoader] = useState(true);
  const [pageLoader, setPageLoader] = useState(true);

  const [totalCount, setTotalCount] = useState(0);
  const fetchIdRef = useRef(0);
  const selectedRows = useRef([]);
  const [showModal, setConfirmModal] = useState(false);
  const [updateTable, setUpdateTable] = useState(moment.now());
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
    pageSize: 25
  });
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
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

  const fetchRoleInfo = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      setLoader(true);
      let roleData = [],
        roleResp = [];
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
        try {
          roleResp = await fetchApi({
            url: "roles/lookup/roles",
            params: params
          });
          roleData = roleResp.data.roles;
          totalCount = roleResp.data.totalCount;
          totalPageCount = Math.ceil(totalCount / pageSize);
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching User list! ${error}`);
        }
        if (state) {
          state["showLastPage"] = false;
        }
        setTblPageData({
          totalPage: totalPageCount,
          pageRecords: roleResp && roleResp.data && roleResp.data.totalCount,
          pageSize: 25
        });
        setRoleData(roleData);
        setTotalCount(totalCount);
        setPageCount(totalPageCount);
        setCurrentPageIndex(page);
        setCurrentPageSize(pageSize);
        setResetPage({ page: gotoPage });
        setLoader(false);
        if (localStorage.getItem("newDataAdded") == "true") {
          scrollToNewData(roleData, roleResp.data.resultSize);
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
      toast.info("Please select atleast one role!!");
    }
  };

  const toggleConfirmModal = () => {
    setConfirmModal((state) => !state);
  };

  const handleConfirmClick = () => {
    handleDeleteClick();
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
            url: `roles/roles/${original.id}`,
            method: "DELETE"
          });
          setBlockUI(false);
        } catch (error) {
          setBlockUI(false);
          if (error?.response?.data?.msgDesc) {
            errorMsg += error.response.data.msgDesc + "\n";
          } else {
            errorMsg +=
              `Error occurred during deleting Role: ${original.name}` + "\n";
          }
          console.log(errorMsg);
        }
      }
      if (errorMsg) {
        toast.error(errorMsg);
      } else {
        toast.success("Role deleted successfully!");
        if (
          (roleListingData.length == 1 ||
            roleListingData.length == selectedRows.current.length) &&
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
        },
      },
      {
        Header: "Users",
        accessor: "users",
        accessor: (raw) => {
          let usersList = _.map(raw.users, "name");
          return !isEmpty(usersList) ? (
            <MoreLess data={usersList} key={raw.id} />
          ) : (
            <div className="text-center">--</div>
          );
        }
      },
      {
        Header: "Groups",
        accessor: "groups",
        accessor: (raw) => {
          let groupsList = _.map(raw.groups, "name");
          return !isEmpty(groupsList) ? (
            <MoreLess data={groupsList} key={raw.id} />
          ) : (
            <div className="text-center">--</div>
          );
        }
      },
      {
        Header: "Roles",
        accessor: "roles",
        accessor: (raw) => {
          let rolesList = _.map(raw.roles, "name");

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

  const addRole = () => {
    navigate("/roles/create", { state: { tblpageData: tblpageData } });
  };

  const searchFilterOption = [
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
      <h4 className="wrap-header font-weight-bold">Role List</h4>
      {pageLoader ? (
        <Loader />
      ) : (
        <React.Fragment>
          <BlockUi isUiBlock={blockUI} />
          <Row className="mb-4">
            <Col md={8} className="usr-grp-role-search-width">
              <StructuredFilter
                key="role-listing-search-filter"
                placeholder="Search for your roles..."
                options={searchFilterOption}
                onTokenAdd={updateSearchFilter}
                onTokenRemove={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
              />
            </Col>
            {isSystemAdmin() && (
              <Col md={4} className="text-right">
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
                  className="ml-2"
                  variant="danger"
                  size="sm"
                  title="Delete"
                  onClick={handleDeleteBtnClick}
                  data-id="deleteUserGroup"
                  data-cy="deleteUserGroup"
                >
                  <i className="fa-fw fa fa-trash"></i>
                </Button>
              </Col>
            )}
          </Row>

          <XATableLayout
            data={roleListingData}
            columns={columns}
            fetchData={fetchRoleInfo}
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
          />

          <Modal show={showModal} onHide={toggleConfirmModal}>
            <Modal.Body>
              Are you sure you want to delete&nbsp;
              {selectedRows.current.length === 1 ? (
                <span>
                  <b>"{selectedRows.current[0].original.name}"</b> role ?
                </span>
              ) : (
                <span>
                  <b>"{selectedRows.current.length}"</b> roles ?
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
        </React.Fragment>
      )}
    </div>
  );
}

export default Roles;
