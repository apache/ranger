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
import XATableLayout from "Components/XATableLayout";
import { GroupSource } from "../../../utils/XAEnums";
import { GroupTypes } from "../../../utils/XAEnums";
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
import { find, isUndefined, isEmpty } from "lodash";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import {
  BlockUi,
  Loader,
  scrollToNewData
} from "../../../components/CommonComponents";

function Groups() {
  const navigate = useNavigate();
  const { state } = useLocation();
  const [groupListingData, setGroupData] = useState([]);
  const [loader, setLoader] = useState(true);
  const [totalCount, setTotalCount] = useState(0);
  const fetchIdRef = useRef(0);
  const selectedRows = useRef([]);
  const toastId = useRef(null);
  const [showModal, setConfirmModal] = useState(false);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [showGroupSyncDetails, setGroupSyncdetails] = useState({
    syncDteails: {},
    showSyncDetails: false
  });
  const [showAssociateUserModal, setAssociateUserModal] = useState(false);
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
  const [pageLoader, setPageLoader] = useState(true);
  const [blockUI, setBlockUI] = useState(false);

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
      scrollToNewData(groupListingData);
    }
  }, [totalCount]);
  const fetchGroupInfo = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      setLoader(true);
      let groupData = [],
        groupResp = [];
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
          groupResp = await fetchApi({
            url: "xusers/groups",
            params: params
          });
          groupData = groupResp.data.vXGroups;
          totalCount = groupResp.data.totalCount;
          totalPageCount = Math.ceil(totalCount / pageSize);
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching User list! ${error}`);
        }
        if (state) {
          state["showLastPage"] = false;
        }
        setGroupData(groupData);
        setTblPageData({
          totalPage: totalPageCount,
          pageRecords: groupResp && groupResp.data && groupResp.data.totalCount,
          pageSize: pageSize
        });
        setTotalCount(totalCount);
        setPageCount(totalPageCount);
        setCurrentPageIndex(page);
        setCurrentPageSize(pageSize);
        setResetPage({ page: gotoPage });
        setLoader(false);
      }
    },
    [updateTable, searchFilterParams]
  );

  const handleDeleteBtnClick = () => {
    if (selectedRows.current.length > 0) {
      toggleConfirmModal();
    } else {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please select atleast one group!!");
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
            url: `xusers/secure/groups/id/${original.id}`,
            method: "DELETE",
            params: {
              forceDelete: true
            }
          });
          setBlockUI(false);
        } catch (error) {
          setBlockUI(false);
          if (error?.response?.data?.msgDesc) {
            errorMsg += error.response.data.msgDesc + "\n";
          } else {
            errorMsg +=
              `Error occurred during deleting Groups: ${original.name}` + "\n";
          }
          console.error(errorMsg);
        }
      }
      if (errorMsg) {
        toast.dismiss(toastId.current);
        toastId.current = toast.error(errorMsg);
      } else {
        toast.dismiss(toastId.current);
        toastId.current = toast.success("Group deleted successfully!");
        if (
          (groupListingData.length == 1 ||
            groupListingData.length == selectedRows.current.length) &&
          currentpageIndex > 0
        ) {
          if (typeof resetPage?.page === "function") {
            resetPage.page(0);
          }
        } else {
          setUpdateTable(moment.now());
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
                selectedRows.current.length === 1 ? "Group is" : "Groups are"
              } already visible`
            : `Selected ${
                selectedRows.current.length === 1 ? "Group is " : "Groups are"
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
            selectedRows.current.length === 1 ? "Group" : "Groups"
          } visibility!!`
        );
        setUpdateTable(moment.now());
      } catch (error) {
        serverError(error);
        console.error(`Error occurred during set Group visibility! ${error}`);
      }
    } else {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("Please select atleast one group!!");
    }
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
                  showGroupAssociateUser(rawValue.row.original);
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
        Cell: (rawValue, model) => {
          if (rawValue.value) {
            return (
              <div className="text-center">
                <button
                  className="btn btn-outline-dark btn-sm"
                  data-id="syncDetailes"
                  data-cy="syncDetailes"
                  data-for="group"
                  title="Sync Details"
                  id={model.id}
                  onClick={() => {
                    toggleGroupSyncModal(rawValue.value);
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
  const addGroup = () => {
    navigate("/group/create", { state: { tblpageData: tblpageData } });
  };
  const toggleGroupSyncModal = (raw) => {
    setGroupSyncdetails({
      syncDteails: JSON.parse(raw),
      showSyncDetails: true
    });
  };
  const toggleGroupSyncModalClose = () => {
    setGroupSyncdetails({
      syncDteails: {},
      showSyncDetails: false
    });
  };
  const showGroupAssociateUser = (raw) => {
    setAssociateUserModal({
      groupID: raw.id,
      groupName: raw.name,
      showAssociateUserDetails: true
    });
  };
  const toggleAssociateUserClose = () => {
    setAssociateUserModal({
      groupID: "",
      showAssociateUserDetails: false
    });
  };

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
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam, { replace: true });

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
    }
  };

  return (
    <div className="wrap">
      {pageLoader ? (
        <Loader />
      ) : (
        <React.Fragment>
          <BlockUi isUiBlock={blockUI} />
          <Row className="mb-4">
            <Col md={8} className="usr-grp-role-search-width">
              <StructuredFilter
                key="user-listing-search-filter"
                placeholder="Search for your groups..."
                options={searchFilterOptions}
                onChange={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
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
            data={groupListingData}
            columns={columns}
            fetchData={fetchGroupInfo}
            totalCount={totalCount}
            pageCount={pageCount}
            currentpageIndex={currentpageIndex}
            currentpageSize={currentpageSize}
            loading={loader}
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

          <Modal show={showModal} onHide={toggleConfirmModal}>
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
            show={showGroupSyncDetails && showGroupSyncDetails.showSyncDetails}
            onHide={toggleGroupSyncModalClose}
            size="xl"
          >
            <Modal.Header>
              <Modal.Title>Sync Source Details</Modal.Title>
            </Modal.Header>
            <Modal.Body>
              <SyncSourceDetails
                syncDetails={showGroupSyncDetails.syncDteails}
              ></SyncSourceDetails>
            </Modal.Body>
            <Modal.Footer>
              <Button
                variant="primary"
                size="sm"
                onClick={toggleGroupSyncModalClose}
              >
                OK
              </Button>
            </Modal.Footer>
          </Modal>
          <Modal
            show={
              showAssociateUserModal &&
              showAssociateUserModal.showAssociateUserDetails
            }
            onHide={toggleAssociateUserClose}
            size="lg"
          >
            <Modal.Header closeButton>
              <Modal.Title>
                <div className="d-flex">
                  User&apos;s List :
                  <div
                    className="ps-2 more-less-width text-truncate"
                    title={showAssociateUserModal?.groupName}
                  >
                    {showAssociateUserModal.groupName}
                  </div>
                </div>
              </Modal.Title>
            </Modal.Header>
            <Modal.Body>
              <GroupAssociateUserDetails
                groupID={showAssociateUserModal.groupID}
              ></GroupAssociateUserDetails>
            </Modal.Body>
            <Modal.Footer>
              <Button
                variant="primary"
                size="sm"
                onClick={toggleAssociateUserClose}
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

export default Groups;
