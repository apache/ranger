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

import React, { useState, useEffect } from "react";
import { useParams, useNavigate, Link, useLocation } from "react-router-dom";
import { fetchApi } from "../../../utils/fetchAPI";
import { Loader } from "../../../components/CommonComponents";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import {
  Button,
  Col,
  Modal,
  Accordion,
  Card,
  Tab,
  Tabs,
  DropdownButton,
  Dropdown
} from "react-bootstrap";
import dateFormat from "dateformat";
import { toast } from "react-toastify";
import { BlockUi } from "../../../components/CommonComponents";
import PrinciplePermissionComp from "../Dataset/PrinciplePermissionComp";
import { Form } from "react-final-form";
import arrayMutators from "final-form-arrays";
import ReactPaginate from "react-paginate";
import AddSharedResourceComp from "./AddSharedResourceComp";
import CustomBreadcrumb from "../../CustomBreadcrumb";
import { isSystemAdmin } from "../../../utils/XAUtils";

const DatashareDetailLayout = () => {
  let { datashareId } = useParams();
  const { state } = useLocation();
  const userAclPerm = state?.userAclPerm;
  const datashareName = state?.datashareName;
  const [activeKey, setActiveKey] = useState("overview");
  const [datashareInfo, setDatashareInfo] = useState({});
  const [datashareDescription, setDatashareDescription] = useState();
  const [datashareTerms, setDatashareTerms] = useState();
  const [loader, setLoader] = useState(true);
  const [resourceContentLoader, setResourceContentLoader] = useState(true);
  const [requestContentLoader, setRequestContentLoader] = useState(true);
  const [sharedResources, setSharedResources] = useState([]);
  const [confirmDeleteModal, setConfirmDeleteModal] = useState({
    sharedResourceDetails: {}
  });
  const [blockUI, setBlockUI] = useState(false);
  const [dataShareRequestsList, setDataShareRequestsList] = useState([]);
  const [dataShareRequestAccordion, setDatashareRequestAccordion] =
    useState(false);
  const [userList, setUserList] = useState([]);
  const [groupList, setGroupList] = useState([]);
  const [roleList, setRoleList] = useState([]);
  const [filteredUserList, setFilteredUserList] = useState([]);
  const [filteredGroupList, setFilteredGroupList] = useState([]);
  const [filteredRoleList, setFilteredRoleList] = useState([]);
  const navigate = useNavigate();
  const [saveCancelButtons, showSaveCancelButton] = useState(false);
  const [conditionModalData, setConditionModalData] = useState();
  const [showConditionModal, setShowConditionModal] = useState(false);
  const [showAddResourceModal, setShowAddResourceModal] = useState(false);
  const [resourceAccordionState, setResourceAccordionState] = useState({});
  const [requestAccordionState, setRequestAccordionState] = useState({});
  const itemsPerPage = 5;
  const [currentPage, setCurrentPage] = useState(0);
  const [requestCurrentPage, setRequestCurrentPage] = useState(0);
  const [sharedResourcePageCount, setSharedResourcePageCount] = useState();
  const [requestPageCount, setRequestPageCount] = useState();
  const [loadSharedResource, setLoadSharedResource] = useState();
  const [showConfirmModal, setShowConfirmModal] = useState(false);
  const [deleteDatashareReqInfo, setDeleteDatashareReqInfo] = useState({});
  const [
    showDatashareRequestDeleteConfirmModal,
    setShowDatashareRequestDeleteConfirmModal
  ] = useState(false);
  const [showDeleteDatashareModal, setShowDeleteDatashareModal] =
    useState(false);
  const [completeSharedResourceList, setCompleteSharedResourceList] = useState(
    []
  );
  const [completeDatashareRequestsList, setCompleteDatashareRequestsList] =
    useState([]);

  const toggleConfirmModalForDatashareDelete = () => {
    setShowDeleteDatashareModal(true);
  };

  const toggleConfirmModalClose = () => {
    setShowConfirmModal(false);
  };

  useEffect(() => {
    fetchDatashareInfo(datashareId);
    fetchSharedResourceForDatashare(datashareName, 0, true);
    fetchDatashareRequestList(undefined, 0, true);
  }, []);

  const handleTabSelect = (key) => {
    if (saveCancelButtons == true) {
      setShowConfirmModal(true);
    } else {
      if (key == "resources") {
        fetchSharedResourceForDatashare(datashareInfo.name, 0, false);
      } else if (key == "sharedWith") {
        fetchDatashareRequestList(undefined, 0, false);
      }
      setActiveKey(key);
    }
  };

  const fetchDatashareInfo = async (datashareId) => {
    try {
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/datashare/${datashareId}`
      });
      setLoader(false);
      setDatashareInfo(resp.data);
      setDatashareDescription(resp.data.description);
      setDatashareTerms(resp.data.termsOfUse);
      if (resp.data.acl != undefined) setPrincipleAccordianData(resp.data.acl);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      console.error(
        `Error occurred while fetching datashare details ! ${error}`
      );
    }
  };

  const showConfitionModal = (data) => {
    setConditionModalData(data);
    setShowConditionModal(true);
  };

  const toggleConditionModalClose = () => {
    setShowConditionModal(false);
  };

  const setPrincipleAccordianData = (principle) => {
    let userPrinciples = principle.users;
    let groupPrinciples = principle.groups;
    let rolePrinciples = principle.roles;

    let tempUserList = [];
    let tempGroupList = [];
    let tempRoleList = [];
    let userList = [];
    let groupList = [];
    let roleList = [];
    if (userPrinciples != undefined) {
      Object.entries(userPrinciples).map(([key, value]) => {
        tempUserList.push({ name: key, type: "USER", perm: value });
      });
    }
    if (groupPrinciples != undefined) {
      Object.entries(groupPrinciples).map(([key, value]) => {
        tempGroupList.push({ name: key, type: "GROUP", perm: value });
      });
    }
    if (rolePrinciples != undefined) {
      Object.entries(rolePrinciples).map(([key, value]) => {
        tempRoleList.push({ name: key, type: "ROLE", perm: value });
      });
    }
    setUserList([...userList, ...tempUserList]);
    setFilteredUserList([...filteredUserList, ...tempUserList]);
    setGroupList([...groupList, ...tempGroupList]);
    setFilteredGroupList([...filteredGroupList, ...tempGroupList]);
    setRoleList([...roleList, ...tempRoleList]);
    setFilteredRoleList([...filteredRoleList, ...tempRoleList]);
  };

  const fetchSharedResourceForDatashare = async (
    datashareName,
    currentPage,
    getCompleteList
  ) => {
    try {
      let params = {};
      let itemPerPageCount = getCompleteList ? 999999999 : itemsPerPage;
      params["pageSize"] = itemPerPageCount;
      params["page"] = currentPage;
      params["startIndex"] = currentPage * itemPerPageCount;
      params["dataShareId"] = datashareId;
      setResourceContentLoader(true);
      const resp = await fetchApi({
        url: `gds/resource`,
        params: params
      });
      setResourceContentLoader(false);
      let accordianState = {};
      resp.data.list.map(
        (item) =>
          (accordianState = { ...accordianState, ...{ [item.id]: false } })
      );
      setResourceAccordionState(accordianState);
      setSharedResourcePageCount(
        Math.ceil(resp.data.totalCount / itemPerPageCount)
      );
      if (!getCompleteList) {
        setSharedResources(resp.data.list);
      } else {
        setCompleteSharedResourceList(resp.data.list);
      }
    } catch (error) {
      setResourceContentLoader(false);
      console.error(
        `Error occurred while fetching shared resource details ! ${error}`
      );
    }
  };

  const handleSharedResourcePageClick = ({ selected }) => {
    setCurrentPage(selected);
    fetchSharedResourceForDatashare(datashareInfo.name, selected, false);
  };

  const handleRequestPageClick = ({ selected }) => {
    setRequestCurrentPage(selected);
    fetchDatashareRequestList(undefined, selected, false);
  };

  const fetchDatashareRequestList = async (
    datasetName,
    currentPage,
    getCompleteList
  ) => {
    try {
      let params = {};
      let itemPerPageCount = getCompleteList ? 999999999 : itemsPerPage;
      params["pageSize"] = itemPerPageCount;
      params["page"] = currentPage;
      params["startIndex"] = currentPage * itemPerPageCount;
      params["dataShareId"] = datashareId;
      setRequestContentLoader(true);
      const resp = await fetchApi({
        url: `gds/datashare/dataset`,
        params: params
      });
      setRequestContentLoader(false);
      let accordianState = {};
      setRequestAccordionState(accordianState);
      setRequestPageCount(Math.ceil(resp.data.totalCount / itemPerPageCount));
      if (!getCompleteList) {
        setDataShareRequestsList(resp.data.list);
      } else {
        setCompleteDatashareRequestsList(resp.data.list);
      }
    } catch (error) {
      setRequestContentLoader(false);
      console.error(
        `Error occurred while fetching Datashare requests details ! ${error}`
      );
    }
  };

  const datashareDescriptionChange = (event) => {
    setDatashareDescription(event.target.value);
    showSaveCancelButton(true);
  };

  const datashareTermsAndConditionsChange = (event) => {
    setDatashareTerms(event.target.value);
    showSaveCancelButton(true);
  };

  const toggleConfirmModalForDelete = (id, name) => {
    setConfirmDeleteModal({
      sharedResourceDetails: { shareId: id, shareName: name },
      showPopup: true
    });
  };

  const toggleAddResourceModalClose = () => {
    setShowAddResourceModal(false);
  };

  const toggleClose = () => {
    setConfirmDeleteModal({
      sharedResourceDetails: {},
      showPopup: false
    });
    setShowDeleteDatashareModal(false);
  };

  const handleSharedResourceDeleteClick = async (shareId) => {
    toggleClose();
    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/resource/${shareId}`,
        method: "DELETE"
      });
      setBlockUI(false);
      toast.success(" Success! Shared resource deleted successfully");
      fetchSharedResourceForDatashare(datashareInfo.name, 0, false);
    } catch (error) {
      setBlockUI(false);
      let errorMsg = "Failed to delete Shared resource  : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error(
        "Error occurred during deleting Shared resource  : " + error
      );
    }
  };

  const handleDataChange = (userList, groupList, roleList) => {
    setUserList(userList);
    setGroupList(groupList);
    setRoleList(roleList);
    showSaveCancelButton(true);
  };

  const redirectToDatasetDetailView = (datasetId) => {
    navigate(`/gds/dataset/${datasetId}/detail`);
  };

  const handleSubmit = () => {};

  const back = () => {
    navigate("/gds/mydatasharelisting");
  };

  const onSharedResourceAccordionChange = (id) => {
    setResourceAccordionState({
      ...resourceAccordionState,
      ...{ [id]: !resourceAccordionState[id] }
    });
  };

  const onRequestAccordionChange = (id) => {
    setRequestAccordionState({
      ...requestAccordionState,
      ...{ [id]: !requestAccordionState[id] }
    });
  };

  const handleSharedResourceChange = () => {
    fetchSharedResourceForDatashare(datashareInfo.name, currentPage, false);
  };

  const updateDatashareDetails = async () => {
    datashareInfo.description = datashareDescription;
    datashareInfo.termsOfUse = datashareTerms;

    datashareInfo.acl = { users: {}, groups: {}, roles: {} };

    userList.forEach((user) => {
      datashareInfo.acl.users[user.name] = user.perm;
    });

    groupList.forEach((group) => {
      datashareInfo.acl.groups[group.name] = group.perm;
    });

    roleList.forEach((role) => {
      datashareInfo.acl.roles[role.name] = role.perm;
    });

    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/datashare/${datashareId}`,
        method: "put",
        data: datashareInfo
      });
      setBlockUI(false);
      toast.success("Datashare updated successfully!!");
      showSaveCancelButton(false);
    } catch (error) {
      setBlockUI(false);
      serverError(error);
      console.error(`Error occurred while updating datashare  ${error}`);
    }
    toggleConfirmModalClose();
  };

  const removeChanges = () => {
    fetchDatashareInfo(datashareId);
    showSaveCancelButton(false);
    toggleConfirmModalClose();
  };

  const toggleRequestDeleteModal = (id, datashareId, name, status) => {
    let deleteMsg = "";
    if (status == "ACTIVE") {
      deleteMsg = `Do you want to remove Dataset ${datashareId} from ${datashareInfo.name}`;
    } else {
      deleteMsg = `Do you want to delete request of Dataset ${datashareId}`;
    }
    let data = { id: id, name: name, status: status, msg: deleteMsg };
    setDeleteDatashareReqInfo(data);
    setShowDatashareRequestDeleteConfirmModal(true);
  };

  const toggleDatashareRequestDelete = () => {
    setShowDatashareRequestDeleteConfirmModal(false);
  };

  const deleteDatashareRequest = async () => {
    try {
      setLoader(true);
      await fetchApi({
        url: `gds/datashare/dataset/${deleteDatashareReqInfo.id}`,
        method: "DELETE"
      });
      let successMsg = "";
      if (deleteDatashareReqInfo.status == "ACTIVE") {
        successMsg = "Success! Datashare removed from dataset successfully";
      } else {
        successMsg = "Success! Datashare request deleted successfully";
      }
      setShowDatashareRequestDeleteConfirmModal(false);
      toast.success(successMsg);
      fetchDatashareRequestList(undefined, requestCurrentPage, false);
      //fetchSharedResourceForDatashare(datashareInfo.name);
      setLoader(false);
    } catch (error) {
      let errorMsg = "";
      if (deleteDatashareReqInfo.status == "ACTIVE") {
        errorMsg = "Failed to remove datashare from dataset ";
      } else {
        errorMsg = "Failed to delete datashare request ";
      }
      setLoader(false);
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error(
        "Error occurred during deleting Datashare request  : " + error
      );
    }
  };

  const handleDatashareDeleteClick = async () => {
    toggleClose();
    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/datashare/${datashareId}`,
        method: "DELETE"
      });
      setBlockUI(false);
      toast.success(" Success! Datashare deleted successfully");
      navigate("/gds/mydatasharelisting");
    } catch (error) {
      setBlockUI(false);
      let errorMsg = "Failed to delete datashare : ";
      if (error?.response?.data?.msgDesc) {
        errorMsg += error.response.data.msgDesc;
      }
      toast.error(errorMsg);
      console.error("Error occurred during deleting datashare : " + error);
    }
  };

  const copyURL = () => {
    navigator.clipboard.writeText(window.location.href).then(() => {
      toast.success("URL copied!!");
    });
  };

  const navigateToFullView = () => {
    navigate(`/gds/datashare/${datashareId}/fullview`, {
      userAclPerm: userAclPerm,
      datashareNamee: datashareName
    });
  };

  const downloadJsonFile = () => {
    let jsonData = datashareInfo;
    jsonData.resources = completeSharedResourceList;
    jsonData.datasets = completeDatashareRequestsList;
    const jsonContent = JSON.stringify(jsonData);
    const blob = new Blob([jsonContent], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = datashareInfo.name + ".json"; // Set the desired file name
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <>
      <Form
        onSubmit={handleSubmit}
        mutators={{
          ...arrayMutators
        }}
        render={({}) => (
          <React.Fragment>
            <div className="gds-header-wrapper gap-half">
              <Button
                variant="light"
                className="border-0 bg-transparent"
                onClick={back}
                size="sm"
                data-id="back"
                data-cy="back"
              >
                <i className="fa fa-angle-left fa-lg font-weight-bold" />
              </Button>
              <h3 className="gds-header bold">
                <span
                  title={datashareInfo.name}
                  className="text-truncate"
                  style={{ maxWidth: "700px", display: "inline-block" }}
                >
                  Datashare : {datashareInfo.name}
                </span>
              </h3>
              <CustomBreadcrumb />

              {(isSystemAdmin() || userAclPerm == "ADMIN") && (
                <span className="pipe"></span>
              )}
              {(isSystemAdmin() || userAclPerm == "ADMIN") && (
                <div>
                  {saveCancelButtons ? (
                    <div className="gds-header-btn-grp">
                      <Button
                        variant="primary"
                        size="sm"
                        onClick={() => removeChanges()}
                        data-id="cancel"
                        data-cy="cancel"
                      >
                        Cancel
                      </Button>
                      <Button
                        variant="primary"
                        onClick={updateDatashareDetails}
                        size="sm"
                        data-id="save"
                        data-cy="save"
                      >
                        Save
                      </Button>
                    </div>
                  ) : (
                    <AddSharedResourceComp
                      datashareId={datashareId}
                      onSharedResourceDataChange={handleSharedResourceChange}
                      onToggleAddResourceClose={toggleAddResourceModalClose}
                      isEdit={false}
                      loadSharedResource={loadSharedResource}
                    />
                  )}
                </div>
              )}

              <div>
                <DropdownButton
                  id="dropdown-item-button"
                  title={<i className="fa fa-ellipsis-v" fontSize="36px" />}
                  size="sm"
                  className="hide-arrow"
                >
                  <Dropdown.Item
                    as="button"
                    onClick={() => navigateToFullView()}
                    data-name="fullView"
                    data-id="fullView"
                    data-cy="fullView"
                  >
                    Full View
                  </Dropdown.Item>
                  <Dropdown.Item
                    as="button"
                    onClick={() => {
                      copyURL();
                    }}
                    data-name="copyDatashareLink"
                    data-id="copyDatashareLink"
                    data-cy="copyDatashareLink"
                  >
                    Copy Datashare Link
                  </Dropdown.Item>
                  <Dropdown.Item
                    as="button"
                    onClick={() => downloadJsonFile()}
                    data-name="downloadJson"
                    data-id="downloadJson"
                    data-cy="downloadJson"
                  >
                    Download Json
                  </Dropdown.Item>
                  <hr />
                  <Dropdown.Item
                    as="button"
                    onClick={() => {
                      toggleConfirmModalForDatashareDelete();
                    }}
                    data-name="deleteDatashare"
                    data-id="deleteDatashare"
                    data-cy="deleteDatashare"
                  >
                    Delete Datashare
                  </Dropdown.Item>
                </DropdownButton>
              </div>
            </div>
            {loader ? (
              <Loader />
            ) : (
              <React.Fragment>
                <div>
                  <Tabs
                    id="DatashareDetailLayout"
                    activeKey={activeKey}
                    onSelect={handleTabSelect}
                  >
                    <Tab eventKey="overview" title="OVERVIEW">
                      {activeKey == "overview" ? (
                        <div>
                          <div className="gds-tab-content gds-content-border px-3">
                            <div className="gds-inline-field-grp">
                              <div className="wrapper">
                                <div
                                  className="gds-left-inline-field pl-1 fnt-14"
                                  height="30px"
                                >
                                  <span className="gds-label-color">
                                    Date Updated
                                  </span>
                                </div>
                                <div className="fnt-14" line-height="30px">
                                  {dateFormat(
                                    datashareInfo.updateTime,
                                    "mm/dd/yyyy hh:MM:ss TT"
                                  )}
                                </div>
                              </div>

                              <div className="wrapper">
                                <div
                                  className="gds-left-inline-field pl-1 fnt-14"
                                  line-height="30px"
                                >
                                  <span className="gds-label-color">
                                    Date Created
                                  </span>
                                </div>
                                <div className="fnt-14" line-height="30px">
                                  {dateFormat(
                                    datashareInfo.createTime,
                                    "mm/dd/yyyy hh:MM:ss TT"
                                  )}
                                </div>
                              </div>
                            </div>
                            <div>
                              <div className="fnt-14 pl-1">
                                <span className="gds-label-color">
                                  Description
                                </span>
                              </div>
                            </div>
                            <div>
                              <div>
                                <textarea
                                  placeholder="Datashare Description"
                                  className="form-control gds-description pl-1"
                                  id="description"
                                  data-cy="description"
                                  onChange={datashareDescriptionChange}
                                  value={datashareDescription}
                                  rows={5}
                                />
                              </div>
                            </div>
                          </div>
                          {(isSystemAdmin() ||
                            userAclPerm == "ADMIN" ||
                            userAclPerm == "AUDIT") && (
                            <PrinciplePermissionComp
                              userList={userList}
                              groupList={groupList}
                              roleList={roleList}
                              onDataChange={handleDataChange}
                            />
                          )}
                        </div>
                      ) : (
                        <div></div>
                      )}
                    </Tab>
                    <Tab eventKey="resources" title="RESOURCES">
                      {activeKey == "resources" ? (
                        <div className="gds-tab-content">
                          <div>
                            <div className="usr-grp-role-search-width mb-4">
                              <StructuredFilter
                                key="user-listing-search-filter"
                                placeholder="Search resources..."
                              />
                            </div>
                          </div>
                          <div>
                            <Card className="border-0">
                              <div>
                                {resourceContentLoader ? (
                                  <Loader />
                                ) : (
                                  <div>
                                    {sharedResources.length > 0 ? (
                                      sharedResources.map((obj, index) => {
                                        return (
                                          // <ResourceAccordian item={obj} />
                                          <div>
                                            <Accordion
                                              className="mg-b-10"
                                              defaultActiveKey="0"
                                            >
                                              <div className="border-bottom">
                                                <Accordion.Toggle
                                                  as={Card.Header}
                                                  eventKey={obj.id}
                                                  onClick={() =>
                                                    onSharedResourceAccordionChange(
                                                      obj.id
                                                    )
                                                  }
                                                  className="border-bottom-0"
                                                  data-id="panel"
                                                  data-cy="panel"
                                                >
                                                  <div className="d-flex justify-content-between align-items-center">
                                                    <div className="d-flex align-items-center gap-1">
                                                      {resourceAccordionState[
                                                        obj.id
                                                      ] ? (
                                                        <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                                      ) : (
                                                        <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                                      )}
                                                      <div className="d-flex justify-content-between">
                                                        <h6 className="m-0">
                                                          {obj.name}
                                                        </h6>
                                                      </div>
                                                    </div>
                                                    {(isSystemAdmin() ||
                                                      userAclPerm ==
                                                        "ADMIN") && (
                                                      <div className="d-flex gap-half align-items-start">
                                                        <AddSharedResourceComp
                                                          datashareId={
                                                            datashareId
                                                          }
                                                          onSharedResourceDataChange={
                                                            handleSharedResourceChange
                                                          }
                                                          onToggleAddResourceClose={
                                                            toggleAddResourceModalClose
                                                          }
                                                          isEdit={true}
                                                          sharedResourceId={
                                                            obj.id
                                                          }
                                                          loadSharedResource={
                                                            loadSharedResource
                                                          }
                                                        />
                                                        <Button
                                                          variant="danger"
                                                          size="sm"
                                                          title="Delete"
                                                          onClick={() =>
                                                            toggleConfirmModalForDelete(
                                                              obj.id,
                                                              obj.name
                                                            )
                                                          }
                                                          data-name="deleteDatashareRequest"
                                                          data-id={obj.id}
                                                          data-cy={obj.id}
                                                        >
                                                          <i className="fa-fw fa fa-trash fa-fw fa fa-large" />
                                                        </Button>
                                                      </div>
                                                    )}
                                                  </div>
                                                </Accordion.Toggle>
                                                <Accordion.Collapse
                                                  eventKey={obj.id}
                                                >
                                                  <Card.Body>
                                                    <div className="gds-added-res-listing">
                                                      {Object.entries(
                                                        obj.resource
                                                      ).map(([key, value]) => {
                                                        console.log(key);
                                                        console.log(value);
                                                        return (
                                                          <div className="mb-1 form-group row">
                                                            <Col sm={3}>
                                                              <label className="form-label fnt-14">
                                                                {key}
                                                              </label>
                                                            </Col>
                                                            <Col sm={9}>
                                                              {value.values.toString()}
                                                            </Col>
                                                          </div>
                                                        );
                                                      })}
                                                      <div className="mb-1 form-group row">
                                                        <Col sm={3}>
                                                          <label className="form-label gds-detail-label fnt-14">
                                                            Additional Info
                                                          </label>
                                                        </Col>
                                                        <Col sm={9}>
                                                          <Link
                                                            className="mb-3"
                                                            to=""
                                                            onClick={() =>
                                                              showConfitionModal(
                                                                obj
                                                              )
                                                            }
                                                          >
                                                            View Access Details
                                                          </Link>
                                                        </Col>
                                                      </div>
                                                    </div>
                                                  </Card.Body>
                                                </Accordion.Collapse>
                                              </div>
                                            </Accordion>
                                          </div>
                                        );
                                      })
                                    ) : (
                                      <div></div>
                                    )}
                                  </div>
                                )}

                                {sharedResourcePageCount > 1 && (
                                  <ReactPaginate
                                    previousLabel={"Previous"}
                                    nextLabel={"Next"}
                                    pageClassName="page-item"
                                    pageLinkClassName="page-link"
                                    previousClassName="page-item"
                                    previousLinkClassName="page-link"
                                    nextClassName="page-item"
                                    nextLinkClassName="page-link"
                                    breakLabel={"..."}
                                    pageCount={sharedResourcePageCount}
                                    onPageChange={handleSharedResourcePageClick}
                                    breakClassName="page-item"
                                    breakLinkClassName="page-link"
                                    containerClassName="pagination"
                                    activeClassName="active"
                                  />
                                )}
                              </div>
                            </Card>
                          </div>
                        </div>
                      ) : (
                        <div></div>
                      )}
                    </Tab>
                    <Tab eventKey="sharedWith" title="SHARED WITH">
                      {activeKey == "sharedWith" ? (
                        <div className="gds-tab-content">
                          <div>
                            <div className="usr-grp-role-search-width mb-4">
                              <StructuredFilter
                                key="user-listing-search-filter"
                                placeholder="Search dataset..."
                              />
                            </div>
                          </div>
                          <div>
                            <div className="usr-grp-role-search-width">
                              <Tabs
                                id="datashareRequestTab"
                                className="mg-b-10"
                              >
                                <Tab eventKey="All" title="All">
                                  <Card className="border-0">
                                    <div>
                                      {dataShareRequestsList != undefined &&
                                      dataShareRequestsList.length > 0 ? (
                                        dataShareRequestsList.map(
                                          (obj, index) => {
                                            return (
                                              <div>
                                                <Accordion
                                                  className="mg-b-10"
                                                  defaultActiveKey="0"
                                                >
                                                  <div className="border-bottom">
                                                    <Accordion.Toggle
                                                      as={Card.Header}
                                                      eventKey="1"
                                                      onClick={() =>
                                                        onRequestAccordionChange(
                                                          obj.id
                                                        )
                                                      }
                                                      className="border-bottom-0"
                                                      data-id="panel"
                                                      data-cy="panel"
                                                    >
                                                      <div className="d-flex justify-content-between align-items-center">
                                                        <div className="d-flex align-items-center gap-half">
                                                          {requestAccordionState[
                                                            obj.id
                                                          ] ? (
                                                            <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                                          ) : (
                                                            <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                                          )}
                                                          <h6 className="m-0">
                                                            {obj.name} Dataset{" "}
                                                            {obj.datasetId}
                                                          </h6>
                                                        </div>
                                                        <div className="d-flex align-items-center gap-half">
                                                          <span
                                                            className={
                                                              obj["status"] ===
                                                              "REQUESTED"
                                                                ? "badge badge-light gds-requested-status"
                                                                : obj[
                                                                    "status"
                                                                  ] ===
                                                                  "GRANTED"
                                                                ? "badge badge-light gds-granted-status"
                                                                : obj[
                                                                    "status"
                                                                  ] === "ACTIVE"
                                                                ? "badge badge-light gds-active-status"
                                                                : "badge badge-light gds-denied-status"
                                                            }
                                                          >
                                                            {obj["status"]}
                                                          </span>
                                                          <Button
                                                            variant="outline-dark"
                                                            size="sm"
                                                            title="View"
                                                            onClick={() =>
                                                              redirectToDatasetDetailView(
                                                                obj.datasetId
                                                              )
                                                            }
                                                            data-name="viewDatashare"
                                                            data-id={obj["id"]}
                                                          >
                                                            <i className="fa-fw fa fa-eye fa-fw fa fa-large" />
                                                          </Button>
                                                          {(isSystemAdmin() ||
                                                            userAclPerm ==
                                                              "ADMIN") && (
                                                            <Button
                                                              variant="danger"
                                                              size="sm"
                                                              title="Delete"
                                                              onClick={() =>
                                                                toggleRequestDeleteModal(
                                                                  obj.id,
                                                                  obj.datasetId,
                                                                  obj.name,
                                                                  obj.status
                                                                )
                                                              }
                                                              data-name="deleteDatashareRequest"
                                                              data-id={
                                                                obj["id"]
                                                              }
                                                              data-cy={
                                                                obj["id"]
                                                              }
                                                            >
                                                              <i className="fa-fw fa fa-trash fa-fw fa fa-large" />
                                                            </Button>
                                                          )}
                                                        </div>
                                                      </div>
                                                    </Accordion.Toggle>
                                                    <Accordion.Collapse eventKey="1">
                                                      <Card.Body>
                                                        <div className="d-flex justify-content-between">
                                                          {false && (
                                                            <div className="gds-inline-field-grp">
                                                              <div className="wrapper">
                                                                <div
                                                                  className="gds-left-inline-field"
                                                                  height="30px"
                                                                >
                                                                  Validity
                                                                  Period
                                                                </div>
                                                                <div line-height="30px">
                                                                  {
                                                                    obj[
                                                                      "service"
                                                                    ]
                                                                  }
                                                                </div>
                                                              </div>
                                                              {obj.validitySchedule !=
                                                              undefined ? (
                                                                <div className="gds-inline-field-grp">
                                                                  <div className="wrapper">
                                                                    <div className="gds-left-inline-field">
                                                                      <span className="gds-label-color">
                                                                        Start
                                                                        Date{" "}
                                                                      </span>
                                                                    </div>
                                                                    <span>
                                                                      {dateFormat(
                                                                        obj
                                                                          .validitySchedule
                                                                          .startTime,
                                                                        "mm/dd/yyyy hh:MM:ss TT"
                                                                      )}
                                                                    </span>
                                                                    <span className="gds-label-color pl-5">
                                                                      {
                                                                        obj
                                                                          .validitySchedule
                                                                          .timeZone
                                                                      }
                                                                    </span>
                                                                  </div>
                                                                  <div className="wrapper">
                                                                    <div className="gds-left-inline-field">
                                                                      <span className="gds-label-color">
                                                                        {" "}
                                                                        End Date{" "}
                                                                      </span>
                                                                    </div>
                                                                    <span>
                                                                      {dateFormat(
                                                                        obj
                                                                          .validitySchedule
                                                                          .endTime,
                                                                        "mm/dd/yyyy hh:MM:ss TT"
                                                                      )}
                                                                    </span>
                                                                  </div>
                                                                </div>
                                                              ) : (
                                                                <p>--</p>
                                                              )}
                                                            </div>
                                                          )}
                                                          <div className="gds-right-inline-field-grp">
                                                            <div className="wrapper">
                                                              <div>Added</div>
                                                              <div className="gds-right-inline-field">
                                                                {dateFormat(
                                                                  obj[
                                                                    "createTime"
                                                                  ],
                                                                  "mm/dd/yyyy hh:MM:ss TT"
                                                                )}
                                                              </div>
                                                            </div>
                                                            <div className="wrapper">
                                                              <div>Updated</div>
                                                              <div className="gds-right-inline-field">
                                                                {dateFormat(
                                                                  obj[
                                                                    "updateTime"
                                                                  ],
                                                                  "mm/dd/yyyy hh:MM:ss TT"
                                                                )}
                                                              </div>
                                                            </div>
                                                            <div className="w-100 text-right">
                                                              <div>
                                                                <Link
                                                                  to={`/gds/request/detail/${obj.id}`}
                                                                >
                                                                  View Request
                                                                </Link>
                                                              </div>
                                                            </div>
                                                          </div>
                                                        </div>
                                                      </Card.Body>
                                                    </Accordion.Collapse>
                                                  </div>
                                                </Accordion>
                                              </div>
                                            );
                                          }
                                        )
                                      ) : (
                                        <div></div>
                                      )}
                                      {requestPageCount > 1 && (
                                        <ReactPaginate
                                          previousLabel={"Previous"}
                                          nextLabel={"Next"}
                                          pageClassName="page-item"
                                          pageLinkClassName="page-link"
                                          previousClassName="page-item"
                                          previousLinkClassName="page-link"
                                          nextClassName="page-item"
                                          nextLinkClassName="page-link"
                                          breakLabel={"..."}
                                          pageCount={requestPageCount}
                                          onPageChange={handleRequestPageClick}
                                          breakClassName="page-item"
                                          breakLinkClassName="page-link"
                                          containerClassName="pagination"
                                          activeClassName="active"
                                        />
                                      )}
                                    </div>
                                  </Card>
                                </Tab>
                                <Tab eventKey="Active" title="Active" />
                                <Tab eventKey="Requested" title="Requested" />
                                <Tab eventKey="Granted" title="Granted" />
                              </Tabs>
                            </div>
                          </div>
                        </div>
                      ) : (
                        <div></div>
                      )}
                    </Tab>

                    {(isSystemAdmin() ||
                      userAclPerm == "ADMIN" ||
                      userAclPerm == "AUDIT") && (
                      <Tab eventKey="history" title="HISTORY"></Tab>
                    )}

                    <Tab eventKey="termsOfUse" title="TERMS OF USE">
                      <div className="gds-tab-content gds-content-border">
                        <div>
                          <div className="usr-grp-role-search-width">
                            <p className="gds-content-header">
                              Terms & Conditions
                            </p>
                          </div>
                        </div>
                        <div>
                          <div>
                            <textarea
                              placeholder="Terms & Conditions"
                              className="form-control"
                              id="termsAndConditions"
                              data-cy="termsAndConditions"
                              onChange={datashareTermsAndConditionsChange}
                              value={datashareTerms}
                              rows={16}
                            />
                          </div>
                        </div>
                      </div>
                    </Tab>
                  </Tabs>
                </div>

                {/* <Modal
                  show={showAddResourceModal}
                  onHide={toggleAddResourceModalClose}
                  //className="mb-7"
                  size="xl"
                >
                  <Modal.Header closeButton>
                    <h5 className="mb-0">Add Resources</h5>
                  </Modal.Header>
                  <Modal.Body>
                    <AddSharedResourceComp
                      datashareId={datashareId}
                      onSharedResourceDataChange={handleSharedResourceChange}
                      onToggleAddResourceClose={toggleAddResourceModalClose}
                      loadSharedResource={loadSharedResource}
                    />
                  </Modal.Body>
                   <Modal.Footer>
                    <Button variant="secondary" size="sm" onClick={toggleClose}>
                      Cancel
                    </Button>
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={() =>
                        handleSharedResourceDeleteClick(
                          confirmDeleteModal.sharedResourceDetails.shareId
                        )
                      }
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal> */}

                <Modal show={confirmDeleteModal.showPopup} onHide={toggleClose}>
                  <Modal.Header closeButton>
                    <span className="text-word-break">
                      Are you sure you want to delete shared resource &nbsp;"
                      <b>
                        {confirmDeleteModal?.sharedResourceDetails?.shareName}
                      </b>
                      " ?
                    </span>
                  </Modal.Header>
                  <Modal.Footer>
                    <Button variant="secondary" size="sm" onClick={toggleClose}>
                      Cancel
                    </Button>
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={() =>
                        handleSharedResourceDeleteClick(
                          confirmDeleteModal.sharedResourceDetails.shareId
                        )
                      }
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal>

                <Modal
                  show={showConditionModal}
                  onHide={toggleConditionModalClose}
                >
                  <Modal.Header closeButton>
                    <h3 className="gds-header bold">Conditions</h3>
                  </Modal.Header>
                  <Modal.Body>
                    <div className="p-1">
                      <div className="gds-inline-field-grp">
                        <div className="wrapper">
                          <div className="gds-left-inline-field" height="30px">
                            Boolean Expression :
                          </div>
                          <div line-height="30px">
                            {conditionModalData?.conditionExpr != undefined
                              ? conditionModalData.conditionExpr
                              : ""}
                          </div>
                        </div>
                        <div className="wrapper">
                          <div className="gds-left-inline-field" height="30px">
                            Access Type :
                          </div>
                          <div line-height="30px">
                            {conditionModalData?.accessTypes != undefined
                              ? conditionModalData.accessTypes.toString()
                              : ""}
                          </div>
                        </div>
                        {false && (
                          <div className="wrapper">
                            <div
                              className="gds-left-inline-field"
                              height="30px"
                            >
                              Row Filter :
                            </div>
                            <div line-height="30px">
                              {conditionModalData?.rowFilter != undefined
                                ? conditionModalData.rowFilter.filterExpr
                                : ""}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </Modal.Body>
                  <Modal.Footer>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={toggleConditionModalClose}
                    >
                      Close
                    </Button>
                  </Modal.Footer>
                </Modal>

                <Modal show={showConfirmModal} onHide={toggleConfirmModalClose}>
                  <Modal.Header closeButton>
                    <h3 className="gds-header bold">
                      Would you like to save the changes?
                    </h3>
                  </Modal.Header>
                  <Modal.Footer>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => removeChanges()}
                    >
                      No
                    </Button>
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={updateDatashareDetails}
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal>

                <Modal
                  show={showDatashareRequestDeleteConfirmModal}
                  onHide={toggleDatashareRequestDelete}
                >
                  <Modal.Header closeButton>
                    <h3 className="gds-header bold">
                      {deleteDatashareReqInfo.msg}
                    </h3>
                  </Modal.Header>
                  <Modal.Footer>
                    <Button
                      variant="secondary"
                      size="sm"
                      onClick={() => removeChanges()}
                    >
                      No
                    </Button>
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={() => deleteDatashareRequest()}
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal>

                <Modal show={showDeleteDatashareModal} onHide={toggleClose}>
                  <Modal.Header closeButton>
                    <span className="text-word-break">
                      Are you sure you want to delete datashare&nbsp;"
                      <b>{datashareInfo.name}</b>" ?
                    </span>
                  </Modal.Header>
                  <Modal.Footer>
                    <Button variant="secondary" size="sm" onClick={toggleClose}>
                      No
                    </Button>
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={() => handleDatashareDeleteClick()}
                    >
                      Yes
                    </Button>
                  </Modal.Footer>
                </Modal>
              </React.Fragment>
            )}
            <BlockUi isUiBlock={blockUI} />
          </React.Fragment>
        )}
      />
    </>
  );
};

export default DatashareDetailLayout;
