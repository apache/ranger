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
import { Button, Accordion, Card, Col, Modal } from "react-bootstrap";
import CustomBreadcrumb from "../../CustomBreadcrumb";
import { Loader } from "../../../components/CommonComponents";
import { Form } from "react-final-form";
import { fetchApi } from "../../../utils/fetchAPI";
import dateFormat from "dateformat";
import PrinciplePermissionComp from "../Dataset/PrinciplePermissionComp";
import ReactPaginate from "react-paginate";
import { isSystemAdmin } from "../../../utils/XAUtils";
import { statusClassMap } from "../../../utils/XAEnums";

const DatashareDetailFullView = () => {
  let { datashareId } = useParams();
  const { state } = useLocation();
  const userAclPerm = state?.userAclPerm;
  const datashareName = state?.datashareName;
  const [loader, setLoader] = useState(true);
  const [datashareInfo, setDatashareInfo] = useState({});
  const [userList, setUserList] = useState([]);
  const [groupList, setGroupList] = useState([]);
  const [roleList, setRoleList] = useState([]);
  const [filteredUserList, setFilteredUserList] = useState([]);
  const [filteredGroupList, setFilteredGroupList] = useState([]);
  const [filteredRoleList, setFilteredRoleList] = useState([]);
  const navigate = useNavigate();
  const [resourceAccordionState, setResourceAccordionState] = useState({});
  const [sharedResourcePageCount, setSharedResourcePageCount] = useState();
  const [sharedResources, setSharedResources] = useState([]);
  const itemsPerPage = 5;
  const [dataShareRequestsList, setDataShareRequestsList] = useState([]);
  const [requestAccordionState, setRequestAccordionState] = useState({});
  const [requestPageCount, setRequestPageCount] = useState();
  const [sharedResourceTotalCount, setSharedResourceTotalCount] = useState(0);
  const [datashareRequestTotalCount, setDatashareRequestTotalCount] =
    useState(0);
  const [conditionModalData, setConditionModalData] = useState();
  const [showConditionModal, setShowConditionModal] = useState(false);

  useEffect(() => {
    fetchDatashareInfo(datashareId);
    fetchDatashareRequestList(undefined, 0, false);
  }, []);

  const fetchDatashareInfo = async (datashareId) => {
    try {
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/datashare/${datashareId}`
      });
      setLoader(false);
      setDatashareInfo(resp.data);
      if (resp.data.acl != undefined) setPrincipleAccordianData(resp.data.acl);
      fetchSharedResourceForDatashare(resp.data.name, 0, false);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      console.error(
        `Error occurred while fetching datashare details ! ${error}`
      );
    }
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
      const resp = await fetchApi({
        url: `gds/resource`,
        params: params
      });
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
      }
      setSharedResourceTotalCount(resp.data.totalCount);
      return resp.data.list;
    } catch (error) {
      console.error(
        `Error occurred while fetching shared resource details ! ${error}`
      );
    }
  };

  const fetchDatashareRequestList = async (
    datasetName,
    currentPage,
    getCompleteList
  ) => {
    try {
      let params = {};
      let itemsPerPageCount = getCompleteList ? 999999999 : itemsPerPage;
      params["pageSize"] = itemsPerPageCount;
      params["page"] = currentPage;
      params["startIndex"] = currentPage * itemsPerPageCount;
      params["dataShareId"] = datashareId;
      const resp = await fetchApi({
        url: `gds/datashare/dataset`,
        params: params
      });
      let accordianState = {};
      resp.data.list.map(
        (item) =>
          (accordianState = { ...accordianState, ...{ [item.id]: false } })
      );
      setRequestAccordionState(accordianState);
      setRequestPageCount(Math.ceil(resp.data.totalCount / itemsPerPageCount));
      if (!getCompleteList) {
        setDataShareRequestsList(resp.data.list);
      }
      setDatashareRequestTotalCount(resp.data.totalCount);
      return resp.data.list;
    } catch (error) {
      console.error(
        `Error occurred while fetching Datashare requests details ! ${error}`
      );
    }
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

  const onSharedResourceAccordionChange = (id) => {
    setResourceAccordionState({
      ...resourceAccordionState,
      ...{ [id]: !resourceAccordionState[id] }
    });
  };

  const handleSharedResourcePageClick = ({ selected }) => {
    fetchSharedResourceForDatashare(datashareInfo.name, selected, false);
  };

  const handleRequestPageClick = ({ selected }) => {
    fetchDatashareRequestList(undefined, selected, false);
  };

  const redirectToDatasetDetailView = (datasetId) => {
    navigate(`/gds/dataset/${datasetId}/detail`);
  };

  const onRequestAccordionChange = (id) => {
    setRequestAccordionState({
      ...requestAccordionState,
      ...{ [id]: !requestAccordionState[id] }
    });
  };

  const downloadJsonFile = async () => {
    let jsonData = datashareInfo;
    jsonData.resources = await fetchSharedResourceForDatashare(
      datashareName,
      0,
      true
    );
    jsonData.datasets = await fetchDatashareRequestList(undefined, 0, true);
    const jsonContent = JSON.stringify(jsonData);
    const blob = new Blob([jsonContent], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = datashareInfo.name + ".json"; // Set the desired file name
    a.click();
    URL.revokeObjectURL(url);
  };

  const showConfitionModal = (data) => {
    setConditionModalData(data);
    setShowConditionModal(true);
  };

  const toggleConditionModalClose = () => {
    setShowConditionModal(false);
  };

  const handleSubmit = () => {};
  return (
    <>
      <Form
        onSubmit={handleSubmit}
        render={({}) => (
          <React.Fragment>
            {loader ? (
              <Loader />
            ) : (
              <>
                <div className="gds-header-wrapper gap-half">
                  <Button
                    variant="light"
                    className="border-0 bg-transparent"
                    onClick={() => window.history.back()}
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
                  <span className="pipe"></span>
                  <div className="gds-header-btn-grp">
                    <Button
                      variant="primary"
                      onClick={() => downloadJsonFile()}
                      size="sm"
                      data-id="save"
                      data-cy="save"
                    >
                      Download JSON
                    </Button>
                  </div>
                </div>
                <div>
                  <div className="gds-tab-content gds-content-border px-3">
                    <div className="gds-inline-field-grp">
                      <div className="wrapper">
                        <div
                          className="gds-left-inline-field pl-1 fnt-14"
                          height="30px"
                        >
                          <span className="gds-label-color">Date Updated</span>
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
                          <span className="gds-label-color">Date Created</span>
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
                        <span className="gds-label-color">Description</span>
                      </div>
                    </div>
                    <div>
                      <div>
                        <textarea
                          placeholder="Datashare Description"
                          className="form-control gds-description pl-1"
                          id="description"
                          data-cy="description"
                          value={datashareInfo.description}
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
                      isEditable={false}
                      type="datashare"
                    />
                  )}
                </div>
                <div className="gds-tab-content gds-content-border">
                  <div className="gds-section-title">
                    <p className="gds-card-heading">Shared Resources</p>
                  </div>
                  {sharedResources.length > 0 ? (
                    sharedResources.map((obj, index) => {
                      return (
                        // <ResourceAccordian item={obj} />
                        <div>
                          <Accordion className="mg-b-10" defaultActiveKey="0">
                            <Accordion.Item className="border-bottom">
                              <Accordion.Header
                                eventKey={obj.id}
                                onClick={() =>
                                  onSharedResourceAccordionChange(obj.id)
                                }
                                className="border-bottom-0"
                                data-id="panel"
                                data-cy="panel"
                              >
                                <div className="d-flex justify-content-between align-items-center">
                                  <div className="d-flex align-items-center gap-1">
                                    {resourceAccordionState[obj.id] ? (
                                      <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                    ) : (
                                      <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                    )}
                                    <div className="d-flex justify-content-between">
                                      <h6 className="m-0">{obj.name}</h6>
                                    </div>
                                  </div>
                                </div>
                              </Accordion.Header>
                              <Accordion.Body eventKey={obj.id}>
                                <Card.Body>
                                  <div className="gds-added-res-listing">
                                    {Object.entries(obj.resource).map(
                                      ([key, value]) => {
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
                                      }
                                    )}
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
                                            showConfitionModal(obj)
                                          }
                                        >
                                          View Access Details
                                        </Link>
                                      </Col>
                                    </div>
                                  </div>
                                </Card.Body>
                              </Accordion.Body>
                            </Accordion.Item>
                          </Accordion>
                        </div>
                      );
                    })
                  ) : (
                    <div>--</div>
                  )}
                  {sharedResourceTotalCount > itemsPerPage && (
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

                <div className="gds-tab-content gds-content-border">
                  <div className="gds-section-title">
                    <p className="gds-card-heading">Shared With</p>
                  </div>
                  <div>
                    {dataShareRequestsList != undefined &&
                    dataShareRequestsList.length > 0 ? (
                      dataShareRequestsList.map((obj, index) => {
                        let statusVal = obj["status"] || "DENIED";
                        return (
                          <div>
                            <Accordion className="mg-b-10" defaultActiveKey="0">
                              <Accordion.Item className="border-bottom">
                                <Accordion.Header
                                  eventKey="1"
                                  onClick={() =>
                                    onRequestAccordionChange(obj.id)
                                  }
                                  className="border-bottom-0"
                                  data-id="panel"
                                  data-cy="panel"
                                >
                                  <div className="d-flex justify-content-between align-items-center">
                                    <div className="d-flex align-items-center gap-half">
                                      {requestAccordionState[obj.id] ? (
                                        <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                      ) : (
                                        <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                      )}
                                      <h6 className="m-0">
                                        {obj.name} Dataset {obj.datasetId}
                                      </h6>
                                    </div>
                                    <div className="d-flex align-items-center gap-half">
                                      <span
                                        className={`${statusClassMap[statusVal]}`}
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
                                    </div>
                                  </div>
                                </Accordion.Header>
                                <Accordion.Body eventKey="1">
                                  <Card.Body>
                                    <div className="d-flex justify-content-between">
                                      {false && (
                                        <div className="gds-inline-field-grp">
                                          <div className="wrapper">
                                            <div
                                              className="gds-left-inline-field"
                                              height="30px"
                                            >
                                              Validity Period
                                            </div>
                                            <div line-height="30px">
                                              {obj["service"]}
                                            </div>
                                          </div>
                                          {obj.validitySchedule != undefined ? (
                                            <div className="gds-inline-field-grp">
                                              <div className="wrapper">
                                                <div className="gds-left-inline-field">
                                                  <span className="gds-label-color">
                                                    Start Date{" "}
                                                  </span>
                                                </div>
                                                <span>
                                                  {dateFormat(
                                                    obj.validitySchedule
                                                      .startTime,
                                                    "mm/dd/yyyy hh:MM:ss TT"
                                                  )}
                                                </span>
                                                <span className="gds-label-color pl-5">
                                                  {
                                                    obj.validitySchedule
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
                                                    obj.validitySchedule
                                                      .endTime,
                                                    "mm/dd/yyyy hh:MM:ss TT"
                                                  )}
                                                </span>
                                              </div>
                                            </div>
                                          ) : (
                                            <div>--</div>
                                          )}
                                        </div>
                                      )}
                                      <div className="gds-right-inline-field-grp">
                                        <div className="wrapper">
                                          <div>Added</div>
                                          <div className="gds-right-inline-field">
                                            {dateFormat(
                                              obj["createTime"],
                                              "mm/dd/yyyy hh:MM:ss TT"
                                            )}
                                          </div>
                                        </div>
                                        <div className="wrapper">
                                          <div>Updated</div>
                                          <div className="gds-right-inline-field">
                                            {dateFormat(
                                              obj["updateTime"],
                                              "mm/dd/yyyy hh:MM:ss TT"
                                            )}
                                          </div>
                                        </div>
                                        <div className="w-100 text-end">
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
                                </Accordion.Body>
                              </Accordion.Item>
                            </Accordion>
                          </div>
                        );
                      })
                    ) : (
                      <div></div>
                    )}
                    {datashareRequestTotalCount > itemsPerPage && (
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
                </div>
                <div className="gds-tab-content gds-content-border">
                  <div>
                    <div className="usr-grp-role-search-width">
                      <p className="gds-content-header">Terms & Conditions</p>
                    </div>
                  </div>
                  <div>
                    <div>
                      <textarea
                        placeholder="Terms & Conditions"
                        className="form-control"
                        id="termsAndConditions"
                        data-cy="termsAndConditions"
                        value={datashareInfo.termsOfUse}
                        rows={16}
                      />
                    </div>
                  </div>
                </div>

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
              </>
            )}
          </React.Fragment>
        )}
      />
    </>
  );
};

export default DatashareDetailFullView;
