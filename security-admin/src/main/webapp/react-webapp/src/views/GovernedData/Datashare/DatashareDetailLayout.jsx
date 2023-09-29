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
import { useParams, useNavigate, Link } from "react-router-dom";
import { Tab, Tabs } from "react-bootstrap";
import { fetchApi } from "../../../utils/fetchAPI";
import { Loader } from "../../../components/CommonComponents";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import { Button, Col, Modal, Accordion, Card } from "react-bootstrap";
import dateFormat from "dateformat";
import { toast } from "react-toastify";
import { BlockUi } from "../../../components/CommonComponents";
import PrinciplePermissionComp from "../Dataset/PrinciplePermissionComp";
import { Form } from "react-final-form";
import arrayMutators from "final-form-arrays";

const DatashareDetailLayout = () => {
  let { datashareId } = useParams();
  const [activeKey, setActiveKey] = useState("overview");
  const [datashareInfo, setDatashareInfo] = useState({});
  const [datashareDescription, setDatashareDescription] = useState();
  const [datashareTerms, setDatashareTerms] = useState();
  const [loader, setLoader] = useState(true);
  const [sharedResources, setSharedResources] = useState([]);
  const [confirmDeleteModal, setConfirmDeleteModal] = useState({
    sharedResourceDetails: {},
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
  const [openSharedRessourceAccordion, setOpenSharedResourceAccordion] =
    useState(true);

  useEffect(() => {
    fetchDatashareInfo(datashareId);
  }, []);

  const changeDatashareRequestAccordion = () => {
    setDatashareRequestAccordion(!dataShareRequestAccordion);
  };

  const handleTabSelect = (key) => {
    if (key == "resources") {
      fetchSharedResourceForDatashare(datashareInfo.name);
    } else if (key == "sharedWith") {
      fetchDatashareRequestList();
    }
    setActiveKey(key);
  };

  const fetchDatashareInfo = async (datashareId) => {
    try {
      const resp = await fetchApi({
        url: `gds/datashare/${datashareId}`,
      });
      setDatashareInfo(resp.data);
      setDatashareDescription(resp.data.description);
      setDatashareTerms(resp.data.termsOfUse);
      if (resp.data.acl != undefined) setPrincipleAccordianData(resp.data.acl);
      setLoader(false);
    } catch (error) {
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

  const fetchSharedResourceForDatashare = async (datashareName) => {
    try {
      let params = {};
      params["dataShareName"] = datashareName;
      const resp = await fetchApi({
        url: `gds/resource`,
        params: params,
      });
      setSharedResources(resp.data.list);
    } catch (error) {
      console.error(
        `Error occurred while fetching shared resource details ! ${error}`
      );
    }
  };

  const fetchDatashareRequestList = async () => {
    try {
      let params = {};
      params["dataShareId"] = datashareId;
      const resp = await fetchApi({
        url: `gds/datashare/dataset`,
        params: params,
      });
      setDataShareRequestsList(resp.data.list);
    } catch (error) {
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
      showPopup: true,
    });
  };

  const toggleClose = () => {
    setConfirmDeleteModal({
      sharedResourceDetails: {},
      showPopup: false,
    });
  };

  const handleSharedResourceDeleteClick = async (shareId) => {
    toggleClose();
    try {
      setBlockUI(true);
      await fetchApi({
        url: `gds/resource/${shareId}`,
        method: "DELETE",
      });
      setBlockUI(false);
      toast.success(" Success! Shared resource deleted successfully");
      fetchSharedResourceForDatashare(datashareInfo.name);
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

  const addResource = () => {
    navigate(`/gds/datashare/resource/${datashareId}`);
  };

  const back = () => {
    navigate("/gds/datasharelisting");
  };

  const onSharedResourceAccordionChange = () => {
    setOpenSharedResourceAccordion(!openSharedRessourceAccordion);
  };

  return (
    <>
      <Form
        onSubmit={handleSubmit}
        mutators={{
          ...arrayMutators,
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
                Datashare : {datashareInfo.name}
              </h3>
              {saveCancelButtons ? (
                <div className="gds-header-btn-grp">
                  <Button
                    variant="primary"
                    size="sm"
                    data-id="cancel"
                    data-cy="cancel"
                  >
                    Cancel
                  </Button>
                  <Button
                    variant="primary"
                    // onClick={
                    //   activeKey != "accessGrants"
                    //     ? updateDatasetDetails
                    //     : updateDatasetAccessGrant
                    // }
                    size="sm"
                    data-id="save"
                    data-cy="save"
                  >
                    Save
                  </Button>
                </div>
              ) : (
                <div className="gds-header-btn-grp">
                  <Button
                    variant="primary"
                    onClick={addResource}
                    size="sm"
                    data-id="save"
                    data-cy="save"
                  >
                    Add a Resource
                  </Button>
                </div>
              )}
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
                                  Date Updated
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
                                  Date Created
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
                              <div className="fnt-14 pl-1">Description</div>
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
                          <PrinciplePermissionComp
                            userList={userList}
                            groupList={groupList}
                            roleList={roleList}
                            onDataChange={handleDataChange}
                          />
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
                              {sharedResources.length > 0 ? (
                                sharedResources.map((obj, index) => {
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
                                              onSharedResourceAccordionChange(
                                                obj
                                              )
                                            }
                                            className="border-bottom-0"
                                            data-id="panel"
                                            data-cy="panel"
                                          >
                                            <div className="d-flex justify-content-between align-items-center">
                                              <div className="d-flex align-items-center gap-1">
                                                {openSharedRessourceAccordion ? (
                                                  <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                                ) : (
                                                  <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                                )}
                                                <div className="d-flex justify-content-between">
                                                  <h6 className="m-0">
                                                    {obj.name}
                                                  </h6>
                                                </div>
                                              </div>
                                              <div className="d-flex gap-half align-items-start">
                                                {obj["status"]}
                                                <Button
                                                  variant="outline-dark"
                                                  size="sm"
                                                  title="Edit"
                                                  // onClick={(e) => {
                                                  //   e.stopPropagation();
                                                  //   openModal(original);
                                                  // }}
                                                  data-name="editSharedResource"
                                                  data-id={obj.id}
                                                >
                                                  <i className="fa-fw fa fa-edit"></i>
                                                </Button>
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
                                            </div>
                                          </Accordion.Toggle>
                                          <Accordion.Collapse eventKey="1">
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
                                                        showConfitionModal(obj)
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
                                                    onClick={changeDatashareRequestAccordion(
                                                      obj
                                                    )}
                                                    className="border-bottom-0"
                                                    data-id="panel"
                                                    data-cy="panel"
                                                  >
                                                    <div className="d-flex justify-content-between align-items-center">
                                                      <div className="d-flex align-items-center gap-half">
                                                        {true ? (
                                                          <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                                        ) : (
                                                          <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                                        )}
                                                        <h6 className="m-0">
                                                          {obj.name} Dataset{" "}
                                                          {index + 1}
                                                        </h6>
                                                      </div>
                                                      <div className="d-flex align-items-center gap-half">
                                                        {obj["status"]}
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
                                                        <Button
                                                          variant="danger"
                                                          size="sm"
                                                          title="Delete"
                                                          // onClick={() =>
                                                          //   toggleConfirmModalForDelete(
                                                          //     obj.id,
                                                          //     obj.name
                                                          //   )
                                                          // }
                                                          data-name="deleteDatashareRequest"
                                                          data-id={obj["id"]}
                                                          data-cy={obj["id"]}
                                                        >
                                                          <i className="fa-fw fa fa-trash fa-fw fa fa-large" />
                                                        </Button>
                                                      </div>
                                                    </div>
                                                  </Accordion.Toggle>
                                                  <Accordion.Collapse eventKey="1">
                                                    <Card.Body>
                                                      <div className="d-flex justify-content-between">
                                                        <div className="gds-inline-field-grp">
                                                          <div className="wrapper">
                                                            <div
                                                              className="gds-left-inline-field"
                                                              height="30px"
                                                            >
                                                              Expiry
                                                            </div>
                                                            <div line-height="30px">
                                                              {obj["service"]}
                                                            </div>
                                                          </div>
                                                        </div>
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
                                                              View Request
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
                    <Tab eventKey="history" title="HISTORY"></Tab>
                    <Tab eventKey="termsOfUser" title="TERMS OF USE">
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
                            {conditionModalData != undefined &&
                            conditionModalData.conditionExpr != undefined
                              ? conditionModalData.conditionExpr
                              : ""}
                          </div>
                        </div>
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
