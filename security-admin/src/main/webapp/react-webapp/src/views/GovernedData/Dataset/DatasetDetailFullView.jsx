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
 * Unless required by applicable law or agreed to in writing,Row
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useEffect, useState } from "react";
import { useParams, useNavigate, useLocation, Link } from "react-router-dom";
import { fetchApi } from "../../../utils/fetchAPI";
import { Loader } from "../../../components/CommonComponents";
import CustomBreadcrumb from "../../CustomBreadcrumb";
import PrinciplePermissionComp from "./PrinciplePermissionComp";
import dateFormat from "dateformat";
import { isSystemAdmin } from "../../../utils/XAUtils";
import { Form } from "react-final-form";
import ReactPaginate from "react-paginate";
import { Button, Accordion, Card } from "react-bootstrap";
import userColourIcon from "../../../images/user-colour.svg";
import groupColourIcon from "../../../images/group-colour.svg";
import roleColourIcon from "../../../images/role-colour.svg";
import Select from "react-select";
import { statusClassMap } from "../../../utils/XAEnums";

const DatasetDetailFullView = () => {
  let { datasetId } = useParams();
  const { state } = useLocation();
  const [userAclPerm, setUserAclPerm] = useState(state?.userAclPerm);
  const [loader, setLoader] = useState(true);
  const [datasetInfo, setDatasetInfo] = useState({});
  const [userList, setUserList] = useState([]);
  const [groupList, setGroupList] = useState([]);
  const [roleList, setRoleList] = useState([]);
  const [dataShareRequestsList, setDatashareRequestsList] = useState([]);
  const itemsPerPage = 5;
  const [requestAccordionState, setRequestAccordionState] = useState({});
  const [requestPageCount, setRequestPageCount] = useState();
  const navigate = useNavigate();
  const [sharedWithPrincipleName, setSharedWithPrincipleName] = useState();
  const [serviceDef, setServiceDef] = useState({});
  const [userSharedWithMap, setUserSharedWithMap] = useState(new Map());
  const [groupSharedWithMap, setGroupSharedWithMap] = useState(new Map());
  const [roleSharedWithMap, setRoleSharedWithMap] = useState(new Map());
  const [filteredUserSharedWithMap, setFilteredUserSharedWithMap] = useState();
  const [filteredGroupSharedWithMap, setFilteredGroupSharedWithMap] =
    useState();
  const [filteredRoleSharedWithMap, setFilteredRoleSharedWithMap] = useState();
  const [sharedWithAccessFilter, setSharedWithAccessFilter] = useState();
  const [completeDatashareRequestsList, setCompleteDatashareRequestsList] =
    useState([]);
  const [datashareRequestTotalCount, setDatashareRequestTotalCount] =
    useState(0);

  useEffect(() => {
    if (userAclPerm == undefined) fetchDatasetSummary(datasetId);
    fetchDatasetInfo(datasetId);
    fetchDatashareRequestList(undefined, 0, false);
    fetchAccessGrantInfo();
  }, []);

  const fetchDatasetSummary = async (datasetId) => {
    try {
      let params = {};
      params["datasetId"] = datasetId;
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/dataset/summary`,
        params: params
      });
      setLoader(false);
      setUserAclPerm(resp.data.list[0].permissionForCaller);
    } catch (error) {
      setLoader(false);
      if (error.response.status == 401 || error.response.status == 400) {
        <ErrorPage errorCode="401" />;
      }
      console.error(
        `Error occurred while fetching dataset summary details ! ${error}`
      );
    }
  };

  const fetchDatasetInfo = async (datasetId) => {
    try {
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/dataset/${datasetId}`
      });
      setLoader(false);
      setDatasetInfo(resp.data);
      if (resp.data.acl != undefined) setPrincipleAccordianData(resp.data.acl);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      if (error.response.status == 401 || error.response.status == 400) {
        <ErrorPage errorCode="401" />;
      }
      console.error(`Error occurred while fetching dataset details ! ${error}`);
    }
  };

  const fetchDatashareRequestList = async (
    datashareName,
    currentPage,
    getCompleteList
  ) => {
    try {
      let params = {};

      params["page"] = currentPage;
      let itemPerPageCount = getCompleteList ? 999999999 : itemsPerPage;
      params["startIndex"] = currentPage * itemPerPageCount;
      params["datasetId"] = datasetId;
      params["pageSize"] = itemPerPageCount;
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
      setRequestPageCount(Math.ceil(resp.data.totalCount / itemPerPageCount));
      if (!getCompleteList) {
        setDatashareRequestsList(resp.data.list);
      } else {
        setCompleteDatashareRequestsList(resp.data.list);
      }
      setDatashareRequestTotalCount(resp.data.totalCount);
    } catch (error) {
      console.error(
        `Error occurred while fetching Datashare requests details ! ${error}`
      );
    }
  };

  const fetchAccessGrantInfo = async () => {
    let policyData = {};
    try {
      const resp = await fetchApi({
        url: `gds/dataset/${datasetId}/policy`
      });
      policyData = resp.data[0];
      fetchServiceDef(policyData.serviceType);
      let grantItems = policyData.policyItems;
      const userMap = new Map();
      const groupMap = new Map();
      const roleMap = new Map();
      grantItems.forEach((item) => {
        if (item.users !== undefined) {
          item.users.forEach((user) => {
            let accessList = [];
            if (userMap.get(user) !== undefined) {
              accessList = userMap.get(user);
            }
            userMap.set(user, [...accessList, ...item.accesses]);
          });
        }

        if (item.groups !== undefined) {
          item.groups.forEach((group) => {
            let accessList = [];
            if (groupMap[group] !== undefined) {
              accessList = groupMap[group];
            }
            groupMap.set(group, [...accessList, ...item.accesses]);
          });
        }

        if (item.roles !== undefined) {
          item.roles.forEach((role) => {
            let accessList = [];
            if (roleMap[role] !== undefined) {
              accessList = roleMap[role];
            }
            roleMap.set(role, [...accessList, ...item.accesses]);
          });
        }
        setUserSharedWithMap(userMap);
        setGroupSharedWithMap(groupMap);
        setRoleSharedWithMap(roleMap);
        setFilteredUserSharedWithMap(userMap);
        setFilteredGroupSharedWithMap(groupMap);
        setFilteredRoleSharedWithMap(roleMap);
      });
      return grantItems;
    } catch (error) {
      if (error.response.status == 404) {
      }
      console.error(
        `Error occurred while fetching dataset access grant details ! ${error}`
      );
    }
  };

  const fetchServiceDef = async (serviceDefName) => {
    let serviceDefsResp = [];
    try {
      serviceDefsResp = await fetchApi({
        url: `plugins/definitions/name/${serviceDefName}`
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Service Definition or CSRF headers! ${error}`
      );
    }
    let modifiedServiceDef = serviceDefsResp.data;
    for (const obj of modifiedServiceDef.resources) {
      obj.recursiveSupported = false;
      obj.excludesSupported = false;
    }
    setServiceDef(modifiedServiceDef);
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
    setGroupList([...groupList, ...tempGroupList]);
    setRoleList([...roleList, ...tempRoleList]);
  };

  const handleSubmit = async (formData) => {};

  const onRequestAccordionChange = (id) => {
    setRequestAccordionState({
      ...requestAccordionState,
      ...{ [id]: !requestAccordionState[id] }
    });
  };

  const viewDatashareDetail = (datashareId) => {
    navigate(`/gds/datashare/${datashareId}/detail`);
  };

  const handleRequestPageClick = ({ selected }) => {
    fetchDatashareRequestList(undefined, selected, false);
  };

  const onChangeSharedWithPrincipleName = (event) => {
    setSharedWithPrincipleName(event.target.value);
    filterSharedWithPrincipleList(event.target.value, true, undefined, false);
  };

  const filterSharedWithPrincipleList = (
    name,
    nameChange,
    perm,
    permChange
  ) => {
    if (name === undefined && !nameChange) {
      name =
        sharedWithPrincipleName != undefined &&
        sharedWithPrincipleName.length > 0
          ? sharedWithPrincipleName
          : undefined;
    }

    if (perm === undefined && !permChange) {
      perm =
        sharedWithAccessFilter != undefined
          ? sharedWithAccessFilter.name
          : undefined;
    }

    let newUserMap = new Map();
    let newGroupMap = new Map();
    let newRoleMap = new Map();

    const conditionFunction = (value, key) => {
      if (name != undefined && perm != undefined) {
        let accessMatch = false;
        for (const accessObj of value) {
          if (accessObj.type == perm) {
            accessMatch = true;
          }
        }
        return key.startsWith(name, 0) && accessMatch;
      } else if (name != undefined) {
        return key.startsWith(name, 0);
      } else if (perm != undefined) {
        for (const accessObj of value) {
          if (accessObj.type == perm) return true;
        }
      } else {
        return true;
      }
    };

    userSharedWithMap.forEach((value, key) => {
      if (conditionFunction(value, key)) {
        newUserMap.set(key, value);
      }
    });

    groupSharedWithMap.forEach((value, key) => {
      if (conditionFunction(value, key)) {
        newGroupMap.set(key, value);
      }
    });

    roleSharedWithMap.forEach((value, key) => {
      if (conditionFunction(value, key)) {
        newRoleMap.set(key, value);
      }
    });

    setFilteredUserSharedWithMap(newUserMap);
    setFilteredGroupSharedWithMap(newGroupMap);
    setFilteredRoleSharedWithMap(newRoleMap);
  };

  const serviceSelectTheme = (theme) => {
    return {
      ...theme,
      colors: {
        ...theme.colors,
        primary: "#0081ab"
      }
    };
  };

  const customStyles = {
    control: (provided) => ({
      ...provided,
      maxHeight: "40px",
      width: "172px"
    }),
    indicatorsContainer: (provided) => ({
      ...provided
    })
  };

  const onSharedWithAccessFilterChange = (e) => {
    setSharedWithAccessFilter(e);
    filterSharedWithPrincipleList(
      undefined,
      false,
      e != undefined ? e.label : undefined,
      true
    );
  };

  const downloadJsonFile = async () => {
    let jsonData = datasetInfo;
    jsonData.datashares = await fetchDatashareRequestList(undefined, 0, true);
    if (
      userAclPerm == "ADMIN" ||
      userAclPerm == "AUDIT" ||
      userAclPerm == "POLICY_ADMIN"
    ) {
      jsonData.sharedWith = { users: {}, groups: {}, roles: {} };
      let policyItems = await fetchAccessGrantInfo();
      for (const item of policyItems) {
        let accessList = [];
        item.accesses.forEach((item) => {
          accessList.push(item.type);
        });
        item.users?.forEach((user) => {
          if (jsonData.sharedWith.users[user] != undefined) {
            let newAccessTypeList = [
              ...jsonData.sharedWith.users[user],
              ...accessList
            ];
            jsonData.sharedWith.users[user] = [...new Set(newAccessTypeList)];
          } else {
            jsonData.sharedWith.users[user] = accessList;
          }
        });
        item.groups?.forEach((group) => {
          if (jsonData.sharedWith.groups[group] != undefined) {
            let newAccessTypeList = [
              ...jsonData.sharedWith.groups[group],
              ...accessList
            ];
            jsonData.sharedWith.groups[group] = [...new Set(newAccessTypeList)];
          } else {
            jsonData.sharedWith.groups[group] = accessList;
          }
        });
        item.roles?.forEach((role) => {
          if (jsonData.sharedWith.roles[role] != undefined) {
            let newAccessTypeList = [
              ...jsonData.sharedWith.roles[role],
              ...accessList
            ];
            jsonData.sharedWith.roles[role] = [...new Set(newAccessTypeList)];
          } else {
            jsonData.sharedWith.roles[role] = accessList;
          }
        });
      }
    }
    const jsonContent = JSON.stringify(jsonData);
    const blob = new Blob([jsonContent], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = datasetInfo.name + ".json";
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <>
      {loader ? (
        <Loader />
      ) : (
        <div>
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
                title={datasetInfo.name}
                className="text-truncate"
                style={{ maxWidth: "700px", display: "inline-block" }}
              >
                Dataset : {datasetInfo.name}
              </span>
            </h3>
            <CustomBreadcrumb />
            <span className="pipe"></span>
            <div className="gds-header-btn-grp">
              <Button
                variant="primary"
                onClick={() => downloadJsonFile()}
                size="sm"
                data-id="downloadJSON"
                data-cy="downloadJSON"
              >
                Download JSON
              </Button>
            </div>
          </div>
          <Form
            onSubmit={handleSubmit}
            render={({}) => (
              <div>
                <div className="gds-tab-content gds-content-border">
                  <div className="gds-inline-field-grp">
                    <div className="wrapper">
                      <div className="gds-left-inline-field" height="30px">
                        <span className="gds-label-color">Date Updated</span>
                      </div>
                      <div line-height="30px">
                        {dateFormat(
                          datasetInfo.updateTime,
                          "mm/dd/yyyy hh:MM:ss TT"
                        )}
                      </div>
                    </div>

                    <div className="wrapper">
                      <div className="gds-left-inline-field" line-height="30px">
                        <span className="gds-label-color">Date Created</span>
                      </div>
                      <div line-height="30px">
                        {dateFormat(
                          datasetInfo.createTime,
                          "mm/dd/yyyy hh:MM:ss TT"
                        )}
                      </div>
                    </div>
                  </div>

                  <div>
                    <div>
                      <span className="gds-label-color">Description</span>
                    </div>
                  </div>
                  <div>
                    <div>
                      <textarea
                        placeholder="Dataset Description"
                        className="form-control gds-description"
                        id="description"
                        data-cy="description"
                        value={datasetInfo.description}
                        rows={5}
                      />
                    </div>
                  </div>
                </div>
                {(isSystemAdmin() ||
                  userAclPerm == "ADMIN" ||
                  userAclPerm == "AUDIT" ||
                  userAclPerm == "POLICY_ADMIN") && (
                  <PrinciplePermissionComp
                    userList={userList}
                    groupList={groupList}
                    roleList={roleList}
                    isEditable={false}
                    type="dataset"
                  />
                )}

                <div className="gds-tab-content gds-content-border">
                  <div className="gds-section-title">
                    <p className="gds-card-heading">Datashares</p>
                  </div>
                  <div>
                    {dataShareRequestsList.length > 0 ? (
                      dataShareRequestsList.map((obj, index) => {
                        const status = obj["status"] || "DENIED";
                        return (
                          <Accordion className="mg-b-10" defaultActiveKey="0">
                            <div className="border-bottom">
                              <Accordion.Header
                                eventKey="1"
                                onClick={() => onRequestAccordionChange(obj.id)}
                                className="border-bottom-0"
                                data-id="panel"
                                data-cy="panel"
                              >
                                <div className="d-flex justify-content-between align-items-center">
                                  <div className="d-flex align-items-center gap-1">
                                    {requestAccordionState[obj.id] ? (
                                      <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                                    ) : (
                                      <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                                    )}
                                    <h5 className="gds-heading-5 m-0">
                                      Datashare {obj.dataShareId}
                                    </h5>
                                  </div>
                                  <div className="d-flex align-items-center gap-1">
                                    <span
                                      className={`${statusClassMap[status]}`}
                                    >
                                      {obj["status"]}
                                    </span>
                                    <Button
                                      variant="outline-dark"
                                      size="sm"
                                      className="me-2"
                                      title="View"
                                      data-name="viewDatashare"
                                      onClick={() =>
                                        viewDatashareDetail(obj.dataShareId)
                                      }
                                      data-id={obj.id}
                                    >
                                      <i className="fa-fw fa fa-eye fa-fw fa fa-large" />
                                    </Button>
                                  </div>
                                </div>
                              </Accordion.Header>
                              <Accordion.Body eventKey="1">
                                <Card.Body>
                                  <div className="d-flex justify-content-between">
                                    <div className="gds-inline-field-grp">
                                      <div className="wrapper">
                                        <div
                                          className="gds-left-inline-field"
                                          height="30px"
                                        >
                                          Service
                                        </div>
                                        <div line-height="30px">
                                          {obj["service"]}
                                        </div>
                                      </div>
                                      <div className="wrapper">
                                        <div
                                          className="gds-left-inline-field"
                                          height="30px"
                                        >
                                          Zone
                                        </div>
                                        <div line-height="30px">
                                          {obj["zone"]}
                                        </div>
                                      </div>
                                      <div className="wrapper">
                                        <div
                                          className="gds-left-inline-field"
                                          height="30px"
                                        >
                                          Resource Count
                                        </div>
                                        <div line-height="30px">
                                          {obj["resourceCount"]}
                                        </div>
                                      </div>
                                    </div>
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
                                        <Link
                                          to={`/gds/request/detail/${obj.id}`}
                                        >
                                          View Request
                                        </Link>
                                      </div>
                                    </div>
                                  </div>
                                </Card.Body>
                              </Accordion.Body>
                            </div>
                          </Accordion>
                        );
                      })
                    ) : (
                      <div></div>
                    )}
                    {datashareRequestTotalCount > itemsPerPage && (
                      <ReactPaginate
                        previousLabel={"Prev"}
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
                      <p className="gds-content-header">Shared With</p>
                    </div>
                    <div className="gds-flex mg-b-10">
                      <input
                        type="search"
                        className="form-control gds-input"
                        placeholder="Search..."
                        onChange={(e) => onChangeSharedWithPrincipleName(e)}
                        value={sharedWithPrincipleName}
                      />

                      <Select
                        theme={serviceSelectTheme}
                        styles={customStyles}
                        options={serviceDef.accessTypes}
                        onChange={(e) => onSharedWithAccessFilterChange(e)}
                        value={sharedWithAccessFilter}
                        menuPlacement="auto"
                        placeholder="All Permissions"
                        isClearable
                      />
                    </div>

                    <Accordion className="mg-b-10" defaultActiveKey="0">
                      <Accordion.Item>
                        <Accordion.Header
                          eventKey="1"
                          data-id="panel"
                          data-cy="panel"
                        >
                          <div className="d-flex align-items-center gap-half">
                            <img
                              src={userColourIcon}
                              height="30px"
                              width="30px"
                            />
                            Users (
                            {filteredUserSharedWithMap == undefined
                              ? 0
                              : filteredUserSharedWithMap.size}
                            )
                          </div>
                        </Accordion.Header>
                        <Accordion.Body eventKey="1">
                          {filteredUserSharedWithMap != undefined &&
                          filteredUserSharedWithMap.size > 0 ? (
                            Array.from(filteredUserSharedWithMap).map(
                              ([key, value]) => (
                                <div
                                  className="gds-principle-listing"
                                  key={key}
                                >
                                  <span title={key}>{key}</span>
                                  <div className="gds-chips gap-one-fourth">
                                    {value.map((accessObj) => (
                                      <span
                                        className="badge text-bg-light badge-sm"
                                        title={accessObj.type}
                                        key={accessObj.type}
                                      >
                                        {accessObj.type}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )
                            )
                          ) : (
                            <p className="mt-1">--</p>
                          )}
                        </Accordion.Body>
                      </Accordion.Item>
                    </Accordion>

                    <Accordion className="mg-b-10" defaultActiveKey="0">
                      <Accordion.Item>
                        <Accordion.Header
                          eventKey="1"
                          data-id="panel"
                          data-cy="panel"
                        >
                          <div className="d-flex align-items-center gap-half">
                            <img
                              src={groupColourIcon}
                              height="30px"
                              width="30px"
                            />
                            Groups (
                            {filteredGroupSharedWithMap == undefined
                              ? 0
                              : filteredGroupSharedWithMap.size}
                            )
                          </div>
                        </Accordion.Header>
                        <Accordion.Body eventKey="1">
                          {filteredGroupSharedWithMap != undefined &&
                          filteredGroupSharedWithMap.size > 0 ? (
                            Array.from(filteredGroupSharedWithMap).map(
                              ([key, value]) => (
                                <div
                                  className="gds-principle-listing"
                                  key={key}
                                >
                                  <span title={key}>{key}</span>
                                  <div className="gds-chips gap-one-fourth">
                                    {value.map((accessObj) => (
                                      <span
                                        className="badge text-bg-light badge-sm"
                                        title={accessObj.type}
                                        key={accessObj.type}
                                      >
                                        {accessObj.type}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )
                            )
                          ) : (
                            <p className="mt-1">--</p>
                          )}
                        </Accordion.Body>
                      </Accordion.Item>
                    </Accordion>

                    <Accordion className="mg-b-10" defaultActiveKey="0">
                      <Accordion.Item>
                        <Accordion.Header
                          eventKey="1"
                          data-id="panel"
                          data-cy="panel"
                        >
                          <div className="d-flex align-items-center gap-half">
                            <img
                              src={roleColourIcon}
                              height="30px"
                              width="30px"
                            />
                            Roles (
                            {filteredRoleSharedWithMap == undefined
                              ? 0
                              : filteredRoleSharedWithMap.size}
                            )
                          </div>
                        </Accordion.Header>
                        <Accordion.Body eventKey="1">
                          {filteredRoleSharedWithMap != undefined &&
                          filteredRoleSharedWithMap.size > 0 ? (
                            Array.from(filteredRoleSharedWithMap).map(
                              ([key, value]) => (
                                <div
                                  className="gds-principle-listing"
                                  key={key}
                                >
                                  <span title={key}>{key}</span>
                                  <div className="gds-chips gap-one-fourth">
                                    {value.map((accessObj) => (
                                      <span
                                        className="badge text-bg-light badge-sm"
                                        title={accessObj.type}
                                        key={accessObj.type}
                                      >
                                        {accessObj.type}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )
                            )
                          ) : (
                            <p className="mt-1">--</p>
                          )}
                        </Accordion.Body>
                      </Accordion.Item>
                    </Accordion>
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
                        value={datasetInfo.termsOfUse}
                        rows={16}
                      />
                    </div>
                  </div>
                </div>
              </div>
            )}
          />
        </div>
      )}
    </>
  );
};

export default DatasetDetailFullView;
