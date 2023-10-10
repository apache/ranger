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

import React, { useState, useReducer } from "react";
import userGreyIcon from "../../../images/user-grey.svg";
import groupGreyIcon from "../../../images/group-grey.svg";
import roleGreyIcon from "../../../images/role-grey.svg";
import userColourIcon from "../../../images/user-colour.svg";
import groupColourIcon from "../../../images/group-colour.svg";
import roleColourIcon from "../../../images/role-colour.svg";
import { Accordion, Card, Button } from "react-bootstrap";
import { Field } from "react-final-form";
import Select from "react-select";
import AsyncSelect from "react-select/async";
import { fetchApi } from "../../../utils/fetchAPI";
import { findIndex } from "lodash";

const initialState = {
  selectedPrinciple: []
};

const principleFormReducer = (state, action) => {
  switch (action.type) {
    case "SET_SELECTED_PRINCIPLE":
      return {
        ...state,
        selectedPrinciple: action.selectedPrinciple
      };
    default:
      throw new Error();
  }
};

const PrinciplePermissionComp = ({
  userList,
  groupList,
  roleList,
  onDataChange
}) => {
  const [userOgList, setUserList] = useState(userList);
  const [groupOgList, setGroupList] = useState(groupList);
  const [roleOgList, setRoleList] = useState(roleList);
  const [filteredUserList, setFilteredUserList] = useState(userList);
  const [filteredGroupList, setFilteredGroupList] = useState(groupList);
  const [filteredRoleList, setFilteredRoleList] = useState(roleList);
  const [selectedACLFilter, setSelectedACLFilter] = useState({});
  const [searchPrinciple, setSearchPrinciple] = useState();
  const [userAccordion, setUserAccordion] = useState(false);
  const [groupAccordion, setGroupAccordion] = useState(false);
  const [roleAccordion, setRoleAccordion] = useState(false);
  const [principleDetails, dispatch] = useReducer(
    principleFormReducer,
    initialState
  );
  const { selectedPrinciple } = principleDetails;

  const [selectedAccess, setSelectedAccess] = useState({
    value: "LIST",
    label: "LIST"
  });

  const accessOptions = [
    { value: "LIST", label: "LIST" },
    { value: "VIEW", label: "VIEW" },
    { value: "ADMIN", label: "ADMIN" }
  ];

  const accessOptionsWithRemove = [
    { value: "LIST", label: "LIST" },
    { value: "VIEW", label: "VIEW" },
    { value: "ADMIN", label: "ADMIN" },
    { value: "Remove Access", label: "Remove Access" }
  ];

  const selectedPrincipal = (e, input) => {
    dispatch({
      type: "SET_SELECTED_PRINCIPLE",
      selectedPrinciple: e
    });
    input.onChange(e);
    console.log("Adding to selectedPrinciple");
    console.log(selectedPrinciple);
  };

  const filterPrincipleOp = ({ data }) => {
    let list = [];

    if (data["type"] == "USER") {
      list = userOgList;
    } else if (data["type"] == "GROUP") {
      list = groupOgList;
    } else if (data["type"] == "ROLE") {
      list = roleOgList;
    }

    return (
      findIndex(
        list.map((obj) => obj.name),
        data.value
      ) === -1
    );
  };

  const fetchPrincipleOp = async (inputValue) => {
    let params = { name: inputValue || "" };
    let data = [];
    const principalResp = await fetchApi({
      url: "xusers/lookup/principals",
      params: params
    });
    data = principalResp.data;
    return data.map((obj) => ({
      label: (
        <div>
          <img
            src={
              obj.type == "USER"
                ? userGreyIcon
                : obj.type == "GROUP"
                ? groupGreyIcon
                : roleGreyIcon
            }
            height="20px"
            width="20px"
          />{" "}
          {obj.name}{" "}
        </div>
      ),
      value: obj.name,
      type: obj.type
    }));
  };

  const setACL = (e, input) => {
    setSelectedAccess(e);
    input.onChange(e);
  };

  const addInSelectedPrincipal = (principle, input) => {
    for (let i = 0; i < principle.length; i++) {
      let acl = { name: "", type: "", perm: "" };
      acl.name = principle[i].value;
      acl.perm = selectedAccess.value;
      acl.type = principle[i].type;
      principle[i] = acl;
    }

    let tempUserList = userOgList;
    let tempGroupList = groupOgList;
    let tempRoleList = roleOgList;
    let principleGroupBy = _.groupBy(principle, "type");
    if (principleGroupBy["USER"] != undefined) {
      principleGroupBy["USER"].forEach((obj) => {
        tempUserList.push(obj);
      });
      setUserList(tempUserList);
      setFilteredUserList(tempUserList);
      console.log("userOglist PP");
      console.log(userOgList);
    }

    if (principleGroupBy["GROUP"] != undefined) {
      principleGroupBy["GROUP"].forEach((obj) => {
        tempGroupList.push(obj);
      });
      setGroupList(tempGroupList);
      setFilteredGroupList(tempGroupList);
    }

    if (principleGroupBy["ROLE"] != undefined) {
      principleGroupBy["ROLE"].forEach((obj) => {
        tempRoleList.push(obj);
      });
      setRoleList(tempRoleList);
      setFilteredRoleList(tempRoleList);
    }

    console.log("groupOgList");
    console.log(groupOgList);
    console.log("roleOgList");
    console.log(roleOgList);

    onDataChange(tempUserList, tempGroupList, tempRoleList);
    setSelectedAccess({ value: "LIST", label: "LIST" });

    dispatch({
      type: "SET_SELECTED_PRINCIPLE",
      selectedPrinciple: []
    });
    console.log("After clearing selectedPrinciple");
    console.log(selectedPrinciple);
  };

  const changeUserAccordion = () => {
    setUserAccordion(!userAccordion);
  };

  const changeGroupAccordion = () => {
    setGroupAccordion(!groupAccordion);
  };

  const changeRoleAccordion = () => {
    setRoleAccordion(!roleAccordion);
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

  const onChangePrincipleName = (event) => {
    setSearchPrinciple(event.target.value);
    filterPrincipleList(
      event.target.value.length > 0 ? event.target.value : undefined,
      true,
      undefined,
      false
    );
  };

  const onACLFilterChange = (e, input) => {
    setSelectedACLFilter(e);
    input.onChange(e);
    filterPrincipleList(
      undefined,
      false,
      e != undefined ? e.value : undefined,
      true
    );
  };

  const filterPrincipleList = (name, nameChange, perm, permChange) => {
    if (name === undefined && !nameChange) {
      name =
        searchPrinciple != undefined && searchPrinciple.length > 0
          ? searchPrinciple
          : undefined;
    }

    if (perm === undefined && !permChange) {
      perm =
        selectedACLFilter != undefined ? selectedACLFilter.value : undefined;
    }

    setFilteredUserList(
      userOgList.filter(function (user) {
        if (name != undefined && perm != undefined) {
          return user.name.startsWith(name, 0) && user.perm == perm;
        } else if (name != undefined) {
          return user.name.startsWith(name, 0);
        } else if (perm != undefined) {
          return user.perm == perm;
        } else {
          return true;
        }
      })
    );
    setFilteredGroupList(
      groupOgList.filter(function (group) {
        if (name != undefined && perm != undefined) {
          return group.name.startsWith(name, 0) && group.perm == perm;
        } else if (name != undefined) {
          return group.name.startsWith(name, 0);
        } else if (perm != undefined) {
          return group.perm == perm;
        } else {
          return true;
        }
      })
    );
    setFilteredRoleList(
      roleOgList.filter(function (role) {
        if (name != undefined && perm != undefined) {
          return role.name.startsWith(name, 0) && role.perm == perm;
        } else if (name != undefined) {
          return role.name.startsWith(name, 0);
        } else if (perm != undefined) {
          return role.perm == perm;
        } else {
          return true;
        }
      })
    );
  };

  const handleTableSelectedValue = (e, input, index, type) => {
    let tempUserList = userOgList;
    let tempGroupList = groupOgList;
    let tempRoleList = roleOgList;
    if (e.label == "Remove Access") {
      if (type == "USER") {
        let list = userOgList;
        list.splice(index, 1);
        setUserList(list);
        setFilteredUserList(list);
        tempUserList = [...tempUserList, ...list];
      } else if (type == "GROUP") {
        let list = groupOgList;
        list.splice(index, 1);
        setGroupList(list);
        setFilteredGroupList(list);
        tempGroupList = [...tempGroupList, ...list];
      } else if (type == "ROLE") {
        let list = roleOgList;
        list.splice(index, 1);
        setRoleList(list);
        setFilteredRoleList(list);
        tempRoleList = [...tempRoleList, ...list];
      }
    } else {
      if (type == "USER") {
        let list = userOgList;
        console.log("----------");
        console.log(list[index]);
        list[index]["perm"] = e.value;
        setUserList(list);
        setFilteredUserList(list);
        tempUserList = [...tempUserList, ...list];
      } else if (type == "GROUP") {
        let list = groupOgList;
        list[index]["perm"] = e.value;
        setGroupList(list);
        setFilteredGroupList(list);
        tempGroupList = [...tempGroupList, ...list];
      } else if (type == "ROLE") {
        let list = roleOgList;
        list[index]["perm"] = e.value;
        setRoleList(list);
        setFilteredRoleList(list);
        tempRoleList = [...tempRoleList, ...list];
      }
    }
    onDataChange(tempUserList, tempGroupList, tempRoleList);
    input.onChange(e);
  };

  return (
    <div className="gds-tab-content gds-content-border">
      <div className="gds-form-input">
        <Field
          className="form-control"
          name="selectedPrinciple"
          render={({ input, meta }) => (
            <div className="gds-add-principle">
              {" "}
              <AsyncSelect
                {...input}
                className="flex-1 gds-text-input"
                onChange={(e) => selectedPrincipal(e, input)}
                value={selectedPrinciple}
                filterOption={filterPrincipleOp}
                loadOptions={fetchPrincipleOp}
                components={{
                  DropdownIndicator: () => null,
                  IndicatorSeparator: () => null
                }}
                defaultOptions
                isMulti
                placeholder="Select Principals"
                data-name="usersSelect"
                data-cy="usersSelect"
              />
              <Field
                name="accessPermList"
                className="form-control"
                render={({ input }) => (
                  <Select
                    theme={serviceSelectTheme}
                    styles={customStyles}
                    options={accessOptions}
                    onChange={(e) => setACL(e, input)}
                    value={selectedAccess}
                    menuPlacement="auto"
                    isClearable
                  />
                )}
              ></Field>
              <Button
                type="button"
                className="btn btn-primary"
                onClick={() => {
                  userOgList;
                  if (
                    !selectedPrinciple ||
                    selectedPrinciple[0].value == undefined ||
                    selectedPrinciple.length === 0
                  ) {
                    toast.dismiss(toastId.current);
                    toastId.current = toast.error("Please select principal!!");
                    return false;
                  }
                  addInSelectedPrincipal(selectedPrinciple, input);
                  filterPrincipleList(
                    undefined,
                    undefined,
                    undefined,
                    undefined
                  );
                }}
                size="md"
                data-name="usersAddBtn"
                data-cy="usersAddBtn"
              >
                Add Principals
              </Button>
            </div>
          )}
        />
      </div>
      <div>
        <Card className="gds-section-card gds-bg-white">
          <div className="gds-section-title">
            <p className="gds-card-heading">Principles & Permissions</p>
          </div>
          <div className="gds-flex mg-b-10">
            <input
              type="search"
              className="form-control gds-input"
              placeholder="Search..."
              onChange={(e) => onChangePrincipleName(e)}
              value={searchPrinciple}
            />

            <Field
              name="accessPermFilter"
              className="form-control"
              render={({ input }) => (
                <Select
                  theme={serviceSelectTheme}
                  styles={customStyles}
                  options={accessOptions}
                  onChange={(e) => onACLFilterChange(e, input)}
                  value={selectedACLFilter}
                  menuPlacement="auto"
                  placeholder="All Access Types"
                  isClearable
                />
              )}
            />
          </div>

          <Accordion className="mg-b-10" defaultActiveKey="0">
            <Card>
              <div className="border-bottom">
                <Accordion.Toggle
                  as={Card.Header}
                  eventKey="1"
                  onClick={changeUserAccordion}
                  className="border-bottom-0 d-flex align-items-center justify-content-between gds-acc-card-header"
                  data-id="panel"
                  data-cy="panel"
                >
                  <div className="d-flex align-items-center gap-half">
                    <img src={userColourIcon} height="30px" width="30px" />
                    Users (
                    {filteredUserList == undefined
                      ? 0
                      : filteredUserList.length}
                    )
                  </div>

                  {userAccordion ? (
                    <i className="fa fa-angle-up  fa-lg font-weight-bold"></i>
                  ) : (
                    <i className="fa fa-angle-down  fa-lg font-weight-bold"></i>
                  )}
                </Accordion.Toggle>
              </div>
              <Accordion.Collapse eventKey="1">
                <Card.Body>
                  {filteredUserList != undefined &&
                  filteredUserList.length > 0 ? (
                    filteredUserList.map((obj, index) => {
                      return (
                        <div className="gds-principle-listing">
                          <span title={obj.name}>{obj.name}</span>

                          <Field
                            name="aclPerms"
                            render={({ input, meta }) => (
                              <Select
                                theme={serviceSelectTheme}
                                options={accessOptionsWithRemove}
                                menuPortalTarget={document.body}
                                onChange={(e) =>
                                  handleTableSelectedValue(
                                    e,
                                    input,
                                    index,
                                    "USER"
                                  )
                                }
                                menuPlacement="auto"
                                placeholder={obj.perm}
                                isClearable
                              />
                            )}
                          />
                        </div>
                      );
                    })
                  ) : (
                    <p className="mt-1">--</p>
                  )}
                </Card.Body>
              </Accordion.Collapse>
            </Card>
          </Accordion>

          <Accordion className="mg-b-10" defaultActiveKey="0">
            <Card>
              <div className="border-bottom">
                <Accordion.Toggle
                  as={Card.Header}
                  eventKey="1"
                  onClick={changeGroupAccordion}
                  className="border-bottom-0 d-flex align-items-center justify-content-between gds-acc-card-header"
                  data-id="panel"
                  data-cy="panel"
                >
                  <div className="d-flex align-items-center gap-half">
                    <img src={groupColourIcon} height="30px" width="30px" />
                    Groups (
                    {filteredGroupList == undefined
                      ? 0
                      : filteredGroupList.length}
                    )
                  </div>
                  {groupAccordion ? (
                    <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                  ) : (
                    <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                  )}
                </Accordion.Toggle>
              </div>
              <Accordion.Collapse eventKey="1">
                <Card.Body>
                  {filteredGroupList != undefined &&
                  filteredGroupList.length > 0 ? (
                    filteredGroupList.map((obj, index) => {
                      return (
                        <div className="gds-principle-listing">
                          <span title={obj.name}>{obj.name}</span>
                          <Field
                            name="aclPerms"
                            render={({ input, meta }) => (
                              <Select
                                theme={serviceSelectTheme}
                                options={accessOptionsWithRemove}
                                menuPortalTarget={document.body}
                                onChange={(e) =>
                                  handleTableSelectedValue(
                                    e,
                                    input,
                                    index,
                                    "GROUP"
                                  )
                                }
                                menuPlacement="auto"
                                placeholder={obj.perm}
                                isClearable
                              />
                            )}
                          />
                        </div>
                      );
                    })
                  ) : (
                    <p className="mt-1">--</p>
                  )}
                </Card.Body>
              </Accordion.Collapse>
            </Card>
          </Accordion>

          <Accordion className="mg-b-10" defaultActiveKey="0">
            <Card>
              <div className="border-bottom">
                <Accordion.Toggle
                  as={Card.Header}
                  eventKey="1"
                  onClick={changeRoleAccordion}
                  className="border-bottom-0 d-flex align-items-center justify-content-between gds-acc-card-header"
                  data-id="panel"
                  data-cy="panel"
                >
                  <div className="d-flex align-items-center gap-half">
                    <img src={roleColourIcon} height="30px" width="30px" />
                    Roles (
                    {filteredRoleList == undefined
                      ? 0
                      : filteredRoleList.length}
                    )
                  </div>
                  {roleAccordion ? (
                    <i className="fa fa-angle-up fa-lg font-weight-bold"></i>
                  ) : (
                    <i className="fa fa-angle-down fa-lg font-weight-bold"></i>
                  )}
                </Accordion.Toggle>
              </div>
              <Accordion.Collapse eventKey="1">
                <Card.Body>
                  {filteredRoleList != undefined &&
                  filteredRoleList.length > 0 ? (
                    filteredRoleList.map((obj, index) => {
                      return (
                        <div className="gds-principle-listing">
                          <span title={obj.name}>{obj.name}</span>
                          <Field
                            name="aclPerms"
                            render={({ input, meta }) => (
                              <Select
                                theme={serviceSelectTheme}
                                options={accessOptionsWithRemove}
                                menuPortalTarget={document.body}
                                onChange={(e) =>
                                  handleTableSelectedValue(
                                    e,
                                    input,
                                    index,
                                    "ROLE"
                                  )
                                }
                                menuPlacement="auto"
                                placeholder={obj.perm}
                                isClearable
                              />
                            )}
                          />
                        </div>
                      );
                    })
                  ) : (
                    <p className="mt-1">--</p>
                  )}
                </Card.Body>
              </Accordion.Collapse>
            </Card>
          </Accordion>
        </Card>
      </div>
    </div>
  );
};

export default PrinciplePermissionComp;
