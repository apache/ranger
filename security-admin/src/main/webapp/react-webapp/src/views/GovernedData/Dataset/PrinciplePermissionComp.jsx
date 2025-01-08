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

import React, { useState, useReducer, useRef } from "react";
import userGreyIcon from "../../../images/user-grey.svg";
import groupGreyIcon from "../../../images/group-grey.svg";
import roleGreyIcon from "../../../images/role-grey.svg";
import userColourIcon from "../../../images/user-colour.svg";
import groupColourIcon from "../../../images/group-colour.svg";
import roleColourIcon from "../../../images/role-colour.svg";
import { Accordion, Card, Button, Modal } from "react-bootstrap";
import { Field } from "react-final-form";
import Select from "react-select";
import AsyncSelect from "react-select/async";
import { fetchApi } from "../../../utils/fetchAPI";
import { findIndex, remove } from "lodash";
import { isSystemAdmin } from "../../../utils/XAUtils";
import { toast } from "react-toastify";

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
  isAdmin,
  type,
  onDataChange,
  isDetailView
}) => {
  const [userOgList, setUserList] = useState(userList);
  const [groupOgList, setGroupList] = useState(groupList);
  const [roleOgList, setRoleList] = useState(roleList);
  const [filteredUserList, setFilteredUserList] = useState(userList);
  const [filteredGroupList, setFilteredGroupList] = useState(groupList);
  const [filteredRoleList, setFilteredRoleList] = useState(roleList);
  const [selectedACLFilter, setSelectedACLFilter] = useState({});
  const [searchPrinciple, setSearchPrinciple] = useState();
  const [principleDetails, dispatch] = useReducer(
    principleFormReducer,
    initialState
  );
  const { selectedPrinciple } = principleDetails;
  const selectVisibilityLevelRef = useRef(null);
  const [selectedAccess, setSelectedAccess] = useState({});
  const [showAddPrincipalModal, setShowAddPrincipalModal] = useState(false);

  const accessOptions = [
    { value: "LIST", label: "LIST" },
    { value: "VIEW", label: "VIEW" },
    { value: "AUDIT", label: "AUDIT" },
    { value: "POLICY_ADMIN", label: "POLICY_ADMIN" },
    { value: "ADMIN", label: "ADMIN" }
  ];

  const accessOptionsWithRemove = [
    { value: "LIST", label: "LIST" },
    { value: "VIEW", label: "VIEW" },
    { value: "AUDIT", label: "AUDIT" },
    { value: "POLICY_ADMIN", label: "POLICY_ADMIN" },
    { value: "ADMIN", label: "ADMIN" },
    { value: "Remove Access", label: "Remove Access" }
  ];

  const selectedPrincipal = (e, input) => {
    dispatch({
      type: "SET_SELECTED_PRINCIPLE",
      selectedPrinciple: e
    });
    input.onChange(e);
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
    return findIndex(list, { name: data.value }) === -1;
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
    if (selectedAccess == undefined || selectedAccess.value == undefined) {
      toast.error("Please select visibility!!");
      return false;
    }
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

    onDataChange(tempUserList, tempGroupList, tempRoleList);

    dispatch({
      type: "SET_SELECTED_PRINCIPLE",
      selectedPrinciple: []
    });
    if (selectVisibilityLevelRef.current) {
      selectVisibilityLevelRef.current.clearValue();
    }
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

  const handleTableSelectedValue = (e, input, index, name, type) => {
    let tempUserList = userOgList;
    let tempGroupList = groupOgList;
    let tempRoleList = roleOgList;
    if (e?.label == "Remove Access") {
      if (type == "USER") {
        let list = userOgList;
        //list.splice(index, 1);
        remove(userOgList, function (n) {
          return n.name == name;
        });
        setUserList(userOgList);
        setFilteredUserList(userOgList);
        tempUserList = [...tempUserList, ...userOgList];
      } else if (type == "GROUP") {
        remove(groupOgList, function (n) {
          return n.name == name;
        });
        setGroupList(groupOgList);
        setFilteredGroupList(groupOgList);
        tempGroupList = [...tempGroupList, ...groupOgList];
      } else if (type == "ROLE") {
        remove(roleOgList, function (n) {
          return n.name == name;
        });
        setRoleList(roleOgList);
        setFilteredRoleList(roleOgList);
        tempRoleList = [...tempRoleList, ...roleOgList];
      }
    } else {
      if (type == "USER") {
        let list = userOgList;
        console.log("----------");
        console.log(list[index]);
        list[index]["perm"] = e.value;
        setUserList(list);
        setFilteredUserList(list);
        tempUserList = list;
      } else if (type == "GROUP") {
        let list = groupOgList;
        list[index]["perm"] = e.value;
        setGroupList(list);
        setFilteredGroupList(list);
        tempGroupList = list;
      } else if (type == "ROLE") {
        let list = roleOgList;
        list[index]["perm"] = e.value;
        setRoleList(list);
        setFilteredRoleList(list);
        tempRoleList = list;
      }
      input.onChange(e);
    }
    onDataChange(tempUserList, tempGroupList, tempRoleList);
  };

  const openAddPrincipalModal = () => {
    setShowAddPrincipalModal(true);
  };

  const toggleAddPrincipalModal = () => {
    setShowAddPrincipalModal(false);
  };

  const containerStyle = {
    display: "flex",
    justifyContent: "space-between"
  };

  return (
    <div className="gds-tab-content">
      {!isDetailView && (
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
                  placeholder="Type to select Principals"
                  data-name="usersSelect"
                  data-cy="usersSelect"
                />
                <Field
                  name="accessPermList"
                  className="form-control"
                  render={({ input }) => (
                    <Select
                      {...input}
                      theme={serviceSelectTheme}
                      styles={customStyles}
                      options={accessOptions}
                      onChange={(e) => setACL(e, input)}
                      ref={selectVisibilityLevelRef}
                      //value={selectedAccess}
                      menuPlacement="auto"
                      isClearable
                      placeholder="Visibility Level"
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
                      selectedPrinciple[0] == undefined ||
                      selectedPrinciple[0].value == undefined ||
                      selectedPrinciple.length === 0
                    ) {
                      toast.error("Please select principal!!");
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
      )}

      <div>
        <Card className="gds-section-card gds-bg-white">
          <div className="gds-section-title">
            <p className="gds-card-heading" style={containerStyle}>
              {type == "dataset"
                ? "Dataset"
                : type == "datashare"
                  ? "Datashare"
                  : ""}{" "}
              Visibility
              {isDetailView && isAdmin && (
                <Button
                  type="button"
                  className="btn btn-primary"
                  onClick={() => openAddPrincipalModal()}
                  size="md"
                  data-name="opnAddPrincipalModal"
                  data-cy="opnAddPrincipalModal"
                >
                  Add Principals
                </Button>
              )}
            </p>
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
                  {...input}
                  theme={serviceSelectTheme}
                  styles={customStyles}
                  options={accessOptions}
                  onChange={(e) => onACLFilterChange(e, input)}
                  //value={selectedACLFilter}
                  menuPlacement="auto"
                  placeholder="Visibility Level"
                  isClearable
                />
              )}
            />
          </div>

          <Accordion className="mg-b-10" defaultActiveKey="0">
            <Accordion.Item>
              <Accordion.Header eventKey="1" data-id="panel" data-cy="panel">
                <div className="d-flex align-items-center gap-half m-t-10 m-b-10">
                  <img src={userColourIcon} height="30px" width="30px" />
                  Users (
                  {filteredUserList == undefined ? 0 : filteredUserList.length})
                </div>
              </Accordion.Header>
              <Accordion.Body eventKey="1">
                {filteredUserList != undefined &&
                filteredUserList.length > 0 ? (
                  filteredUserList.map((obj, index) => {
                    return (
                      <div className="gds-principle-listing" key={obj.name}>
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
                                  obj.name,
                                  "USER"
                                )
                              }
                              menuPlacement="auto"
                              defaultValue={[
                                { label: obj.perm, value: obj.perm }
                              ]}
                              isDisabled={isDetailView && !isAdmin}
                            />
                          )}
                        />
                      </div>
                    );
                  })
                ) : (
                  <p className="mt-1">--</p>
                )}
              </Accordion.Body>
            </Accordion.Item>
          </Accordion>

          <Accordion className="mg-b-10" defaultActiveKey="0">
            <Accordion.Item>
              <Accordion.Header eventKey="1" data-id="panel" data-cy="panel">
                <div className="d-flex align-items-center gap-half">
                  <img src={groupColourIcon} height="30px" width="30px" />
                  Groups (
                  {filteredGroupList == undefined
                    ? 0
                    : filteredGroupList.length}
                  )
                </div>
              </Accordion.Header>
              <Accordion.Body eventKey="1">
                {filteredGroupList != undefined &&
                filteredGroupList.length > 0 ? (
                  filteredGroupList.map((obj, index) => {
                    return (
                      <div className="gds-principle-listing" key={obj.name}>
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
                                  obj.name,
                                  "GROUP"
                                )
                              }
                              menuPlacement="auto"
                              defaultValue={[
                                { label: obj.perm, value: obj.perm }
                              ]}
                              isDisabled={isDetailView && !isAdmin}
                            />
                          )}
                        />
                      </div>
                    );
                  })
                ) : (
                  <p className="mt-1">--</p>
                )}
              </Accordion.Body>
            </Accordion.Item>
          </Accordion>

          <Accordion className="mg-b-10" defaultActiveKey="0">
            <Accordion.Item>
              <Accordion.Header eventKey="1" data-id="panel" data-cy="panel">
                <div className="d-flex align-items-center gap-half">
                  <img src={roleColourIcon} height="30px" width="30px" />
                  Roles (
                  {filteredRoleList == undefined ? 0 : filteredRoleList.length})
                </div>
              </Accordion.Header>
              <Accordion.Body eventKey="1">
                {filteredRoleList?.length > 0 ? (
                  filteredRoleList.map((obj, index) => {
                    return (
                      <div className="gds-principle-listing" key={obj.name}>
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
                                  obj.name,
                                  "ROLE"
                                )
                              }
                              isDisabled={isDetailView && !isAdmin}
                              menuPlacement="auto"
                              defaultValue={[
                                { label: obj.perm, value: obj.perm }
                              ]}
                            />
                          )}
                        />
                      </div>
                    );
                  })
                ) : (
                  <p className="mt-1">--</p>
                )}
              </Accordion.Body>
            </Accordion.Item>
          </Accordion>
        </Card>
      </div>
      <Modal
        show={showAddPrincipalModal}
        onHide={toggleAddPrincipalModal}
        size="lg"
      >
        <Modal.Header closeButton>
          <h3 className="gds-header bold">Add Principals</h3>
        </Modal.Header>
        <div className="gds-form-input">
          <Field
            className="form-control"
            name="selectedPrinciple"
            render={({ input, meta }) => (
              <div>
                <Modal.Body>
                  <div className="d-flex gap-half">
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
                      placeholder="Type to select Principals"
                      data-name="usersSelect"
                      data-cy="usersSelect"
                    />
                    <Field
                      name="accessPermList"
                      className="form-control"
                      render={({ input }) => (
                        <Select
                          {...input}
                          theme={serviceSelectTheme}
                          styles={customStyles}
                          options={accessOptions}
                          onChange={(e) => setACL(e, input)}
                          ref={selectVisibilityLevelRef}
                          //value={selectedAccess}
                          menuPlacement="auto"
                          isClearable
                          placeholder="Visibility Level"
                        />
                      )}
                    />
                  </div>
                </Modal.Body>
                <Modal.Footer>
                  <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => toggleAddPrincipalModal()}
                  >
                    Close
                  </Button>
                  <Button
                    variant="primary"
                    size="sm"
                    onClick={() => {
                      userOgList;
                      if (
                        !selectedPrinciple ||
                        selectedPrinciple[0] == undefined ||
                        selectedPrinciple[0].value == undefined ||
                        selectedPrinciple.length === 0
                      ) {
                        toast.dismiss(toastId.current);
                        toastId.current = toast.error(
                          "Please select principal!!"
                        );
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
                  >
                    Add
                  </Button>
                </Modal.Footer>
              </div>
            )}
          />
        </div>
      </Modal>
    </div>
  );
};

export default PrinciplePermissionComp;
