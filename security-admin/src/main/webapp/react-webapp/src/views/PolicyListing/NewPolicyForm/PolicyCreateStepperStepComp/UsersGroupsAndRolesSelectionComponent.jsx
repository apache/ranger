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

import React, { useState, useMemo, useCallback } from "react";
import {
  Modal,
  Button,
  Form,
  Row,
  Col,
  Tab,
  Tabs,
  Table
} from "react-bootstrap";
import AsyncSelect from "react-select/async";
import { debounce, isEmpty } from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import userGreyIcon from "Images/user-grey.svg";
import groupGreyIcon from "Images/group-grey.svg";
import roleGreyIcon from "Images/role-grey.svg";
import userColourIcon from "Images/user-colour.svg";
import groupColourIcon from "Images/group-colour.svg";
import roleColourIcon from "Images/role-colour.svg";

// Normalise any principal into { value: "USER:admin", name: "admin", type: "USER" }
const normalisePrincipal = (obj) => ({
  value: obj.value || `${obj.type}:${obj.name}`,
  name: obj.name || obj.value,
  type: obj.type
});

const UsersGroupsAndRolesSelectionComponent = ({
  value = { users: [], groups: [], roles: [] },
  onChange,
  name,
  placeholder = "Select users, groups, roles...",
  showModal = true
}) => {
  const [modalShow, setModalShow] = useState(false);
  const [draftSelectedItems, setDraftSelectedItems] = useState({
    users: [],
    groups: [],
    roles: []
  });
  const [activeTab, setActiveTab] = useState("all");
  const [searchInput, setSearchInput] = useState("");
  const [checkedValues, setCheckedValues] = useState(new Set());

  // Saved (committed) value from form state
  const savedSelectedItems = {
    users: Array.isArray(value?.users) ? value.users : [],
    groups: Array.isArray(value?.groups) ? value.groups : [],
    roles: Array.isArray(value?.roles) ? value.roles : []
  };

  // Flat list for AsyncSelect - use userList if available (preserves selection order),
  // otherwise fall back to users + groups + roles concatenated
  const flatSavedItems = useMemo(() => {
    if (Array.isArray(value?.userList) && value.userList.length > 0) {
      return value.userList;
    }
    return [
      ...savedSelectedItems.users,
      ...savedSelectedItems.groups,
      ...savedSelectedItems.roles
    ];
  }, [value]);

  const flatDraftItems = useMemo(() => {
    return [
      ...draftSelectedItems.users,
      ...draftSelectedItems.groups,
      ...draftSelectedItems.roles
    ];
  }, [draftSelectedItems]);

  const getIcon = useCallback((type, colour = false) => {
    if (type === "USER") return colour ? userColourIcon : userGreyIcon;
    if (type === "GROUP") return colour ? groupColourIcon : groupGreyIcon;
    if (type === "ROLE") return colour ? roleColourIcon : roleGreyIcon;
    return userGreyIcon;
  }, []);

  // AsyncSelect option label (rendered at display time – no JSX stored in form state)
  const renderOptionLabel = useCallback(
    (option) => (
      <div className="d-flex align-items-center">
        <img
          src={getIcon(option.type)}
          height="20px"
          width="20px"
          alt={option.type}
        />
        <span className="ms-2">{option.name}</span>
      </div>
    ),
    [getIcon]
  );

  // API fetch for AsyncSelect
  const fetchPrincipals = async (inputValue) => {
    if (!inputValue || inputValue.length < 1) return [];
    try {
      const resp = await fetchApi({
        url: "xusers/lookup/principals",
        params: { name: inputValue }
      });
      return (resp.data || []).map((obj) =>
        normalisePrincipal({
          value: `${obj.type}:${obj.name}`,
          name: obj.name,
          type: obj.type
        })
      );
    } catch {
      return [];
    }
  };

  const debouncedFetch = useMemo(
    () =>
      debounce((inputValue, callback) => {
        fetchPrincipals(inputValue).then(callback);
      }, 300),
    []
  );

  const loadOptions = (inputValue, callback) => {
    if (isEmpty(inputValue)) {
      callback([]);
      return;
    }
    debouncedFetch(inputValue, callback);
  };

  // AsyncSelect change – store flat ordered list + split by type
  const handleChange = (selected) => {
    const items = (selected || []).map(normalisePrincipal);

    if (onChange) {
      onChange({
        userList: items, // flat ordered list (preserves selection order)
        users: items.filter((i) => i.type === "USER"),
        groups: items.filter((i) => i.type === "GROUP"),
        roles: items.filter((i) => i.type === "ROLE")
      });
    }
  };

  // Open modal – copy current saved selections into draft
  const handleViewManage = () => {
    setDraftSelectedItems({
      users: [...savedSelectedItems.users],
      groups: [...savedSelectedItems.groups],
      roles: [...savedSelectedItems.roles]
    });
    setCheckedValues(new Set());
    setSearchInput("");
    setActiveTab("all");
    setModalShow(true);
  };

  const handleSaveModal = () => {
    if (onChange) onChange(draftSelectedItems);
    setModalShow(false);
  };

  const handleCancelModal = () => {
    setDraftSelectedItems({ users: [], groups: [], roles: [] });
    setModalShow(false);
  };

  // Remove a single item – track order removal
  const handleRemoveItem = (item) => {
    const key =
      item.type === "USER"
        ? "users"
        : item.type === "GROUP"
          ? "groups"
          : "roles";
    setDraftSelectedItems((prev) => ({
      ...prev,
      [key]: prev[key].filter((i) => i.value !== item.value)
    }));

    // Remove from order tracking
    setCheckedValues((prev) => {
      const s = new Set(prev);
      s.delete(item.value);
      return s;
    });
  };

  // Items visible in the modal – ordered by selection
  const getFilteredItems = () => {
    const typeMap = { users: "USER", groups: "GROUP", roles: "ROLE" };
    let items =
      activeTab === "all"
        ? flatDraftItems
        : flatDraftItems.filter((i) => i.type === typeMap[activeTab]);

    if (searchInput.trim()) {
      const q = searchInput.toLowerCase();
      items = items.filter((i) => i.name.toLowerCase().includes(q));
    }
    return items;
  };

  // Toggle a row checkbox
  const handleToggleCheck = (itemValue) => {
    setCheckedValues((prev) => {
      const s = new Set(prev);
      if (s.has(itemValue)) s.delete(itemValue);
      else s.add(itemValue);
      return s;
    });
  };

  // Select-all / deselect-all for visible rows
  const handleSelectAll = (checked) => {
    if (checked) {
      setCheckedValues(new Set(filteredItems.map((i) => i.value)));
    } else {
      setCheckedValues(new Set());
    }
  };

  // Delete all currently checked rows
  const handleDeleteChecked = () => {
    setDraftSelectedItems((prev) => ({
      users: prev.users.filter((i) => !checkedValues.has(i.value)),
      groups: prev.groups.filter((i) => !checkedValues.has(i.value)),
      roles: prev.roles.filter((i) => !checkedValues.has(i.value))
    }));
    setCheckedValues(new Set());
  };

  const counts = {
    all: flatDraftItems.length,
    users: draftSelectedItems.users.length,
    groups: draftSelectedItems.groups.length,
    roles: draftSelectedItems.roles.length
  };

  const filteredItems = getFilteredItems();
  const allChecked =
    filteredItems.length > 0 &&
    filteredItems.every((i) => checkedValues.has(i.value));
  const someChecked = filteredItems.some((i) => checkedValues.has(i.value));
  const checkedCount = filteredItems.filter((i) =>
    checkedValues.has(i.value)
  ).length;

  const serviceSelectTheme = (theme) => ({
    ...theme,
    colors: { ...theme.colors, primary: "#0081ab" }
  });

  return (
    <>
      <Row>
        <Col>
          <AsyncSelect
            name={name}
            value={flatSavedItems}
            onChange={handleChange}
            loadOptions={loadOptions}
            isMulti
            placeholder={placeholder}
            noOptionsMessage={({ inputValue }) =>
              !inputValue
                ? "Type to search users, groups, or roles..."
                : "No results found"
            }
            cacheOptions
            theme={serviceSelectTheme}
            formatOptionLabel={renderOptionLabel}
            getOptionValue={(option) => option.value}
            loadingMessage={() => "Loading..."}
            isClearable={false}
          />
        </Col>
        <Col className="column-3-fixed">
          {showModal && (
            <Button
              variant="link"
              onClick={handleViewManage}
              className="text-primary"
              style={{ whiteSpace: "nowrap" }}
            >
              View &amp; Manage All
            </Button>
          )}
        </Col>
      </Row>

      <Modal
        show={modalShow}
        onHide={handleCancelModal}
        size="lg"
        backdrop="static"
      >
        <Modal.Header closeButton>
          <Modal.Title>Manage Users, Groups &amp; Roles</Modal.Title>
        </Modal.Header>

        <Modal.Body>
          {/* Search */}
          <Form.Group className="mb-3">
            <Form.Control
              type="text"
              placeholder="Search by name..."
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
            />
          </Form.Group>

          {/* Tabs */}
          <Tabs
            activeKey={activeTab}
            onSelect={(k) => setActiveTab(k)}
            className="mb-3"
          >
            <Tab
              eventKey="all"
              title={`All${counts.all > 0 ? ` (${counts.all})` : ""}`}
            />
            <Tab
              eventKey="users"
              title={
                <span className="d-flex align-items-center gap-1">
                  <img
                    src={userColourIcon}
                    height="16px"
                    width="16px"
                    alt="Users"
                  />
                  {`Users${counts.users > 0 ? ` (${counts.users})` : ""}`}
                </span>
              }
            />
            <Tab
              eventKey="groups"
              title={
                <span className="d-flex align-items-center gap-1">
                  <img
                    src={groupColourIcon}
                    height="16px"
                    width="16px"
                    alt="Groups"
                  />
                  {`Groups${counts.groups > 0 ? ` (${counts.groups})` : ""}`}
                </span>
              }
            />
            <Tab
              eventKey="roles"
              title={
                <span className="d-flex align-items-center gap-1">
                  <img
                    src={roleColourIcon}
                    height="16px"
                    width="16px"
                    alt="Roles"
                  />
                  {`Roles${counts.roles > 0 ? ` (${counts.roles})` : ""}`}
                </span>
              }
            />
          </Tabs>

          {/* Empty state */}
          {filteredItems.length === 0 && (
            <div className="text-center text-muted py-4">
              {searchInput.trim()
                ? `No results for "${searchInput}"`
                : `No ${activeTab === "all" ? "users, groups, or roles" : activeTab} selected yet. Use the search box on the form to add.`}
            </div>
          )}

          {/* Table – only selected items; removing a row makes it disappear */}
          {filteredItems.length > 0 && (
            <>
              {/* Bulk-action toolbar – visible only when rows are checked */}
              {someChecked && (
                <div className="d-flex align-items-center gap-3 mb-2 px-1">
                  <span className="text-muted small">
                    {checkedCount} item{checkedCount !== 1 ? "s" : ""} selected
                  </span>
                  <Button
                    variant="danger"
                    size="sm"
                    onClick={handleDeleteChecked}
                  >
                    <i className="fa fa-trash me-1" />
                    Delete Selected
                  </Button>
                </div>
              )}

              <div style={{ maxHeight: "350px", overflowY: "auto" }}>
                <Table bordered hover size="sm" className="mb-0">
                  <thead className="table-light sticky-top">
                    <tr>
                      <th style={{ width: "40px" }} className="text-center">
                        <Form.Check
                          type="checkbox"
                          checked={allChecked}
                          onChange={(e) => handleSelectAll(e.target.checked)}
                          title="Select / deselect all"
                        />
                      </th>
                      <th>Name</th>
                      <th style={{ width: "60px" }} className="text-center">
                        Remove
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredItems.map((item) => {
                      const checked = checkedValues.has(item.value);
                      return (
                        <tr
                          key={item.value}
                          className={checked ? "table-primary" : ""}
                        >
                          <td className="text-center align-middle">
                            <Form.Check
                              type="checkbox"
                              checked={checked}
                              onChange={() => handleToggleCheck(item.value)}
                            />
                          </td>
                          <td className="align-middle">
                            <span className="d-flex align-items-center gap-2">
                              <img
                                src={getIcon(item.type, true)}
                                height="18px"
                                width="18px"
                                alt={item.type}
                              />
                              <span className="fw-medium">{item.name}</span>
                            </span>
                          </td>
                          <td className="text-center align-middle">
                            <Button
                              variant="link"
                              size="sm"
                              className="text-danger p-0"
                              onClick={() => handleRemoveItem(item)}
                              title="Remove"
                            >
                              <i className="fa fa-trash" />
                            </Button>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </Table>
              </div>
            </>
          )}
        </Modal.Body>

        <Modal.Footer>
          <Button variant="secondary" onClick={handleCancelModal}>
            Cancel
          </Button>
          <Button variant="primary" onClick={handleSaveModal}>
            Save
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
};

export default UsersGroupsAndRolesSelectionComponent;
