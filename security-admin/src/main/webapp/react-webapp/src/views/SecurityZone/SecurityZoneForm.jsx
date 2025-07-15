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

import React, { useState, useEffect, useRef } from "react";
import { Form, Field } from "react-final-form";
import { Button, Row, Col } from "react-bootstrap";
import { useParams, useNavigate } from "react-router-dom";
import Select from "react-select";
import AsyncSelect from "react-select/async";
import { fetchApi } from "Utils/fetchAPI";
import {
  groupBy,
  findIndex,
  isEmpty,
  pickBy,
  find,
  maxBy,
  sortBy,
  map,
  cloneDeep
} from "lodash";
import { Table } from "react-bootstrap";
import { FieldArray } from "react-final-form-arrays";
import arrayMutators from "final-form-arrays";
import ModalResourceComp from "Views/Resources/ModalResourceComp";
import { RegexValidation } from "Utils/XAEnums";
import { toast } from "react-toastify";
import { commonBreadcrumb, serverError } from "Utils/XAUtils";
import {
  BlockUi,
  Loader,
  scrollToError,
  selectInputCustomStyles,
  selectInputCustomErrorStyles
} from "Components/CommonComponents";
import usePrompt from "Hooks/usePrompt";
import { getServiceDef } from "Utils/appState";

const noneOptions = {
  label: "None",
  value: "none"
};

const PromtDialog = (props) => {
  const { isDirtyField, isUnblock } = props;
  usePrompt("Are you sure you want to leave", isDirtyField && !isUnblock);
  return null;
};

const SecurityZoneForm = () => {
  const navigate = useNavigate();
  const params = useParams();
  const toastId = useRef(null);
  const { allServiceDefs } = cloneDeep(getServiceDef());
  const [services, setServices] = useState([]);
  const [zone, setZone] = useState({});
  const [resourceServiceDef, setResourceServiceDef] = useState({});
  const [resourceService, setResourceService] = useState({});
  const [resourceServicesOpt, setResourceServicesOpt] = useState([]);
  const [loader, setLoader] = useState(true);
  const [modelState, setModalstate] = useState({
    showModalResource: false,
    data: null,
    inputval: null,
    index: 0
  });
  const [preventUnBlock, setPreventUnblock] = useState(false);
  const [blockUI, setBlockUI] = useState(false);
  const [userLoading, setUserLoading] = useState(false);
  const [groupLoading, setGroupLoading] = useState(false);
  const [roleLoading, setRoleLoading] = useState(false);
  const [tagServiceLoading, setTagServiceLoading] = useState(false);
  const [defaultUserOptions, setDefaultUserOptions] = useState([]);
  const [defaultGroupOptions, setDefaultGroupOptions] = useState([]);
  const [defaultRoleOptions, setDefaultRoleOptions] = useState([]);
  const [defaultTagServiceOptions, setDefaultTagServiceOptions] = useState([]);

  useEffect(() => {
    fetchInitalData();
  }, [params.zoneId]);

  const validate = (values) => {
    const errors = {};
    if (!values.name) {
      errors.name = {
        required: true,
        text: "Required"
      };
    } else {
      if (
        !RegexValidation.NAME_VALIDATION.regexforNameValidation.test(
          values.name
        )
      ) {
        errors.name = {
          text: RegexValidation.NAME_VALIDATION.regexforNameValidationMessage
        };
      }
    }

    if (
      isEmpty(values?.adminUsers) &&
      isEmpty(values?.adminUserGroups) &&
      isEmpty(values?.adminRoles)
    ) {
      errors.adminRoles = {
        required: true,
        text: "Please provide atleast one admin user or group or role !"
      };
      errors.adminUserGroups = {
        required: true,
        text: ""
      };
      errors.adminUsers = {
        required: true,
        text: ""
      };
    }

    if (
      isEmpty(values?.auditUsers) &&
      isEmpty(values?.auditUserGroups) &&
      isEmpty(values?.auditRoles)
    ) {
      errors.auditRoles = {
        required: true,
        text: "Please provide atleast one audit user or group or role !"
      };
      errors.auditUserGroups = {
        required: true,
        text: ""
      };
      errors.auditUsers = {
        required: true,
        text: ""
      };
    }
    return errors;
  };

  const handleClose = () => {
    setModalstate({
      showModalResource: false,
      data: null,
      inputval: null,
      index: 0
    });
  };

  const fetchInitalData = async () => {
    await fetchResourceServices();
    await fetchZones();
  };

  const fetchResourceServices = async () => {
    const serviceDefnsResp = await fetchApi({
      url: "plugins/services"
    });

    const filterServices = serviceDefnsResp.data.services.filter(
      (obj) => obj.type !== "tag" && obj.type !== "kms"
    );

    setServices(filterServices);

    const servicesByType = groupBy(filterServices, "type");

    let resourceServices = [];

    for (let key of Object.keys(servicesByType)) {
      resourceServices.push({
        label: <span className="fw-bold text-body h6">{key}</span>,
        options: servicesByType[key].map((name) => {
          return { label: name.name, value: name.name };
        })
      });
    }

    setResourceServicesOpt(resourceServices);
  };

  const fetchZones = async () => {
    let zoneResp;

    if (params.zoneId !== undefined) {
      let zoneId = params.zoneId;

      try {
        zoneResp = await fetchApi({
          url: `zones/zones/${zoneId}`
        });
      } catch (error) {
        console.error(
          `Error occurred while fetching Zone or CSRF headers! ${error}`
        );
        if (error?.response?.data?.msgDesc) {
          toast.error(error.response.data.msgDesc);
        }
      }
      setZone(zoneResp?.data);
    }

    setLoader(false);
  };

  const renderResourcesModal = (resourceInput, resourceField) => {
    let filterServiceDef = find(allServiceDefs, [
      "name",
      resourceField.serviceType
    ]);
    let filterService = find(services, ["name", resourceField.serviceName]);

    for (const obj of filterServiceDef.resources) {
      obj.recursiveSupported = false;
      obj.excludesSupported = false;
      if (obj.level !== 10) {
        obj.mandatory = false;
      }
    }

    setModalstate({
      showModalResource: true,
      data: {},
      inputval: resourceInput,
      index: -1
    });

    setResourceServiceDef(filterServiceDef);
    setResourceService(filterService);
  };

  const editResourcesModal = (resourceIndex, resourceInput, resourceField) => {
    let editData = resourceInput.input.value[resourceIndex];
    let filterServiceDef = find(allServiceDefs, [
      "name",
      resourceField.serviceType
    ]);
    let filterService = find(services, ["name", resourceField.serviceName]);

    for (const obj of filterServiceDef.resources) {
      obj.recursiveSupported = false;
      obj.excludesSupported = false;
      if (obj.level !== 10) {
        obj.mandatory = false;
      }
    }

    setModalstate({
      showModalResource: true,
      data: editData,
      inputval: resourceInput,
      index: resourceIndex
    });

    setResourceServiceDef(filterServiceDef);
    setResourceService(filterService);
  };

  const handleSubmit = async (values) => {
    let zoneId;
    let apiMethod;
    let apiUrl;
    let apiSuccess;
    let apiError;
    let zoneData = {};
    let zoneResp;

    if (params.zoneId !== undefined) {
      zoneId = params.zoneId;
      apiMethod = "put";
      apiUrl = `/zones/zones/${zoneId}`;
      apiSuccess = "updated";
      zoneData["id"] = zoneId;
      apiError = "Error occurred while updating a zone";
    } else {
      apiMethod = "post";
      apiUrl = `/zones/zones`;
      apiSuccess = "created";
      apiError = "Error occurred while creating a zone!";
    }

    zoneData.name = values.name;
    zoneData.description = values.description || "";
    zoneData.adminUsers = [];
    if (values.adminUsers) {
      for (let key of Object.keys(values.adminUsers)) {
        zoneData.adminUsers.push(values.adminUsers[key].value);
      }
    }
    zoneData.adminUserGroups = [];

    if (values.adminUserGroups) {
      for (let key of Object.keys(values.adminUserGroups)) {
        zoneData.adminUserGroups.push(values.adminUserGroups[key].label || "");
      }
    }

    zoneData.auditUsers = [];

    if (values.auditUsers) {
      for (let key of Object.keys(values.auditUsers)) {
        zoneData.auditUsers.push(values.auditUsers[key].label || "");
      }
    }

    zoneData.auditUserGroups = [];
    if (values.auditUserGroups) {
      for (let key of Object.keys(values.auditUserGroups)) {
        zoneData.auditUserGroups.push(values.auditUserGroups[key].label || "");
      }
    }

    if (values.adminRoles) {
      zoneData.adminRoles = [];
      for (let key of Object.keys(values.adminRoles)) {
        zoneData.adminRoles.push(values.adminRoles[key].label || "");
      }
    }

    if (values.auditRoles) {
      zoneData.auditRoles = [];
      for (let key of Object.keys(values.auditRoles)) {
        zoneData.auditRoles.push(values.auditRoles[key].label || "");
      }
    }

    zoneData.tagServices = [];

    if (values.tagServices) {
      for (let key of Object.keys(values.tagServices)) {
        zoneData.tagServices.push(values.tagServices[key].label || "");
      }
    }

    zoneData.services = {};
    if (!isEmpty(Object.keys(values.tableList || {}))) {
      for (let key of Object.keys(values.tableList)) {
        let serviceName = values.tableList[key].serviceName;
        let resourcesName = values.tableList[key].resources;
        zoneData.services[serviceName] = {};
        zoneData.services[serviceName].resources = [];
        resourcesName.map((obj) => {
          let serviceResourceData = {};
          pickBy(obj, (key, val) => {
            if (
              obj[`value-${key.level}`] &&
              obj[`value-${key.level}`].length > 0
            ) {
              if (val.includes("resourceName")) {
                serviceResourceData[key.name] = obj[`value-${key.level}`].map(
                  (value) => value.value
                );
              }
            }
          });
          return zoneData.services[serviceName].resources.push(
            serviceResourceData
          );
        });
      }
    }
    if (params.zoneId) {
      zoneData = {
        ...zone,
        ...zoneData
      };
    }

    setPreventUnblock(true);
    try {
      setBlockUI(true);
      zoneResp = await fetchApi({
        url: apiUrl,
        method: apiMethod,
        data: zoneData
      });
      setBlockUI(false);
      toast.dismiss(toastId.current);
      toast.current = toast.success(
        `Success! Security zone ${apiSuccess} successfully`
      );
      navigate(`/zones/zone/${zoneResp.data.id}`);
    } catch (error) {
      setBlockUI(false);
      serverError(error);
      console.error(`Error occurred while ${apiError} Zone`);
    }
  };
  const EditFormData = () => {
    const zoneData = {};

    zoneData.name = zone.name;
    zoneData.description = zone.description;

    zoneData.adminUserGroups = [];
    if (zone.adminUserGroups) {
      zone.adminUserGroups.map((name) =>
        zoneData.adminUserGroups.push({ label: name, value: name })
      );
    }

    zoneData.adminUsers = [];
    if (zone.adminUsers) {
      zone.adminUsers.map((name) =>
        zoneData.adminUsers.push({ label: name, value: name })
      );
    }

    zoneData.auditUserGroups = [];
    if (zone.auditUserGroups) {
      zone.auditUserGroups.map((name) =>
        zoneData.auditUserGroups.push({ label: name, value: name })
      );
    }

    zoneData.auditUsers = [];
    if (zone.auditUsers) {
      zone.auditUsers.map((name) =>
        zoneData.auditUsers.push({ label: name, value: name })
      );
    }

    zoneData.adminRoles = [];
    if (zone.adminRoles) {
      zone.adminRoles.map((name) =>
        zoneData.adminRoles.push({ label: name, value: name })
      );
    }

    zoneData.auditRoles = [];
    if (zone.auditRoles) {
      zone.auditRoles.map((name) =>
        zoneData.auditRoles.push({ label: name, value: name })
      );
    }

    zoneData.tagServices = [];
    if (zone.tagServices) {
      zone.tagServices.map((name) =>
        zoneData.tagServices.push({ label: name, value: name })
      );
    }

    zoneData.resourceServices = [];
    if (zone.services) {
      Object.keys(zone.services).map((name) =>
        zoneData.resourceServices.push({ label: name, value: name })
      );
    }

    zoneData.tableList = [];
    for (let name of Object.keys(zone.services || {})) {
      let tableValues = {};

      tableValues["serviceName"] = name;

      let serviceType = find(services, ["name", name]);
      tableValues["serviceType"] = serviceType.type;

      let filterServiceDef = find(allServiceDefs, ["name", serviceType.type]);

      for (const obj of filterServiceDef.resources) {
        obj.recursiveSupported = false;
        obj.excludesSupported = false;
        if (obj.level !== 10) {
          obj.mandatory = false;
        }
      }

      tableValues["resources"] = [];
      if (!isEmpty(zone.services[name].resources)) {
        zone.services[name].resources?.map((obj) => {
          let serviceResource = {};
          let lastResourceLevel = [];
          Object.entries(obj).map(([key, value]) => {
            let setResources = find(filterServiceDef.resources, ["name", key]);
            serviceResource[`resourceName-${setResources.level}`] =
              setResources;
            serviceResource[`value-${setResources.level}`] = value.map((m) => {
              return { label: m, value: m };
            });
            lastResourceLevel.push({
              level: setResources.level,
              name: setResources.name
            });
          });
          lastResourceLevel = maxBy(lastResourceLevel, "level");
          let setLastResources = find(
            sortBy(filterServiceDef.resources, "itemId"),
            ["parent", lastResourceLevel.name]
          );
          if (setLastResources && setLastResources?.isValidLeaf) {
            serviceResource[`resourceName-${setLastResources.level}`] = {
              label: "None",
              value: "none"
            };
          }
          tableValues["resources"].push(serviceResource);
        });
      }

      zoneData.tableList.push(tableValues);
    }
    return zoneData;
  };

  const fetchUsersData = async (inputValue) => {
    let params = { isVisible: 1 };
    let usersOp = [];

    if (inputValue) {
      params["name"] = inputValue || "";
    }

    try {
      const userResp = await fetchApi({
        url: "xusers/lookup/users",
        params: params
      });
      usersOp = userResp.data?.vXStrings;
    } catch (error) {
      console.error(`Error occurred while fetching Users! ${error}`);
      serverError(error);
    }

    return map(usersOp, function (user) {
      return { value: user.value, label: user.value };
    });
  };

  const fetchGroupsData = async (inputValue) => {
    let params = { isVisible: 1 };
    let groupsOp = [];

    if (inputValue) {
      params["name"] = inputValue || "";
    }

    try {
      const groupResp = await fetchApi({
        url: "xusers/lookup/groups",
        params: params
      });
      groupsOp = groupResp.data?.vXStrings;
    } catch (error) {
      console.error(`Error occurred while fetching Groups! ${error}`);
      serverError(error);
    }

    return map(groupsOp, function (group) {
      return { label: group.value, value: group.value };
    });
  };

  const fetchRolesData = async (inputValue) => {
    let params = { roleNamePartial: inputValue || "" };
    let op = [];

    try {
      const roleResp = await fetchApi({
        url: "roles/roles",
        params: params
      });
      op = roleResp.data.roles;
    } catch (error) {
      console.error(`Error occurred while fetching Roles! ${error}`);
      serverError(error);
    }
    return op.map((obj) => ({
      label: obj.name,
      value: obj.name
    }));
  };

  const fetchTagServices = async (inputValue) => {
    let params = {};
    if (inputValue) {
      params["serviceNamePartial"] = inputValue || "";
      params["serviceType"] = "tag" || "";
    }
    const serviceResp = await fetchApi({
      url: "plugins/services",
      params: params
    });
    const filterServices = serviceResp.data.services.filter(
      (obj) => obj.type == "tag"
    );
    return filterServices.map(({ name }) => ({
      label: name,
      value: name
    }));
  };

  const resourceServicesOnChange = (e, input, values, push, remove) => {
    if (e.action == "select-option") {
      let serviceType = find(services, { name: e.option.value });
      push("tableList", {
        serviceName: e.option.value,
        serviceType: serviceType.type,
        resources: []
      });
    }

    if (e.action == "remove-value" || e.action == "pop-value") {
      let removeItemIndex = findIndex(input.value, [
        "value",
        e?.removedValue?.value
      ]);
      remove("tableList", removeItemIndex);
    }

    input.onChange(values);
  };

  const handleSave = () => {
    if (modelState.index === -1) {
      let add = [];
      add = modelState.inputval.input.value;
      add.push(modelState.data);
      modelState.inputval.input.onChange(add);
      handleClose();
    } else {
      let edit = modelState.inputval.input.value;
      edit[modelState.index] = modelState.data;
      modelState.inputval.input.onChange(edit);
      handleClose();
    }
  };

  const handleRemove = (resourceIndex, resourceInput) => {
    resourceInput.input.value.splice(resourceIndex, 1);
    handleClose();
  };

  const showResources = (resourceObj, serviceType) => {
    let data = {};
    let filterServiceDef = find(allServiceDefs, ["name", serviceType]);

    for (const obj of filterServiceDef.resources) {
      obj.recursiveSupported = false;
      obj.excludesSupported = false;
      if (obj.level !== 10) {
        obj.mandatory = false;
      }
    }

    const grpResources = groupBy(filterServiceDef.resources || [], "level");

    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    data.resources = {};

    for (const level of grpResourcesKeys) {
      if (
        resourceObj[`resourceName-${level}`] &&
        resourceObj[`resourceName-${level}`].value !== noneOptions.value
      ) {
        data.resources[resourceObj[`resourceName-${level}`].name] = {
          isExcludes: resourceObj[`isExcludesSupport-${level}`] || false,
          isRecursive: resourceObj[`isRecursiveSupport-${level}`] || false,
          values:
            resourceObj[`value-${level}`] !== undefined
              ? resourceObj[`value-${level}`].map(({ value }) => value)
              : ""
        };
      }
    }
    return Object.keys(data.resources).map((obj, index) => (
      <p key={index}>
        {data.resources[obj].values.length !== 0 ? (
          <>
            <strong>{`${obj} : `}</strong>
            <span>
              {data.resources[obj].values.map((val) => val).join(", ")}
            </span>
          </>
        ) : (
          ""
        )}
      </p>
    ));
  };

  const onFocusUserSelect = () => {
    setUserLoading(true);
    fetchUsersData().then((opts) => {
      setDefaultUserOptions(opts);
      setUserLoading(false);
    });
  };

  const onFocusGroupSelect = () => {
    setGroupLoading(true);
    fetchGroupsData().then((opts) => {
      setDefaultGroupOptions(opts);
      setGroupLoading(false);
    });
  };

  const onFocusRoleSelect = () => {
    setRoleLoading(true);
    fetchRolesData().then((opts) => {
      setDefaultRoleOptions(opts);
      setRoleLoading(false);
    });
  };

  const onFocusTagServiceSelect = () => {
    setTagServiceLoading(true);
    fetchTagServices().then((opts) => {
      setDefaultTagServiceOptions(opts);
      setTagServiceLoading(false);
    });
  };

  return (
    <React.Fragment>
      <div className="clearfix">
        <div className="header-wraper">
          <h3 className="wrap-header bold">
            {params.zoneId !== undefined ? `Edit` : `Create`} Zone
          </h3>
          {commonBreadcrumb(
            [
              "SecurityZone",
              params.zoneId !== undefined ? "ZoneEdit" : "ZoneCreate"
            ],
            params.zoneId
          )}
        </div>
      </div>
      {loader ? (
        <Loader />
      ) : (
        <div className="wrap">
          <Form
            onSubmit={handleSubmit}
            keepDirtyOnReinitialize={true}
            initialValuesEqual={() => true}
            initialValues={params.zoneId !== undefined && EditFormData()}
            mutators={{
              ...arrayMutators
            }}
            validate={validate}
            render={({
              handleSubmit,
              dirty,
              values,
              invalid,
              errors,
              form: {
                mutators: { push, remove }
              },
              form,
              submitting
            }) => (
              <Row>
                <PromtDialog isDirtyField={dirty} isUnblock={preventUnBlock} />
                <Col sm={12}>
                  <form
                    onSubmit={(event) => {
                      handleSubmit(event);
                    }}
                  >
                    <p className="form-header">Zone Details:</p>
                    <Field name="name">
                      {({ input, meta }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Zone Name *
                            </label>
                          </Col>
                          <Col xs={4}>
                            <input
                              {...input}
                              type="text"
                              id={
                                meta.error && meta.touched ? "isError" : "name"
                              }
                              className={
                                meta.error && meta.touched
                                  ? "form-control border-danger"
                                  : "form-control"
                              }
                              data-cy="name"
                            />
                            {meta.error && meta.touched && (
                              <span className="invalid-field">
                                {meta.error.text}
                              </span>
                            )}
                          </Col>
                        </Row>
                      )}
                    </Field>

                    <Field name="description">
                      {({ input }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Zone Description
                            </label>
                          </Col>
                          <Col xs={4}>
                            <textarea
                              {...input}
                              className="form-control"
                              data-cy="description"
                            />
                          </Col>
                        </Row>
                      )}
                    </Field>
                    <p className="form-header">Zone Administration:</p>

                    <Field
                      name="adminUsers"
                      render={({ input, meta }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Admin Users
                            </label>
                          </Col>
                          <Col xs={4}>
                            <AsyncSelect
                              {...input}
                              styles={
                                meta.error && meta.touched
                                  ? selectInputCustomErrorStyles
                                  : selectInputCustomStyles
                              }
                              id={
                                meta.error && meta.touched
                                  ? "isError"
                                  : "auditUsers"
                              }
                              cacheOptions
                              loadOptions={fetchUsersData}
                              onFocus={() => {
                                onFocusUserSelect();
                              }}
                              defaultOptions={defaultUserOptions}
                              noOptionsMessage={() =>
                                userLoading ? "Loading..." : "No options"
                              }
                              isMulti
                              components={{
                                DropdownIndicator: () => null,
                                IndicatorSeparator: () => null
                              }}
                              isClearable={true}
                              placeholder="Select Users"
                            />
                          </Col>
                        </Row>
                      )}
                    />

                    <Field
                      name="adminUserGroups"
                      render={({ input, meta }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Admin Usergroups
                            </label>
                          </Col>
                          <Col xs={4}>
                            <AsyncSelect
                              {...input}
                              styles={
                                meta.error && meta.touched
                                  ? selectInputCustomErrorStyles
                                  : selectInputCustomStyles
                              }
                              id={
                                meta.error && meta.touched
                                  ? "isError"
                                  : "adminUserGroups"
                              }
                              cacheOptions
                              loadOptions={fetchGroupsData}
                              onFocus={() => {
                                onFocusGroupSelect();
                              }}
                              defaultOptions={defaultGroupOptions}
                              noOptionsMessage={() =>
                                groupLoading ? "Loading..." : "No options"
                              }
                              isMulti
                              components={{
                                DropdownIndicator: () => null,
                                IndicatorSeparator: () => null
                              }}
                              isClearable={true}
                              placeholder="Select Groups"
                            />
                          </Col>
                        </Row>
                      )}
                    />

                    <Field
                      name="adminRoles"
                      render={({ input, meta }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Admin Roles
                            </label>
                          </Col>
                          <Col xs={4}>
                            <AsyncSelect
                              {...input}
                              styles={
                                meta.error && meta.touched
                                  ? selectInputCustomErrorStyles
                                  : selectInputCustomStyles
                              }
                              id={
                                meta.error && meta.touched
                                  ? "isError"
                                  : "adminRoles"
                              }
                              cacheOptions
                              loadOptions={fetchRolesData}
                              onFocus={() => {
                                onFocusRoleSelect();
                              }}
                              defaultOptions={defaultRoleOptions}
                              noOptionsMessage={() =>
                                roleLoading ? "Loading..." : "No options"
                              }
                              isMulti
                              components={{
                                DropdownIndicator: () => null,
                                IndicatorSeparator: () => null
                              }}
                              isClearable={true}
                              placeholder="Select Roles"
                            />
                            {meta.touched && meta.error && (
                              <span className="invalid-field">
                                {meta.error.text}
                              </span>
                            )}
                          </Col>
                        </Row>
                      )}
                    />

                    <Field
                      name="auditUsers"
                      render={({ input, meta }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Auditor Users
                            </label>
                          </Col>
                          <Col xs={4}>
                            <AsyncSelect
                              {...input}
                              styles={
                                meta.error && meta.touched
                                  ? selectInputCustomErrorStyles
                                  : selectInputCustomStyles
                              }
                              id={
                                meta.error && meta.touched
                                  ? "isError"
                                  : "auditUsers"
                              }
                              cacheOptions
                              loadOptions={fetchUsersData}
                              onFocus={() => {
                                onFocusUserSelect();
                              }}
                              defaultOptions={defaultUserOptions}
                              noOptionsMessage={() =>
                                userLoading ? "Loading..." : "No options"
                              }
                              isMulti
                              components={{
                                DropdownIndicator: () => null,
                                IndicatorSeparator: () => null
                              }}
                              isClearable={true}
                              placeholder="Select Users"
                            />
                          </Col>
                        </Row>
                      )}
                    />

                    <Field
                      name="auditUserGroups"
                      render={({ input, meta }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Auditor Usergroups
                            </label>
                          </Col>
                          <Col xs={4}>
                            <AsyncSelect
                              {...input}
                              styles={
                                meta.error && meta.touched
                                  ? selectInputCustomErrorStyles
                                  : selectInputCustomStyles
                              }
                              id={
                                meta.error && meta.touched
                                  ? "isError"
                                  : "auditUserGroups"
                              }
                              cacheOptions
                              loadOptions={fetchGroupsData}
                              onFocus={() => {
                                onFocusGroupSelect();
                              }}
                              defaultOptions={defaultGroupOptions}
                              noOptionsMessage={() =>
                                groupLoading ? "Loading..." : "No options"
                              }
                              isMulti
                              components={{
                                DropdownIndicator: () => null,
                                IndicatorSeparator: () => null
                              }}
                              isClearable={true}
                              placeholder="Select Groups"
                            />
                          </Col>
                        </Row>
                      )}
                    />

                    <Field
                      name="auditRoles"
                      render={({ input, meta }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Auditor Roles
                            </label>
                          </Col>
                          <Col xs={4}>
                            <AsyncSelect
                              {...input}
                              styles={
                                meta.error && meta.touched
                                  ? selectInputCustomErrorStyles
                                  : selectInputCustomStyles
                              }
                              id={
                                meta.error && meta.touched
                                  ? "isError"
                                  : "auditRoles"
                              }
                              cacheOptions
                              loadOptions={fetchRolesData}
                              onFocus={() => {
                                onFocusRoleSelect();
                              }}
                              defaultOptions={defaultRoleOptions}
                              noOptionsMessage={() =>
                                roleLoading ? "Loading..." : "No options"
                              }
                              isMulti
                              components={{
                                DropdownIndicator: () => null,
                                IndicatorSeparator: () => null
                              }}
                              isClearable={true}
                              placeholder="Select Roles"
                            />
                            {meta.error && meta.touched && (
                              <span className="invalid-field">
                                {meta.error.text}
                              </span>
                            )}
                          </Col>
                        </Row>
                      )}
                    />

                    <p className="form-header">Services:</p>
                    <Field
                      name="tagServices"
                      render={({ input }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Tag Services
                            </label>
                          </Col>
                          <Col xs={6}>
                            <AsyncSelect
                              {...input}
                              cacheOptions
                              loadOptions={fetchTagServices}
                              onFocus={() => {
                                onFocusTagServiceSelect();
                              }}
                              defaultOptions={defaultTagServiceOptions}
                              noOptionsMessage={() =>
                                tagServiceLoading ? "Loading..." : "No options"
                              }
                              isMulti
                              components={{
                                DropdownIndicator: () => null,
                                IndicatorSeparator: () => null
                              }}
                              isClearable={true}
                              placeholder="Select Tag Services"
                              styles={selectInputCustomStyles}
                            />
                          </Col>
                        </Row>
                      )}
                    />

                    <Field
                      name="resourceServices"
                      render={({ input }) => (
                        <Row className="form-group">
                          <Col xs={3}>
                            <label className="form-label float-end">
                              Resource Services
                            </label>
                          </Col>
                          <Col xs={6}>
                            <Select
                              {...input}
                              onChange={(values, e) =>
                                resourceServicesOnChange(
                                  e,
                                  input,
                                  values,
                                  push,
                                  remove
                                )
                              }
                              isMulti
                              components={{
                                DropdownIndicator: () => null,
                                IndicatorSeparator: () => null
                              }}
                              options={resourceServicesOpt}
                              isClearable={false}
                              isSearchable={true}
                              placeholder="Select Resource Services"
                              styles={selectInputCustomStyles}
                            />
                          </Col>
                        </Row>
                      )}
                    />

                    <Table bordered>
                      <thead>
                        <tr>
                          <th className="p-3 mb-2 bg-white text-dark  align-middle text-center">
                            Service Name
                          </th>
                          <th className="p-3 mb-2 bg-white text-dark align-middle text-center">
                            Service Type
                          </th>
                          <th className="p-3 mb-2 bg-white text-dark align-middle text-center">
                            Resource
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        <FieldArray name="tableList">
                          {({ fields }) =>
                            fields.value && fields.value.length > 0 ? (
                              fields.map((name, index) => (
                                <tr className="bg-white" key={index}>
                                  <td className="align-middle" width="20%">
                                    {fields.value[index].serviceName}
                                  </td>
                                  <td className="align-middle" width="20%">
                                    {fields.value[index].serviceType}
                                  </td>
                                  <td
                                    className="text-center"
                                    key={name}
                                    width="33%"
                                    height="55px"
                                  >
                                    <Field
                                      name={`${name}.resources`}
                                      render={(input) => (
                                        <React.Fragment>
                                          {input.input.value &&
                                          input.input.value.length > 0
                                            ? input.input.value.map(
                                                (
                                                  resourceObj,
                                                  resourceIndex
                                                ) => (
                                                  <div
                                                    className="resource-group text-break"
                                                    key={resourceIndex}
                                                  >
                                                    <Row>
                                                      <Col xs={9}>
                                                        <span className="m-t-xs">
                                                          {showResources(
                                                            resourceObj,
                                                            fields.value[index]
                                                              .serviceType
                                                          )}
                                                        </span>
                                                      </Col>
                                                      <Col xs={3}>
                                                        <Button
                                                          title="edit"
                                                          className="btn btn-primary m-r-xs btn-mini m-r-5"
                                                          size="sm"
                                                          onClick={() =>
                                                            editResourcesModal(
                                                              resourceIndex,
                                                              input,
                                                              fields.value[
                                                                index
                                                              ]
                                                            )
                                                          }
                                                          data-action="editResource"
                                                          data-cy="editResource"
                                                        >
                                                          <i className="fa-fw fa fa-edit"></i>
                                                        </Button>
                                                        <Button
                                                          title="delete"
                                                          className="btn btn-danger active  btn-mini"
                                                          size="sm"
                                                          onClick={() =>
                                                            handleRemove(
                                                              resourceIndex,
                                                              input
                                                            )
                                                          }
                                                          data-action="delete"
                                                          data-cy="delete"
                                                        >
                                                          <i className="fa-fw fa fa-remove"></i>
                                                        </Button>
                                                      </Col>
                                                    </Row>
                                                  </div>
                                                )
                                              )
                                            : ""}
                                          <div>
                                            <Button
                                              title="add"
                                              className="btn btn-mini pull-left"
                                              variant="outline-secondary"
                                              size="sm"
                                              onClick={() =>
                                                renderResourcesModal(
                                                  input,
                                                  fields.value[index]
                                                )
                                              }
                                              data-action="addResource"
                                              data-cy="addResource"
                                            >
                                              <i className="fa-fw fa fa-plus "></i>
                                            </Button>
                                          </div>
                                        </React.Fragment>
                                      )}
                                    />
                                  </td>
                                </tr>
                              ))
                            ) : (
                              <tr>
                                <td
                                  colSpan="3"
                                  className="text-center text-secondary"
                                >
                                  <h6>No Zone Data Found!!</h6>
                                </td>
                              </tr>
                            )
                          }
                        </FieldArray>
                      </tbody>
                    </Table>

                    <Row className="form-actions">
                      <Col sm={{ span: 9, offset: 3 }}>
                        <Button
                          variant="primary"
                          onClick={() => {
                            if (invalid) {
                              let selector =
                                document.getElementById("isError") ||
                                document.getElementById(
                                  Object.keys(errors)[0]
                                ) ||
                                document.querySelector(
                                  `input[name=${Object.keys(errors)[0]}]`
                                ) ||
                                document.querySelector(
                                  `input[id=${Object.keys(errors)[0]}]`
                                ) ||
                                document.querySelector(
                                  `span[className="invalid-field"]`
                                );
                              scrollToError(selector);
                            }
                            handleSubmit(values);
                          }}
                          size="sm"
                          disabled={submitting}
                          data-id="save"
                          data-cy="save"
                        >
                          Save
                        </Button>
                        <Button
                          variant="secondary"
                          type="button"
                          size="sm"
                          onClick={() => {
                            form.reset;
                            setPreventUnblock(true);
                            navigate(-1);
                          }}
                          data-id="cancel"
                          data-cy="cancel"
                        >
                          Cancel
                        </Button>
                      </Col>
                    </Row>
                  </form>
                  <ModalResourceComp
                    serviceDetails={resourceService}
                    serviceCompDetails={resourceServiceDef}
                    cancelButtonText="Cancel"
                    actionButtonText="Submit"
                    modelState={modelState}
                    handleSave={handleSave}
                    handleClose={handleClose}
                  />
                </Col>
              </Row>
            )}
          />
          <BlockUi isUiBlock={blockUI} />
        </div>
      )}
    </React.Fragment>
  );
};

export default SecurityZoneForm;
