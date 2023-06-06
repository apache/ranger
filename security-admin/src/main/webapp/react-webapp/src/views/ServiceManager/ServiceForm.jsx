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

import React, { Component } from "react";
import { Button, Modal, Table, Row, Col } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import { toast } from "react-toastify";
import arrayMutators from "final-form-arrays";
import { FieldArray } from "react-final-form-arrays";
import Select from "react-select";
import AsyncSelect from "react-select/async";
import { RegexValidation } from "Utils/XAEnums";
import { fetchApi } from "Utils/fetchAPI";
import ServiceAuditFilter from "./ServiceAuditFilter";
import TestConnection from "./TestConnection";
import {
  commonBreadcrumb,
  serverError,
  updateTagActive
} from "../../utils/XAUtils";
import {
  BlockUi,
  Condition,
  CustomPopover,
  Loader,
  scrollToError
} from "../../components/CommonComponents";
import {
  difference,
  flatMap,
  groupBy,
  keys,
  map,
  find,
  reject,
  uniq,
  isEmpty,
  isUndefined,
  has,
  split,
  without,
  maxBy
} from "lodash";
import withRouter from "Hooks/withRouter";
import { RangerPolicyType } from "../../utils/XAEnums";
import { getServiceDef } from "../../utils/appState";

class ServiceForm extends Component {
  constructor(props) {
    super(props);
    this.serviceDefData = getServiceDef();
    this.configsJson = {};
    this.initialValuesObj = {
      isEnabled: "true",
      configs: {},
      customConfigs: [undefined],
      auditFilters: []
    };
    this.state = {
      serviceDef: {},
      service: {},
      tagService: [],
      editInitialValues: {},
      usersDataRef: null,
      groupsDataRef: null,
      rolesDataRef: null,
      showDelete: false,
      loader: true,
      blockUI: false,
      defaultTagOptions: [],
      loadingOptions: false
    };
  }

  showDeleteModal = () => {
    this.setState({ showDelete: true });
  };

  hideDeleteModal = () => {
    this.setState({ showDelete: false });
  };

  componentDidMount() {
    this.fetchServiceDef();
  }

  onSubmit = async (values) => {
    let serviceId;
    let apiMethod;
    let apiUrl;
    let apiSuccess;
    let apiError;

    if (this.props.params.serviceId !== undefined) {
      serviceId = this.props.params.serviceId;
      apiMethod = "put";
      apiUrl = `plugins/services/${serviceId}`;
      apiSuccess = "updated";
      apiError = "Error occurred while updating a service";
    } else {
      apiMethod = "post";
      apiUrl = `plugins/services`;
      apiSuccess = "created";
      apiError = "Error occurred while creating a service!";
    }

    try {
      this.setState({ blockUI: true });
      await fetchApi({
        url: apiUrl,
        method: apiMethod,
        data: this.getServiceConfigsToSave(values)
      });
      this.setState({ blockUI: false });
      toast.success(`Successfully ${apiSuccess} the service`);
      if (this.props?.location?.state != "services") {
        if (this?.props?.params?.serviceId !== undefined) {
          this.props.navigate(
            `/service/${this.props.params.serviceId}/policies/${RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value}`
          );
        } else {
          return this.props.navigate(
            this.state?.serviceDef?.name === "tag"
              ? "/policymanager/tag"
              : "/policymanager/resource"
          );
        }
      } else {
        this.props.navigate(
          this.state?.serviceDef?.name === "tag"
            ? "/policymanager/tag"
            : "/policymanager/resource"
        );
      }
    } catch (error) {
      this.setState({ blockUI: false });
      serverError(error);
      console.error(apiError);
    }
  };

  getServiceConfigsToSave = (values) => {
    const serviceJson = {};

    if (this.props.params.serviceId !== undefined) {
      serviceJson["id"] = this.props.params.serviceId;
    }

    serviceJson["name"] = values.name;
    serviceJson["displayName"] = values.displayName;
    serviceJson["description"] = values.description;
    serviceJson["type"] = this.state.serviceDef.name;
    serviceJson["tagService"] =
      values.tagService == null ? "" : values.tagService.value;
    serviceJson["isEnabled"] = values.isEnabled === "true";

    serviceJson["configs"] = {};
    for (const config in values.configs) {
      for (const jsonConfig in this.configsJson) {
        if (config === this.configsJson[jsonConfig]) {
          serviceJson["configs"][jsonConfig] = values.configs[config];
        }
      }
    }

    if (values.customConfigs !== undefined) {
      values.customConfigs.map((config) => {
        config !== undefined &&
          (serviceJson["configs"][config.name] = config.value);
      });
    }

    if (values.isAuditFilter) {
      serviceJson["configs"]["ranger.plugin.audit.filters"] =
        this.getAuditFiltersToSave(values.auditFilters);
    } else {
      serviceJson["configs"]["ranger.plugin.audit.filters"] = "";
    }

    return { ...this.state.service, ...serviceJson };
  };

  getAuditFiltersToSave = (auditFilters) => {
    let auditFiltersArray = [];
    let serviceDef = this.state.serviceDef;

    auditFilters.map((item) => {
      let obj = {};

      if (!isUndefined(item) && !isEmpty(item)) {
        Object.entries(item).map(([key, value]) => {
          if (key === "isAudited") {
            obj.isAudited = value === "true";
          }

          if (key === "accessResult") {
            obj.accessResult = value.value;
          }

          if (key === "resources" && !isEmpty(value)) {
            obj.resources = {};

            let levels = uniq(map(serviceDef.resources, "level"));

            levels.map((level) => {
              let resourceObj = find(serviceDef.resources, {
                level: level,
                name: value[`resourceName-${level}`]?.name
              });
              if (
                value[`resourceName-${level}`] !== undefined &&
                value[`value-${level}`] !== undefined
              ) {
                obj.resources[value[`resourceName-${level}`].name] = {
                  values: map(value[`value-${level}`], "value")
                };

                if (
                  value[`isRecursiveSupport-${level}`] !== undefined &&
                  resourceObj.recursiveSupported
                ) {
                  obj.resources[
                    value[`resourceName-${level}`].name
                  ].isRecursive = value[`isRecursiveSupport-${level}`];
                } else if (
                  value[`isRecursiveSupport-${level}`] === undefined &&
                  resourceObj.recursiveSupported
                ) {
                  obj.resources[
                    value[`resourceName-${level}`].name
                  ].isRecursive = resourceObj.recursiveSupported;
                }

                if (
                  value[`isExcludesSupport-${level}`] !== undefined &&
                  resourceObj.excludesSupported
                ) {
                  obj.resources[
                    value[`resourceName-${level}`].name
                  ].isExcludes = value[`isExcludesSupport-${level}`];
                } else if (
                  value[`isExcludesSupport-${level}`] === undefined &&
                  resourceObj.excludesSupported
                ) {
                  obj.resources[
                    value[`resourceName-${level}`].name
                  ].isExcludes = resourceObj.excludesSupported;
                }
              }
            });
          }

          if (key === "actions") {
            obj.actions = map(value, "value");
          }

          if (key === "accessTypes") {
            if (serviceDef.name == "tag") {
              obj.accessTypes = flatMap(map(value.tableList, "permission"));
            } else {
              obj.accessTypes = map(value, "value");
            }
          }

          if (key === "users") {
            obj.users = map(value, "value");
          }

          if (key === "groups") {
            obj.groups = map(value, "value");
          }

          if (key === "roles") {
            obj.roles = map(value, "value");
          }
        });

        if (!has(obj, "isAudited")) {
          obj.isAudited = true;
        }

        auditFiltersArray.push(obj);
      }
    });

    return JSON.stringify(auditFiltersArray).replace(/"/g, "'");
  };

  fetchServiceDef = () => {
    const serviceJson = this.initialValuesObj;
    let serviceDef;
    let serviceDefId = this.props.params.serviceDefId;
    let isTagView = false;
    serviceDef = this.serviceDefData.allServiceDefs.find((servicedef) => {
      return servicedef.id == serviceDefId;
    });
    isTagView =
      serviceDef?.name !== undefined && serviceDef?.name === "tag"
        ? true
        : false;
    updateTagActive(isTagView);

    if (serviceDef.resources !== undefined) {
      for (const obj of serviceDef.resources) {
        if (
          this.props.params.serviceId === undefined &&
          obj.lookupSupported !== undefined &&
          obj.lookupSupported
        ) {
          obj.lookupSupported = false;
        }
      }
    }

    let auditFilters = find(serviceDef.configs, {
      name: "ranger.plugin.audit.filters"
    });

    serviceJson["auditFilters"] = [];

    if (
      auditFilters &&
      auditFilters !== undefined &&
      this.props.params.serviceId === undefined
    ) {
      auditFilters = isEmpty(auditFilters?.defaultValue)
        ? []
        : JSON.parse(auditFilters.defaultValue.replace(/'/g, '"'));
      serviceJson["isAuditFilter"] = auditFilters.length > 0 ? true : false;

      serviceJson["auditFilters"] = this.getAuditFilters(
        auditFilters,
        serviceDef
      );
    }

    this.setState({
      serviceDef: serviceDef,
      createInitialValues: serviceJson,
      loader: this.props.params.serviceId !== undefined ? true : false
    });

    if (this.props.params.serviceId !== undefined) {
      this.fetchService();
    }
  };

  fetchService = async () => {
    let serviceResp;
    let serviceId = this.props.params.serviceId;
    try {
      serviceResp = await fetchApi({
        url: `plugins/services/${serviceId}`
      });
    } catch (error) {
      console.error(
        `Error occurred while fetching Service or CSRF headers! ${error}`
      );
    }

    const serviceJson = {};
    serviceJson["name"] = serviceResp.data.name;
    serviceJson["displayName"] = serviceResp.data.displayName;
    serviceJson["description"] = serviceResp.data.description;
    serviceJson["isEnabled"] = JSON.stringify(serviceResp.data.isEnabled);

    serviceJson["tagService"] =
      serviceResp.data.tagService !== undefined
        ? {
            value: serviceResp.data.tagService,
            label: serviceResp.data.tagService
          }
        : null;

    serviceJson["configs"] = {};

    let serviceDefConfigs = map(this.state.serviceDef.configs, "name");
    let serviceCustomConfigs = without(
      difference(keys(serviceResp.data.configs), serviceDefConfigs),
      "ranger.plugin.audit.filters"
    );

    serviceDefConfigs.map((config) => {
      serviceJson["configs"][config.replaceAll(".", "_").replaceAll("-", "_")] =
        serviceResp?.data?.configs?.[config];
    });

    let editCustomConfigs = serviceCustomConfigs.map((config) => {
      return { name: config, value: serviceResp.data.configs[config] };
    });

    serviceJson["customConfigs"] =
      editCustomConfigs.length == 0 ? [undefined] : editCustomConfigs;

    let editAuditFilters =
      serviceResp?.data?.configs?.["ranger.plugin.audit.filters"];

    serviceJson["auditFilters"] = [];

    if (
      editAuditFilters &&
      editAuditFilters !== undefined &&
      this.props.params.serviceId !== undefined
    ) {
      editAuditFilters = isEmpty(editAuditFilters)
        ? []
        : JSON.parse(editAuditFilters.replace(/'/g, '"'));
      serviceJson["isAuditFilter"] = editAuditFilters.length > 0 ? true : false;
      serviceJson["auditFilters"] = this.getAuditFilters(
        editAuditFilters,
        this.state.serviceDef
      );
    }

    this.setState({
      service: serviceResp.data,
      editInitialValues: serviceJson,
      loader: false
    });
  };

  fetchTagService = async (inputValue) => {
    let params = { serviceNamePartial: inputValue || "" };
    let op = [];

    const tagServiceResp = await fetchApi({
      url: `plugins/services?serviceType=tag`,
      params: params
    });
    op = tagServiceResp.data.services;

    return op.map((obj) => ({
      label: obj.displayName,
      value: obj.displayName
    }));
  };

  onFocusTagService = () => {
    this.setState({ loadingOptions: true });
    this.fetchTagService().then((opts) => {
      this.setState({ defaultTagOptions: opts, loadingOptions: false });
    });
  };
  deleteService = async (serviceId) => {
    let localStorageZoneDetails = localStorage.getItem("zoneDetails");
    let zonesResp = [];
    this.hideDeleteModal();
    try {
      this.setState({ blockUI: true });
      await fetchApi({
        url: `plugins/services/${serviceId}`,
        method: "delete"
      });
      if (
        localStorageZoneDetails !== undefined &&
        localStorageZoneDetails !== null
      ) {
        zonesResp = await fetchApi({
          url: `public/v2/api/zones/${
            JSON.parse(localStorageZoneDetails)?.value
          }/service-headers`
        });

        if (isEmpty(zonesResp?.data)) {
          localStorage.removeItem("zoneDetails");
        }
      }
      this.setState({ blockUI: false });
      toast.success("Successfully deleted the service");
      this.props.navigate(
        this.state?.serviceDef?.name === "tag"
          ? "/policymanager/tag"
          : "/policymanager/resource"
      );
    } catch (error) {
      this.setState({ blockUI: false });
      serverError(error);
      console.error(
        `Error occurred while deleting Service id - ${serviceId}!  ${error}`
      );
    }
  };

  getAuditFilters = (auditFilters, serviceDef) => {
    let auditFiltersArray = [];

    auditFilters.map((item) => {
      let obj = {};
      Object.entries(item).map(([key, value]) => {
        if (key === "isAudited") {
          obj.isAudited = JSON.stringify(value);
        }

        if (key === "accessResult") {
          obj.accessResult = { value: value, label: value };
        }

        if (key === "resources") {
          obj.resources = {};
          let lastResourceLevel = [];

          Object.entries(item.resources).map(([key, value]) => {
            let setResources = find(serviceDef.resources, ["name", key]);
            obj.resources[`resourceName-${setResources.level}`] = setResources;
            obj.resources[`value-${setResources.level}`] = value.values.map(
              (m) => {
                return { label: m, value: m };
              }
            );
            if (setResources.excludesSupported) {
              obj.resources[`isExcludesSupport-${setResources.level}`] =
                value?.isExcludes != false;
            }
            if (setResources.recursiveSupported) {
              obj.resources[`isRecursiveSupport-${setResources.level}`] =
                value.isRecursive != false;
            }
            lastResourceLevel.push({
              level: setResources.level,
              name: setResources.name
            });
          });

          lastResourceLevel = maxBy(lastResourceLevel, "level");
          let setLastResources = find(serviceDef.resources, [
            "parent",
            lastResourceLevel.name
          ]);

          if (setLastResources) {
            obj.resources[`resourceName-${setLastResources.level}`] = {
              label: "None",
              value: "none"
            };
          }
        }

        if (key === "actions") {
          obj.actions = value.map((action) => {
            return { value: action, label: action };
          });
        }

        if (key === "accessTypes") {
          if (serviceDef.name == "tag") {
            let accessTypes = groupBy(value, function (obj) {
              return split(obj, ":", 1);
            });
            let accessTypeObj = {};
            accessTypeObj["tableList"] = [];
            Object.entries(accessTypes).map(([key, values]) =>
              accessTypeObj["tableList"].push({
                serviceName: key,
                permission: values
              })
            );
            obj.accessTypes = accessTypeObj;
          } else {
            obj.accessTypes = value.map((accessType) => {
              let accessTypeObj = find(serviceDef.accessTypes, [
                "name",
                accessType
              ]);
              return { value: accessType, label: accessTypeObj.label };
            });
          }
        }

        if (key === "users") {
          obj.users = value.map((user) => {
            return { value: user, label: user };
          });
        }

        if (key === "groups") {
          obj.groups = value.map((group) => {
            return { value: group, label: group };
          });
        }

        if (key === "roles") {
          obj.roles = value.map((role) => {
            return { value: role, label: role };
          });
        }
      });

      auditFiltersArray.push(obj);
    });

    return auditFiltersArray;
  };

  getConfigInfo = (configInfo) => {
    if (configInfo !== undefined && configInfo !== "") {
      let infoObj = JSON.parse(configInfo);
      return infoObj.info !== undefined ? [infoObj.info] : [];
    }
    return [];
  };

  getServiceConfigs = (serviceDef) => {
    if (serviceDef.configs !== undefined) {
      let formField = [];
      const filterServiceConfigs = reject(serviceDef.configs, {
        name: "ranger.plugin.audit.filters"
      });
      filterServiceConfigs.map((configParam) => {
        this.configsJson[configParam.name] = configParam.name
          .replaceAll(".", "_")
          .replaceAll("-", "_");
        this.initialValuesObj.configs[this.configsJson[configParam.name]] =
          configParam.defaultValue;
        let configInfo = this.getConfigInfo(configParam.uiHint);
        switch (configParam.type) {
          case "string":
          case "int":
            formField.push(
              <Field
                name={"configs." + this.configsJson[configParam.name]}
                key={configParam.itemId}
                validate={this.validateRequired(configParam.mandatory)}
              >
                {({ input, meta }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label pull-right">
                        {configParam.label !== undefined
                          ? configParam.label
                          : configParam.name}
                        {configParam.mandatory ? " * " : ""}
                      </label>
                    </Col>
                    <Col xs={4}>
                      <input
                        {...input}
                        type="text"
                        id={
                          meta.error && meta.touched
                            ? "isError"
                            : "configs." + this.configsJson[configParam.name]
                        }
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                        data-cy={
                          "configs." + this.configsJson[configParam.name]
                        }
                      />
                    </Col>
                    {configInfo.length === 1 && (
                      <span className="service-config-info-icon">
                        <CustomPopover
                          title=""
                          content={configInfo}
                          placement="right"
                          icon="fa-fw fa fa-info-circle"
                          trigger={["hover", "focus"]}
                          dangerousInnerHtml={true}
                        />
                      </span>
                    )}
                    {meta.error && meta.touched && (
                      <div className="col-sm-6 offset-sm-3 invalid-field">
                        {meta.error}
                      </div>
                    )}
                  </Row>
                )}
              </Field>
            );
            break;
          case "enum":
            const paramEnum = serviceDef.enums.find(
              (e) => e.name == configParam.subType
            );
            formField.push(
              <Field
                name={"configs." + this.configsJson[configParam.name]}
                key={configParam.itemId}
                validate={this.validateRequired(configParam.mandatory)}
              >
                {({ input, meta }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label pull-right">
                        {configParam.label !== undefined
                          ? configParam.label
                          : configParam.name}
                        {configParam.mandatory ? " * " : ""}
                      </label>
                    </Col>
                    <Col xs={4}>
                      <select
                        {...input}
                        id={
                          meta.error && meta.touched
                            ? "isError"
                            : "configs." + this.configsJson[configParam.name]
                        }
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                      >
                        {this.enumOptions(paramEnum)}
                      </select>
                    </Col>
                    {configInfo.length === 1 && (
                      <span className="d-inline">
                        <CustomPopover
                          title=""
                          content={configInfo}
                          placement="right"
                          icon="fa-fw fa fa-info-circle"
                          trigger={["hover", "focus"]}
                        />
                      </span>
                    )}
                    {meta.error && meta.touched && (
                      <span className="col-sm-6 offset-sm-3 invalid-field">
                        {meta.error}
                      </span>
                    )}
                  </Row>
                )}
              </Field>
            );
            break;
          case "bool":
            formField.push(
              <Field
                name={"configs." + this.configsJson[configParam.name]}
                key={configParam.itemId}
                validate={this.validateRequired(configParam.mandatory)}
              >
                {({ input, meta }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label pull-right">
                        {configParam.label !== undefined
                          ? configParam.label
                          : configParam.name}
                        {configParam.mandatory ? " * " : ""}
                      </label>
                    </Col>
                    <Col xs={4}>
                      <select
                        {...input}
                        id={
                          meta.error && meta.touched
                            ? "isError"
                            : "configs." + this.configsJson[configParam.name]
                        }
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                        data-js="isAudited"
                        data-cy="isAudited"
                      >
                        {this.booleanOptions(configParam.subType)}
                      </select>
                    </Col>
                    {configInfo.length === 1 && (
                      <span className="d-inline">
                        <CustomPopover
                          title=""
                          content={configInfo}
                          placement="right"
                          icon="fa-fw fa fa-info-circle"
                          trigger={["hover", "focus"]}
                        />
                      </span>
                    )}
                    {meta.error && meta.touched && (
                      <span className="col-sm-6 offset-sm-3 invalid-field">
                        {meta.error}
                      </span>
                    )}
                  </Row>
                )}
              </Field>
            );
            break;
          case "password":
            formField.push(
              <Field
                name={"configs." + this.configsJson[configParam.name]}
                key={configParam.itemId}
                validate={this.validateRequired(configParam.mandatory)}
              >
                {({ input, meta }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label pull-right">
                        {configParam.label !== undefined
                          ? configParam.label
                          : configParam.name}
                        {configParam.mandatory ? " * " : ""}
                      </label>
                    </Col>
                    <Col xs={4}>
                      <input
                        {...input}
                        type="password"
                        autoComplete="off"
                        id={
                          meta.error && meta.touched
                            ? "isError"
                            : "configs." + this.configsJson[configParam.name]
                        }
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                      />
                    </Col>
                    {configInfo.length === 1 && (
                      <span className="d-inline">
                        <CustomPopover
                          title=""
                          content={configInfo}
                          placement="right"
                          icon="fa-fw fa fa-info-circle"
                          trigger={["hover", "focus"]}
                        />
                      </span>
                    )}
                    {meta.error && meta.touched && (
                      <span className="col-sm-6 offset-sm-3 invalid-field">
                        {meta.error}
                      </span>
                    )}
                  </Row>
                )}
              </Field>
            );
            break;
        }
      });
      return formField;
    }
  };

  enumOptions = (paramEnum) => {
    let optionField = [];
    paramEnum.elements.map((e) => {
      optionField.push(
        <option value={e.name} key={e.name}>
          {e.label}
        </option>
      );
    });
    return optionField;
  };

  booleanOptions = (paramBool) => {
    let optionField = [];
    let b = paramBool.split(":");
    b = [b[0].substr(0, b[0].length - 4), b[1].substr(0, b[1].length - 5)];
    b.map((e) => {
      optionField.push(
        <option value={e === "Yes" ? true : false} key={e}>
          {e}
        </option>
      );
    });
    return optionField;
  };

  validateRequired = (isRequired) =>
    isRequired ? (value) => (value ? undefined : "Required") : () => {};

  validateServiceName = (value) =>
    !RegexValidation.NAME_VALIDATION.regexforServiceNameValidation.test(value)
      ? RegexValidation.NAME_VALIDATION.serviceNameValidationMessage
      : undefined;

  composeValidators =
    (...validators) =>
    (value) =>
      validators.reduce(
        (error, validator) => error || validator(value),
        undefined
      );

  SelectField = ({ input, ...rest }) => (
    <Select {...input} {...rest} searchable />
  );

  AsyncSelectField = ({ input, ...rest }) => (
    <AsyncSelect {...input} {...rest} cacheOptions />
  );

  fetchUsers = async (inputValue) => {
    let params = { name: inputValue || "", isVisible: 1 };
    let op = [];
    const userResp = await fetchApi({
      url: "xusers/lookup/users",
      params: params
    });
    op = userResp.data.vXStrings;

    return op.map((obj) => ({
      label: obj.value,
      value: obj.value
    }));
  };

  fetchGroups = async (inputValue) => {
    let params = { name: inputValue || "", isVisible: 1 };
    let op = [];
    const userResp = await fetchApi({
      url: "xusers/lookup/groups",
      params: params
    });
    op = userResp.data.vXStrings;
    if (!inputValue) {
      this.state.groupsDataRef = op;
    }

    return op.map((obj) => ({
      label: obj.value,
      value: obj.value
    }));
  };

  fetchRoles = async (inputValue) => {
    let params = { roleNamePartial: inputValue || "", isVisible: 1 };
    let op = [];
    const roleResp = await fetchApi({
      url: "roles/roles",
      params: params
    });
    op = roleResp.data.roles;
    if (!inputValue) {
      this.state.rolesDataRef = op;
    }

    return op.map((obj) => ({
      label: obj.name,
      value: obj.name
    }));
  };
  ServiceDefnBreadcrumb = () => {
    let serviceDetails = {};
    serviceDetails["serviceDefId"] = this.state.serviceDef.id;
    serviceDetails["serviceId"] = this.props.params.serviceId;
    if (this.state.serviceDef.name === "tag") {
      return commonBreadcrumb(
        [
          "TagBasedServiceManager",
          this.props.params.serviceId !== undefined
            ? "ServiceEdit"
            : "ServiceCreate"
        ],
        serviceDetails
      );
    } else {
      return commonBreadcrumb(
        [
          "ServiceManager",
          this.props.params.serviceId !== undefined
            ? "ServiceEdit"
            : "ServiceCreate"
        ],
        serviceDetails
      );
    }
  };
  render() {
    return (
      <React.Fragment>
        <div className="clearfix">
          <div className="header-wraper">
            <h3 className="wrap-header bold">
              {this.props.params.serviceId !== undefined ? `Edit` : `Create`}{" "}
              Service
            </h3>
            {this.ServiceDefnBreadcrumb()}
          </div>
        </div>
        {this.state.loader ? (
          <Loader />
        ) : (
          <div className="wrap">
            <div className="row">
              <div className="col-sm-12">
                <Form
                  onSubmit={this.onSubmit}
                  mutators={{
                    ...arrayMutators
                  }}
                  initialValues={
                    this.props.params.serviceId !== undefined
                      ? this.state.editInitialValues
                      : this.state.createInitialValues
                  }
                  render={({
                    handleSubmit,
                    form,
                    submitting,
                    values,
                    invalid,
                    errors,
                    form: {
                      mutators: { push: addItem, pop: removeItem }
                    }
                  }) => (
                    <form
                      onSubmit={(event) => {
                        let selector;
                        if (invalid) {
                          if (errors?.configs !== undefined) {
                            selector =
                              document.getElementById("isError") ||
                              document.querySelector(
                                `input[name=${Object.keys(errors)[0]}]`
                              ) ||
                              document.querySelector(
                                `input[name="configs.${
                                  Object.keys(errors.configs)[0]
                                }`
                              );
                          } else {
                            selector =
                              document.getElementById("isError") ||
                              document.querySelector(
                                `input[name=${Object.keys(errors)[0]}]`
                              );
                          }
                          scrollToError(selector);
                        }
                        handleSubmit(event);
                      }}
                    >
                      <Row>
                        <Col xs={12}>
                          <p className="form-header">Service Details :</p>
                          <Field
                            name="name"
                            id="name"
                            validate={this.composeValidators(
                              this.validateRequired(true),
                              this.validateServiceName
                            )}
                          >
                            {({ input, meta }) => (
                              <Row className="form-group">
                                <Col xs={3}>
                                  <label className="form-label pull-right">
                                    Service Name *
                                  </label>
                                </Col>
                                <Col xs={4}>
                                  <input
                                    {...input}
                                    type="text"
                                    id={
                                      meta.error && meta.touched
                                        ? "isError"
                                        : "name"
                                    }
                                    className={
                                      meta.error && meta.touched
                                        ? "form-control border-danger"
                                        : "form-control"
                                    }
                                    data-cy="name"
                                  />
                                </Col>
                                {meta.error && meta.touched && (
                                  <span className="col-sm-6 offset-sm-3 invalid-field">
                                    {meta.error}
                                  </span>
                                )}
                              </Row>
                            )}
                          </Field>
                          <Field name="displayName">
                            {({ input, meta }) => (
                              <Row className="form-group">
                                <Col xs={3}>
                                  <label className="form-label pull-right">
                                    Display Name
                                  </label>
                                </Col>
                                <Col xs={4}>
                                  <input
                                    {...input}
                                    type="text"
                                    className={
                                      meta.error && meta.touched
                                        ? "form-control border border-danger"
                                        : "form-control"
                                    }
                                    data-cy="displayName"
                                  />
                                </Col>
                                {meta.error && meta.touched && (
                                  <span className="col-sm-6 offset-sm-3 invalid-field">
                                    {meta.error}
                                  </span>
                                )}
                              </Row>
                            )}
                          </Field>
                          <Field name="description">
                            {({ input, meta }) => (
                              <Row className="form-group">
                                <Col xs={3}>
                                  <label className="form-label pull-right">
                                    Description
                                  </label>
                                </Col>
                                <Col xs={4}>
                                  <textarea
                                    {...input}
                                    className={
                                      meta.error && meta.touched
                                        ? "form-control border border-danger"
                                        : "form-control"
                                    }
                                    data-cy="description"
                                  />
                                </Col>
                                {meta.error && meta.touched && (
                                  <span className="col-sm-6 offset-sm-3 invalid-field">
                                    {meta.error}
                                  </span>
                                )}
                              </Row>
                            )}
                          </Field>
                          <Row className="form-group">
                            <Col xs={3}>
                              <label className="form-label pull-right">
                                Active Status
                              </label>
                            </Col>
                            <Col xs={4}>
                              <span>
                                <div className="form-control border-0">
                                  <div className="form-check form-check-inline">
                                    <Field
                                      name="isEnabled"
                                      component="input"
                                      type="radio"
                                      className="form-check-input"
                                      value="true"
                                      data-cy="isEnabled"
                                    />
                                    <label className="form-check-label">
                                      Enabled
                                    </label>
                                  </div>
                                  <div className="form-check form-check-inline">
                                    {" "}
                                    <Field
                                      name="isEnabled"
                                      className="form-check-input"
                                      component="input"
                                      type="radio"
                                      value="false"
                                      data-cy="isEnabled"
                                    />
                                    <label className="form-check-label">
                                      Disabled
                                    </label>
                                  </div>
                                </div>
                              </span>
                            </Col>
                          </Row>
                          {this.state.serviceDef.name !== "tag" && (
                            <Row className="form-group">
                              <Col xs={3}>
                                <label className="form-label pull-right">
                                  Select Tag Service
                                </label>
                              </Col>
                              <Col xs={4}>
                                <Field
                                  name="tagService"
                                  component={this.AsyncSelectField}
                                  loadOptions={this.fetchTagService}
                                  onFocus={() => {
                                    this.onFocusTagService();
                                  }}
                                  defaultOptions={this.state.defaultTagOptions}
                                  placeholder="Select Tag Service"
                                  isLoading={this.state.loadingOptions}
                                  isClearable={true}
                                  cacheOptions
                                />
                              </Col>
                            </Row>
                          )}
                        </Col>
                      </Row>
                      <Row>
                        <Col xs={12}>
                          <p className="form-header">Config Properties :</p>
                          {this.getServiceConfigs(this.state.serviceDef)}
                          <Row className="form-group">
                            <Col xs={3}>
                              <label className="form-label pull-right">
                                Add New Configurations
                              </label>
                            </Col>
                            <Col xs={5}>
                              <Table bordered size="sm" className="no-bg-color">
                                <thead>
                                  <tr>
                                    <th className="text-center">Name</th>
                                    <th className="text-center" colSpan="2">
                                      Value
                                    </th>
                                  </tr>
                                </thead>
                                <tbody>
                                  <FieldArray name="customConfigs">
                                    {({ fields }) =>
                                      fields.map((name, index) => (
                                        <tr key={name}>
                                          <td className="text-center">
                                            <Field
                                              name={`${name}.name`}
                                              component="input"
                                              className="form-control"
                                              data-js="name"
                                              data-cy="name"
                                            />
                                          </td>
                                          <td className="text-center">
                                            <Field
                                              name={`${name}.value`}
                                              component="input"
                                              className="form-control"
                                              data-js="value"
                                              data-cy="value"
                                            />
                                          </td>
                                          <td className="text-center">
                                            <Button
                                              variant="danger"
                                              size="sm"
                                              className="btn-mini"
                                              title="Remove"
                                              onClick={() =>
                                                fields.remove(index)
                                              }
                                              data-action="delete"
                                              data-cy="delete"
                                            >
                                              <i className="fa-fw fa fa-remove"></i>
                                            </Button>
                                          </td>
                                        </tr>
                                      ))
                                    }
                                  </FieldArray>
                                </tbody>
                              </Table>
                            </Col>
                          </Row>
                          <Row className="form-group">
                            <div className="col-sm-4 offset-sm-3">
                              <Button
                                variant="outline-dark"
                                className="btn-mini btn-sm"
                                onClick={() =>
                                  addItem("customConfigs", undefined)
                                }
                                data-action="addGroup"
                                data-cy="addGroup"
                              >
                                <i className="fa-fw fa fa-plus"></i>
                              </Button>
                            </div>
                          </Row>
                        </Col>
                      </Row>
                      <div className="row">
                        <div className="col-sm-12">
                          <div className="form-group row form-header p-0">
                            <label className="col-sm-1 col-form-label form-check-label mr-2">
                              Audit Filter :
                            </label>
                            <div className="col-sm-10 mt-2">
                              <Field
                                name="isAuditFilter"
                                component="input"
                                type="checkbox"
                                className="form-check-input"
                              />
                            </div>
                          </div>
                          <div className="row">
                            <div className="col-sm-12">
                              <Condition when="isAuditFilter" is={true}>
                                <ServiceAuditFilter
                                  serviceDetails={this.state.service}
                                  serviceDefDetails={this.state.serviceDef}
                                  fetchUsersData={this.fetchUsers}
                                  fetchGroupsData={this.fetchGroups}
                                  fetchRolesData={this.fetchRoles}
                                  addAuditFilter={addItem}
                                  formValues={values}
                                />
                              </Condition>
                            </div>
                          </div>
                          <div className="form-group row mt-2 text-right">
                            <div className="col-sm-3 col-form-label">
                              {!isEmpty(this.state.serviceDef) && (
                                <TestConnection
                                  getTestConnConfigs={
                                    this.getServiceConfigsToSave
                                  }
                                  formValues={values}
                                />
                              )}
                            </div>
                          </div>
                        </div>
                      </div>
                      <div className="row form-actions">
                        <div className="col-md-9 offset-md-3">
                          <Button
                            variant="primary"
                            type="submit"
                            className="btn-sm"
                            size="sm"
                            disabled={submitting}
                            data-id="save"
                            data-cy="save"
                          >
                            {this.props.params.serviceId !== undefined
                              ? `Save`
                              : `Add`}
                          </Button>
                          <Button
                            variant="secondary"
                            type="button"
                            className="btn-sm"
                            size="sm"
                            onClick={() => {
                              if (this.props?.location?.state != "services") {
                                if (
                                  this?.props?.params?.serviceId !== undefined
                                ) {
                                  return this.props.navigate(
                                    `/service/${this.props.params.serviceId}/policies/${RangerPolicyType.RANGER_ACCESS_POLICY_TYPE.value}`
                                  );
                                } else {
                                  return this.props.navigate(
                                    this.state?.serviceDef?.name === "tag"
                                      ? "/policymanager/tag"
                                      : "/policymanager/resource"
                                  );
                                }
                              } else {
                                return this.props.navigate(
                                  this.state?.serviceDef?.name === "tag"
                                    ? "/policymanager/tag"
                                    : "/policymanager/resource"
                                );
                              }
                            }}
                            data-id="cancel"
                            data-cy="cancel"
                          >
                            Cancel
                          </Button>
                          {this.props.params.serviceId !== undefined && (
                            <Button
                              variant="danger"
                              type="button"
                              className="btn-sm"
                              size="sm"
                              onClick={() => {
                                this.showDeleteModal();
                              }}
                            >
                              Delete
                            </Button>
                          )}
                        </div>
                        {this.props.params.serviceId !== undefined && (
                          <Modal
                            show={this.state.showDelete}
                            onHide={this.hideDeleteModal}
                          >
                            <Modal.Header closeButton>
                              {`Are you sure want to delete ?`}
                            </Modal.Header>
                            <Modal.Footer>
                              <Button
                                variant="secondary"
                                size="sm"
                                title="Cancel"
                                onClick={this.hideDeleteModal}
                              >
                                Cancel
                              </Button>
                              <Button
                                variant="primary"
                                size="sm"
                                title="Yes"
                                onClick={() =>
                                  this.deleteService(
                                    this.props.params.serviceId
                                  )
                                }
                              >
                                Yes
                              </Button>
                            </Modal.Footer>
                          </Modal>
                        )}
                      </div>
                    </form>
                  )}
                />
              </div>
            </div>
            <BlockUi isUiBlock={this.state.blockUI} />
          </div>
        )}
      </React.Fragment>
    );
  }
}

export default withRouter(ServiceForm);
