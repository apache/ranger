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
import { useLocation, useNavigate } from "react-router-dom";
import {
  Accordion,
  Button,
  ButtonGroup,
  Dropdown,
  Col,
  InputGroup,
  Row
} from "react-bootstrap";
import { Form, Field } from "react-final-form";
import Select from "react-select";
import AsyncSelect from "react-select/async";
import AsyncCreatableSelect from "react-select/async-creatable";
import {
  filter,
  find,
  isEmpty,
  isUndefined,
  join,
  map,
  sortBy,
  split,
  has,
  uniq,
  cloneDeep
} from "lodash";
import { toast } from "react-toastify";
import { fetchApi } from "Utils/fetchAPI";
import { useQuery, selectInputCustomStyles } from "Components/CommonComponents";
import SearchPolicyTable from "./SearchPolicyTable";
import { isAuditor, isKeyAdmin, isKMSAuditor } from "Utils/XAUtils";
import CustomBreadcrumb from "Views/CustomBreadcrumb";
import moment from "moment-timezone";
import { getServiceDef } from "Utils/appState";

function UserAccessLayout() {
  const isKMSRole = isKeyAdmin() || isKMSAuditor();
  const isAuditRole = isAuditor() || isKMSAuditor();
  const [contentLoader, setContentLoader] = useState(true);
  const [serviceDefs, setServiceDefs] = useState([]);
  const [filterServiceDefs, setFilterServiceDefs] = useState([]);
  const [serviceDefOpts, setServiceDefOpts] = useState([]);
  const [services, setServices] = useState([]);
  const [zones, setZones] = useState([]);
  const [zoneNameOpts, setZoneNameOpts] = useState([]);
  const [searchParamsObj, setSearchParamsObj] = useState({});
  const navigate = useNavigate();
  const location = useLocation();
  const searchParams = useQuery();
  const { allServiceDefs } = cloneDeep(getServiceDef());

  useEffect(() => {
    fetchInitialData();

    if (!isKMSRole) {
      fetchZones();
    }

    setContentLoader(false);
  }, []);

  useEffect(() => {
    getSearchParams();
  }, [searchParams]);

  const fetchInitialData = async () => {
    await fetchData();
  };

  const fetchData = async () => {
    let servicesResp;
    let resourceServices;

    try {
      servicesResp = await fetchApi({
        url: "plugins/services"
      });

      resourceServices = filter(servicesResp.data.services, (service) =>
        isKMSRole ? service.type == "kms" : service.type != "kms"
      );
    } catch (error) {
      console.error(
        `Error occurred while fetching Services or CSRF headers! ${error}`
      );
    }

    let resourceServiceDefs = filter(allServiceDefs, (serviceDef) =>
      isKMSRole ? serviceDef.name == "kms" : serviceDef.name != "kms"
    );

    let filterResourceServiceDefs = resourceServiceDefs;
    if (searchParams.get("serviceType")) {
      filterResourceServiceDefs = [];
      let serviceTypes = split(searchParams.get("serviceType"), ",");
      serviceTypes.map((serviceType) => {
        let findObj = find(resourceServiceDefs, { name: serviceType });
        if (!isUndefined(findObj)) {
          filterResourceServiceDefs.push(findObj);
        }
      });
    }

    let serviceDefsList = map(allServiceDefs, function (serviceDef) {
      return { value: serviceDef.name, label: serviceDef.name };
    });

    setServiceDefs(resourceServiceDefs);
    setServices(resourceServices);
    setFilterServiceDefs(filterResourceServiceDefs);
    setServiceDefOpts(serviceDefsList);
  };

  const fetchZones = async () => {
    let zonesResp = [];
    try {
      const response = await fetchApi({
        url: "public/v2/api/zone-headers"
      });
      zonesResp = response?.data || [];
    } catch (error) {
      console.error(`Error occurred while fetching Zones! ${error}`);
    }

    let zonesList = map(sortBy(zonesResp, ["name"]), function (zone) {
      return { value: zone.name, label: zone.name };
    });

    setZones(zonesResp || []);
    setZoneNameOpts(zonesList);
  };

  const fetchPolicyLabels = async (inputValue) => {
    let params = {};

    if (inputValue) {
      params["policyLabel"] = inputValue || "";
    }

    let policyLabelsResp = await fetchApi({
      url: "plugins/policyLabels",
      params: params
    });

    let policyLabelsList = map(policyLabelsResp.data, function (policyLabel) {
      return { value: policyLabel, label: policyLabel };
    });

    return policyLabelsList;
  };

  const onSubmit = async (values) => {
    let urlSearchParams = "";
    const searchFields = {};

    if (values.policyType !== undefined) {
      urlSearchParams = `policyType=${values.policyType.value}`;
      searchFields.policyType = values.policyType.value;
    }

    if (values.policyNamePartial !== undefined) {
      urlSearchParams = `${urlSearchParams}&policyNamePartial=${values.policyNamePartial}`;
      searchFields.policyNamePartial = values.policyNamePartial;
    }

    let serviceDefsList = serviceDefs;
    if (values.serviceType !== undefined && !isEmpty(values.serviceType)) {
      let serviceType = join(map(values.serviceType, "value"));

      serviceDefsList = map(map(values.serviceType, "value"), function (obj) {
        return find(serviceDefs, { name: obj });
      });

      urlSearchParams = `${urlSearchParams}&serviceType=${serviceType}`;
      searchFields.serviceType = serviceType;
    }

    if (values.polResource !== undefined) {
      urlSearchParams = `${urlSearchParams}&polResource=${values.polResource}`;
      searchFields.polResource = values.polResource;
    }

    if (
      values.policyLabelsPartial !== undefined &&
      values.policyLabelsPartial
    ) {
      urlSearchParams = `${urlSearchParams}&policyLabelsPartial=${values.policyLabelsPartial.value}`;
      searchFields.policyLabelsPartial = values.policyLabelsPartial.value;
    }

    if (values.zoneName !== undefined && values.zoneName) {
      urlSearchParams = `${urlSearchParams}&zoneName=${values.zoneName.value}`;
      searchFields.zoneName = values.zoneName.value;

      setZoneDetails(values.zoneName.value);
    }

    if (values.searchByValue !== undefined && values.searchByValue) {
      if (values.searchBy.value == "searchByUser") {
        urlSearchParams = `${urlSearchParams}&user=${values.searchByValue.value}`;
        searchFields.user = values.searchByValue.value;
      } else if (values.searchBy.value == "searchByRole") {
        urlSearchParams = `${urlSearchParams}&role=${values.searchByValue.value}`;
        searchFields.role = values.searchByValue.value;
      } else {
        urlSearchParams = `${urlSearchParams}&group=${values.searchByValue.value}`;
        searchFields.group = values.searchByValue.value;
      }
    }

    navigate(`/reports/userAccess?${urlSearchParams}`);

    setFilterServiceDefs(serviceDefsList);
    setSearchParamsObj(searchFields);
  };

  const getInitialSearchParams = () => {
    const initialSearchFields = {};

    if (searchParams.get("policyNamePartial")) {
      initialSearchFields.policyNamePartial =
        searchParams.get("policyNamePartial");
    }

    if (searchParams.get("policyType")) {
      let policyTypeLabel;

      if (searchParams.get("policyType") == 1) {
        policyTypeLabel = "Masking";
      } else if (searchParams.get("policyType") == 2) {
        policyTypeLabel = "Row Level Filter";
      } else {
        policyTypeLabel = "Access";
      }

      initialSearchFields.policyType = {
        value: searchParams.get("policyType"),
        label: policyTypeLabel
      };
    }

    if (searchParams.get("serviceType")) {
      let serviceTypes = split(searchParams.get("serviceType"), ",");
      initialSearchFields.serviceType = map(
        serviceTypes,
        function (serviceType) {
          return { value: serviceType, label: serviceType };
        }
      );
    }

    if (searchParams.get("polResource")) {
      initialSearchFields.polResource = searchParams.get("polResource");
    }

    if (searchParams.get("policyLabelsPartial")) {
      initialSearchFields.policyLabelsPartial = {
        value: searchParams.get("policyLabelsPartial"),
        label: searchParams.get("policyLabelsPartial")
      };
    }

    if (searchParams.get("zoneName")) {
      initialSearchFields.zoneName = {
        value: searchParams.get("zoneName"),
        label: searchParams.get("zoneName")
      };
    }

    initialSearchFields.searchBy = {
      value: "searchByGroup",
      label: "Group"
    };

    if (searchParams.get("user")) {
      initialSearchFields.searchBy = {
        value: "searchByUser",
        label: "Username"
      };
      initialSearchFields.searchByValue = {
        value: searchParams.get("user"),
        label: searchParams.get("user")
      };
    }

    if (searchParams.get("role")) {
      initialSearchFields.searchBy = {
        value: "searchByRole",
        label: "Rolename"
      };
      initialSearchFields.searchByValue = {
        value: searchParams.get("role"),
        label: searchParams.get("role")
      };
    }

    if (searchParams.get("group")) {
      initialSearchFields.searchByValue = {
        value: searchParams.get("group"),
        label: searchParams.get("group")
      };
    }

    return initialSearchFields;
  };

  const getSearchParams = () => {
    const searchFields = {};
    let resourceServiceDefs = filter(serviceDefs, (serviceDef) =>
      isKMSRole ? serviceDef.name == "kms" : serviceDef.name != "kms"
    );

    let filterResourceServiceDefs = [...resourceServiceDefs];

    if (searchParams.get("policyNamePartial")) {
      searchFields.policyNamePartial = searchParams.get("policyNamePartial");
    }

    if (searchParams.get("policyType")) {
      searchFields.policyType = searchParams.get("policyType");
    }

    if (searchParams.get("serviceType")) {
      searchFields.serviceType = searchParams.get("serviceType");
      filterResourceServiceDefs = [];
      let serviceTypes = split(searchParams.get("serviceType"), ",");
      uniq(serviceTypes).map((serviceType) => {
        let findObj = find(resourceServiceDefs, { name: serviceType });
        if (!isUndefined(findObj)) {
          filterResourceServiceDefs.push(findObj);
        }
      });
    }

    if (searchParams.get("polResource")) {
      searchFields.polResource = searchParams.get("polResource");
    }

    if (searchParams.get("policyLabelsPartial")) {
      searchFields.policyLabelsPartial = searchParams.get(
        "policyLabelsPartial"
      );
    }

    if (searchParams.get("zoneName")) {
      searchFields.zoneName = searchParams.get("zoneName");
      setZoneDetails(searchParams.get("zoneName"));
    }

    if (searchParams.get("user")) {
      searchFields.user = searchParams.get("user");
    }

    if (searchParams.get("group")) {
      searchFields.group = searchParams.get("group");
    }

    if (searchParams.get("role")) {
      searchFields.role = searchParams.get("role");
    }

    if (JSON.stringify(searchParamsObj) !== JSON.stringify(searchFields)) {
      if (has(searchFields, "serviceType")) {
        searchFields.serviceType = uniq(
          searchFields.serviceType.split(",")
        ).join(",");
      }
      setSearchParamsObj(searchFields);
      setFilterServiceDefs(filterResourceServiceDefs);
      if (!has(Object.fromEntries(searchParams), "policyType")) {
        searchParams.set("policyType", "0");
        searchFields.policyType = "0";
        navigate(`${location.pathname}?${searchParams}`);
        setSearchParamsObj(searchFields);
      }
      if (has(searchFields, "serviceType")) {
        let filterServiceType = searchFields.serviceType.split(",");
        searchParams.set("serviceType", uniq(filterServiceType).join(","));
        navigate(`${location.pathname}?${searchParams}`);
      }
    }
  };

  const setZoneDetails = (zoneName) => {
    let zoneDetails = {};
    let zone = find(zones, { name: zoneName });

    if (zone) {
      zoneDetails["label"] = zoneName;
      zoneDetails["value"] = zone.id;
      localStorage.setItem("zoneDetails", JSON.stringify(zoneDetails));
    }
  };

  const exportPolicy = async (exportType) => {
    let exportResp;
    let exportApiUrl = "/plugins/policies/exportJson";

    if (exportType === "downloadExcel") {
      exportApiUrl = "/plugins/policies/downloadExcel";
    } else if (exportType === "csv") {
      exportApiUrl = "/plugins/policies/csv";
    }

    try {
      exportResp = await fetchApi({
        url: exportApiUrl,
        params: searchParamsObj,
        responseType: "blob"
      });

      if (exportResp.status === 200) {
        downloadFile({
          exportType: exportType,
          apiResponse: exportResp.data
        });
      } else {
        toast.warning("No policies found to export");
      }
    } catch (error) {
      console.error(`Error occurred while exporting policies ${error}`);
    }
  };

  const downloadFile = ({ exportType, apiResponse }) => {
    let fileExtension;

    if (exportType === "downloadExcel") {
      fileExtension = ".xls";
    } else if (exportType === "csv") {
      fileExtension = ".csv";
    } else {
      fileExtension = ".json";
    }

    const fileName =
      "Ranger_Policies_" +
      moment(moment()).format("YYYYMMDD_hhmmss") +
      fileExtension;

    const downloadUrl = window.URL.createObjectURL(apiResponse);
    const link = document.createElement("a");

    link.href = downloadUrl;
    link.download = fileName;

    const clickEvt = new MouseEvent("click", {
      view: window,
      bubbles: true,
      cancelable: true
    });

    document.body.appendChild(link);
    link.dispatchEvent(clickEvt);
    link.remove();
  };

  const onChangeSearchBy = (e, input, values) => {
    if (e.label !== input.value.label) {
      delete values["searchByValue"];
    }
    for (const obj in input.value) {
      delete input.value[obj];
    }

    input.onChange({ value: e.value, label: e.label });
  };

  const onChangeCurrentSearchByOpt = (option, input) => {
    input.onChange(option);
  };

  return (
    <React.Fragment>
      <div className="clearfix">
        <div className="header-wraper">
          <h3 className="wrap-header bold">Reports</h3>
          <CustomBreadcrumb />
        </div>
      </div>
      <div className="wrap report-page">
        <Row>
          <Col sm={12}>
            <Accordion defaultActiveKey="0">
              <Accordion.Item eventKey="0">
                <Accordion.Header>
                  <div className="clearfix">
                    <span className="bold float-start">Search Criteria</span>
                  </div>
                </Accordion.Header>
                <Accordion.Body>
                  <Form
                    onSubmit={onSubmit}
                    initialValues={getInitialSearchParams}
                    render={({ handleSubmit, submitting, values }) => (
                      <form onSubmit={handleSubmit}>
                        <Row className="form-group">
                          <Col sm={2} className="text-end">
                            <label className="col-form-label ">
                              Policy Name
                            </label>
                          </Col>
                          <Col sm={4}>
                            <Field name="policyNamePartial">
                              {({ input }) => (
                                <input
                                  {...input}
                                  type="text"
                                  placeholder="Enter Policy Name"
                                  className="form-control"
                                  data-js="policyName"
                                  data-cy="policyName"
                                />
                              )}
                            </Field>
                          </Col>
                          <Col sm={2} className="text-end">
                            <label className="col-form-label ">
                              Policy Type
                            </label>
                          </Col>
                          <Col sm={4}>
                            <Field name="policyType">
                              {({ input }) => (
                                <Select
                                  {...input}
                                  isClearable={false}
                                  options={[
                                    { value: "0", label: "Access" },
                                    { value: "1", label: "Masking" },
                                    {
                                      value: "2",
                                      label: "Row Level Filter"
                                    }
                                  ]}
                                  menuPlacement="auto"
                                  placeholder="Select Policy Type"
                                />
                              )}
                            </Field>
                          </Col>
                        </Row>
                        <Row className="form-group">
                          <Col sm={2} className="text-end">
                            <label className="col-form-label ">Component</label>
                          </Col>
                          <Col sm={4}>
                            <Field name="serviceType">
                              {({ input }) => (
                                <Select
                                  {...input}
                                  isMulti
                                  isClearable={false}
                                  options={serviceDefOpts}
                                  menuPlacement="auto"
                                  placeholder="Select Component Type"
                                  styles={selectInputCustomStyles}
                                />
                              )}
                            </Field>
                          </Col>
                          <Col sm={2} className="text-end">
                            <label className="col-form-label ">Resource</label>
                          </Col>
                          <Col sm={4}>
                            <Field name="polResource">
                              {({ input }) => (
                                <input
                                  {...input}
                                  type="text"
                                  placeholder="Enter Resource Name"
                                  className="form-control"
                                  data-js="resourceName"
                                  data-cy="resourceName"
                                />
                              )}
                            </Field>
                          </Col>
                        </Row>
                        <Row className="form-group">
                          <Col sm={2} className="text-end">
                            <label className="col-form-label ">
                              Policy Label
                            </label>
                          </Col>
                          <Col sm={4}>
                            <Field name="policyLabelsPartial">
                              {({ input }) => (
                                <AsyncCreatableSelect
                                  {...input}
                                  defaultOptions
                                  isClearable={true}
                                  loadOptions={fetchPolicyLabels}
                                  placeholder="Select Policy Label"
                                  styles={selectInputCustomStyles}
                                />
                              )}
                            </Field>
                          </Col>
                          {!isKMSRole && (
                            <React.Fragment>
                              <Col sm={2} className="text-end">
                                <label className="col-form-label ">
                                  Zone Name
                                </label>
                              </Col>
                              <Col sm={4}>
                                <Field name="zoneName">
                                  {({ input }) => (
                                    <Select
                                      {...input}
                                      isClearable={true}
                                      options={zoneNameOpts}
                                      menuPlacement="auto"
                                      placeholder="Select Zone Name"
                                      styles={selectInputCustomStyles}
                                    />
                                  )}
                                </Field>
                              </Col>
                            </React.Fragment>
                          )}
                        </Row>
                        <Row>
                          <Col sm={2} className="text-end">
                            <label className="col-form-label ">Search By</label>
                          </Col>
                          <Col sm={10}>
                            <InputGroup className="mb-3">
                              <Field name="searchBy">
                                {({ input }) => (
                                  <Select
                                    {...input}
                                    isClearable={false}
                                    options={[
                                      {
                                        value: "searchByGroup",
                                        label: "Groupname"
                                      },
                                      {
                                        value: "searchByUser",
                                        label: "Username"
                                      },
                                      {
                                        value: "searchByRole",
                                        label: "Rolename"
                                      }
                                    ]}
                                    menuPlacement="auto"
                                    placeholder="Select Search By"
                                    onChange={(e) =>
                                      onChangeSearchBy(e, input, values)
                                    }
                                  />
                                )}
                              </Field>
                              <SearchByAsyncSelect
                                searchByOptName={values.searchBy}
                                onChange={(opt, input) =>
                                  onChangeCurrentSearchByOpt(opt, input)
                                }
                              />
                            </InputGroup>
                          </Col>
                        </Row>
                        <Row>
                          <Col sm={{ span: 10, offset: 2 }}>
                            <Button
                              variant="primary"
                              type="submit"
                              size="sm"
                              disabled={submitting}
                              data-js="searchBtn"
                              data-cy="searchBtn"
                            >
                              <i className="fa-fw fa fa-search"></i>
                              Search
                            </Button>
                          </Col>
                        </Row>
                      </form>
                    )}
                  />
                </Accordion.Body>
              </Accordion.Item>
            </Accordion>
          </Col>
        </Row>
        <Row>
          <Col sm={12} className="mt-3 text-end">
            <Dropdown
              as={ButtonGroup}
              key="left"
              drop="start"
              size="sm"
              className="manage-export"
              title="Export all below policies"
            >
              <Dropdown.Toggle
                data-name="downloadFormatBtn"
                data-cy="downloadFormatBtn"
              >
                <i className="fa-fw fa fa-external-link-square"></i> Export
              </Dropdown.Toggle>
              <Dropdown.Menu>
                <Dropdown.Item onClick={() => exportPolicy("downloadExcel")}>
                  Excel file
                </Dropdown.Item>
                <Dropdown.Divider />
                <Dropdown.Item onClick={() => exportPolicy("csv")}>
                  CSV file
                </Dropdown.Item>
                {!isAuditRole && (
                  <React.Fragment>
                    <Dropdown.Divider />
                    <Dropdown.Item onClick={() => exportPolicy("exportJson")}>
                      JSON file
                    </Dropdown.Item>
                  </React.Fragment>
                )}
              </Dropdown.Menu>
            </Dropdown>
          </Col>
        </Row>
        {filterServiceDefs?.map((serviceDef) => (
          <SearchPolicyTable
            key={serviceDef.name}
            serviceDef={serviceDef}
            services={services}
            searchParams={searchParamsObj}
            searchParamsUrl={location.search}
            contentLoader={contentLoader}
          ></SearchPolicyTable>
        ))}
      </div>
    </React.Fragment>
  );
}

export default UserAccessLayout;

function SearchByAsyncSelect(props) {
  const [searchByOptName, setSearchByOptName] = useState({
    value: "searchByGroup",
    label: "Group"
  });

  useEffect(() => {
    setSearchByOptName(props.searchByOptName);
  }, [props.searchByOptName]);

  const onChangeSearchByValue = (option, input) => {
    if (typeof props.onChange === "function") {
      props.onChange(option, input);
    }
  };

  const customStyles = {
    control: (provided) => ({
      ...provided,
      minHeight: "30px",
      height: "38px",
      marginLeft: 5
    }),
    indicatorsContainer: (provided) => ({
      ...provided,
      height: "30px"
    }),
    valueContainer: (provided) => ({
      ...provided,
      width: 215,
      padding: 4
    })
  };

  const fetchOpts = async (inputValue) => {
    let apiUrl = "";
    let params = {};
    let optsList = [];
    let serverResp = [];

    if (inputValue) {
      params["name"] = inputValue || "";
    }
    if (searchByOptName.value == "searchByGroup") {
      apiUrl = "xusers/lookup/groups";
    } else if (searchByOptName.value == "searchByUser") {
      apiUrl = "xusers/lookup/users";
    } else if (searchByOptName.value == "searchByRole") {
      apiUrl = "roles/roles";
    }
    if (!isEmpty(apiUrl)) {
      serverResp = await fetchApi({
        url: apiUrl,
        params: params
      });
    }

    if (searchByOptName.value == "searchByUser") {
      optsList = serverResp.data.vXStrings.map((obj) => ({
        label: obj["value"],
        value: obj["value"]
      }));
    } else if (searchByOptName.value == "searchByRole") {
      optsList = serverResp.data.roles.map(({ name }) => ({
        label: name,
        value: name
      }));
    } else {
      optsList = serverResp.data.vXStrings.map((obj) => ({
        label: obj["value"],
        value: obj["value"]
      }));
    }

    return optsList;
  };

  return (
    <Field name="searchByValue">
      {({ input }) => (
        <AsyncSelect
          {...input}
          key={JSON.stringify(searchByOptName)}
          onChange={(option) => onChangeSearchByValue(option, input)}
          styles={customStyles}
          cacheOptions
          isClearable={true}
          defaultOptions
          loadOptions={fetchOpts}
          components={{
            DropdownIndicator: () => null,
            IndicatorSeparator: () => null
          }}
        />
      )}
    </Field>
  );
}
