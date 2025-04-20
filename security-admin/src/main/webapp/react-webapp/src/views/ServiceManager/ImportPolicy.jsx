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
import { Form, Field } from "react-final-form";
import Select from "react-select";
import { Alert, Button, Col, Modal, Row } from "react-bootstrap";
import { FieldArray } from "react-final-form-arrays";
import arrayMutators from "final-form-arrays";
import { toast } from "react-toastify";
import {
  find,
  groupBy,
  has,
  isEmpty,
  map,
  toString,
  split,
  uniq
} from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import { serverError } from "Utils/XAUtils";
import { selectInputCustomStyles } from "Components/CommonComponents";

class ImportPolicy extends Component {
  constructor(props) {
    super(props);
    this.state = {
      file: null,
      fileName: null,
      fileJsonData: null,
      sourceServicesMap: null,
      destServices: null,
      sourceZoneName: "",
      destZoneName: "",
      initialFormFields: {
        isOverride: false
      },
      filterFormFields: {},
      fileNotJson: false
    };
  }

  removeFile = () => {
    this.setState({
      fileName: null,
      fileJsonData: null,
      fileNotJson: false,
      sourceServicesMap: null
    });
  };

  handleFileUpload = (e, values) => {
    e.preventDefault();
    const fileReader = new FileReader();
    fileReader.readAsText(e.target.files[0]);
    if (e.target && e.target.files.length > 0) {
      let file = e.target.files[0];
      let fileName = e.target.files[0].name;
      let fileExtention = split(fileName, ".").pop();

      if (fileExtention !== "json") {
        this.setState({
          fileName: fileName,
          fileNotJson: true
        });
        return;
      }
      this.setState({
        fileName: fileName,
        file: file
      });
    } else {
      return;
    }

    fileReader.onload = (e) => {
      let jsonParseFileData = JSON.parse(e.target.result);
      let servicesJsonParseFile = groupBy(
        jsonParseFileData.policies,
        function (policy) {
          return policy.service;
        }
      );

      let zoneNameJsonParseFile;
      if (
        has(jsonParseFileData, "policies") &&
        jsonParseFileData.policies.length > 0
      ) {
        zoneNameJsonParseFile = jsonParseFileData.policies[0].zoneName;
      }

      let serviceFieldsFromJson = Object.keys(servicesJsonParseFile).map(
        (obj) => {
          if (this.props.isParentImport) {
            return {
              sourceServiceName: { value: obj, label: obj },
              destServiceName: { value: obj, label: obj }
            };
          } else {
            let sameDefType = find(this.props.services, {
              name: obj,
              type: this.props.serviceDef.name
            });
            return {
              sourceServiceName: { value: obj, label: obj },
              destServiceName:
                sameDefType !== undefined
                  ? { value: obj, label: obj }
                  : undefined
            };
          }
        }
      );

      const formFields = {};
      formFields["serviceFields"] = serviceFieldsFromJson;
      formFields["sourceZoneName"] = zoneNameJsonParseFile;
      formFields["isOverride"] = values.isOverride;

      this.setState({
        fileJsonData: jsonParseFileData,
        sourceServicesMap: servicesJsonParseFile,
        destServices: this.props.isParentImport
          ? this.props.allServices
          : this.props.services,
        sourceZoneName: zoneNameJsonParseFile,
        initialFormFields: formFields,
        filterFormFields: formFields
      });
    };
  };

  importJsonFile = async (values) => {
    let serviceTypeList;
    let importDataResp;
    let servicesMapJson = {};
    let zoneMapJson = {};
    let importData = new FormData();

    map(values.serviceFields, function (field) {
      return (
        field !== undefined &&
        (servicesMapJson[field.sourceServiceName.value] =
          field.destServiceName.value)
      );
    });

    zoneMapJson[values.sourceZoneName] = this.state.destZoneName;

    importData.append("file", this.state.file);
    importData.append(
      "servicesMapJson",
      new Blob([JSON.stringify(servicesMapJson)], {
        type: "application/json"
      })
    );
    importData.append(
      "zoneMapJson",
      new Blob([JSON.stringify(zoneMapJson)], {
        type: "application/json"
      })
    );

    if (this.props.isParentImport) {
      serviceTypeList = toString(uniq(map(this.props.services, "type")));
    } else {
      serviceTypeList = this.props.serviceDef.name;
    }

    try {
      this.props.onHide();
      this.props.showBlockUI(true, importDataResp);
      importDataResp = await fetchApi({
        url: "/plugins/policies/importPoliciesFromFile",
        params: {
          serviceType: serviceTypeList,
          isOverride: values.isOverride
        },
        method: "post",
        data: importData
      });
      this.props.showBlockUI(false, importDataResp);
      toast.success("Successfully imported the file");
    } catch (error) {
      this.props.showBlockUI(false, error?.response);
      serverError(error);
      console.error(`Error occurred while importing policies! ${error}`);
    }
  };

  handleSelectedZone = async (e, values) => {
    const formFields = {};
    let zonesResp = [];

    try {
      if (e && e !== undefined) {
        zonesResp = await fetchApi({
          url: `public/v2/api/zones/${e && e.value}/service-headers`
        });

        let zoneServiceNames = map(zonesResp.data, "name");

        let zoneServices = zoneServiceNames.map((zoneService) => {
          return this.props.allServices.filter((service) => {
            return service.name === zoneService;
          });
        });

        zoneServices = zoneServices.flat();

        let serviceFieldsFromJson = Object.keys(
          this.state.sourceServicesMap
        ).map((obj) => {
          let zoneServiceType = find(zoneServices, {
            name: obj
          });
          return {
            sourceServiceName: { value: obj, label: obj },
            destServiceName:
              zoneServiceType !== undefined
                ? { value: obj, label: obj }
                : undefined
          };
        });

        formFields["serviceFields"] = serviceFieldsFromJson;
        formFields["sourceZoneName"] =
          this.state.initialFormFields["sourceZoneName"];
        formFields["isOverride"] = values.isOverride;

        this.setState({
          destZoneName: e && e.label,
          destServices: zoneServices,
          filterFormFields: formFields
        });
      } else {
        formFields["serviceFields"] =
          this.state.initialFormFields["serviceFields"];
        formFields["sourceZoneName"] =
          this.state.initialFormFields["sourceZoneName"];
        formFields["isOverride"] = values.isOverride;

        this.setState({
          destZoneName: "",
          destServices: this.props.isParentImport
            ? this.props.allServices
            : this.props.services,
          filterFormFields: formFields
        });
      }
    } catch (error) {
      serverError(error);
      console.error(
        `Error occurred while fetching Services from selected Zone! ${error}`
      );
    }
  };

  getSourceServiceOptions = () => {
    return Object.keys(this.state.sourceServicesMap).map((obj) => ({
      value: obj,
      label: obj
    }));
  };

  getDestServiceOptions = () => {
    return this.state.destServices.map((service) => ({
      value: service.name,
      label: service.name
    }));
  };

  Theme = (theme) => {
    return {
      ...theme,
      colors: {
        ...theme.colors,
        primary: "#0081ab"
      }
    };
  };

  CustomStyles = {
    ...selectInputCustomStyles,
    option: (provided, state) => ({
      ...provided,
      color: state.isSelected ? "white" : "black"
    })
  };

  requiredField = (value) =>
    value ? undefined : "Please select/enter service name";

  requiredFieldArray = (value) => {
    if (value && value.length > 0) {
      value.map((v) => {
        return v.sourceServiceName !== v.destServiceName
          ? "Source service name should not be same."
          : "";
      });
    }
  };

  render() {
    return (
      <React.Fragment>
        <Modal
          show={this.props.show}
          onHide={this.props.onHide}
          size={!isEmpty(this.props.services) ? "lg" : "md"}
        >
          <Form
            onSubmit={this.importJsonFile}
            mutators={{
              ...arrayMutators
            }}
            initialValues={this.state.filterFormFields}
            render={({
              handleSubmit,
              values,
              form: {
                mutators: { push: addItem }
              }
            }) => (
              <form onSubmit={handleSubmit}>
                <Modal.Header closeButton>
                  <Modal.Title>
                    {!isEmpty(this.props.services) ? (
                      isEmpty(this.props.selectedZone) ? (
                        "Import Policy"
                      ) : (
                        "Import Policy For Zone"
                      )
                    ) : (
                      <small>No service found to import policies.</small>
                    )}
                  </Modal.Title>
                </Modal.Header>
                {!isEmpty(this.props.services) && (
                  <Modal.Body>
                    <React.Fragment>
                      <Row>
                        <Row sm={12}>
                          <Col sm={7}>
                            <Field name="uploadPolicyFile">
                              {({ input }) => (
                                <div className="form-group col-sm-6">
                                  <label className="btn btn-sm border">
                                    Select File :
                                    <i className="fa-fw fa fa-arrow-circle-o-up"></i>
                                    <input
                                      {...input}
                                      style={{ display: "none" }}
                                      type="file"
                                      className="form-control-file"
                                      accept=" .json "
                                      onChange={(e) =>
                                        this.handleFileUpload(e, values)
                                      }
                                    />
                                  </label>
                                </div>
                              )}
                            </Field>
                          </Col>
                          <Col sm={5}>
                            <div className="form-check">
                              <Field
                                name="isOverride"
                                component="input"
                                type="checkbox"
                                className="form-check-input"
                              />
                              <label className="form-check-label">
                                Override Policy
                              </label>
                            </div>
                          </Col>
                        </Row>
                        <Col sm={12}>
                          {this.state.fileName ? (
                            <span>
                              {this.state.fileName}
                              <label
                                className="fa fa-fw fa-remove fa-remove-btn"
                                onClick={() => {
                                  this.removeFile();
                                }}
                              ></label>
                            </span>
                          ) : (
                            <span className="ms-1">No File Chosen</span>
                          )}
                        </Col>
                        <Col sm={12}>
                          {this.state.fileName && this.state.fileNotJson && (
                            <span className="invalid-field">
                              Please upload json format file
                            </span>
                          )}
                        </Col>
                      </Row>
                      {this.state.fileJsonData &&
                        !isEmpty(this.state.sourceServicesMap) && (
                          <React.Fragment>
                            <hr />
                            <Row>
                              <Col sm={12}>
                                <Alert variant="warning" show={true}>
                                  <i className="fa-fw fa fa-info-circle searchInfo m-r-xs"></i>
                                  All services gets listed on service
                                  destination when Zone destination is blank.
                                  When zone is selected at destination, then
                                  only services associated with that zone will
                                  be listed.
                                </Alert>
                              </Col>
                            </Row>
                            <Row>
                              <Col sm={12}>
                                <p className="fw-bold">
                                  Specify Zone Mapping :
                                </p>
                              </Col>
                            </Row>
                            <Row className="mt-3">
                              <Col sm={4}>
                                <div className="col text-center">Source</div>
                              </Col>
                              <Col sm={4}>
                                <div className="col text-center">
                                  Destination
                                </div>
                              </Col>
                            </Row>
                            <Row className="mt-3">
                              <Col sm={4}>
                                <Field name="sourceZoneName">
                                  {({ input }) => (
                                    <input
                                      {...input}
                                      type="text"
                                      className="form-control"
                                      disabled
                                    />
                                  )}
                                </Field>
                              </Col>
                              <Col sm={1}>To</Col>
                              <Col sm={4}>
                                <Select
                                  onChange={(e) =>
                                    this.handleSelectedZone(e, values)
                                  }
                                  isClearable
                                  components={{
                                    IndicatorSeparator: () => null
                                  }}
                                  theme={this.Theme}
                                  styles={this.CustomStyles}
                                  options={this.props.zones.map((zone) => {
                                    return {
                                      value: zone.id,
                                      label: zone.name
                                    };
                                  })}
                                  placeholder="No zone selected"
                                />
                              </Col>
                            </Row>
                            <hr />
                            <Row>
                              <Col sm={12}>
                                <p className="fw-bold">
                                  Specify Service Mapping :
                                </p>
                              </Col>
                            </Row>
                            <Row>
                              <Col sm={4}>
                                <div className="col text-center">Source</div>
                              </Col>
                              <Col sm={4}>
                                <div className="col text-center">
                                  Destination
                                </div>
                              </Col>
                            </Row>
                            <FieldArray name="serviceFields">
                              {({ fields }) =>
                                fields.map((name, index) => (
                                  <Row className="mt-2" key={name}>
                                    <Col sm={4}>
                                      <Field
                                        name={`${name}.sourceServiceName`}
                                        validate={this.requiredField}
                                      >
                                        {({ input, meta }) => (
                                          <React.Fragment>
                                            <Select
                                              {...input}
                                              searchable
                                              isClearable
                                              options={this.getSourceServiceOptions()}
                                              menuPlacement="auto"
                                              placeholder="Enter service name"
                                              theme={this.Theme}
                                              styles={this.CustomStyles}
                                            />
                                            {meta.error && meta.touched && (
                                              <span className="invalid-field">
                                                {meta.error}
                                              </span>
                                            )}
                                          </React.Fragment>
                                        )}
                                      </Field>
                                    </Col>
                                    <Col sm={1}>To</Col>
                                    <Col sm={4}>
                                      <Field
                                        name={`${name}.destServiceName`}
                                        validate={this.requiredField}
                                      >
                                        {({ input, meta }) => (
                                          <React.Fragment>
                                            <Select
                                              {...input}
                                              searchable
                                              isClearable
                                              options={this.getDestServiceOptions()}
                                              menuPlacement="auto"
                                              placeholder="Select service name"
                                              theme={this.Theme}
                                              styles={this.CustomStyles}
                                            />
                                            {meta.error && meta.touched && (
                                              <span className="invalid-field">
                                                {meta.error}
                                              </span>
                                            )}
                                          </React.Fragment>
                                        )}
                                      </Field>
                                    </Col>
                                    <Col sm={1}>
                                      <Button
                                        className="mt-1"
                                        variant="danger"
                                        size="sm"
                                        title="Remove"
                                        onClick={() => fields.remove(index)}
                                      >
                                        <i className="fa-fw fa fa-remove"></i>
                                      </Button>
                                    </Col>
                                  </Row>
                                ))
                              }
                            </FieldArray>
                            <Row className="mt-3">
                              <Col sm={2}>
                                <Button
                                  variant="outline-dark"
                                  size="sm"
                                  onClick={() =>
                                    addItem("serviceFields", undefined)
                                  }
                                >
                                  <i className="fa-fw fa fa-plus"></i>
                                </Button>
                              </Col>
                            </Row>
                          </React.Fragment>
                        )}
                      {this.state.fileJsonData &&
                        isEmpty(this.state.sourceServicesMap) && (
                          <React.Fragment>
                            <p className="invalid-field mt-2">
                              Json file uploaded is invalid.
                            </p>
                          </React.Fragment>
                        )}
                    </React.Fragment>
                  </Modal.Body>
                )}
                <Modal.Footer>
                  {!isEmpty(this.props.services) ? (
                    <>
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={this.props.onHide}
                      >
                        Cancel
                      </Button>
                      <Button
                        variant="primary"
                        type="submit"
                        size="sm"
                        disabled={isEmpty(this.state.sourceServicesMap)}
                      >
                        Import
                      </Button>
                    </>
                  ) : (
                    <Button
                      variant="primary"
                      size="sm"
                      onClick={this.props.onHide}
                    >
                      OK
                    </Button>
                  )}
                </Modal.Footer>
              </form>
            )}
          />
        </Modal>
      </React.Fragment>
    );
  }
}

export default ImportPolicy;
