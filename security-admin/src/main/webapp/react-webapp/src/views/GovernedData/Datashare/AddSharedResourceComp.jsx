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
import ResourceComp from "../../Resources/ResourceComp";
import { useParams, useNavigate, Link } from "react-router-dom";
import { fetchApi } from "../../../utils/fetchAPI";
import { Loader } from "../../../components/CommonComponents";
import {
  Button,
  Col,
  Accordion,
  Card,
  Modal,
  Form as FormB
} from "react-bootstrap";
import { Form, Field, useForm } from "react-final-form";
import arrayMutators from "final-form-arrays";
import Select from "react-select";
import { groupBy, isArray, maxBy, find } from "lodash";
import { toast } from "react-toastify";
import moment from "moment-timezone";
import { getServiceDef } from "../../../utils/appState";
import { capitalizeFirstLetter } from "../../../utils/XAUtils";

const AddSharedResourceComp = ({
  datashareId,
  sharedResource,
  showModal,
  setShowModal,
  resourceModalUpdateTable,
  datashareInfo,
  setResourceUpdateTable,
  serviceDetails,
  isEdit
}) => {
  const [accessType, setAccessType] = useState();
  const [showMaskInput, setShowMaskInput] = useState(false);
  const [showRowFilterInput, setShowRowFilterInput] = useState(false);
  const [loader, setLoader] = useState(false);
  const [showAddResourceModal, setShowAddResourceModal] = useState(
    showModal ? true : false
  );
  let closeModalFag = false;
  const [formData, setFormData] = useState();
  const [selectedResShareMask, setSelectedResShareMask] = useState({});
  const { allServiceDefs } = getServiceDef();

  const serviceDef = allServiceDefs?.find((servicedef) => {
    return servicedef.name == serviceDetails?.type;
  });

  useEffect(() => {
    loadData();
  }, [resourceModalUpdateTable]);

  const loadData = () => {
    if (serviceDef != null && serviceDef != undefined) {
      if (Object.keys(serviceDef).length != 0) {
        if (Object.keys(serviceDef.rowFilterDef).length !== 0) {
          setShowMaskInput(true);
        }
        if (Object.keys(serviceDef.dataMaskDef).length !== 0) {
          setShowRowFilterInput(true);
        }
      }
    }

    if (sharedResource !== undefined) {
      setFormData(generateFormData(sharedResource, serviceDef));
    }
  };

  const generateFormData = (obj, serviceDef) => {
    let data = {};
    data.shareName = obj?.name;
    let serviceCompResourcesDetails = serviceDef?.resources;
    if (obj?.resource) {
      let lastResourceLevel = [];
      Object.entries(obj?.resource).map(([key, value]) => {
        let setResources = find(serviceCompResourcesDetails, ["name", key]);
        data[`resourceName-${setResources?.level}`] = setResources;
        data[`value-${setResources?.level}`] = value.values.map((m) => {
          return { label: m, value: m };
        });
        lastResourceLevel.push({
          level: setResources.level,
          name: setResources.name
        });
      });
      lastResourceLevel = maxBy(lastResourceLevel, "level");
      let setLastResources = find(serviceCompResourcesDetails, [
        "parent",
        lastResourceLevel.name
      ]);
      if (setLastResources && setLastResources?.isValidLeaf) {
        data[`resourceName-${setLastResources.level}`] = {
          label: "None",
          value: "none"
        };
      }
    }
    if (obj?.accessTypes != undefined) {
      data.permission = obj.accessTypes.map((item) => ({
        label: capitalizeFirstLetter(item),
        value: item
      }));
    }
    if (obj?.rowFilter != undefined) {
      data.rowFilter = obj.rowFilter["filterExpr"];
    }
    data.booleanExpression = obj?.conditionExpr;
    return data;
  };

  const noneOptions = {
    label: "None",
    value: "none"
  };

  const getAccessTypeOptions = (formValues) => {
    let srcOp = [];
    if (serviceDef != undefined) {
      const { resources = [] } = serviceDef;
      const grpResources = groupBy(resources, "level");
      let grpResourcesKeys = [];
      for (const resourceKey in grpResources) {
        grpResourcesKeys.push(+resourceKey);
      }
      grpResourcesKeys = grpResourcesKeys.sort();
      for (let i = grpResourcesKeys.length - 1; i >= 0; i--) {
        let selectedResource = `resourceName-${grpResourcesKeys[i]}`;
        if (
          formValues[selectedResource] &&
          formValues[selectedResource].value !== noneOptions.value
        ) {
          srcOp = serviceDef.accessTypes;
          if (formValues[selectedResource].accessTypeRestrictions?.length > 0) {
            let op = [];
            for (const name of formValues[selectedResource]
              .accessTypeRestrictions) {
              let typeOp = find(srcOp, { name });
              if (typeOp) {
                op.push(typeOp);
              }
            }
            srcOp = op;
          }
          break;
        }
      }

      return srcOp.map(({ label, name: value }) => ({
        label,
        value
      }));
    }
  };

  const onAccessTypeChange = (event, input) => {
    setAccessType(event);
    input.onChange(event);
  };

  const handleSubmit = async (values, form) => {
    let data = {};
    data.dataShareId = datashareId;
    let serviceCompRes = serviceDef.resources;
    const grpResources = groupBy(serviceCompRes || [], "level");
    let grpResourcesKeys = [];
    for (const resourceKey in grpResources) {
      grpResourcesKeys.push(+resourceKey);
    }
    grpResourcesKeys = grpResourcesKeys.sort();
    data.resource = {};
    for (const level of grpResourcesKeys) {
      if (
        values[`resourceName-${level}`] &&
        values[`resourceName-${level}`].value !== noneOptions.value
      ) {
        let defObj = serviceCompRes.find(function (m) {
          if (m.name == values[`resourceName-${level}`].name) {
            return m;
          }
        });
        data.resource[values[`resourceName-${level}`].name] = {
          values: isArray(values[`value-${level}`])
            ? values[`value-${level}`]?.map(({ value }) => value)
            : [values[`value-${level}`].value]
        };
        data.resource[values[`resourceName-${level}`].name]["isRecursive"] =
          values[`resourceName-${level}`].recursiveSupported;
      }
    }
    data.conditionExpr = values.booleanExpression;
    data.name = values.shareName;
    data.accessTypes = values.permission?.map((item) => item.value);
    data.rowFilter = values.rowFilter;
    data.resourceMask = values.resourceMask;
    let errorList = [];
    if (isEdit) {
      setLoader(true);
      try {
        data.guid = sharedResource.guid;
        await fetchApi({
          url: `gds/resource/${sharedResource.id}`,
          method: "put",
          data: data
        });
        toast.success("Shared resource updated successfully!!");
        setShowModal(false);
      } catch (error) {
        errorList.push(error);
        toast.error("Error occurred while updating Shared resource");
        console.error(`Error occurred while updating Shared resource ${error}`);
      }
    } else {
      try {
        await fetchApi({
          url: `gds/resource`,
          method: "post",
          data: data,
          skipNavigate: true
        });
        toast.success("Shared resource created successfully!!");
        if (closeModalFag) {
          closeModalFag = false;
          setShowModal(false);
          values = {};
        } else {
          Object.keys(values).forEach((key) => {
            if (!key.includes("resourceName")) {
              form.change(key, undefined);
            }
          });
        }
      } catch (error) {
        errorList.push(error);
        let errorMsg = "Error occurred while creating Shared resource";
        if (error?.response?.data?.msgDesc) {
          errorMsg = error.response.data.msgDesc;
        }
        toast.error(errorMsg);
        console.error(`Error occurred while creating Shared resource ${error}`);
      }
    }
    setResourceUpdateTable(moment.now());
    setLoader(false);
  };

  const toggleAddResourceClose = () => {
    setShowModal(false);
  };

  const onResShareMaskChange = (event, input) => {
    setSelectedResShareMask(event);
    input.onChange(event);
  };

  const addResource = () => {
    setShowAddResourceModal(true);
  };

  const toggleAddResourceModalClose = () => {
    setShowModal(false);
  };

  return (
    <>
      {loader ? (
        <Loader />
      ) : (
        <>
          <Modal
            show={showModal}
            onHide={toggleAddResourceModalClose}
            size="xl"
          >
            <Modal.Header closeButton>
              <h5 className="mb-0">{!isEdit ? "Add" : "Edit"} Resources</h5>
            </Modal.Header>
            <div>
              <div className="mb-2 gds-chips flex-wrap">
                <span
                  title={datashareInfo?.name}
                  className="badge text-bg-light text-truncate"
                  style={{ maxWidth: "250px", display: "inline-block" }}
                >
                  Datashare: {datashareInfo?.name}
                </span>
                <span
                  title={datashareInfo?.service}
                  className="badge text-bg-light text-truncate"
                  style={{ maxWidth: "250px", display: "inline-block" }}
                >
                  Service: {datashareInfo?.service}
                </span>
                {datashareInfo?.zone != undefined ? (
                  <span
                    title={datashareInfo.zone}
                    className="badge text-bg-light text-truncate"
                    style={{ maxWidth: "250px", display: "inline-block" }}
                  >
                    Security Zone: {datashareInfo.zone}
                  </span>
                ) : (
                  <div></div>
                )}
              </div>
              <Form
                onSubmit={handleSubmit}
                initialValues={isEdit ? formData : {}}
                mutators={{
                  ...arrayMutators
                }}
                render={({
                  handleSubmit,
                  values,
                  invalid,
                  errors,
                  required,
                  form
                }) => (
                  <div>
                    <form
                      className="mb-5"
                      onSubmit={(event) => {
                        if (invalid) {
                          forIn(errors, function (value, key) {
                            if (
                              has(errors, "policyName") ||
                              resourceErrorCheck(errors, values)
                            ) {
                              let selector =
                                document.getElementById("isError") ||
                                document.getElementById(key) ||
                                document.querySelector(`input[name=${key}]`) ||
                                document.querySelector(`input[id=${key}]`) ||
                                document.querySelector(
                                  `span[className="invalid-field"]`
                                );
                            } else {
                              getValidatePolicyItems(errors?.[key]);
                            }
                          });
                        }
                      }}
                    >
                      <Modal.Body>
                        <div className="mb-4 mt-4">
                          <h6>Resource Details</h6>
                          <hr className="mt-1 mb-1 gds-hr" />
                        </div>
                        <div className="mb-3 form-group row">
                          <Col sm={3}>
                            <label className="form-label float-end fnt-14">
                              Shared Resource Name *
                            </label>
                          </Col>
                          <Col sm={9}>
                            <Field
                              name="shareName"
                              render={({ input, meta }) => (
                                <div className="flex-1">
                                  <input
                                    {...input}
                                    type="text"
                                    name="shareName"
                                    className="form-control"
                                    data-cy="shareName"
                                  />
                                </div>
                              )}
                            />
                          </Col>
                        </div>
                        <div className="gds-resource-hierarchy">
                          <ResourceComp
                            serviceDetails={serviceDetails}
                            serviceCompDetails={serviceDef}
                            formValues={values}
                            policyType={0}
                            policyItem={false}
                            policyId={0}
                            isGds={true}
                          />
                        </div>

                        <Accordion className="mb-3" defaultActiveKey="0">
                          <Accordion.Item>
                            <Accordion.Header
                              className="border-0"
                              eventKey="1"
                              data-id="panel"
                              data-cy="panel"
                            >
                              Add permission and conditions (Optional)
                            </Accordion.Header>
                            <Accordion.Body eventKey="1">
                              <div className="mb-3 form-group row">
                                <Col sm={3}>
                                  <label className="form-label float-end fnt-14">
                                    Permission
                                  </label>
                                </Col>
                                <Field
                                  name="permission"
                                  render={({ input, meta }) => (
                                    <Col sm={9}>
                                      <Select
                                        {...input}
                                        options={getAccessTypeOptions(values)}
                                        onChange={(e) =>
                                          onAccessTypeChange(e, input)
                                        }
                                        menuPlacement="auto"
                                        isClearable
                                        isMulti
                                      />
                                    </Col>
                                  )}
                                />
                              </div>
                              {false && showRowFilterInput ? (
                                <div className="mb-3 form-group row">
                                  <Col sm={3}>
                                    <label className="form-label float-end fnt-14">
                                      Row Filter
                                    </label>
                                  </Col>

                                  <Field
                                    name={`rowFilter`}
                                    render={({ input, meta }) => (
                                      <Col sm={9}>
                                        <input
                                          {...input}
                                          type="text"
                                          name="rowFilter"
                                          className="form-control gds-placeholder"
                                          data-cy="rowFilter"
                                        />
                                      </Col>
                                    )}
                                  />
                                </div>
                              ) : (
                                <div></div>
                              )}

                              {false && showMaskInput ? (
                                <div className="mb-3 form-group row">
                                  <Col sm={3}>
                                    <label className="form-label float-end fnt-14">
                                      Mask
                                    </label>
                                  </Col>

                                  <Field
                                    name={`maskType`}
                                    render={({ input, meta }) => (
                                      <Col sm={9}>
                                        <Field
                                          name="masking"
                                          render={({ input, meta }) => (
                                            <div className="d-flex ">
                                              <div className="w-50">
                                                <Select
                                                  {...input}
                                                  options={
                                                    serviceDef.dataMaskDef
                                                      .maskTypes
                                                  }
                                                  onChange={(e) =>
                                                    onResShareMaskChange(
                                                      e,
                                                      input
                                                    )
                                                  }
                                                  menuPlacement="auto"
                                                  isClearable
                                                />
                                              </div>
                                              {selectedResShareMask?.label ==
                                                "Custom" && (
                                                <div className="pl-2 w-50">
                                                  <Field
                                                    className="form-control"
                                                    name={`resShareDataMaskInfo.valueExpr`}
                                                    validate={required}
                                                    render={({
                                                      input,
                                                      meta
                                                    }) => (
                                                      <>
                                                        <FormB.Control
                                                          type="text"
                                                          className="gds-input"
                                                          {...input}
                                                          placeholder="Enter masked value or expression..."
                                                        />
                                                        {meta.error &&
                                                          meta.touched && (
                                                            <span className="invalid-field">
                                                              {meta.error}
                                                            </span>
                                                          )}
                                                      </>
                                                    )}
                                                  />
                                                </div>
                                              )}
                                            </div>
                                          )}
                                        />
                                      </Col>
                                    )}
                                  />
                                </div>
                              ) : (
                                <div></div>
                              )}

                              <div className="mb-0 form-group row">
                                <Col sm={3}>
                                  <label className="form-label float-end fnt-14">
                                    Condition
                                  </label>
                                </Col>
                                <Field
                                  name={`booleanExpression`}
                                  render={({ input, meta }) => (
                                    <Col sm={9}>
                                      <textarea
                                        {...input}
                                        placeholder="Enter Boolean Expression"
                                        className="form-control gds-placeholder"
                                        id="booleanExpression"
                                        data-cy="booleanExpression"
                                        rows={4}
                                      />
                                    </Col>
                                  )}
                                />
                              </div>
                            </Accordion.Body>
                          </Accordion.Item>
                        </Accordion>
                      </Modal.Body>
                      <Modal.Footer>
                        <div className="d-flex gap-half justify-content-end w-100">
                          <Button
                            variant="secondary"
                            size="sm"
                            onClick={toggleAddResourceClose}
                          >
                            Cancel
                          </Button>
                          <Button
                            variant="primary"
                            size="sm"
                            name="SaveAndClose"
                            onClick={() => {
                              closeModalFag = true;
                              handleSubmit();
                            }}
                          >
                            {isEdit ? "Update" : "Save & Close"}
                          </Button>
                          {!isEdit ? (
                            <Button
                              name="Save"
                              onClick={() => {
                                closeModalFag = false;
                                handleSubmit();
                              }}
                              variant="primary"
                              size="sm"
                              data-id="save"
                              data-cy="save"
                            >
                              Save & Add Another
                            </Button>
                          ) : (
                            <div></div>
                          )}
                        </div>
                      </Modal.Footer>
                    </form>
                  </div>
                )}
              />
            </div>
          </Modal>
        </>
      )}
    </>
  );
};

export default AddSharedResourceComp;
