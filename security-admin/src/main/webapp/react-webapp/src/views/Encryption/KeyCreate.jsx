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

import React, { useReducer, useEffect, useState, useRef } from "react";
import { Button, Table, Row, Col } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import { toast } from "react-toastify";
import { FieldArray } from "react-final-form-arrays";
import arrayMutators from "final-form-arrays";
import { fetchApi } from "Utils/fetchAPI";
import {
  BlockUi,
  Loader,
  scrollToError
} from "../../components/CommonComponents";
import { commonBreadcrumb, serverError } from "../../utils/XAUtils";
import { cloneDeep, find, isEmpty, values } from "lodash";
import withRouter from "Hooks/withRouter";
import { useLocation, useNavigate } from "react-router-dom";
import usePrompt from "Hooks/usePrompt";
import { getServiceDef } from "../../utils/appState";

const initialState = {
  service: {},
  definition: {},
  loader: true
};

const keyCreateReducer = (state, action) => {
  switch (action.type) {
    case "SET_LOADER":
      return {
        ...state,
        loader: action.loader
      };
    case "SET_DATA":
      return {
        ...state,
        service: action.service,
        definition: action.definition,
        loader: action.loader
      };
    default:
      throw new Error();
  }
};

const PromtDialog = (props) => {
  const { isDirtyField, isUnblock } = props;
  usePrompt("Are you sure you want to leave", isDirtyField && !isUnblock);
  return null;
};

function KeyCreate(props) {
  const [keyDetails, dispatch] = useReducer(keyCreateReducer, initialState);
  const { loader, service, definition } = keyDetails;
  const { state } = useLocation();
  const navigate = useNavigate();
  const [preventUnBlock, setPreventUnblock] = useState(false);
  const [blockUI, setBlockUI] = useState(false);
  const toastId = useRef(null);
  const { allServiceDefs } = cloneDeep(getServiceDef());

  useEffect(() => {
    fetchInitialData();
  }, []);

  const fetchInitialData = async () => {
    await fetchKmsServices();
  };

  const handleSubmit = async (values) => {
    const serviceJson = {};
    if (values?.attributes.length > 0) {
      for (let key of Object.keys(values.attributes))
        if (
          !isEmpty(values?.attributes[key]?.name) ||
          !isEmpty(values?.attributes[key]?.value)
        ) {
          toast.dismiss(toastId.current);
          if (isEmpty(values?.attributes[key]?.name)) {
            toast.error(
              `Please enter Name for ${values?.attributes[key]?.value}`
            );
            return;
          }
          if (isEmpty(values?.attributes[key]?.value)) {
            toast.error(
              `Please enter Value for ${values?.attributes[key]?.name}`
            );
            return;
          }
        }
    }

    serviceJson.name = values.name;
    serviceJson.cipher = values.cipher;
    serviceJson.length = values.length;
    serviceJson.description = values.description;
    serviceJson.attributes = {};

    for (let key of Object.keys(values.attributes)) {
      if (
        !isEmpty(values?.attributes[key]?.name) &&
        !isEmpty(values?.attributes[key]?.value)
      ) {
        serviceJson.attributes[values.attributes[key].name] =
          values.attributes[key].value;
      }
    }
    setPreventUnblock(true);
    try {
      setBlockUI(true);
      await fetchApi({
        url: "keys/key",
        method: "post",
        params: {
          provider: props.params.serviceName
        },
        data: serviceJson
      });
      setBlockUI(false);
      toast.success(`Success! Key created successfully`);
      navigate(`/kms/keys/edit/manage/${state.detail}`, {
        state: {
          detail: state.detail
        }
      });
    } catch (error) {
      setBlockUI(false);
      serverError(error);
      console.error(`Error occurred while creating key! ${error}`);
    }
  };
  const fetchKmsServices = async () => {
    let serviceResp;
    dispatch({
      type: "SET_LOADER",
      loader: true
    });
    try {
      serviceResp = await fetchApi({
        url: `plugins/services/name/${props.params.serviceName}`
      });
    } catch (error) {
      console.error(`Error occurred while fetching Services! ${error}`);
    }

    dispatch({
      type: "SET_DATA",
      service: serviceResp,
      definition: find(allServiceDefs, { name: "kms" }),
      loader: false
    });
  };

  const closeForm = () => {
    navigate(`/kms/keys/edit/manage/${props.params.serviceName}`);
  };
  const validate = (values) => {
    const errors = {};
    if (!values.name) {
      errors.name = {
        required: true,
        text: "Required"
      };
    }
    return errors;
  };
  const keyCreateBreadcrumb = () => {
    let serviceDetails = {};
    serviceDetails["serviceDefId"] = definition && definition?.id;
    serviceDetails["serviceId"] = service.data && service.data.id;
    serviceDetails["serviceName"] = props.params.serviceName;
    return commonBreadcrumb(
      ["Kms", "KmsKeyForService", "KmsKeyCreate"],
      serviceDetails
    );
  };
  return loader ? (
    <Loader />
  ) : (
    <div>
      <div className="header-wraper">
        <h3 className="wrap-header bold">Key Detail</h3>
        {keyCreateBreadcrumb()}
      </div>
      <Form
        onSubmit={handleSubmit}
        keepDirtyOnReinitialize={true}
        mutators={{
          ...arrayMutators
        }}
        validate={validate}
        initialValues={{
          attributes: [{ name: "", value: "" }],
          cipher: "AES/CTR/NoPadding",
          length: "128"
        }}
        render={({
          handleSubmit,
          form,
          submitting,
          invalid,
          errors,
          dirty,
          form: {
            mutators: { push: addItem }
          }
        }) => (
          <div className="wrap">
            <PromtDialog isDirtyField={dirty} isUnblock={preventUnBlock} />
            <form
              onSubmit={(event) => {
                handleSubmit(event);
              }}
            >
              <Field name="name">
                {({ input, meta }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label float-end">Key Name *</label>
                    </Col>
                    <Col xs={4}>
                      <input
                        {...input}
                        name="name"
                        type="text"
                        id={meta.error && meta.touched ? "isError" : "name"}
                        className={
                          meta.error && meta.touched
                            ? "form-control border-danger"
                            : "form-control"
                        }
                        data-cy="name"
                      />
                      {meta.error && meta.touched && (
                        <span className="invalid-field">{meta.error.text}</span>
                      )}
                    </Col>
                  </Row>
                )}
              </Field>

              <Field name="cipher">
                {({ input }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label float-end">Cipher</label>
                    </Col>
                    <Col xs={4}>
                      <input
                        {...input}
                        name="cipher"
                        type="text"
                        className="form-control"
                        data-cy="cipher"
                      />
                    </Col>
                  </Row>
                )}
              </Field>

              <Field name="length">
                {({ input }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label float-end">Length</label>
                    </Col>
                    <Col xs={4}>
                      <input
                        {...input}
                        name="length"
                        type="number"
                        className="form-control"
                        data-cy="length"
                      />
                    </Col>
                  </Row>
                )}
              </Field>
              <Field name="description">
                {({ input }) => (
                  <Row className="form-group">
                    <Col xs={3}>
                      <label className="form-label float-end">
                        Description
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
              <Row className="form-group">
                <Col xs={3}>
                  <label className="form-label float-end">Attributes</label>
                </Col>
                <Col xs={6}>
                  <Table bordered size="sm" className="no-bg-color w-75">
                    <thead>
                      <tr>
                        <th className="text-center">Name</th>
                        <th className="text-center" colSpan="2">
                          Value
                        </th>
                      </tr>
                    </thead>
                    <tbody>
                      <FieldArray name="attributes">
                        {({ fields }) =>
                          fields.map((name, index) => (
                            <tr key={name}>
                              <td className="text-center">
                                <Field
                                  name={`${name}.name`}
                                  component="input"
                                  className="form-control"
                                />
                              </td>
                              <td className="text-center">
                                <Field
                                  name={`${name}.value`}
                                  component="input"
                                  className="form-control"
                                />
                              </td>
                              <td className="text-center">
                                <Button
                                  variant="danger"
                                  size="sm"
                                  title="Yes"
                                  onClick={() => fields.remove(index)}
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
                <Col xs={3}>
                  <label className="form-label float-end"></label>
                </Col>
                <Col xs={6}>
                  <Button
                    variant="outline-secondary"
                    size="sm"
                    onClick={() => addItem("attributes")}
                    data-action="addGroup"
                    data-cy="addGroup"
                  >
                    <i className="fa-fw fa fa-plus"></i>
                  </Button>
                </Col>
              </Row>
              <Row className="form-actions">
                <Col sm={{ span: 9, offset: 3 }}>
                  <Button
                    variant="primary"
                    onClick={() => {
                      if (invalid) {
                        let selector =
                          document.getElementById("isError") ||
                          document.querySelector(
                            `input[name=${Object.keys(errors)[0]}]`
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
                      closeForm();
                    }}
                    disabled={submitting}
                    data-id="cancel"
                    data-cy="cancel"
                  >
                    Cancel
                  </Button>
                </Col>
              </Row>
            </form>
          </div>
        )}
      />
      <BlockUi isUiBlock={blockUI} />
    </div>
  );
}

export default withRouter(KeyCreate);
