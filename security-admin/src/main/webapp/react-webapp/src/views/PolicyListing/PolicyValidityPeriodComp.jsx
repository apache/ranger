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

import React, { useState } from "react";
import { Button, Modal, Table, InputGroup, FormControl } from "react-bootstrap";
import { FieldArray } from "react-final-form-arrays";
import { Field } from "react-final-form";
import Select from "react-select";
import Datetime from "react-datetime";
import { getAllTimeZoneList } from "Utils/XAUtils";
import { isEmpty } from "lodash";
import moment from "moment-timezone";
import {
  CustomTooltip,
  selectInputCustomStyles
} from "Components/CommonComponents";

export default function PolicyValidityPeriodComp(props) {
  const { addPolicyItem } = props;
  const [showModal, setModal] = useState(false);
  const toggleModal = () => setModal((open) => !open);

  const handleBtnClick = () => {
    setModal(true);
  };

  const RenderInput = (props) => {
    function clear() {
      props.dateProps.onChange({ target: { value: "" } });
    }
    return (
      <>
        <InputGroup className="mb-2">
          <FormControl {...props.dateProps} readOnly />
          <InputGroup.Text onClick={clear} className="cursor-pointer">
            {" "}
            X{" "}
          </InputGroup.Text>
        </InputGroup>
      </>
    );
  };

  const validationForTimePeriod = () => {
    setModal(false);
  };

  const calEndDate = (sDate, currentDate) => {
    if (sDate && sDate?.endTime) {
      return currentDate.isBefore(sDate.endTime);
    } else {
      return true;
    }
  };

  const calStartDate = (sDate, currentDate) => {
    if (sDate && sDate?.startTime) {
      return currentDate.isAfter(sDate.startTime);
    } else {
      let yesterday = moment().subtract(1, "day");
      return currentDate.isAfter(yesterday);
    }
  };

  return (
    <>
      <Button
        onClick={handleBtnClick}
        variant="primary"
        size="sm"
        className="float-end btn-sm"
        data-js="policyTimeBtn"
        data-cy="policyTimeBtn"
      >
        <i className="fa fa-clock-o"></i> Add Validity Period
      </Button>
      <Modal show={showModal} size="lg" onHide={toggleModal} backdrop="static">
        <Modal.Header closeButton>
          <Modal.Title>Policy Validity Period</Modal.Title>
        </Modal.Header>
        <Modal.Body>
          <Table bordered>
            <thead>
              <tr>
                <th>Start Date</th>
                <th>End Date</th>
                <th>
                  Time Zone{" "}
                  <span>
                    <CustomTooltip
                      placement="right"
                      content={
                        <p className="pd-10" style={{ fontSize: "small" }}>
                          Please Select StartDate or EndDate to enable Time Zone
                        </p>
                      }
                      icon="fa-fw fa fa-info-circle"
                    />
                  </span>
                </th>
              </tr>
            </thead>
            <tbody>
              <FieldArray name="validitySchedules">
                {({ fields }) =>
                  fields.map((name, index) => (
                    <tr key={name}>
                      <td className="text-center">
                        <Field
                          className="form-control"
                          name={`${name}.startTime`}
                          placeholder="&#xF007; Username"
                          render={({ input, meta }) => (
                            <div>
                              <Datetime
                                {...input}
                                renderInput={(props) => (
                                  <RenderInput dateProps={props} />
                                )}
                                dateFormat="MM-DD-YYYY"
                                timeFormat="HH:mm:ss"
                                closeOnSelect
                                isValidDate={(currentDate) =>
                                  calEndDate(fields.value[index], currentDate)
                                }
                              />
                              {meta.touched && meta.error && (
                                <span>{meta.error}</span>
                              )}
                            </div>
                          )}
                        />
                      </td>
                      <td className="text-center">
                        <Field
                          className="form-control"
                          name={`${name}.endTime`}
                          render={({ input, meta }) => (
                            <div>
                              <Datetime
                                {...input}
                                renderInput={(props) => (
                                  <RenderInput dateProps={props} />
                                )}
                                dateFormat="MM-DD-YYYY"
                                timeFormat="HH:mm:ss"
                                closeOnSelect
                                isValidDate={(currentDate) =>
                                  calStartDate(fields.value[index], currentDate)
                                }
                              />
                              {meta.touched && meta.error && (
                                <span>{meta.error}</span>
                              )}
                            </div>
                          )}
                        />
                      </td>
                      <td className="text-center">
                        <Field
                          className="form-control"
                          name={`${name}.timeZone`}
                          render={({ input, meta }) => (
                            <div>
                              <Select
                                {...input}
                                options={getAllTimeZoneList()}
                                getOptionLabel={(obj) => obj.text}
                                getOptionValue={(obj) => obj.id}
                                isClearable={true}
                                isDisabled={
                                  isEmpty(fields?.value?.[index]?.startTime) &&
                                  isEmpty(fields?.value?.[index]?.endTime)
                                    ? true
                                    : false
                                }
                                styles={selectInputCustomStyles}
                              />
                              {meta.touched && meta.error && (
                                <span>{meta.error}</span>
                              )}
                            </div>
                          )}
                        />
                      </td>
                      <td className="text-center">
                        <Button
                          variant="danger"
                          size="sm"
                          className="btn-mini"
                          title="Remove"
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
          <Button
            type="button"
            className="btn-mini"
            onClick={() => addPolicyItem("validitySchedules", undefined)}
            data-action="addTime"
            data-cy="addTime"
          >
            <i className="fa-fw fa fa-plus"></i>
          </Button>
        </Modal.Body>
        <Modal.Footer>
          <Button variant="primary" size="sm" onClick={validationForTimePeriod}>
            Close
          </Button>
        </Modal.Footer>
      </Modal>
    </>
  );
}
