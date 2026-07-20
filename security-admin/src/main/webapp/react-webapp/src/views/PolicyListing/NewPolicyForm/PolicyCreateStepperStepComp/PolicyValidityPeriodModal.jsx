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
import { Button, Modal, Table, InputGroup, FormControl } from "react-bootstrap";
import Select from "react-select";
import Datetime from "react-datetime";
import { getAllTimeZoneList } from "Utils/XAUtils";
import { isEmpty, cloneDeep } from "lodash";
import moment from "moment-timezone";
import {
  CustomTooltip,
  selectInputCustomStyles
} from "Components/CommonComponents";

const PolicyValidityPeriodModal = ({
  showModal,
  handleCloseModal,
  currentValues,
  onSave
}) => {
  const [draftValues, setDraftValues] = useState([]);

  // Initialize draft values when modal opens
  useEffect(() => {
    if (showModal) {
      setDraftValues(
        currentValues && currentValues.length > 0
          ? cloneDeep(currentValues)
          : []
      );
    }
  }, [showModal, currentValues]);

  const RenderInput = (props) => {
    function clear() {
      props.dateProps.onChange({ target: { value: "" } });
    }
    return (
      <InputGroup className="mb-2">
        <FormControl {...props.dateProps} readOnly />
        <InputGroup.Text onClick={clear} className="cursor-pointer">
          X
        </InputGroup.Text>
      </InputGroup>
    );
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

  const handleSave = () => {
    // Save draft values to actual form
    onSave(draftValues);
    handleCloseModal();
  };

  const handleCancel = () => {
    // Discard changes and close
    setDraftValues([]);
    handleCloseModal();
  };

  const handleAddTimePeriod = () => {
    setDraftValues([...draftValues, {}]);
  };

  const handleRemoveTimePeriod = (index) => {
    const newValues = [...draftValues];
    newValues.splice(index, 1);
    setDraftValues(newValues);
  };

  const handleFieldChange = (index, field, value) => {
    const newValues = [...draftValues];
    if (!newValues[index]) {
      newValues[index] = {};
    }
    newValues[index][field] = value;
    setDraftValues(newValues);
  };

  return (
    <Modal show={showModal} size="lg" onHide={handleCancel} backdrop="static">
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
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {draftValues.length > 0 ? (
              draftValues.map((schedule, index) => (
                <tr key={index}>
                  <td className="text-center">
                    <Datetime
                      value={schedule?.startTime || ""}
                      renderInput={(props) => <RenderInput dateProps={props} />}
                      dateFormat="MM-DD-YYYY"
                      timeFormat="HH:mm:ss"
                      closeOnSelect
                      isValidDate={(currentDate) =>
                        calEndDate(schedule, currentDate)
                      }
                      onChange={(value) =>
                        handleFieldChange(index, "startTime", value)
                      }
                    />
                  </td>
                  <td className="text-center">
                    <Datetime
                      value={schedule?.endTime || ""}
                      renderInput={(props) => <RenderInput dateProps={props} />}
                      dateFormat="MM-DD-YYYY"
                      timeFormat="HH:mm:ss"
                      closeOnSelect
                      isValidDate={(currentDate) =>
                        calStartDate(schedule, currentDate)
                      }
                      onChange={(value) =>
                        handleFieldChange(index, "endTime", value)
                      }
                    />
                  </td>
                  <td className="text-center">
                    <Select
                      value={schedule?.timeZone || null}
                      options={getAllTimeZoneList()}
                      getOptionLabel={(obj) => obj.text}
                      getOptionValue={(obj) => obj.id}
                      isClearable={true}
                      isDisabled={
                        isEmpty(schedule?.startTime) &&
                        isEmpty(schedule?.endTime)
                      }
                      styles={selectInputCustomStyles}
                      onChange={(value) =>
                        handleFieldChange(index, "timeZone", value)
                      }
                    />
                  </td>
                  <td className="text-center">
                    <Button
                      variant="danger"
                      size="sm"
                      className="btn-mini"
                      title="Remove"
                      onClick={() => handleRemoveTimePeriod(index)}
                      data-action="delete"
                      data-cy="delete"
                    >
                      <i className="fa-fw fa fa-remove"></i>
                    </Button>
                  </td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan="4" className="text-center text-muted">
                  No time periods added. Click Add Time &quot;Period&quot; to
                  get started.
                </td>
              </tr>
            )}
          </tbody>
        </Table>
        <Button
          type="button"
          variant="outline-primary"
          className="btn-sm"
          onClick={handleAddTimePeriod}
          data-action="addTime"
          data-cy="addTime"
        >
          <i className="fa-fw fa fa-plus"></i> Add Time Period
        </Button>
      </Modal.Body>
      <Modal.Footer>
        <Button variant="secondary" size="sm" onClick={handleCancel}>
          Cancel
        </Button>
        <Button variant="primary" size="sm" onClick={handleSave}>
          Save
        </Button>
      </Modal.Footer>
    </Modal>
  );
};

export default PolicyValidityPeriodModal;
