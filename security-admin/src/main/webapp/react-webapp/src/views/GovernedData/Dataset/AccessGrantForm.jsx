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
import { Form as FormB, Button, Row, Col, Table, InputGroup, FormControl } from "react-bootstrap";
import { Form, Field } from "react-final-form";
import arrayMutators from "final-form-arrays";
import AsyncSelect from "react-select/async";
import Select from "react-select";
import { FieldArray } from "react-final-form-arrays";
import moment from "moment-timezone";
import BootstrapSwitchButton from "bootstrap-switch-button-react";
import Datetime from "react-datetime";
import PolicyValidityPeriodComp from "../../PolicyListing/PolicyValidityPeriodComp";
import { getAllTimeZoneList } from "../../../utils/XAUtils";
import { isEmpty } from "lodash";


function AccessGrantForm(props) {

  const { addPolicyItem } = props;
  const [validitySchedules, setValiditySchedules] = useState([{name: "validitySchedules[0]",
      value: ''}]);


  // useEffect(() => {
  //   // Should not ever set state during rendering, so do this in useEffect instead.
  //   const newObj = {
  //     name: "validitySchedules[0]",
  //     value: ''
  //   };
  //   setValiditySchedules(newObj);
  //   console.log(validitySchedules)
  // }, []);

     const handleSubmit = async (formData) => {
     }
    
    const serviceSelectTheme = (theme) => {
    return {
      ...theme,
      colors: {
        ...theme.colors,
        primary: "#0081ab"
      }
    };
    };
    
    const RenderInput = (props, openCalendar, closeCalendar) => {
    function clear() {
      props.dateProps.onChange({ target: { value: "" } });
    }
    return (
      <>
        <InputGroup className="mb-2">
          <FormControl {...props.dateProps} readOnly />
          <InputGroup.Prepend>
            <InputGroup.Text onClick={clear}> X </InputGroup.Text>
          </InputGroup.Prepend>
        </InputGroup>
      </>
    );
    };

    const calStartDate = (sDate, currentDate) => {
        if (sDate && sDate?.startTime) {
        return currentDate.isAfter(sDate.startTime);
        } else {
        let yesterday = moment().subtract(1, "day");
        return currentDate.isAfter(yesterday);
        }
    };
  
  const calEndDate = (sDate, currentDate) => {
    if (sDate && sDate?.endTime) {
      return currentDate.isBefore(sDate.endTime);
    } else {
      return true;
    }
  };

  const removeValiditySchedule = (index) => { 
    validitySchedules.splice(index, 1);
    renderValiditySchedule();
  }

    const serviceSelectCustomStyles = {
    option: (provided, state) => ({
      ...provided,
      color: state.isSelected ? "white" : "black"
    }),
    control: (provided) => ({
      ...provided,
      maxHeight: "32px",
      minHeight: "32px"
    }),
      indicatorsContainer: (provided) => ({
      
      ...provided,
      maxHeight: "30px"
    }),
    dropdownIndicator: (provided) => ({
      ...provided,
      padding: "5px"
    }),
    clearIndicator: (provided) => ({
      ...provided,
      padding: "5px"
    }),
    container: (styles) => ({ ...styles, width: "150px" })
    };
  
  
  const addValiditySchedule = (e) => {
    const size = validitySchedules.length == undefined ? 0 : validitySchedules.length;
    const newObj = {
      name: "validitySchedules["+size+"]",
      value: ''
    };
    setValiditySchedules([...validitySchedules, newObj]);
  }



  const renderValiditySchedule = () => { 
    return validitySchedules?.map((obj, index) => {
      return <tr key={obj.name}>
                <td className="text-center">
                                        <Field
                                          className="form-control"
                                          name={`${obj.name}.startTime`}
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
                                                isValidDate={(currentDate, selectedDate) =>
                                                  calEndDate(validitySchedules[index], currentDate)
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
                          name={`${obj.name}.endTime`}
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
                                isValidDate={(currentDate, selectedDate) =>
                                  calStartDate(validitySchedules[index], currentDate)
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
                          name={`${obj.name}.timeZone`}
                          render={({ input, meta }) => (
                            <div>
                              <Select
                                {...input}
                                options={getAllTimeZoneList()}
                                getOptionLabel={(obj) => obj.text}
                                getOptionValue={(obj) => obj.id}
                                isClearable={true}
                                /*isDisabled={
                                  isEmpty(validitySchedules?.[index]?.startTime) &&
                                  isEmpty(validitySchedules?.[index]?.endTime)
                                    ? true
                                    : false
                                }*/
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
                          onClick={() => removeValiditySchedule(index)}
                          data-action="delete"
                          data-cy="delete"
                        >
                          <i className="fa-fw fa fa-remove"></i>
                        </Button>
                      </td>
              </tr>
    })
  }
    
    return (
        <>
            <div className="wrap-gds">
                <Form
                    onSubmit={handleSubmit}
                    mutators={{
                    ...arrayMutators
                    }}
                    render={({
                    handleSubmit,
                    form: {
                    mutators: { push, pop }
                    },
                    form,
                    submitting,
                    invalid,
                    errors,
                    values,
                    fields,
                    pristine,
                    dirty
                }) => (
                    <div className="wrap user-role-grp-form">

                    <form
                        onSubmit={(event) => {
                        handleSubmit(event);
                        }}
                    >
                    <Row className="form-group">
                        <Col sm={5}>    <p className="formHeader">Basic Details</p>   </Col>
                    </Row>
                        <Field name="policyName">
                        {({ input, meta }) => (
                            <Row className="form-group">
                                <Col sm={5}>
                                    <FormB.Control
                                      {...input}
                                      placeholder="Policy Name"
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
                                      data-cy="policyName"
                                    />
                                    {meta.touched && meta.error && (
                                      <span className="invalid-field">
                                        {meta.error.text}
                                      </span>
                                    )}
                                    </Col>
                                  <Field
                                    className="form-control"
                                    name="enableAccessGrant"
                                    render={({ input }) => (
                                    <>
                                        <FormB.Label column sm={3}>
                                            <span className="pull-right fnt-14">
                                                Enable Access Grant
                                            </span>
                                        </FormB.Label>                
                                        <>
                                            <Col sm={1}>
                                                <BootstrapSwitchButton
                                                {...input}
                                                className="abcd"
                                                checked={!(input.value === false)}
                                                onstyle="primary"
                                                offstyle="outline-secondary"
                                                style="w-100"
                                                size="xs"
                                                key="isEnabled"
                                                />
                                            </Col>
                                         </>
                                    </>
                                     )}
                                    />  
                            </Row>
                        )}
                        </Field>
                                
                        <Field name="policyLabel">
                        {({ input, meta }) => (
                            <Row className="form-group">
                                <Col sm={5}>
                                    <FormB.Control
                                      {...input}
                                      placeholder="Policy Label"
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
                                      data-cy="policyLabel"
                                    />
                                    {meta.touched && meta.error && (
                                      <span className="invalid-field">
                                        {meta.error.text}
                                      </span>
                                    )}
                                    </Col>
                                     <Col sm={5}>    <p className="formHeader">Conditions</p>   </Col>
                            </Row>
                        )}
                        </Field>
                                

                        <Field name="policyDscription">
                        {({ input, meta }) => (
                            <Row className="form-group">
                              <Col sm={5}>
                                <FormB.Control
                                  {...input}
                                  as="textarea"
                                  rows={3}
                                  placeholder="Policy Description"
                                  data-cy="description"
                                />
                                 </Col>
                                <Col sm={5}>
                                <FormB.Control
                                  {...input}
                                  as="textarea"
                                  rows={3}
                                  placeholder="Enter Boolean Expression"
                                  data-cy="policyCondition"
                                />
                              </Col>
                            </Row>
                        )}
                         </Field>
                                
                        <Row className="form-group">
                            <Col sm={10}>    <p className="formHeader">Validity Period</p>   </Col>
                        </Row>
                        <Row key={name}>
                            <FieldArray name="validitySchedules">
                                  {/*validitySchedules.map((obj, index) => (
                                    <Col sm={3}>
                                        <Field
                                          className="form-control"
                                          name={`${obj.name}.endTime`}
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
                                                                    isValidDate={(currentDate, selectedDate) =>
                                                                        calStartDate(validitySchedules.value[index], currentDate)
                                                                    }
                                                                />
                                                                {meta.touched && meta.error && (
                                                                    <span>{meta.error}</span>
                                                                )}
                                                            </div>
                                                        )}
                                       />
                                    </Col>
                                                                ))*/}
                              
                              {/*({ validitySchedules, ...arg }) =>
                                validitySchedules?.map((obj, index) => (
                                    <tr key={obj.name}>
                                      <td className="text-center">
                                        <Field
                                          className="form-control"
                                          name={`${obj.name}.startTime`}
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
                                                isValidDate={(currentDate, selectedDate) =>
                                                  calEndDate(validitySchedules.value[index], currentDate)
                                                }
                                              />
                                              {meta.touched && meta.error && (
                                                <span>{meta.error}</span>
                                              )}
                                            </div>
                                          )}
                                        />
                                      </td>

                                    </tr>
                                  ))*/
                                
                              }
                              { /*renderValiditySchedule*/}

                              {({ fields, ...arg }) =>
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
                                              isValidDate={(currentDate, selectedDate) =>
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
                                              isValidDate={(currentDate, selectedDate) =>
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
                                </Row> 
                                <Row>
                                    <Button
                                        type="button"
                                        className="btn-mini"
                                        onClick={addValiditySchedule}
                                        data-action="addTime"
                                        data-cy="addTime"
                                    >
                                        Add More
                                    </Button>                                
                          </Row>
                          <div className="mb-4">
                            <PolicyValidityPeriodComp
                              addPolicyItem={push}
                            />
                          </div>
                        <Row className="form-group">
                            <Col sm={10}>    <p className="formHeader">Grants</p>   </Col>
                                </Row>
                                <Row>
                                    <Col sm={1}>1</Col>
                                    <Field name="principle">
                                        {({ input }) => (
                                        <Col sm={9}>
                                            <AsyncSelect
                                                placeholder="Select users, groups, roles"
                                                defaultOptions
                                                isMulti
                                                data-name="usersSelect"
                                                data-cy="usersSelect"
                                            />
                                        </Col>
                                        )}
                                    </Field>
                                </Row>
                                <Row>
                                    <Col sm={1}></Col>
                                    <Field name="conditions">
                                        {({ input, meta }) => (
                                        <Col sm={5}>
                                            <FormB.Control
                                                {...input}
                                                placeholder="consitions"
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
                                                data-cy="consitions"
                                                />
                                        </Col>
                                        )}
                                    </Field>
                                    <Field name="permissions">
                                        {({ input }) => (
                                        <Col sm={4}>
                                            <AsyncSelect
                                                placeholder="Add permissions"
                                                defaultOptions
                                                isMulti
                                                data-name="usersSelect"
                                                data-cy="usersSelect"
                                            />
                                        </Col>
                                        )}
                                    </Field>
                                </Row>
                    </form>
                    </div>
                )}
                />
             </div>
        </>
    );
}

export default AccessGrantForm;