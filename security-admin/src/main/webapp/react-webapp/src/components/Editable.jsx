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

import React, { useEffect, useReducer, useRef, useState } from "react";
import {
  OverlayTrigger,
  Popover,
  Button,
  Form,
  Row,
  Col,
  Badge
} from "react-bootstrap";
import { find, findIndex, isArray, isEmpty, sortBy } from "lodash";
import { isObject } from "Utils/XAUtils";
import CreatableSelect from "react-select/creatable";
import Select from "react-select";
import { InfoIcon } from "Utils/XAUtils";
import { RegexMessage } from "Utils/XAMessages";
import { selectInputCustomStyles } from "Components/CommonComponents";

const esprima = require("esprima");
const TYPE_SELECT = "select";
const TYPE_CHECKBOX = "checkbox";
const TYPE_INPUT = "input";
const TYPE_RADIO = "radio";
const TYPE_CUSTOM = "custom";

const CheckboxComp = (props) => {
  const { options, value = [], valRef, showSelectAll, selectAllLabel } = props;
  const [selectedVal, setVal] = useState(value);

  const handleChange = (e, obj) => {
    let val = [...selectedVal];
    if (e.target.checked) {
      val.push(obj);
    } else {
      let index = findIndex(selectedVal, obj);
      val.splice(index, 1);
    }
    valRef.current = val;
    setVal(val);
  };

  const handleAllChecked = (e) => {
    let val = [];
    if (e.target.checked) {
      val = [...options];
    }
    valRef.current = val;
    setVal(val);
  };

  const isChecked = (obj) => {
    return findIndex(selectedVal, obj) !== -1;
  };

  const isAllChecked = () => {
    return selectedVal.length === options.length;
  };

  return (
    <>
      {options.map((obj) => (
        <Form.Group className="mb-3" controlId={obj.label} key={obj.label}>
          <Form.Check
            checked={isChecked(obj)}
            type="checkbox"
            label={obj.label}
            onChange={(e) => handleChange(e, obj)}
          />
        </Form.Group>
      ))}
      {showSelectAll && options?.length > 1 && (
        <Form.Group className="mb-3" controlId={selectAllLabel}>
          <Form.Check
            checked={isAllChecked()}
            type="checkbox"
            label={selectAllLabel}
            onChange={(e) => handleAllChecked(e)}
          />
        </Form.Group>
      )}
    </>
  );
};

const RadioBtnComp = (props) => {
  const { options, value, valRef } = props;
  const [selectedVal, setVal] = useState(value);

  const handleChange = (e, val) => {
    valRef.current = val;
    setVal(val);
  };

  return options.map((obj) => (
    <Form.Group className="mb-3" controlId={obj.label} key={obj.label}>
      <Form.Check
        type="radio"
        value={obj}
        label={obj.label}
        checked={
          isObject(selectedVal)
            ? selectedVal.value === obj.value
            : selectedVal === obj.value
        }
        onChange={(e) => handleChange(e, obj)}
      />
    </Form.Group>
  ));
};

const InputBoxComp = (props) => {
  const { value = "", valRef } = props;
  const [selectedInputVal, setInputVal] = useState(value);
  const handleChange = (e) => {
    valRef.current = e.currentTarget.value;
    setInputVal(e.currentTarget.value);
  };
  return (
    <>
      <Form.Group className="mb-3" controlId="expression">
        <Form.Control
          type="text"
          defaultValue={selectedInputVal == "" ? null : selectedInputVal}
          placeholder="enter expression"
          onChange={(e) => handleChange(e)}
        />
      </Form.Group>
    </>
  );
};

const CustomCondition = (props) => {
  const { value, valRef, conditionDefVal, selectProps, validExpression } =
    props;
  const tagAccessData = (val, key) => {
    if (!isObject(valRef.current)) {
      valRef.current = {};
    }
    valRef.current[key] = val;
  };

  return (
    <>
      {conditionDefVal?.length > 0 &&
        conditionDefVal.map((m) => {
          let uiHintAttb =
            m.uiHint != undefined && m.uiHint != "" ? JSON.parse(m.uiHint) : "";
          if (uiHintAttb != "") {
            if (uiHintAttb?.singleValue) {
              const [selectedCondVal, setCondSelect] = useState(
                value?.[m.name] || value
              );
              const accessedOpt = [
                { value: "yes", label: "Yes" },
                { value: "no", label: "No" }
              ];
              const accessedVal = (val) => {
                let value = null;
                if (val) {
                  let opObj = accessedOpt?.filter((m) => {
                    if (m.value == (val[0]?.value || val)) {
                      return m;
                    }
                  });
                  if (opObj) {
                    value = opObj;
                  }
                }
                return value;
              };
              const selectHandleChange = (e, name) => {
                let filterVal = accessedOpt?.filter((m) => {
                  if (m.value != e[0]?.value) {
                    return m;
                  }
                });
                setCondSelect(
                  !isEmpty(e) ? (e?.length > 1 ? filterVal : e) : null
                );
                tagAccessData(
                  !isEmpty(e)
                    ? e?.length > 1
                      ? filterVal[0].value
                      : e[0].value
                    : null,
                  name
                );
              };
              return (
                <div key={m.name}>
                  <Form.Group className="mb-3">
                    <b>{m.label}:</b>
                    <Select
                      options={accessedOpt}
                      onChange={(e) => selectHandleChange(e, m.name)}
                      value={
                        selectedCondVal?.value
                          ? accessedVal(selectedCondVal.value)
                          : accessedVal(selectedCondVal)
                      }
                      isMulti={true}
                      isClearable={false}
                    />
                  </Form.Group>
                </div>
              );
            }
            if (uiHintAttb?.isMultiline) {
              const [selectedJSCondVal, setJSCondVal] = useState(
                value?.[m.name] || value
              );
              const expressionVal = (val) => {
                let value = null;
                if (val != "" && typeof val != "object") {
                  valRef.current[m.name] = val;
                  return (value = val);
                }
                return value !== null ? value : "";
              };
              const textAreaHandleChange = (e, name) => {
                setJSCondVal(e.target.value);
                tagAccessData(e.target.value, name);
              };
              return (
                <div key={m.name}>
                  <Form.Group className="mb-3">
                    <Row>
                      <Col>
                        <b>{m.label}:</b>
                        <InfoIcon
                          position="right"
                          message={
                            <p className="pd-10">
                              {RegexMessage.MESSAGE.policyConditionInfoIcon}
                            </p>
                          }
                        />
                      </Col>
                    </Row>
                    <Row>
                      <Col>
                        <Form.Control
                          as="textarea"
                          rows={3}
                          key={m.name}
                          value={expressionVal(selectedJSCondVal)}
                          onChange={(e) => textAreaHandleChange(e, m.name)}
                          isInvalid={validExpression.state}
                        />
                        {validExpression.state && (
                          <div className="text-danger">
                            {validExpression.errorMSG}
                          </div>
                        )}
                      </Col>
                    </Row>
                  </Form.Group>
                </div>
              );
            }
            if (uiHintAttb?.isMultiValue) {
              const [selectedInputVal, setSelectVal] = useState(
                value?.[m.name] || []
              );
              const handleChange = (e, name) => {
                setSelectVal(e);
                tagAccessData(e, name);
              };
              return (
                <div key={m.name}>
                  <Form.Group
                    className="mb-3"
                    controlId="Ip-range"
                    key={m.name}
                  >
                    <b>{m.label}:</b>
                    <CreatableSelect
                      {...selectProps}
                      defaultValue={
                        selectedInputVal == "" ? null : selectedInputVal
                      }
                      onChange={(e) => handleChange(e, m.name)}
                      placeholder=""
                      width="500px"
                      isClearable={false}
                      styles={selectInputCustomStyles}
                    />
                  </Form.Group>
                </div>
              );
            }
          }
        })}
    </>
  );
};

const initialState = (props) => {
  const { type, selectProps, value } = props;
  let val = value;
  if (!val) {
    if (type === TYPE_SELECT) {
      val = null;
      if (selectProps && selectProps.isMulti) {
        val = [];
      }
    }
  }
  return {
    value: val,
    target: null
  };
};

function reducer(state, action) {
  switch (action.type) {
    case "SET_VALUE":
      return {
        ...state,
        value: action.value,
        show: action.show,
        target: action.target
      };
    case "SET_POPOVER":
      return {
        ...state,
        show: action.show,
        target: action.target
      };
    default:
      throw new Error();
  }
}

const Editable = (props) => {
  const {
    type,
    value: editableValue,
    placement = "bottom",
    className,
    displayFormat,
    onChange,
    options = [],
    conditionDefVal,
    servicedefName
  } = props;

  const initialLoad = useRef(true);
  const popoverRef = useRef(null);
  const selectValRef = useRef(null);
  const [validExpression, setValidated] = useState({
    state: false,
    errorMSG: ""
  });
  const [state, dispatch] = useReducer(reducer, props, initialState);
  const { show, value, target } = state;
  let isListenerAttached = false;

  const handleClickOutside = (e) => {
    if (
      document.getElementById("popover-basic")?.contains(e?.target) == false
    ) {
      dispatch({
        type: "SET_POPOVER",
        show: false,
        target: null
      });
    }
    e?.stopPropagation();
  };

  useEffect(() => {
    if (!isListenerAttached) {
      document?.addEventListener("mousedown", handleClickOutside);
      isListenerAttached = true;
      return;
    }
    return () => {
      document?.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);

  const displayValue = () => {
    let val = "--";
    const selectVal = value;
    const policyConditionDisplayValue = () => {
      let ipRangVal, uiHintVal;
      if (selectVal) {
        return sortBy(Object.keys(selectVal)).map((property) => {
          let conditionObj = find(conditionDefVal, function (m) {
            if (m.name == property) {
              return m;
            }
          });
          if (conditionObj?.uiHint && conditionObj?.uiHint != "") {
            uiHintVal = JSON.parse(conditionObj.uiHint);
          }
          if (isArray(selectVal[property])) {
            ipRangVal = selectVal[property]
              ?.map(function (m) {
                return m.value;
              })
              .join(", ");
          }
          return (
            <div
              key={property}
              className={`${
                uiHintVal?.isMultiline ? "editable-label" : "badge bg-dark"
              }`}
            >
              {`${conditionObj.label}: ${
                isArray(selectVal[property]) ? ipRangVal : selectVal[property]
              }`}
            </div>
          );
        });
      }
    };
    if (displayFormat) {
      val = displayFormat(selectVal);
    } else {
      if (type === TYPE_SELECT) {
        if (selectVal?.length > 0 || Object.keys(selectVal).length > 0) {
          let ipRangVal =
            servicedefName == "knox" && !isArray(selectVal)
              ? selectVal["ip-range"]
              : selectVal
                  .map(function (m) {
                    return m.value;
                  })
                  .join(", ");
          val = (
            <h6 className="d-inline me-1">
              <span
                className="editable-edit-text badge bg-dark"
                style={{ display: "block" }}
              >
                {conditionDefVal.name} : {ipRangVal}
              </span>
              <Button
                className="mg-10 btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
              >
                <i className="fa-fw fa fa-pencil"></i>
              </Button>
            </h6>
          );
        } else {
          val = (
            <div className="text-center">
              <span className="editable-add-text">Add Conditions</span>
              <Button
                className="mg-10 btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
                s
              >
                <i className="fa-fw fa fa-plus"></i>
              </Button>
            </div>
          );
        }
      } else if (type === TYPE_CHECKBOX) {
        val =
          selectVal && selectVal?.length > 0 ? (
            <>
              <span className="editable-edit-text">
                {selectVal.map((op, index) => (
                  <h6 className="d-inline me-1" key={index}>
                    <Badge bg="info">{op.label}</Badge>
                  </h6>
                ))}
              </span>
              <Button
                className="mg-10 mx-auto d-block btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
              >
                <i className="fa-fw fa fa-pencil"></i>
              </Button>
            </>
          ) : (
            <div className="text-center">
              <span
                className="editable-add-text"
                data-js="permissions"
                data-cy="permissions"
              >
                Add Permissions
              </span>
              <Button
                className="mg-10 mx-auto d-block btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
                title="add"
              >
                <i className="fa-fw fa fa-plus"></i>
              </Button>
            </div>
          );
      } else if (type === TYPE_INPUT) {
        val =
          selectVal && selectVal !== "" ? (
            <>
              <span className="editable-edit-text">
                <h6 className="d-inline me-1">
                  <Badge bg="info">{selectVal}</Badge>
                </h6>
              </span>
              <Button
                className="mg-10 mx-auto d-block btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
              >
                <i className="fa-fw fa fa-pencil"></i>
              </Button>
            </>
          ) : (
            <div className="text-center">
              <span className="editable-add-text">Add Row Filter</span>
              <Button
                className="mg-10 mx-auto d-block btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
              >
                <i className="fa-fw fa fa-plus"></i>
              </Button>
            </div>
          );
      } else if (type === TYPE_RADIO) {
        val =
          selectVal && selectVal?.label ? (
            <>
              <span className="editable-edit-text">
                <h6 className="d-inline me-1">
                  <Badge bg="info">{selectVal.label}</Badge>
                </h6>
              </span>
              <Button
                className="mg-10 mx-auto d-block btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
              >
                <i className="fa-fw fa fa-pencil"></i>
              </Button>
            </>
          ) : (
            <div className="text-center">
              <span className="editable-add-text">Select Masking Option</span>
              <Button
                className="mg-10 mx-auto d-block  btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
              >
                <i className="fa-fw fa fa-plus"></i>
              </Button>
            </div>
          );
      } else if (type === TYPE_CUSTOM) {
        for (const key in selectVal) {
          if (selectVal[key] == null || selectVal[key] == "") {
            delete selectVal[key];
          }
        }
        if (Object.keys(selectVal).length != 0) {
          val = (
            <h6>
              {policyConditionDisplayValue()}
              <Button
                className="mg-10 mx-auto d-block btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
              >
                <i className="fa-fw fa fa-pencil"></i>
              </Button>
            </h6>
          );
        } else {
          val = (
            <div className="text-center">
              <span className="editable-add-text">Add Conditions</span>
              <Button
                className="mg-10 mx-auto d-block btn-mini"
                variant="outline-dark"
                size="sm"
                type="button"
              >
                <i className="fa-fw fa fa-plus"></i>
              </Button>
            </div>
          );
        }
      } else {
        val = selectVal || "--";
      }
    }
    return val;
  };

  useEffect(() => {
    if (!initialLoad.current) {
      onChange(editableValue);
      dispatch({
        type: "SET_VALUE",
        value: editableValue,
        show: false,
        target: null
      });
    } else {
      initialLoad.current = false;
    }
    type === TYPE_CUSTOM
      ? (selectValRef.current = { ...editableValue })
      : (selectValRef.current = editableValue);
  }, [editableValue]);

  const handleApply = () => {
    let errors, uiHintVal;
    if (selectValRef?.current) {
      sortBy(Object.keys(selectValRef.current)).map((property) => {
        let conditionObj = find(conditionDefVal, function (m) {
          if (m.name == property) {
            return m;
          }
        });
        if (conditionObj != undefined && conditionObj?.uiHint != "") {
          uiHintVal = JSON.parse(conditionObj.uiHint);
          if (
            uiHintVal?.isMultiline &&
            selectValRef.current[conditionObj.name] != "" &&
            selectValRef.current[conditionObj.name] != undefined
          ) {
            try {
              esprima.parseScript(selectValRef.current[conditionObj.name]);
            } catch (e) {
              errors = e.message;
            }
          }
        }
      });
    }
    if (errors) {
      setValidated({ state: true, errorMSG: errors });
    } else {
      setValidated({ state: false, errorMSG: "" });
      dispatch({
        type: "SET_VALUE",
        value: selectValRef.current,
        show: !show,
        target: null
      });
      onChange(selectValRef.current);
    }
  };

  const handleClose = () => {
    setValidated({ state: false, errorMSG: "" });
    dispatch({
      type: "SET_POPOVER",
      show: !show,
      target: null
    });
  };

  const popoverComp = (
    <Popover
      id="popover-basic"
      className={`editable-popover ${
        type === TYPE_CHECKBOX && "popover-maxHeight popover-minHeight"
      }`}
    >
      <Popover.Header>
        {type === TYPE_CHECKBOX ? "Select" : "Enter"}
      </Popover.Header>
      <Popover.Body>
        {type === TYPE_CHECKBOX ? (
          <CheckboxComp
            value={value}
            options={options}
            valRef={selectValRef}
            showSelectAll={props.showSelectAll}
            selectAllLabel={props.selectAllLabel}
          />
        ) : type === TYPE_RADIO ? (
          <RadioBtnComp value={value} options={options} valRef={selectValRef} />
        ) : type === TYPE_INPUT ? (
          <InputBoxComp value={value} valRef={selectValRef} />
        ) : type === TYPE_CUSTOM ? (
          <CustomCondition
            value={value}
            valRef={selectValRef}
            conditionDefVal={props.conditionDefVal}
            selectProps={props.selectProps}
            validExpression={validExpression}
          />
        ) : null}
      </Popover.Body>
      <div className="popover-footer-buttons">
        <Button
          variant="success"
          size="sm"
          onClick={handleApply}
          className="me-2 btn-mini ms-2"
        >
          <i className="fa fa-fw fa-check"></i>
        </Button>
        <Button
          className="btn-mini"
          variant="danger"
          size="sm"
          onClick={handleClose}
        >
          <i className="fa fa-fw fa-close"></i>
        </Button>
      </div>
    </Popover>
  );

  const handleClick = (e) => {
    setValidated({ state: false, errorMSG: "" });
    let display = !show;
    dispatch({
      type: "SET_POPOVER",
      show: display,
      target: e.target
    });
  };

  return (
    <div ref={popoverRef}>
      <OverlayTrigger
        show={show}
        target={target}
        trigger="click"
        placement={placement}
        overlay={popoverComp}
        container={popoverRef.current}
        rootClose={true}
        rootCloseEvent="click"
      >
        <div className={`editable ${className || ""}`} onClick={handleClick}>
          {displayValue()}
        </div>
      </OverlayTrigger>
    </div>
  );
};

export default Editable;
