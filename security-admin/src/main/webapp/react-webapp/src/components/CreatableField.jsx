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

import React, { useState, useRef, useEffect } from "react";
import CreatableSelect from "react-select/creatable";
import { selectInputCustomStyles } from "Components/CommonComponents";

const CreatableField = (props) => {
  const { actionValues, creatableOnChange } = props;
  const [actionValue, setActionValue] = useState(actionValues);
  const [actionInputValue, setActionInputValue] = useState("");
const prevProps = useRef(actionValues);

useEffect(()=>{
  if(JSON.stringify(prevProps) !== JSON.stringify(actionValues)){
   prevProps.current = actionValues
   setActionValue(prevProps.current)
  }
},[actionValues])


  const handleChange = (value) => {
    setActionValue(value);
    creatableOnChange(value);
  };

  const handleKeyDown = (e) => {
    if (!actionInputValue) return;
    switch (e.key) {
      case "Enter":
      case "Tab":
        setActionInputValue("");
        setActionValue([...actionValue, createOption(actionInputValue)]);
        creatableOnChange([...actionValue, createOption(actionInputValue)]);
        e.preventDefault();
    }
  };

  const createOption = (label) => ({
    value: label,
    label: label
  });

  const handleInputChange = (value) => {
    setActionInputValue(value);
  };

  return (
    <CreatableSelect
      components={{
        DropdownIndicator: () => null
      }}
      menuIsOpen={false}
      isClearable={false}
      isMulti
      placeholder="Type Operations Name"
      value={actionValue}
      inputValue={actionInputValue}
      onChange={(actionValue) => handleChange(actionValue)}
      onInputChange={handleInputChange}
      onKeyDown={(e) => handleKeyDown(e)}
      styles={selectInputCustomStyles}
    />
  );
};

export default CreatableField;
