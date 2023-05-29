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

var React = require("react");
import Popover from "./popover";
import Calendar from "./calendar";
import DateInput from "./date_input";
import createReactClass from "create-react-class";
import PropTypes from "prop-types";

var DatePicker = createReactClass({
  propTypes: {
    onChange: PropTypes.func,
    onKeyDown: PropTypes.func
  },

  getInitialState: function () {
    return {
      focus: true
    };
  },

  handleFocus: function () {
    this.setState({
      focus: true
    });
  },

  hideCalendar: function () {
    this.setState({
      focus: false
    });
  },

  handleSelect: function (date) {
    this.hideCalendar();
    this.setSelected(date);
  },

  setSelected: function (date) {
    this.props.onChange(date);
  },

  onInputClick: function () {
    this.setState({
      focus: true
    });
  },

  calendar: function () {
    if (this.state.focus) {
      return (
        <Popover>
          <Calendar
            onSelect={this.handleSelect}
            hideCalendar={this.hideCalendar}
            value={this.props.value}
            allSelected={this.props.allSelected}
            currentCategory={this.props.currentCategory}
          />
        </Popover>
      );
    }
  },

  render: function () {
    return (
      <div>
        <DateInput
          ref="dateinput"
          focus={this.state.focus}
          onFocus={this.handleFocus}
          onKeyDown={this.props.onKeyDown}
          handleClick={this.onInputClick}
          handleEnter={this.hideCalendar}
          setSelected={this.setSelected}
          hideCalendar={this.hideCalendar}
          placeholderText={this.props.placeholderText}
          value={this.props.value}
        />
        {this.calendar()}
      </div>
    );
  }
});

export default DatePicker;
