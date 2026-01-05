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
import createReactClass from "create-react-class";
import PropTypes from "prop-types";

var DateInput = createReactClass({
  propTypes: {
    onKeyDown: PropTypes.func
  },

  componentDidMount: function () {
    this.toggleFocus(this.props.focus);
  },

  componentWillReceiveProps: function (newProps) {
    this.toggleFocus(newProps.focus);
  },

  toggleFocus: function (focus) {
    if (focus) {
      this.refs.entry.focus();
    } else {
      this.refs.entry.blur();
    }
  },

  handleKeyDown: function (event) {
    switch (event.key) {
      case "Enter":
        event.preventDefault();
        this.props.handleEnter(event);
        break;
      case "Backspace":
        this.props.onKeyDown(event);
        break;
    }
  },

  handleClick: function (event) {
    this.props.handleClick(event);
  },

  render: function () {
    return (
      <input
        ref="entry"
        type="text"
        readOnly
        onClick={this.handleClick}
        onKeyDown={this.handleKeyDown}
        onFocus={this.props.onFocus}
        className="datepicker__input"
        placeholder={this.props.placeholderText}
        defaultValue={this.props.value}
        style={{ width: 70 }}
      />
    );
  }
});

export default DateInput;
