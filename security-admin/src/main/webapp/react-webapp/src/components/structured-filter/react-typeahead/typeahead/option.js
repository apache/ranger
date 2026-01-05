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
var classNames = require("classnames");
import PropTypes from "prop-types";
import createReactClass from "create-react-class";

/**
 * A single option within the TypeaheadSelector
 */
var TypeaheadOption = createReactClass({
  propTypes: {
    customClasses: PropTypes.object,
    onClick: PropTypes.func,
    children: PropTypes.string
  },

  getDefaultProps: function () {
    return {
      customClasses: {},
      onClick: function (event) {
        event.preventDefault();
      }
    };
  },

  getInitialState: function () {
    return {
      hover: false
    };
  },

  render: function () {
    var classes = {
      hover: this.props.hover
    };
    classes[this.props.customClasses.listItem] =
      !!this.props.customClasses.listItem;
    var classList = classNames(classes);

    return (
      <li className={classList} onClick={this._onClick}>
        <a href="#" className={this._getClasses()} ref="anchor">
          {this.props.children}
        </a>
      </li>
    );
  },

  _getClasses: function () {
    var classes = {
      "typeahead-option": true
    };
    classes[this.props.customClasses.listAnchor] =
      !!this.props.customClasses.listAnchor;
    return classNames(classes);
  },

  _onClick: function (event) {
    event.preventDefault();
    return this.props.onClick();
  }
});

export default TypeaheadOption;
