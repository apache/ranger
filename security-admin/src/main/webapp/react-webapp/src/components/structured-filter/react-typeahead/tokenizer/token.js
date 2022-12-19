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

/**
 * Encapsulates the rendering of an option that has been "selected" in a
 * TypeaheadTokenizer
 */
var Token = createReactClass({
  propTypes: {
    children: PropTypes.object,
    onRemove: PropTypes.func,
    categoryLabel: PropTypes.string
  },

  getInitialState: function () {
    return {
      isHovering: false
    };
  },

  handleMouseOver: function () {
    this.setState({
      isHovering: true
    });
  },

  handleMouseOut: function () {
    this.setState({
      isHovering: false
    });
  },

  render: function () {
    return (
      <div
        className={
          this.state.isHovering
            ? "typeahead-token typeahead-token-maybe-delete"
            : "typeahead-token"
        }
      >
        <span className="typeahead-token-label text-uppercase mr-2 font-weight-bold">
          {this.props.categoryLabel}
        </span>
        <span className="typeahead-token-value">
          {this.props.categoryValue}
        </span>
        {this._makeCloseButton()}
      </div>
    );
  },

  _makeCloseButton: function () {
    if (!this.props.onRemove) {
      return "";
    }
    return (
      <a
        className="typeahead-token-close ml-1"
        href="#"
        onClick={function (event) {
          this.props.onRemove(this.props.children);
          event.preventDefault();
        }.bind(this)}
      >
        <span
          className="typeahead-token-icon-close"
          onMouseOver={this.handleMouseOver}
          onMouseOut={this.handleMouseOut}
        ></span>
      </a>
    );
  }
});

export default Token;
