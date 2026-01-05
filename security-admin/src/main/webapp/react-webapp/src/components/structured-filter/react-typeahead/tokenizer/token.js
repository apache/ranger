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
import Typeahead from "../typeahead";
import { reject } from "lodash";

/**
 * Encapsulates the rendering of an option that has been "selected" in a
 * TypeaheadTokenizer
 */
var Token = createReactClass({
  propTypes: {
    children: PropTypes.object,
    onRemove: PropTypes.func,
    categoryLabel: PropTypes.string,
    selectedCategory: PropTypes.string
  },

  getInitialState: function () {
    return {
      isHovering: false,
      isEditView: false
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

  onValueClick: function () {
    this.setState({
      isEditView: true
    });
  },

  _addTokenForValue: function (value) {
    let editAllSelected = this.props.allSelected;
    let obj = editAllSelected[this.props.index];
    if (value !== obj.value) {
      if (value === "") {
        editAllSelected = reject(editAllSelected, obj);
      } else {
        obj.value = value;
      }
      this.props.setFiltersValue(editAllSelected);
    } else {
      this.setState({
        isEditView: false
      });
    }
    return;
  },

  _onKeyDown: function (e) {
    return;
  },

  render: function () {
    const { selectedCategory, allSelected } = this.props;
    return (
      <div
        className={
          this.state.isEditView
            ? "typeahead-token typeahead-token-is-edit"
            : "typeahead-token"
        }
      >
        {this._makeCloseButton()}
        <span className="typeahead-token-label ms-3 me-1 text-uppercase fw-bold">
          {this.props.categoryLabel} :
        </span>
        {this.state.isEditView ? (
          <Typeahead
            ref="typeaheadValue"
            className="typehead-edit-view"
            placeholder=""
            customClasses={{ input: "token-edit-view" }}
            currentCategory={selectedCategory}
            fullOptions={this.props._getFullOptions}
            options={this.props._getOptionsForTypeaheadValue(selectedCategory)}
            optionsLabel={this.props._getValueOptionsLabel(selectedCategory)}
            header="Value"
            datatype={this.props._getInputType(selectedCategory)}
            defaultValue={this.props.categoryValue}
            onOptionSelected={this._addTokenForValue}
            onKeyDown={this._onKeyDown}
            focus={true}
            allSelected={allSelected}
          />
        ) : (
          <span className="typeahead-token-value" onClick={this.onValueClick}>
            {this.props.categoryValue.replace(/ /g, "\u00A0")}
          </span>
        )}
      </div>
    );
  },

  _makeCloseButton: function () {
    if (!this.props.onRemove) {
      return "";
    }
    return (
      <a
        className="typeahead-token-close"
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
