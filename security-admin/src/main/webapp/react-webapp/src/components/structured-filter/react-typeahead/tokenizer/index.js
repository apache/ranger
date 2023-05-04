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

const React = require("react");
import Token from "./token";
import KeyEvent from "../keyevent";
import Typeahead from "../typeahead";
import createReactClass from "create-react-class";
import PropTypes from "prop-types";
import { find, map, difference, includes } from "lodash";
var classNames = require("classnames");
/**
 * A typeahead that, when an option is selected, instead of simply filling
 * the text entry widget, prepends a renderable "token", that may be deleted
 * by pressing backspace on the beginning of the line with the keyboard.
 */

var TypeaheadTokenizer = createReactClass({
  propTypes: {
    options: PropTypes.array,
    customClasses: PropTypes.object,
    defaultSelected: PropTypes.array,
    defaultValue: PropTypes.string,
    placeholder: PropTypes.string,
    onTokenRemove: PropTypes.func,
    onTokenAdd: PropTypes.func
  },

  componentWillReceiveProps(nextProps) {
    if (
      JSON.stringify(this.props.defaultSelected) !==
      JSON.stringify(nextProps.defaultSelected)
    ) {
      this.setState({ selected: nextProps.defaultSelected });
    }
  },

  getInitialState: function () {
    return {
      selected: this.props.defaultSelected,
      category: ""
    };
  },

  getDefaultProps: function () {
    return {
      options: [],
      defaultSelected: [],
      customClasses: {},
      defaultValue: "",
      placeholder: "",
      onTokenAdd: function () {},
      onTokenRemove: function () {}
    };
  },

  // TODO: Support initialized tokens
  _renderTokens: function () {
    var tokenClasses = {};
    tokenClasses[this.props.customClasses.token] =
      !!this.props.customClasses.token;
    var classList = classNames(tokenClasses);
    var result = this.state.selected.map(function (selected) {
      let mykey = selected.category + selected.value;
      let categoryLabel = this._getFilterCategoryLabel(selected.category);
      let categoryValue = this._getFilterCategoryLabelForOption(
        selected.category,
        selected.value
      );
      return (
        <Token
          key={mykey}
          className={classList}
          onRemove={this._removeTokenForValue}
          categoryLabel={categoryLabel}
          categoryValue={categoryValue}
        >
          {selected}
        </Token>
      );
    }, this);
    return result;
  },

  _getOptionsForTypeahead: function () {
    if (this.state.category == "") {
      var categories = [];
      let selectedCategory = [];
      selectedCategory = map(this.state.selected, "category");
      for (var i = 0; i < this.props.options.length; i++) {
        selectedCategory.indexOf(this.props.options[i].category) === -1 &&
          categories.push(this.props.options[i].category);
      }
      return categories;
    } else {
      var options = this._getCategoryOptions();
      if (options == null) return [];
      else return map(options(), "value");
    }

    return this.props.options;
  },

  _getHeader: function () {
    if (this.state.category == "") {
      return "Category";
    } else {
      return "Value";
    }

    return this.props.options;
  },

  _getCategoryType: function () {
    for (var i = 0; i < this.props.options.length; i++) {
      if (this.props.options[i].category == this.state.category) {
        return this.props.options[i].type;
      }
    }
  },

  _getCategoryOptions: function () {
    for (var i = 0; i < this.props.options.length; i++) {
      if (this.props.options[i].category == this.state.category) {
        return this.props.options[i].options;
      }
    }
  },

  _onKeyDown: function (event) {
    // We only care about intercepting backspaces
    if (event.keyCode !== KeyEvent.DOM_VK_BACK_SPACE) {
      return;
    }

    // Remove token ONLY when bksp pressed at beginning of line
    // without a selection
    var entry = this.refs.typeahead.instanceRef.inputRef();
    if (
      entry.selectionStart == entry.selectionEnd &&
      entry.selectionStart == 0
    ) {
      if (this.state.category != "") {
        this.setState({ category: "" });
      } else {
        // No tokens
        if (!this.state.selected.length) {
          return;
        }
        this._removeTokenForValue(
          this.state.selected[this.state.selected.length - 1]
        );
      }
      event.preventDefault();
    }
  },

  _removeTokenForValue: function (value) {
    var index = this.state.selected.indexOf(value);
    if (index == -1) {
      return;
    }

    this.state.selected.splice(index, 1);
    this.setState({ selected: this.state.selected });
    this.props.onTokenRemove(this.state.selected);

    return;
  },

  _addTokenForValue: function (value) {
    if (this.state.category == "") {
      this.setState({ category: value });
      this.refs.typeahead.instanceRef.setEntryText("");
      return;
    }

    if (this.state.category == value) {
      this.refs.typeahead.instanceRef.setEntryText("");
      return;
    }

    value = {
      category: this.state.category,
      value: value
    };

    this.state.selected.push(value);
    this.setState({ selected: this.state.selected });
    this.refs.typeahead.instanceRef.setEntryText("");
    this.props.onTokenAdd(this.state.selected);

    this.setState({ category: "" });

    return;
  },

  /***
   * Returns the data type the input should use ("date" or "text")
   */
  _getInputType: function () {
    if (this.state.category != "") {
      return this._getCategoryType();
    } else {
      return "text";
    }
  },

  _getOptionsLabel: function () {
    var currentHeader = this._getHeader();
    var optionsLabel = [];
    let selectedCategory = map(this.state.selected, "category");
    if (currentHeader == "Category") {
      for (var i = 0; i < this.props.options.length; i++) {
        selectedCategory.indexOf(this.props.options[i].category) === -1 &&
          optionsLabel.push(this.props.options[i].label);
      }
    } else if (currentHeader == "Value") {
      var options = this._getCategoryOptions();
      if (options == null) return [];
      else return map(options(), "label");
    }
    return optionsLabel;
  },

  _getFilterCategoryLabel: function (filterCategory) {
    for (var i = 0; i < this.props.options.length; i++) {
      if (this.props.options[i].category == filterCategory) {
        return this.props.options[i].label;
      }
    }

    return filterCategory;
  },

  _getFilterCategoryLabelForOption: function (selectedCategory, selectedValue) {
    for (var i = 0; i < this.props.options.length; i++) {
      if (this.props.options[i].category == selectedCategory) {
        if (
          this.props.options[i].options !== undefined &&
          this.props.options[i].options().length > 0
        ) {
          let option = find(this.props.options[i].options(), {
            value: selectedValue
          });
          return option !== undefined ? option.label : selectedValue;
        }
      }
    }

    return selectedValue;
  },

  _onClearAll: function () {
    this.setState({ selected: [], category: "" }, () => {
      this.props.onTokenRemove(this.state.selected);
    });
  },

  _getClearAllButton: function () {
    return (
      <span className="token-clear-all">
        <a
          className="typeahead-token-close"
          href="#"
          onClick={function (event) {
            this._onClearAll();
            event.preventDefault();
          }.bind(this)}
        >
          <span
            className="typeahead-token-icon-close"
            onMouseOver={this.handleMouseOver}
            onMouseOut={this.handleMouseOut}
          ></span>
        </a>
      </span>
    );
  },

  render: function () {
    var classes = {};
    classes[this.props.customClasses.typeahead] =
      !!this.props.customClasses.typeahead;
    var classList = classNames(classes);
    return (
      <div className="filter-tokenizer">
        <span className="input-group-addon">
          <i className="fa fa-search"></i>
        </span>
        <div className="token-collection">
          {this._renderTokens()}

          <div className="filter-input-group">
            <div className="filter-category text-uppercase mr-2">
              {this._getFilterCategoryLabel(this.state.category)}
            </div>

            <Typeahead
              ref="typeahead"
              className={classList}
              placeholder={
                this.state.selected.length === 0 && this.state.category === ""
                  ? this.props.placeholder
                  : ""
              }
              customClasses={this.props.customClasses}
              currentCategory={this.state.category}
              fullOptions={this.props.options}
              options={this._getOptionsForTypeahead()}
              optionsLabel={this._getOptionsLabel()}
              header={this._getHeader()}
              datatype={this._getInputType()}
              defaultValue={this.props.defaultValue}
              onOptionSelected={this._addTokenForValue}
              onKeyDown={this._onKeyDown}
            />
          </div>
        </div>
        {this._getClearAllButton()}
      </div>
    );
  }
});

export default TypeaheadTokenizer;
