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

var React = window.React || require("react");
var divInputRef = React.createRef();
import TypeaheadSelector from "./selector";
import KeyEvent from "../keyevent";
import fuzzy from "fuzzy";
import DatePicker from "../../react-datepicker/datepicker.js";
import createReactClass from "create-react-class";
import PropTypes from "prop-types";
import onClickOutside from "react-onclickoutside";
import { find, isEmpty, trim } from "lodash";
var classNames = require("classnames");

/**
 * A "typeahead", an auto-completing text input
 *
 * Renders an text input that shows options nearby that you can use the
 * keyboard or mouse to select.  Requires CSS for MASSIVE DAMAGE.
 */
var Typeahead = onClickOutside(
  createReactClass({
    propTypes: {
      customClasses: PropTypes.object,
      currentCategory: PropTypes.string,
      maxVisible: PropTypes.number,
      fullOptions: PropTypes.array,
      options: PropTypes.array,
      optionsLabel: PropTypes.array,
      header: PropTypes.string,
      datatype: PropTypes.string,
      defaultValue: PropTypes.string,
      placeholder: PropTypes.string,
      onOptionSelected: PropTypes.func,
      onKeyDown: PropTypes.func
    },

    getDefaultProps: function () {
      return {
        options: [],
        header: "Category",
        datatype: "text",
        customClasses: {},
        defaultValue: "",
        placeholder: "",
        onKeyDown: function (event) {
          return;
        },
        onOptionSelected: function (option) {}
      };
    },

    getInitialState: function () {
      return {
        // The set of all options... Does this need to be state?  I guess for lazy load...
        options: this.props.options,
        header: this.props.header,
        datatype: this.props.datatype,

        focused: this.props.focus || false,

        // The currently visible set of options
        visible: this.getOptionsForValue(
          this.props.defaultValue,
          this.props.options
        ),

        // This should be called something else, "entryValue"
        entryValue: this.props.defaultValue,

        // A valid typeahead value
        selection: null
      };
    },

    componentDidMount: function () {
      this.setState({
        tokenWidth: divInputRef.current?.clientWidth,
        visible:
          this.props.className == "typehead-edit-view" &&
          this.state.datatype == "textoptions"
            ? this.state.options
            : []
      });
      this.props.className == "typehead-edit-view" &&
        this.state.datatype !== "date" &&
        this.refs.entry.focus();
    },

    componentWillReceiveProps: function (nextProps) {
      this.setState({
        options: nextProps.options,
        header: nextProps.header,
        datatype: nextProps.datatype,
        visible: nextProps.options
      });
    },

    getOptionsForValue: function (value, options) {
      var result = fuzzy.filter(value, options).map(function (res) {
        return res.string;
      });

      if (this.props.maxVisible) {
        result = result.slice(0, this.props.maxVisible);
      }
      return result;
    },

    getOptionsLabelForValue: function (value, options) {
      var result = fuzzy
        .filter(value, this.props.optionsLabel)
        .map(function (res) {
          return res.string;
        });

      result = result.map((option) => {
        if (this.props.header == "Category") {
          let actualCategory = find(this.props.fullOptions, ["label", option]);
          return actualCategory.category;
        } else if (
          this.props.header == "Value" &&
          !isEmpty(this.props.optionsLabel)
        ) {
          let valueCategory = find(this.props.fullOptions, [
            "category",
            this.props.currentCategory
          ]);
          let valueOptions = valueCategory.options();
          let actualValue = find(valueOptions, ["label", option]);
          return actualValue.value;
        }
      });

      if (this.props.maxVisible) {
        result = result.slice(0, this.props.maxVisible);
      }
      return result;
    },

    setEntryText: function (value) {
      if (this.refs.entry != null) {
        this.refs.entry.value = value;
      }
      this._onTextEntryUpdated();
    },

    _renderIncrementalSearchResults: function () {
      if (!this.state.focused) {
        return "";
      }

      // Something was just selected
      if (this.state.selection) {
        return "";
      }

      // There are no typeahead / autocomplete suggestions
      if (!this.state.visible.length) {
        return "";
      }

      return (
        <TypeaheadSelector
          ref="sel"
          options={this.state.visible}
          header={this.state.header}
          onOptionSelected={this._onOptionSelected}
          customClasses={this.props.customClasses}
          optionsLabel={this.props.optionsLabel}
          fullOptions={this.props.fullOptions}
          currentCategory={this.props.currentCategory}
        />
      );
    },

    _onOptionSelected: function (option) {
      var nEntry = this.refs.entry;
      nEntry.focus();
      nEntry.value = option;
      this.setState({
        visible: this.getOptionsForValue(option, this.state.options),
        selection: option,
        entryValue: option
      });

      this.props.onOptionSelected(option);
    },

    _onTextEntryUpdated: function () {
      var value = "";
      if (this.refs.entry != null) {
        value = this.refs.entry.value;
      }

      var calculateTokenWidth = divInputRef.current?.clientWidth;
      if (
        this.refs.input !== undefined &&
        this.refs.input.childNodes.length >= 2
      ) {
        this.refs.input.childNodes[1].innerHTML = value.replace(/ /g, "\u00A0");

        if (divInputRef.current?.clientWidth == 0) {
          calculateTokenWidth = 3;
        } else {
          calculateTokenWidth = divInputRef.current?.clientWidth;
        }
      }

      value = trim(value);

      this.setState({
        visible: this.getOptionsLabelForValue(value, this.state.options),
        selection: null,
        entryValue: value,
        tokenWidth: calculateTokenWidth
      });
    },

    _onEnter: function (event) {
      if (!this.refs.sel.state.selection) {
        return this.props.onKeyDown(event);
      }

      this._onOptionSelected(this.refs.sel.state.selection);
    },

    _onEscape: function () {
      this.refs.sel.setSelectionIndex(null);
    },

    _onTab: function (event) {
      var option = this.refs.sel.state.selection
        ? this.refs.sel.state.selection
        : this.state.visible[0];
      this._onOptionSelected(option);
    },

    eventMap: function (event) {
      var events = {};

      events[KeyEvent.DOM_VK_UP] = this.refs.sel.navUp;
      events[KeyEvent.DOM_VK_DOWN] = this.refs.sel.navDown;
      events[KeyEvent.DOM_VK_RETURN] = events[KeyEvent.DOM_VK_ENTER] =
        this._onEnter;
      events[KeyEvent.DOM_VK_ESCAPE] = this._onEscape;
      events[KeyEvent.DOM_VK_TAB] = this._onTab;

      return events;
    },

    _onKeyDown: function (event) {
      // If Enter pressed
      if (
        event.keyCode === KeyEvent.DOM_VK_RETURN ||
        event.keyCode === KeyEvent.DOM_VK_ENTER
      ) {
        // If no options were provided so we can match on anything
        if (this.props.options.length === 0) {
          this._onOptionSelected(this.state.entryValue);
        }

        // If what has been typed in is an exact match of one of the options
        if (this.props.options.indexOf(this.state.entryValue) > -1) {
          this._onOptionSelected(this.state.entryValue);
        }
      }

      // If there are no visible elements, don't perform selector navigation.
      // Just pass this up to the upstream onKeydown handler
      if (!this.refs.sel) {
        return this.props.onKeyDown(event);
      }

      var handler = this.eventMap()[event.keyCode];

      if (handler) {
        handler(event);
      } else {
        return this.props.onKeyDown(event);
      }
      // Don't propagate the keystroke back to the DOM/browser
      event.preventDefault();
    },

    _onFocus: function (event) {
      this.setState({ focused: true });
    },

    handleClickOutside: function (event) {
      this.setState({ focused: false });
    },

    isDescendant: function (parent, child) {
      var node = child.parentNode;
      while (node != null) {
        if (node == parent) {
          return true;
        }
        node = node.parentNode;
      }
      return false;
    },

    _handleDateChange: function (date) {
      this.props.onOptionSelected(date.format("MM/DD/YYYY"));
    },

    _showDatePicker: function () {
      if (this.state.datatype == "date") {
        return true;
      }
      return false;
    },

    inputRef: function () {
      if (this._showDatePicker()) {
        return this.refs.datepicker.refs.dateinput.refs.entry;
      } else {
        return this.refs.entry;
      }
    },

    calculateWidth: function () {
      if (this.state.tokenWidth !== undefined) {
        return this.state.tokenWidth;
      } else {
        return divInputRef.current?.clientWidth;
      }
    },

    render: function () {
      var inputClasses = {};
      inputClasses[this.props.customClasses.input] =
        !!this.props.customClasses.input;
      var inputClassList = classNames(inputClasses);

      var classes = {
        typeahead: true
      };
      classes[this.props.className] = !!this.props.className;
      var classList = classNames(classes);

      if (this._showDatePicker()) {
        return (
          <span ref="input" className={classList} onFocus={this._onFocus}>
            <DatePicker
              ref="datepicker"
              onChange={this._handleDateChange}
              onKeyDown={this._onKeyDown}
              value={this.state.entryValue}
              allSelected={this.props.allSelected}
              currentCategory={this.props.currentCategory}
            />
          </span>
        );
      }

      return (
        <div ref="input" className={classList} onFocus={this._onFocus}>
          <input
            ref="entry"
            type="text"
            placeholder={!this.state.focused ? this.props.placeholder : ""}
            className={inputClassList}
            defaultValue={this.state.entryValue}
            onChange={this._onTextEntryUpdated}
            onKeyDown={this._onKeyDown}
            style={{
              padding: 0,
              marginTop: 1,
              marginLeft: 2,
              width:
                this.props.className === "typehead-edit-view" &&
                this.calculateWidth()
            }}
          />
          <div ref={divInputRef} className="typehead-input-value">
            {this.state.entryValue.replace(/ /g, "\u00A0")}
          </div>
          {this._renderIncrementalSearchResults()}
        </div>
      );
    }
  })
);

export default Typeahead;
