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
import Tether from "tether";
import ReactDOM from "react-dom";
import createReactClass from "create-react-class";

var Popover = createReactClass({
  displayName: "Popover",

  componentWillMount: function () {
    let popoverContainer = document.createElement("span");
    popoverContainer.className = "datepicker__container";

    this._popoverElement = popoverContainer;

    document.querySelector("body").appendChild(this._popoverElement);
  },

  componentDidMount: function () {
    this._renderPopover();
  },

  componentDidUpdate: function () {
    this._renderPopover();
  },

  _popoverComponent: function () {
    let current = ReactDOM.findDOMNode(this);
    var className = this.props.className;
    return <div className={className}>{this.props.children}</div>;
  },

  _tetherOptions: function () {
    let current = ReactDOM.findDOMNode(this);
    return {
      element: this._popoverElement,
      target: current.parentElement,
      attachment: "top left",
      targetAttachment: "bottom left",
      targetOffset: "10px 0",
      optimizations: {
        moveElement: false // always moves to <body> anyway!
      },
      constraints: [
        {
          to: "window",
          attachment: "together",
          pin: true
        }
      ]
    };
  },

  _renderPopover: function () {
    ReactDOM.render(this._popoverComponent(), this._popoverElement);

    if (this._tether != null) {
      this._tether.setOptions(this._tetherOptions());
    } else {
      this._tether = new Tether(this._tetherOptions());
    }
  },

  componentWillUnmount: function () {
    this._tether.destroy();
    ReactDOM.unmountComponentAtNode(this._popoverElement);
    if (this._popoverElement.parentNode) {
      this._popoverElement.parentNode.removeChild(this._popoverElement);
    }
  },

  render: function () {
    return <span />;
  }
});

export default Popover;
