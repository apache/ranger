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
import onClickOutside from "react-onclickoutside";
import Datetime from "react-datetime";

var Calendar = onClickOutside(
  createReactClass({
    handleClickOutside: function () {
      this.props.hideCalendar();
    },

    isValidDate: function (allSelected, currentDate, currentCategory) {
      let sDate = allSelected?.find((element) => {
        return element.category == "startDate";
      });
      let eDate = allSelected?.find((element) => {
        return element.category == "endDate";
      });

      if (sDate !== undefined && eDate !== undefined) {
        if (currentCategory === "startDate") {
          return currentDate.isBefore(eDate?.value);
        }

        if (currentCategory === "endDate") {
          return currentDate.isAfter(sDate?.value);
        }
      } else if (sDate !== undefined && currentCategory === "endDate") {
        return currentDate.isAfter(sDate?.value);
      } else if (eDate !== undefined && currentCategory === "startDate") {
        return currentDate.isBefore(eDate?.value);
      } else {
        return true;
      }
    },

    render: function () {
      return (
        <Datetime
          ref="datetime"
          dateFormat="MM/DD/YYYY"
          timeFormat={false}
          closeOnSelect
          input={false}
          className="typehead-token-datetime"
          value={this.props.value}
          onChange={this.props.onSelect}
          isValidDate={(currentDate) =>
            this.isValidDate(
              this.props.allSelected,
              currentDate,
              this.props.currentCategory
            )
          }
        />
      );
    }
  })
);

export default Calendar;
