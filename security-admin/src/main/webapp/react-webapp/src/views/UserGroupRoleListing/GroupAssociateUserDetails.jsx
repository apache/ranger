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

import React, { useState, useEffect } from "react";
import { fetchApi } from "Utils/fetchAPI";
import { Button, Row, Col } from "react-bootstrap";
import { isAuditor, isKMSAuditor, serverError } from "Utils/XAUtils";
import { toast } from "react-toastify";
import { ModalLoader } from "../../components/CommonComponents";

function GroupAssociateUserDetails(props) {
  const { groupID } = props;
  const [userListData, setUserDataList] = useState([]);
  const [filterUserListData, setFilterUserDataList] = useState([]);
  const [loader, setLoader] = useState(true);
  useEffect(() => {
    getUserList();
  }, []);
  const getUserList = async () => {
    let errorMsg = "",
      userList;
    try {
      userList = await fetchApi({
        url: `xusers/${groupID}/users`,
        method: "GET",
        params: {
          pageSize: 100,
          startIndex: 0
        }
      });
    } catch (error) {
      serverError(error);
      console.error(error);
    }

    let userData = _.map(userList.data.vXUsers, function (value) {
      return { value: value.name, id: value.id };
    });
    console.log(userData);
    setUserDataList(userData);
    setFilterUserDataList(userData);
    setLoader(false);
  };

  const onChangeSearch = (e) => {
    console.log(e);
    let userList = userListData.filter((v) => {
      return v.value.includes(e.currentTarget.value);
    });
    setFilterUserDataList(userList);
  };
  const copyText = (e) => {
    let userCopytext = "";
    userCopytext = filterUserListData
      .map((val) => {
        return val.value;
      })
      .join(" | ");
    if (userCopytext.length == 0) {
      toast.warning("No user list find for copy");
    } else {
      toast.success("User list copied successfully!!");
    }
    return userCopytext;
  };

  return loader ? (
    <ModalLoader />
  ) : (
    <>
      {userListData && userListData.length > 0 ? (
        <>
          <Row>
            <Col className="col-sm-11">
              <input
                className="form-control"
                type="text"
                onChange={onChangeSearch}
                placeholder="Search"
                data-id="userInput"
                data-cy="userInput"
              ></input>
            </Col>
            <Col className="col-sm-1">
              <Button
                className="mr-2 rounded-pill border"
                size="sm"
                variant="link"
                onClick={() => navigator.clipboard.writeText(copyText())}
                title="Copy All Users Name"
              >
                <i className="fa-fw fa fa-copy"> </i>
              </Button>
            </Col>
          </Row>
          <br />
          <Row>
            <Col>
              {filterUserListData.map((val, index) => {
                return (
                  <Button
                    variant="link"
                    href={`#/user/${val.id}`}
                    size="sm"
                    className={`mr-2 rounded-pill border text-truncate more-less-width ${
                      isAuditor() || isKMSAuditor()
                        ? "disabled-link text-secondary"
                        : ""
                    }`}
                    title={val.value}
                    key={index}
                  >
                    {val.value}
                  </Button>
                );
              })}
            </Col>
          </Row>
        </>
      ) : (
        <>
          <center className="text-muted">
            No user associate with this group.!!
          </center>
        </>
      )}
    </>
  );
}
export default GroupAssociateUserDetails;
