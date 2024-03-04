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

import React, { useState, useEffect, useRef } from "react";
import { fetchApi } from "Utils/fetchAPI";
import { Button, Row, Col, Alert } from "react-bootstrap";
import { isAuditor, isKMSAuditor, serverError } from "Utils/XAUtils";
import { toast } from "react-toastify";
import { ModalLoader } from "../../components/CommonComponents";
import { map } from "lodash";

function GroupAssociateUserDetails(props) {
  const { groupID } = props;
  const [userListData, setUserDataList] = useState([]);
  const [filterUserListData, setFilterUserDataList] = useState({
    searchUser: null,
    usrData: []
  });
  const [loader, setLoader] = useState({
    modalLoader: true,
    contentLoader: false
  });
  const [showAlluser, setShowAllUser] = useState(false);
  const [totalCount, setTotalCount] = useState(null);
  const toastId = useRef(null);

  useEffect(() => {
    getUserList();
  }, [showAlluser]);

  const getUserList = async () => {
    let userList;
    try {
      userList = await fetchApi({
        url: `xusers/${groupID}/users`,
        method: "GET",
        params: {
          pageSize: totalCount || 100,
          startIndex: 0
        }
      });
    } catch (error) {
      serverError(error);
      console.error(error);
    }

    let userData = map(userList.data.vXUsers, function (value) {
      return { value: value.name, id: value.id };
    });
    setUserDataList(userData);
    if (
      filterUserListData?.searchUser !== undefined &&
      filterUserListData?.searchUser !== null
    ) {
      let userList = userData.filter((v) => {
        return v.value
          .toLowerCase()
          .includes(filterUserListData?.searchUser.toLowerCase());
      });
      setFilterUserDataList({ usrData: userList });
    } else {
      setFilterUserDataList({ usrData: userData });
    }
    setTotalCount(userList.data.totalCount);
    setLoader({ modalLoader: false, contentLoader: false });
  };

  const onChangeSearch = (e) => {
    let userList = userListData.filter((v) => {
      return v.value
        .toLowerCase()
        .includes(e.currentTarget.value.toLowerCase());
    });
    setFilterUserDataList({
      searchUser: e.currentTarget.value,
      usrData: userList
    });
  };
  const copyText = () => {
    let userCopytext = "";
    userCopytext = filterUserListData?.usrData
      .map((val) => {
        return val.value;
      })
      .join(" | ");
    if (userCopytext.length == 0) {
      toast.dismiss(toastId.current);
      toastId.current = toast.warning("No user list find for copy");
    } else {
      toast.dismiss(toastId.current);
      toastId.current = toast.success("User list copied successfully!!");
    }
    return userCopytext;
  };

  return loader.modalLoader ? (
    <ModalLoader />
  ) : (
    <>
      {userListData && userListData.length > 0 ? (
        <>
          {totalCount > 100 && (
            <Alert variant="warning">
              Initially search filter is applied for first hundred users. To get
              more users click on{" "}
              <Button
                variant="outline-secondary"
                size="sm"
                className={`${showAlluser ? "not-allowed" : ""} ms-2 btn-mini`}
                onClick={() => {
                  setShowAllUser(true);
                  setLoader({ modalLoader: false, contentLoader: true });
                }}
                data-id="Show All Users"
                data-cy="Show All Users"
                title="Show All Users"
                disabled={showAlluser ? true : false}
              >
                Show All Users
              </Button>
            </Alert>
          )}
          <Row className="mb-2">
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
                className="me-2 rounded-pill border"
                size="sm"
                variant="link"
                onClick={() => navigator.clipboard.writeText(copyText())}
                title="Copy All Users Name"
              >
                <i className="fa-fw fa fa-copy"> </i>
              </Button>
            </Col>
          </Row>

          <Row>
            <Col>
              {loader.contentLoader ? (
                <ModalLoader />
              ) : filterUserListData?.usrData?.length > 0 ? (
                filterUserListData?.usrData.map((val, index) => {
                  return (
                    <Button
                      variant="link"
                      href={`#/user/${val.id}`}
                      size="sm"
                      className={`me-2 mb-2 rounded-pill border text-truncate more-less-width ${
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
                })
              ) : (
                "No users found."
              )}
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
