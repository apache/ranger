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

import React, { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { fetchApi } from "Utils/fetchAPI";
import { AuthStatus, AuthType } from "../../utils/XAEnums";
import { Modal, Table, Button } from "react-bootstrap";
import { has } from "lodash";
import { ModalLoader } from "../../components/CommonComponents";
import { currentTimeZone } from "../../utils/XAUtils";

export const AdminModal = (props) => {
  const [authSession, setAuthSession] = useState([]);
  const [loader, setLoader] = useState(true);
  const [searchParams] = useSearchParams();

  useEffect(() => {
    if (props.show) {
      handleShow();
    }
  }, [props.show]);
  const handleShow = async () => {
    let authlogs = [];
    try {
      setLoader(true);
      const authResp = await fetchApi({
        url: "xusers/authSessions",
        params: {
          id: props.data
        }
      });
      authlogs = authResp.data.vXAuthSessions;
      setAuthSession(authlogs);
      setLoader(false);
    } catch (error) {
      setLoader(false);
      console.error(`Error occurred while fetching Admin logs! ${error}`);
    }
  };

  const setSessionId = () => {
    props.onHide();
    const currentParams = Object.fromEntries([...searchParams]);
    if (!has(currentParams, "sessionId")) {
      props.updateSessionId(props.data);
    }
  };

  return (
    <Modal show={props.show} size="lg" onHide={props.onHide}>
      <Modal.Header closeButton>
        <Modal.Title>Session Detail</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        {loader ? (
          <ModalLoader />
        ) : (
          <Table bordered hover>
            <tbody>
              <tr>
                <th>Login ID</th>
                <td>
                  {authSession.map((obj) => {
                    return obj.loginId;
                  })}
                </td>
              </tr>
              <tr>
                <th>Result</th>
                <td>
                  {authSession.map((obj) => {
                    var result = "";
                    Object.keys(AuthStatus).map((item) => {
                      if (obj.authStatus == AuthStatus[item].value) {
                        let label = AuthStatus[item].label;
                        if (AuthStatus[item].value == 1) {
                          result = label;
                        } else if (AuthStatus[item].value == 2) {
                          result = label;
                        } else {
                          result = label;
                        }
                      }
                    });
                    return result;
                  })}
                </td>
              </tr>
              <tr>
                <th>Login Type</th>
                <td>
                  {authSession.map((obj) => {
                    var type = "";
                    Object.keys(AuthType).map((item) => {
                      if (obj.authType == AuthType[item].value) {
                        return (type = AuthType[item].label);
                      }
                    });
                    return type;
                  })}
                </td>
              </tr>
              <tr>
                <th>IP</th>
                <td>
                  {authSession.map((obj) => {
                    return obj.requestIP;
                  })}
                </td>
              </tr>
              <tr>
                <th>User Agent</th>
                <td>
                  {authSession.map((obj) => {
                    return obj.requestUserAgent;
                  })}
                </td>
              </tr>
              <tr>
                <th>Login Time</th>
                <td>
                  {authSession.map((obj) => {
                    return currentTimeZone(obj.authTime);
                  })}
                </td>
              </tr>
            </tbody>
          </Table>
        )}
        <Button
          variant="link"
          className="link-tag"
          size="sm"
          onClick={setSessionId}
          data-id="showAction"
          data-cy="showAction"
        >
          Show Actions
        </Button>
      </Modal.Body>

      <Modal.Footer>
        <Button variant="primary" size="sm" onClick={props.onHide}>
          OK
        </Button>
      </Modal.Footer>
    </Modal>
  );
};
export default AdminModal;
