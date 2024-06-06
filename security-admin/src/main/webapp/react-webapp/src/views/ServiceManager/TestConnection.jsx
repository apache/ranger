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

import React, { useState } from "react";
import { Button, Modal } from "react-bootstrap";
import { fetchApi } from "Utils/fetchAPI";
import { isEmpty } from "lodash";

function TestConnection(props) {
  const { formValues, getTestConnConfigs } = props;

  const [modelState, setModalState] = useState({
    showTestConnModal: false,
    showMore: true
  });

  const [modelContent, setModalContent] = useState({
    showMoreModalContent: "",
    testConnModalContent: ""
  });

  const hideTestConnModal = () =>
    setModalState({
      showTestConnModal: false,
      showMore: true
    });

  const showMoreClass = (val) => {
    setModalState({
      showTestConnModal: true,
      showMore: val
    });
  };

  const validateConfig = async () => {
    let testConnResp = {},
      msgModal = "",
      msgListModal = [];

    try {
      testConnResp = await fetchApi({
        url: "plugins/services/validateConfig",
        method: "post",
        data: getTestConnConfigs(formValues)
      });

      let respMsg = testConnResp.data.msgDesc;
      let respStatusCode = testConnResp.data.statusCode;
      let respMsgList = testConnResp.data.messageList;

      if (respStatusCode !== undefined && respStatusCode === 1) {
        msgModal = [
          <div
            className="test-conn-content"
            key={`${respStatusCode}.msgContent`}
          >
            <b>Connection Failed</b>
            <p>{respMsg}</p>
          </div>
        ];
        if (respMsgList !== undefined && respMsgList.length === 1) {
          msgListModal = [
            <div
              className="test-conn-content"
              key={`${respStatusCode}.msgList`}
            >
              <p className="connection-error mt-2">{respMsgList[0].message}</p>
            </div>
          ];
        }
      } else {
        msgModal = [
          <div className="test-conn-content" key={respStatusCode}>
            Connected Successfully.
          </div>
        ];
      }
      setModalState({
        showTestConnModal: true,
        showMore: true
      });
      setModalContent({
        testConnModalContent: msgModal,
        showMoreModalContent: msgListModal
      });
    } catch (error) {
      if (error?.response?.data?.msgDesc) {
        msgModal = error.response.data.msgDesc;
      }
      setModalState({
        showTestConnModal: true,
        showMore: true
      });
      setModalContent({
        testConnModalContent: msgModal,
        showMoreModalContent: msgListModal
      });
      console.error(`Error occurred while validating the configs!  ${error}`);
    }
  };

  return (
    <React.Fragment>
      <Button
        variant="outline-dark"
        size="sm"
        className="btn-sm"
        onClick={() => {
          validateConfig();
        }}
        data-id="testConn"
        data-cy="testConn"
      >
        Test Connection
      </Button>
      <Modal show={modelState.showTestConnModal} onHide={hideTestConnModal}>
        <Modal.Body>
          <div className="overflow-y-auto">
            {modelContent.testConnModalContent}
            {!modelState.showMore && modelContent.showMoreModalContent}
          </div>
        </Modal.Body>

        <Modal.Footer>
          {!isEmpty(modelContent.showMoreModalContent) &&
            (modelState.showMore ? (
              <Button
                variant="outline-primary"
                size="sm"
                onClick={() => {
                  showMoreClass(false);
                }}
              >
                Show More..
              </Button>
            ) : (
              <Button
                variant="outline-primary"
                size="sm"
                onClick={() => {
                  showMoreClass(true);
                }}
              >
                Show Less..
              </Button>
            ))}
          <Button variant="primary" size="sm" onClick={hideTestConnModal}>
            OK
          </Button>
        </Modal.Footer>
      </Modal>
    </React.Fragment>
  );
}

export default TestConnection;
