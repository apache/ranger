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

import { fetchApi } from "Utils/fetchAPI";
import { indexOf, isUndefined } from "lodash";
import React, { useEffect, useState } from "react";
import { Button, Modal } from "react-bootstrap";
import { PolicyViewDetails } from "Views/AuditEvent/Admin/AdminLogs/PolicyViewDetails";

export const AccessLogPolicyModal = (props) => {
  const [policyData, setPolicyData] = useState(null);
  const [policyVersionList, setPolicyVersion] = useState([]);

  useEffect(() => {
    if (props.showPolicyModal) {
      setPolicyData(props.policyData);
      fetchPolicyVersion(props.policyData.policyId);
    }
  }, [props.showPolicyModal]);

  const hidePolicyModal = () => {
    setPolicyData(null);
    setPolicyVersion([]);
    props.hidePolicyModal();
  };

  const refreshTable = () => {
    setPolicyData(null);
    setPolicyVersion([]);
    props.hidePolicyModal();
    props.refreshTable();
  };

  const fetchPolicyVersion = async (policyId) => {
    let versionsResp = {};

    try {
      versionsResp = await fetchApi({
        url: `plugins/policy/${policyId}/versionList`
      });
    } catch (error) {
      console.error(`Error occurred while fetching policy version : ${error}`);
    }

    setPolicyVersion(
      versionsResp.data.value
        .split(",")
        .map(Number)
        .sort(function (a, b) {
          return a - b;
        })
    );
  };

  const getUpdatedPolicyData = (e) => {
    e.preventDefault();

    let updatedPolicyData = {};
    let policyVersion = 1;

    if (e.currentTarget.classList.contains("active")) {
      let currentPolicyVersion = policyData && policyData.policyVersion;
      let currentPolicyVersionList = policyVersionList;

      if (e.currentTarget.title == "previous") {
        policyVersion =
          currentPolicyVersionList[
            (indexOf(currentPolicyVersionList, currentPolicyVersion) - 1) %
              currentPolicyVersionList.length
          ];
      } else {
        policyVersion =
          currentPolicyVersionList[
            (indexOf(currentPolicyVersionList, currentPolicyVersion) + 1) %
              currentPolicyVersionList.length
          ];
      }
    }

    updatedPolicyData.policyVersion = policyVersion;
    updatedPolicyData.policyId = policyData.policyId;
    updatedPolicyData.isChangeVersion = true;
    setPolicyData(updatedPolicyData);
  };

  const getRevertedPolicyData = (e) => {
    e.preventDefault();

    let revertPolicyData = {};
    let policyVersion = policyData && policyData.policyVersion;

    revertPolicyData.policyVersion = policyVersion;
    revertPolicyData.policyId = policyData.policyId;
    revertPolicyData.isRevert = true;
    setPolicyData(revertPolicyData);
  };

  return (
    <Modal show={props.showPolicyModal} onHide={hidePolicyModal} size="xl">
      <Modal.Header closeButton>
        <Modal.Title>Policy Details</Modal.Title>
      </Modal.Header>
      {policyData !== null && (
        <React.Fragment>
          <Modal.Body>
            <PolicyViewDetails
              paramsData={policyData}
              policyView={props.policyView}
              refreshTable={refreshTable}
            />
          </Modal.Body>
          <Modal.Footer>
            <div className="policy-version">
              <span>
                <i
                  className={
                    policyData && policyData.policyVersion > 1
                      ? "fa-fw fa fa-chevron-left active"
                      : "fa-fw fa fa-chevron-left"
                  }
                  title="previous"
                  onClick={(e) =>
                    e.currentTarget.classList.contains("active") &&
                    getUpdatedPolicyData(e)
                  }
                ></i>
                <span>{`Version ${
                  policyData && policyData.policyVersion
                }`}</span>
                <i
                  className={
                    !isUndefined(
                      policyVersionList[
                        indexOf(
                          policyVersionList,
                          policyData && policyData.policyVersion
                        ) + 1
                      ]
                    )
                      ? "fa-fw fa fa-chevron-right active"
                      : "fa-fw fa fa-chevron-right"
                  }
                  title="next"
                  onClick={(e) =>
                    e.currentTarget.classList.contains("active") &&
                    getUpdatedPolicyData(e)
                  }
                ></i>
              </span>
              {props.policyRevert &&
                !isUndefined(
                  policyVersionList[
                    indexOf(
                      policyVersionList,
                      policyData && policyData.policyVersion
                    ) + 1
                  ]
                ) && (
                  <Button
                    variant="primary"
                    size="sm"
                    onClick={(e) => getRevertedPolicyData(e)}
                  >
                    Revert
                  </Button>
                )}
            </div>
            <Button variant="primary" size="sm" onClick={hidePolicyModal}>
              OK
            </Button>
          </Modal.Footer>
        </React.Fragment>
      )}
    </Modal>
  );
};
export default AccessLogPolicyModal;
