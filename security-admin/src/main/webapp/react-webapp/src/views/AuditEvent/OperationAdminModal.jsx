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
import { ClassTypes } from "../../utils/XAEnums";
import { Modal, Button, Table } from "react-bootstrap";
import { fetchApi } from "Utils/fetchAPI";
import SecurityZonelogs from "./AdminLogs/SecurityZonelogs";
import UserLogs from "./AdminLogs/UserLogs";
import GroupLogs from "./AdminLogs/GroupLogs";
import RoleLogs from "./AdminLogs/RoleLogs";
import UserAssociationWithGroupLogs from "./AdminLogs/UserAssociationWithGroupLogs";
import ServiceLogs from "./AdminLogs/ServiceLogs";
import PolicyLogs from "./AdminLogs/PolicyLogs";
import PasswordLogs from "./AdminLogs/PasswordLogs";
import UserprofileLogs from "./AdminLogs/UserprofileLogs";
import { ModalLoader } from "../../components/CommonComponents";

export const OperationAdminModal = ({ onHide, show, data = {} }) => {
  const [reportdata, setReportData] = useState([]);
  const [loader, setLoader] = useState(false);
  const [showview, setShowview] = useState(null);
  const { objectClassType, action, objectId, transactionId } = data;

  useEffect(() => {
    show && rowModal();
  }, [show]);

  const rowModal = async () => {
    setLoader(true);
    let authlogs = [];

    try {
      const authResp = await fetchApi({
        url: `assets/report/${transactionId}`
      });
      authlogs = authResp?.data?.vXTrxLogs || [];
    } catch (error) {
      console.error(`Error occurred while fetching Admin logs! ${error}`);
    }

    setShowview(objectId);
    setReportData(authlogs);
    setLoader(false);
  };

  return (
    <Modal show={show} size="lg" onHide={onHide}>
      <Modal.Header closeButton>
        <Modal.Title>Operation :{action || ""}</Modal.Title>
      </Modal.Header>

      <Modal.Body className="overflow-auto p-3 mb-3 mb-md-0 me-md-3">
        {loader ? (
          <>
            <ModalLoader />
          </>
        ) : (
          <div>
            {/* SERVICE */}

            {objectClassType == ClassTypes.CLASS_TYPE_RANGER_SERVICE.value && (
              <ServiceLogs reportdata={reportdata} data={data} />
            )}

            {/* POLICY */}

            {objectClassType == ClassTypes.CLASS_TYPE_RANGER_POLICY.value && (
              <PolicyLogs reportdata={reportdata} data={data} />
            )}

            {objectClassType == ClassTypes.CLASS_TYPE_USER_PROFILE.value && (
              <UserprofileLogs reportdata={reportdata} data={data} />
            )}
            {/* SECURITY ZONE */}

            {objectClassType ==
              ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value && (
              <SecurityZonelogs reportdata={reportdata} data={data} />
            )}

            {/* USER */}

            {objectClassType == ClassTypes.CLASS_TYPE_XA_USER.value && (
              <UserLogs reportdata={reportdata} data={data} />
            )}

            {/* USER ADDED TO GROUP */}

            {objectClassType == ClassTypes.CLASS_TYPE_XA_GROUP_USER.value && (
              <UserAssociationWithGroupLogs
                reportdata={reportdata}
                data={data}
              />
            )}

            {/* GROUP */}

            {objectClassType == ClassTypes.CLASS_TYPE_XA_GROUP.value && (
              <GroupLogs reportdata={reportdata} data={data} />
            )}
            {/* ROLE */}

            {objectClassType == ClassTypes.CLASS_TYPE_RANGER_ROLE.value && (
              <RoleLogs reportdata={reportdata} data={data} />
            )}

            {/* PASSWORD CHANGE */}

            {objectClassType == ClassTypes.CLASS_TYPE_PASSWORD_CHANGE.value && (
              <PasswordLogs reportdata={reportdata} data={data} />
            )}
          </div>
        )}
      </Modal.Body>

      <Modal.Footer>
        <Button variant="primary" size="sm" onClick={onHide}>
          OK
        </Button>
      </Modal.Footer>
    </Modal>
  );
};
export default OperationAdminModal;
