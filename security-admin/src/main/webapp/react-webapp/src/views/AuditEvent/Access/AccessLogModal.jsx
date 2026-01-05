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

import React from "react";
import { Button, Modal } from "react-bootstrap";
import { Link } from "react-router-dom";
import AccessLogTable from "./AccessLogTable";

export const AccessLogModal = (props) => {
  return (
    <Modal show={props.showRowModal} size="lg" onHide={props.hideRowModal}>
      <Modal.Header closeButton>
        <Modal.Title>
          <h4>
            Audit Access Log Detail
            <Link
              className="text-info"
              target="_blank"
              title="Show log details in next tab"
              to={{
                pathname: `/reports/audit/eventlog/${props.rowData.eventId}`
              }}
            >
              <i className="fa-fw fa fa-external-link float-end text-info"></i>
            </Link>
          </h4>
        </Modal.Title>
      </Modal.Header>
      <Modal.Body className="overflow-auto p-3 mb-3 mb-md-0 me-md-3">
        <AccessLogTable data={props.rowData}></AccessLogTable>
      </Modal.Body>
      <Modal.Footer>
        <Button variant="primary" size="sm" onClick={props.hideRowModal}>
          OK
        </Button>
      </Modal.Footer>
    </Modal>
  );
};
export default AccessLogModal;
