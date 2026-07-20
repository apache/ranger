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
import { Tab, Tabs, Accordion, Row, Col } from "react-bootstrap";
import { Field } from "react-final-form";
import BootstrapSwitchButton from "bootstrap-switch-button-react";
import PolicyPermissionItemVertical from "./PolicyPermissionItemVertical";
import { isEmpty } from "lodash";

const hasItems = (arr) =>
  Array.isArray(arr) && arr.some((item) => !isItemEmpty(item));

const isItemEmpty = (item) => {
  if (!item || typeof item !== "object") return true;

  // Check if object is completely empty
  if (Object.keys(item).length === 0) return true;

  // Check if users/groups/roles have actual content
  const hasUsers =
    item.users?.users?.length > 0 ||
    item.users?.groups?.length > 0 ||
    item.users?.roles?.length > 0 ||
    (Array.isArray(item.users) && item.users.length > 0) ||
    (Array.isArray(item.groups) && item.groups.length > 0) ||
    (Array.isArray(item.roles) && item.roles.length > 0);

  // Check if accesses have content (handle both regular and tag-based policies)
  const hasAccesses =
    // Regular policy: accesses is an array
    (Array.isArray(item.accesses) && item.accesses.length > 0) ||
    // Tag-based policy: accesses is an object with tableList
    (item.accesses?.tableList &&
      Array.isArray(item.accesses.tableList) &&
      item.accesses.tableList.some(
        (table) =>
          Array.isArray(table.permission) && table.permission.length > 0
      ));

  // Check if other meaningful fields exist (for different policy types)
  // delegateAdmin should only count as content when it's true (checkbox checked)
  const hasOtherContent =
    item.dataMaskInfo ||
    item.rowFilterInfo ||
    item.delegateAdmin === true ||
    (item.conditions &&
      typeof item.conditions === "object" &&
      Object.keys(item.conditions).length > 0);

  // Special case: if object only has delegateAdmin: false and nothing else, consider it empty
  const onlyFalseDelegateAdmin =
    Object.keys(item).length === 1 && item.delegateAdmin === false;

  // Special case: if object only has empty conditions and nothing else, consider it empty
  const onlyEmptyConditions =
    Object.keys(item).length === 1 &&
    item.conditions &&
    typeof item.conditions === "object" &&
    Object.keys(item.conditions).length === 0;

  if (onlyFalseDelegateAdmin || onlyEmptyConditions) return true;

  // An item is empty if it has no meaningful content
  const isEmpty = !hasUsers && !hasAccesses && !hasOtherContent;

  return isEmpty;
};

const AccessRulesStep = ({ values, selectedServiceComponentDef }) => {
  const isDenyAllElseEnabled = values?.isDenyAllElse === true;
  const enableDenyAndExceptions =
    selectedServiceComponentDef?.options?.enableDenyAndExceptionsInPolicies ===
    "true";
  const policyPermissionItems = [];
  const policyType = values.policyType;

  // Count items in each category
  const allowRulesCount = hasItems(values?.policyItems)
    ? values.policyItems.filter((item) => !isEmpty(item)).length
    : 0;

  const allowExceptionsCount = hasItems(values?.allowExceptions)
    ? values.allowExceptions.filter((item) => !isEmpty(item)).length
    : 0;

  const denyRulesCount = hasItems(values?.denyPolicyItems)
    ? values.denyPolicyItems.filter((item) => !isEmpty(item)).length
    : 0;

  const denyExceptionsCount = hasItems(values?.denyExceptions)
    ? values.denyExceptions.filter((item) => !isEmpty(item)).length
    : 0;

  // Total counts for tab display
  const allowTabTotal = allowRulesCount + allowExceptionsCount;
  const denyTabTotal = denyRulesCount + denyExceptionsCount;

  // Auto-open "Exclude from Allow Rules" accordion item if it has saved values
  const hasAllowExceptions = hasItems(values?.allowExceptions);
  const allowAccordionDefault = hasAllowExceptions ? ["0", "1"] : ["0"];

  if (policyType == 1) {
    policyPermissionItems.push({
      attrName: "dataMaskPolicyItems",
      title: "Mask Rules",
      msg: "Mask rules grants access to resources when the specific criteria are met."
    });
  } else if (policyType == 2) {
    policyPermissionItems.push({
      attrName: "rowFilterPolicyItems",
      title: "Row Filter Rules",
      msg: "Row filter rules grants access to resources when the specific criteria are met."
    });
  } else if (policyType == 0 && !enableDenyAndExceptions) {
    policyPermissionItems.push({
      attrName: "policyItems",
      title: "Allow Rules",
      msg: "Allow rules grants access to resources when the specific criteria are met."
    });
  }

  return (
    <Row>
      <Col xxl={12}>
        <p className="stepper-instruction-text">
          Configure access permissions by setting allow conditions to grant
          access to specific users/groups/roles and deny conditions to
          explicitly block access
        </p>
        {policyType == 0 && isEmpty(policyPermissionItems) && (
          <>
            <Tabs defaultActiveKey="allow" transition={false} className="mb-3">
              <Tab
                eventKey="allow"
                title={
                  <span className="d-flex align-items-center gap-2">
                    Allow
                    {allowTabTotal > 0 && (
                      <span
                        className="badge bg-primary text-light"
                        style={{
                          fontSize: "0.75rem",
                          padding: "0.25rem 0.5rem",
                          borderRadius: "12px",
                          margin: "0px"
                        }}
                      >
                        {allowTabTotal}
                      </span>
                    )}
                  </span>
                }
              >
                <Accordion
                  defaultActiveKey={allowAccordionDefault}
                  alwaysOpen
                  className="policy-permission-items-accordion"
                >
                  <Accordion.Item eventKey="0">
                    <Accordion.Header>
                      <span className="d-flex align-items-center justify-content-between w-100">
                        <span>Allow Rules</span>
                      </span>
                    </Accordion.Header>
                    <Accordion.Body>
                      <PolicyPermissionItemVertical
                        serviceCompDetails={selectedServiceComponentDef}
                        formValues={values}
                        attrName="policyItems"
                        msg="Allow rules grants access to resources when the specific criteria are met."
                      />
                    </Accordion.Body>
                  </Accordion.Item>

                  <Accordion.Item eventKey="1">
                    <Accordion.Header>
                      <span className="d-flex align-items-center justify-content-between w-100">
                        <span>Exclude from Allow Rules</span>
                      </span>
                    </Accordion.Header>
                    <Accordion.Body>
                      <PolicyPermissionItemVertical
                        serviceCompDetails={selectedServiceComponentDef}
                        formValues={values}
                        attrName="allowExceptions"
                        msg="Exclude from Allow Rules grants access to resources when the specific criteria are met."
                      />
                    </Accordion.Body>
                  </Accordion.Item>

                  <Accordion.Item eventKey="2">
                    <Accordion.Header>Deny All Other Accesses</Accordion.Header>
                    <Accordion.Body>
                      <p className="stepper-instruction-text mb-3">
                        When enabled, this option denies all access types (read,
                        write, execute, etc.) to every user, group and role
                        except those explicitly allowed in this policy. Any
                        user/group/role not listed in the &apos;Allow&apos;
                        section will automatically be blocked from accessing the
                        resource.
                      </p>

                      <Field name="isDenyAllElse">
                        {({ input }) => (
                          <Row>
                            <Col className="column-1-fixed">
                              <span>Deny all other accesses</span>
                            </Col>
                            <Col className="resource-toggle-switch">
                              <BootstrapSwitchButton
                                {...input}
                                checked={input.value || false}
                                onlabel="True"
                                onstyle="primary"
                                offlabel="False"
                                offstyle="outline-secondary"
                                size="xs"
                                width={100}
                              />
                            </Col>
                          </Row>
                        )}
                      </Field>
                    </Accordion.Body>
                  </Accordion.Item>
                </Accordion>
              </Tab>

              <Tab
                eventKey="deny"
                disabled={isDenyAllElseEnabled}
                title={
                  <span className="d-flex align-items-center gap-2">
                    Deny
                    {!isDenyAllElseEnabled && denyTabTotal > 0 && (
                      <span
                        className="badge bg-primary text-light"
                        style={{
                          fontSize: "0.75rem",
                          padding: "0.25rem 0.5rem",
                          borderRadius: "12px",
                          margin: "0px"
                        }}
                      >
                        {denyTabTotal}
                      </span>
                    )}
                  </span>
                }
              >
                <Accordion
                  defaultActiveKey={["0"]}
                  alwaysOpen
                  className="policy-permission-items-accordion"
                >
                  <Accordion.Item eventKey="0">
                    <Accordion.Header>
                      <span className="d-flex align-items-center justify-content-between w-100">
                        <span>Deny Rules</span>
                      </span>
                    </Accordion.Header>
                    <Accordion.Body>
                      <PolicyPermissionItemVertical
                        serviceCompDetails={selectedServiceComponentDef}
                        formValues={values}
                        attrName="denyPolicyItems"
                        msg="Deny rules explicitly block access to resources when the specific
            criteria are met."
                      />
                    </Accordion.Body>
                  </Accordion.Item>

                  <Accordion.Item eventKey="1">
                    <Accordion.Header>
                      <span className="d-flex align-items-center justify-content-between w-100">
                        <span>Exclude from Deny Rules</span>
                      </span>
                    </Accordion.Header>
                    <Accordion.Body>
                      <PolicyPermissionItemVertical
                        serviceCompDetails={selectedServiceComponentDef}
                        formValues={values}
                        attrName="denyExceptions"
                        msg="Exclude from Deny Rules explicitly block access to resources when the specific
            criteria are met."
                      />
                    </Accordion.Body>
                  </Accordion.Item>
                </Accordion>
              </Tab>
            </Tabs>
          </>
        )}
        {!isEmpty(policyPermissionItems) && (
          <>
            {policyPermissionItems.map((item) => (
              <PolicyPermissionItemVertical
                key={item.attrName}
                serviceCompDetails={selectedServiceComponentDef}
                formValues={values}
                attrName={item.attrName}
                msg={item.msg}
              />
            ))}
          </>
        )}
      </Col>
    </Row>
  );
};

export default AccessRulesStep;
