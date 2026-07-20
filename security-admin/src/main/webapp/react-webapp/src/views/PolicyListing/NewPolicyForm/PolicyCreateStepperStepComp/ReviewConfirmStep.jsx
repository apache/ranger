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
import { Accordion, Badge, Table } from "react-bootstrap";
import { isEmpty, isArray, find, map } from "lodash";
import moment from "moment";
import {
  getResourcesDefVal,
  getPolicyConditionDisplayLbl
} from "Utils/XAUtils";
import { DefStatus, RangerPolicyType } from "Utils/XAEnums";
import userColourIcon from "Images/user-colour.svg";
import groupColourIcon from "Images/group-colour.svg";
import roleColourIcon from "Images/role-colour.svg";

// ─── reusable building blocks ────────────────────────────────────────────────

const Empty = () => <span className="text-muted fst-italic">—</span>;

const StatusBadge = ({ on, onLabel = "Yes", offLabel = "No" }) => (
  <Badge bg={on ? "success" : "secondary"}>{on ? onLabel : offLabel}</Badge>
);

/** Icon + name badges for users / groups / roles */
const PrincipalRow = ({ icon, alt, items = [] }) =>
  items.length === 0 ? null : (
    <div className="d-flex align-items-center flex-wrap gap-1 mb-1">
      <img src={icon} height="24" width="24" alt={alt} className="me-1" />
      {items.map((p, i) => (
        <Badge
          key={i}
          bg="light"
          text="dark"
          className="border"
          style={{ fontSize: "13px", fontWeight: "400" }}
        >
          {p.name || p.value || p}
        </Badge>
      ))}
    </div>
  );

/**
 * One label + value row inside a section.
 * label column is fixed at 190 px; value fills the rest.
 * ✅ NEW: Accepts isLast prop to remove bottom border from last item
 */
const InfoRow = ({ label, children, isLast = false }) => (
  <div
    className="d-flex align-items-start py-2"
    style={{
      borderBottom: isLast ? "none" : "1px solid #dee2e6"
    }}
  >
    <span className="me-3" style={{ minWidth: 210, fontSize: "14px" }}>
      {label}
    </span>
    <div className="flex-grow-1" style={{ fontSize: "14px" }}>
      {children ?? <Empty />}
    </div>
  </div>
);

/** Accordion section header with step number + icon */
const SectionHeader = ({ title }) => (
  <span className="d-flex align-items-center gap-2">{title}</span>
);

// ─── main component ──────────────────────────────────────────────────────────

const ReviewConfirmStep = ({ values, selectedServiceComponentDef }) => {
  const policyType = values.policyType;
  const isMasking =
    policyType == RangerPolicyType.RANGER_MASKING_POLICY_TYPE.value;
  const isRowFilter =
    policyType == RangerPolicyType.RANGER_ROW_FILTER_POLICY_TYPE.value;
  const enableDenyAndExceptions =
    selectedServiceComponentDef?.options?.enableDenyAndExceptionsInPolicies ===
    "true";

  const policyTypeLabel = isMasking
    ? "Masking"
    : isRowFilter
      ? "Row Filter"
      : "Access";

  const displayTagPermissionItem = (permission) => {
    let srcOp = [];
    if (isMasking) {
      srcOp = selectedServiceComponentDef.dataMaskDef?.accessTypes || [];
    } else if (isRowFilter) {
      srcOp = selectedServiceComponentDef.rowFilterDef?.accessTypes || [];
    } else {
      srcOp = selectedServiceComponentDef.accessTypes;
    }
    const labels = map(permission, (perm) => {
      return find(srcOp, { name: perm })?.label;
    });
    return labels;
  };

  const resourcesArray = (values.additionalResources || []).filter(
    (r) => r && !isEmpty(r)
  );

  const renderResources = () => {
    if (resourcesArray.length === 0) return <Empty />;

    const resourceDef =
      getResourcesDefVal(selectedServiceComponentDef, policyType) || [];

    // Collect all numeric levels present in the def
    const levels = [...new Set(resourceDef.map((d) => d.level ?? 0))].sort(
      (a, b) => a - b
    );

    return resourcesArray.map((resourceItem, ridx) => {
      // Build rows by iterating levels
      const rows = [];
      for (const level of levels) {
        const nameObj = resourceItem[`resourceName-${level}`];
        const valueArr = resourceItem[`value-${level}`];
        if (!nameObj || !valueArr) continue;

        const resourceName = nameObj.name || nameObj;
        const defItem = find(resourceDef, { name: resourceName });
        if (!defItem) continue;

        // Extract plain string values
        const vals = isArray(valueArr)
          ? valueArr
              .map((v) => (typeof v === "object" ? (v.value ?? v.label) : v))
              .filter(Boolean)
          : valueArr
            ? [
                typeof valueArr === "object"
                  ? (valueArr.value ?? valueArr.label)
                  : valueArr
              ]
            : [];

        rows.push({
          label: defItem.label || resourceName,
          level,
          vals,
          isExcludes: resourceItem[`isExcludesSupport-${level}`] === false,
          isRecursive: resourceItem[`isRecursiveSupport-${level}`] !== false,
          excludesSupported: defItem.excludesSupported,
          recursiveSupported: defItem.recursiveSupported
        });
      }

      if (rows.length === 0) return null;

      return (
        <div key={ridx}>
          {resourcesArray.length > 1 && (
            <p className="text-uppercase small mb-2 mt-3">#{ridx + 1}</p>
          )}
          <Table bordered size="sm" className="mb-3 align-middle">
            <tbody>
              {rows.map((r, i) => (
                <tr key={i}>
                  <td style={{ width: 210 }}>{r.label}</td>
                  <td>
                    <div className="d-flex flex-wrap gap-1">
                      {r.vals.length > 0 ? (
                        r.vals.map((v, vi) => (
                          <Badge key={vi} bg="primary">
                            {v}
                          </Badge>
                        ))
                      ) : (
                        <Empty />
                      )}
                    </div>
                  </td>
                  <td style={{ width: 224 }}>
                    <div className="d-flex flex-wrap gap-1">
                      {r.excludesSupported && (
                        <Badge
                          bg={r.isExcludes ? "warning" : "success"}
                          text={r.isExcludes ? "dark" : undefined}
                          className="text-capitalize"
                        >
                          {r.isExcludes
                            ? DefStatus.ExcludeStatus.STATUS_EXCLUDE.label
                            : DefStatus.ExcludeStatus.STATUS_INCLUDE.label}
                        </Badge>
                      )}
                      {r.recursiveSupported && (
                        <Badge bg="secondary" className="text-capitalize">
                          {r.isRecursive
                            ? DefStatus.RecursiveStatus.STATUS_RECURSIVE.label
                            : DefStatus.RecursiveStatus.STATUS_NONRECURSIVE
                                .label}
                        </Badge>
                      )}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </Table>
        </div>
      );
    });
  };

  // ── single rule card ─────────────────────────────────────────────────────
  const RuleCard = ({ item, idx, ruleLabel }) => {
    if (!item || isEmpty(item)) return null;

    const users = item.users?.users || (isArray(item.users) ? item.users : []);
    const groups =
      item.users?.groups || (isArray(item.groups) ? item.groups : []);
    const roles = item.users?.roles || (isArray(item.roles) ? item.roles : []);
    const hasPrincipal =
      users.length > 0 || groups.length > 0 || roles.length > 0;
    const isTagAccesses = !!item.accesses?.tableList;
    const hasAccesses = isTagAccesses
      ? item.accesses.tableList.length > 0
      : isArray(item.accesses);
    const hasConditions =
      (item.conditions && !isEmpty(item.conditions)) ||
      selectedServiceComponentDef?.policyConditions?.length > 0;
    const hasDataMask = !!item.dataMaskInfo?.value;
    const hasRowFilter = !!(
      item.rowFilterInfo && String(item.rowFilterInfo).trim()
    );
    const hasDelegateAdmin = !isMasking && !isRowFilter && !isTagAccesses;

    if (
      !hasPrincipal &&
      !hasAccesses &&
      !hasConditions &&
      !hasDataMask &&
      !hasRowFilter &&
      !hasDelegateAdmin
    ) {
      return null;
    }

    return (
      <div className="border rounded mb-3 mt-2 overflow-hidden">
        {/* rule header */}
        <div className="px-3 py-2">
          <span className="text-primary">
            {ruleLabel} #{idx + 1}
          </span>
        </div>

        {/* rule body */}
        <div className="px-3 py-2">
          {hasAccesses && (
            <InfoRow label="Permissions">
              {isTagAccesses ? (
                <Table bordered size="sm" className="mb-0 align-middle">
                  <thead className="table-light">
                    <tr>
                      <th>Service</th>
                      <th>Permissions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {item.accesses.tableList.map((t, ti) => (
                      <tr key={ti}>
                        <td className="text-uppercase small">
                          {t.serviceName}
                        </td>
                        <td>
                          <div className="d-flex flex-wrap gap-1">
                            {(
                              displayTagPermissionItem(t.permission || []) || []
                            ).map((p, pi) => (
                              <Badge key={pi} bg="primary">
                                {p}
                              </Badge>
                            ))}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </Table>
              ) : item.accesses.length > 0 ? (
                <div className="d-flex flex-wrap gap-1">
                  {item.accesses.map((a, ai) => (
                    <Badge key={ai} bg="primary">
                      {a?.label}
                    </Badge>
                  ))}
                </div>
              ) : (
                <Empty />
              )}
            </InfoRow>
          )}

          {/* Users / Groups / Roles - ONLY SHOW IF HAS PRINCIPAL */}
          {hasPrincipal && (
            <InfoRow label="Users / Groups / Roles">
              <PrincipalRow icon={userColourIcon} alt="Users" items={users} />
              <PrincipalRow
                icon={groupColourIcon}
                alt="Groups"
                items={groups}
              />
              <PrincipalRow icon={roleColourIcon} alt="Roles" items={roles} />
            </InfoRow>
          )}

          {/* Rule conditions - ONLY SHOW IF HAS CONDITIONS */}
          {hasConditions && (
            <InfoRow label="Rule Conditions">
              {item?.conditions && !isEmpty(item?.conditions) ? (
                <Table bordered size="sm" className="mb-0 align-middle">
                  <thead className="table-light">
                    <tr>
                      <th className="small p-2">Condition</th>
                      <th className="small p-2">Value</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.keys(item.conditions).map((k) => {
                      const v = item.conditions[k];
                      if (v === "" || v == null) return null;
                      const def = find(
                        selectedServiceComponentDef?.policyConditions,
                        (m) => m.name === k
                      );
                      return (
                        <tr key={k}>
                          <td style={{ minWidth: "210px" }}>
                            {getPolicyConditionDisplayLbl(def?.label)}
                          </td>
                          <td className="text-word-break">
                            {isArray(v) ? v.join(", ") : String(v)}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </Table>
              ) : (
                <Empty />
              )}
            </InfoRow>
          )}

          {/* Masking option - ONLY SHOW IF HAS DATA MASK */}
          {hasDataMask && (
            <InfoRow label="Masking Option" isLast={true}>
              <Badge bg="warning" text="dark">
                {item.dataMaskInfo.label || item.dataMaskInfo.value}
              </Badge>
              {item.dataMaskInfo.valueExpr && (
                <code className="ms-2 text-muted small">
                  {item.dataMaskInfo.valueExpr}
                </code>
              )}
            </InfoRow>
          )}

          {/* Row filter - ONLY SHOW IF HAS ROW FILTER */}
          {hasRowFilter && (
            <InfoRow label="Row Level Filter" isLast={true}>
              <code className="bg-light border rounded px-2 py-1 small d-inline-block">
                {String(item.rowFilterInfo)}
              </code>
            </InfoRow>
          )}

          {/* Delegate Admin - ONLY SHOW IF HAS DELEGATE ADMIN */}
          {hasDelegateAdmin && (
            <InfoRow label="Delegate Admin" isLast={true}>
              <Badge bg={item?.delegateAdmin ? "success" : "secondary"}>
                {item?.delegateAdmin ? "Yes" : "No"}
              </Badge>
            </InfoRow>
          )}
        </div>
      </div>
    );
  };

  // ── rule group (e.g. "Allow Rules") ──────────────────────────────────────
  const RuleGroup = ({ attrName, title }) => {
    const items = values[attrName] || [];
    const valid = items.filter((it) => it && !isEmpty(it));
    const ruleLabel = title.replace(/ Rules$/, " Rule");
    return (
      <div className="mb-4">
        <p
          className="fw-bold m-0"
          style={{
            borderLeft: "4px solid #0d6efd",
            paddingLeft: "0.6rem",
            width: "226px",
            display: valid.length === 0 ? "inline-block" : "block"
          }}
        >
          {title}
        </p>
        {valid.length === 0 ? (
          <p className="text-muted fst-italic ms-2 mb-0 d-inline-block">
            No rules configured
          </p>
        ) : (
          valid.map((item, idx) => (
            <RuleCard
              key={idx}
              item={item}
              idx={idx}
              ruleLabel={ruleLabel}
              className="d-block"
            />
          ))
        )}
      </div>
    );
  };

  // ── access rules block ────────────────────────────────────────────────────
  const renderAccessRules = () => {
    if (isMasking)
      return <RuleGroup attrName="dataMaskPolicyItems" title="Mask Rules" />;
    if (isRowFilter)
      return (
        <RuleGroup attrName="rowFilterPolicyItems" title="Row Filter Rules" />
      );

    return (
      <>
        <RuleGroup attrName="policyItems" title="Allow Rules" />
        <RuleGroup
          attrName="allowExceptions"
          title="Exclude from Allow Rules"
        />

        {/* Deny All */}
        <div
          className="d-flex align-items-center gap-2 rounded px-3 py-2 mb-4"
          style={{
            background: "#fff8e1",
            border: "1px solid #ffe082",
            fontSize: "0.85rem"
          }}
        >
          <span style={{ width: 190 }}>Deny All Other Accesses</span>
          <StatusBadge
            on={values.isDenyAllElse}
            onLabel="True"
            offLabel="False"
          />
        </div>

        {enableDenyAndExceptions && !values.isDenyAllElse && (
          <>
            <RuleGroup attrName="denyPolicyItems" title="Deny Rules" />
            <RuleGroup
              attrName="denyExceptions"
              title="Exclude from Deny Rules"
            />
          </>
        )}
      </>
    );
  };

  // ── policy conditions helper ──────────────────────────────────────────────
  const renderPolicyConditions = () => {
    if (!values.conditions || isEmpty(values.conditions)) return null;
    const entries = Object.keys(values.conditions).filter(
      (k) => values.conditions[k] !== "" && values.conditions[k] != null
    );
    if (entries.length === 0) return null;
    return (
      <Table bordered size="sm" className="mb-0 align-middle">
        <thead className="table-light">
          <tr>
            <th className="small p-2">Condition</th>
            <th className="small p-2">Value</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((k) => {
            const v = values.conditions[k];
            const def = find(
              selectedServiceComponentDef?.policyConditions,
              (m) => m.name === k
            );
            return (
              <tr key={k}>
                <td style={{ minWidth: "210px" }}>
                  {getPolicyConditionDisplayLbl(def?.label)}
                </td>
                <td className="text-word-break">
                  {isArray(v) ? v.join(", ") : String(v)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </Table>
    );
  };

  // ── validity schedules helper ──────────────────────────────────────────────
  const renderValidity = () => {
    const schedules = (values.validitySchedules || []).filter((s) => s != null);
    if (schedules.length === 0) return null;
    return (
      <Table bordered size="sm" className="mb-0 align-middle">
        <thead className="table-light">
          <tr>
            <th>Start Date</th>
            <th>End Date</th>
            <th>Time Zone</th>
          </tr>
        </thead>
        <tbody>
          {schedules.map((s, i) => (
            <tr key={i}>
              <td className="small">
                {s?.startTime
                  ? moment(s.startTime).format("YYYY/MM/DD HH:mm:ss")
                  : "—"}
              </td>
              <td className="small">
                {s?.endTime
                  ? moment(s.endTime).format("YYYY/MM/DD HH:mm:ss")
                  : "—"}
              </td>
              <td className="small">{s?.timeZone?.text || "—"}</td>
            </tr>
          ))}
        </tbody>
      </Table>
    );
  };

  const policyConditionsContent = renderPolicyConditions();
  const validityContent = renderValidity();

  return (
    <div>
      <Accordion
        defaultActiveKey={["1", "2", "3", "4"]}
        alwaysOpen
        className="policy-permission-items-accordion"
      >
        {/* Step 1 — Service */}
        <Accordion.Item eventKey="1">
          <Accordion.Header>
            <SectionHeader title="Service" />
          </Accordion.Header>
          <Accordion.Body>
            <InfoRow label="Service Type">
              {values.serviceType?.label || <Empty />}
            </InfoRow>
            <InfoRow label="Service Name">
              {values.serviceName?.label || values.serviceName || <Empty />}
            </InfoRow>
            <InfoRow label="Security Zone">
              {values.zoneName?.label || <Empty />}
            </InfoRow>
            <InfoRow label="Policy Type" isLast={true}>
              <Badge
                bg={isMasking ? "warning" : isRowFilter ? "info" : "primary"}
                text={isMasking ? "dark" : undefined}
              >
                {policyTypeLabel}
              </Badge>
            </InfoRow>
          </Accordion.Body>
        </Accordion.Item>

        {/* Step 2 — Resources */}
        <Accordion.Item eventKey="2">
          <Accordion.Header>
            <SectionHeader title="Policy Resources" />
          </Accordion.Header>
          <Accordion.Body>{renderResources()}</Accordion.Body>
        </Accordion.Item>

        {/* Step 3 — Access Rules */}
        <Accordion.Item eventKey="3">
          <Accordion.Header>
            <SectionHeader title="Access Rules" />
          </Accordion.Header>
          <Accordion.Body>{renderAccessRules()}</Accordion.Body>
        </Accordion.Item>

        {/* Step 4 — Policy Details */}
        <Accordion.Item eventKey="4">
          <Accordion.Header>
            <SectionHeader title="Policy Details" />
          </Accordion.Header>
          <Accordion.Body>
            <InfoRow label="Policy Name">
              {values.policyName || <Empty />}
            </InfoRow>

            <InfoRow label="Policy Labels">
              {values.policyLabels?.length > 0 ? (
                <div className="d-flex flex-wrap gap-1">
                  {values.policyLabels.map((l, i) => (
                    <Badge key={i} bg="secondary">
                      {l.label || l.value || l}
                    </Badge>
                  ))}
                </div>
              ) : (
                <Empty />
              )}
            </InfoRow>

            {values.description && (
              <InfoRow label="Description">{values.description}</InfoRow>
            )}

            <InfoRow label="Policy Status">
              <StatusBadge
                on={values.isEnabled !== false}
                onLabel="Enabled"
                offLabel="Disabled"
              />
            </InfoRow>

            <InfoRow label="Policy Priority">
              <Badge
                bg={values.policyPriority === 1 ? "warning" : "info"}
                text={values.policyPriority === 1 ? "dark" : undefined}
              >
                {values.policyPriority === 1 ? "Override" : "Normal"}
              </Badge>
            </InfoRow>

            {validityContent && (
              <InfoRow label="Policy Validity">{validityContent}</InfoRow>
            )}

            {policyConditionsContent && (
              <InfoRow label="Policy Conditions">
                {policyConditionsContent}
              </InfoRow>
            )}

            <InfoRow label="Audit Logging" isLast={true}>
              <StatusBadge
                on={values.isAuditEnabled !== false}
                onLabel="Enabled"
                offLabel="Disabled"
              />
            </InfoRow>
          </Accordion.Body>
        </Accordion.Item>
      </Accordion>
    </div>
  );
};

export default ReviewConfirmStep;
