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

/**
 * Keeps policyItems[].conditions["action-matches"] in sync when permissions or
 * resources change. Mounted from PolicyPermissionItem; does not touch ip-range
 * or other per-row conditions.
 */

import { useEffect, useMemo } from "react";
import { isArray, find, isEqual } from "lodash";
import {
  getSelectedAccessTypesForRow,
  getAllowedActionMatchesForCondition,
  shouldDeferActionMatchesPrune,
  isActionMatcherEnabled,
  parseConditionUiHint
} from "Utils/policyConditionUtils";

const ACTION_MATCHES = "action-matches";

/**
 * Stable dependency string: only permission accesses and per-row condition values.
 * Avoids re-running when unrelated form fields (user/group names, etc.) change.
 */
const serializePruneDeps = (formValues, attrName) => {
  const items = formValues?.[attrName];
  if (!Array.isArray(items)) {
    return "[]";
  }
  return JSON.stringify(
    items.map((item, index) => ({
      accesses: getSelectedAccessTypesForRow(formValues, attrName, index),
      actionMatches: item?.conditions?.[ACTION_MATCHES] ?? null
    }))
  );
};

export const usePruneStaleConditions = ({
  formValues,
  attrName,
  form,
  leafResourceTypes,
  serviceCompDetails,
  conditionDefVal,
  actionReqsMap,
  // When true, skip resource-driven prune while multi-resource rows are empty or mid-edit.
  enableResourcePruneDefer = false
}) => {
  const actionMatcherEnabled = isActionMatcherEnabled(
    serviceCompDetails?.options
  );
  const serializedPruneDeps = serializePruneDeps(formValues, attrName);
  const serializedLeafTypes = leafResourceTypes
    ? [...leafResourceTypes].sort().join(",")
    : "";

  const actionMatchesUiHint = useMemo(() => {
    const actionMatchesDef = find(conditionDefVal, { name: ACTION_MATCHES });
    return actionMatchesDef
      ? parseConditionUiHint(actionMatchesDef.uiHint)
      : null;
  }, [conditionDefVal]);

  useEffect(() => {
    if (!actionMatcherEnabled) {
      return;
    }

    const items = formValues?.[attrName];
    if (!items || !isArray(items)) {
      return;
    }

    const deferResourcePrune =
      enableResourcePruneDefer &&
      shouldDeferActionMatchesPrune(serviceCompDetails, formValues);

    const baseActionFilterContext = {
      leafResourceTypes,
      accessTypeDefs: serviceCompDetails?.accessTypes
    };

    const changedRows = [];

    items.forEach((item, index) => {
      if (!item?.conditions) {
        return;
      }

      const accesses = getSelectedAccessTypesForRow(
        formValues,
        attrName,
        index
      );

      // Always sync when permissions are cleared — defer must not block this.
      if (accesses.length === 0) {
        const actionMatches = item.conditions[ACTION_MATCHES];
        if (Array.isArray(actionMatches) && actionMatches.length > 0) {
          const newConditions = { ...item.conditions };
          delete newConditions[ACTION_MATCHES];
          changedRows.push({ index, conditions: newConditions });
        }
        return;
      }

      // Defer only resource-driven prune while a multi-resource row is empty or mid-edit.
      if (deferResourcePrune) {
        return;
      }

      const current = item.conditions[ACTION_MATCHES];
      if (!Array.isArray(current) || current.length === 0) {
        return;
      }

      const { prunedSelection } = getAllowedActionMatchesForCondition({
        conditionName: ACTION_MATCHES,
        actionFilterContext: {
          ...baseActionFilterContext,
          selectedAccessTypes: accesses
        },
        actionReqsMap,
        servicedefName: serviceCompDetails?.name,
        uiHintAttb: actionMatchesUiHint,
        currentSelection: current
      });

      if (!isEqual(current, prunedSelection)) {
        const newConditions = { ...item.conditions };
        if (prunedSelection && prunedSelection.length > 0) {
          newConditions[ACTION_MATCHES] = prunedSelection;
        } else {
          delete newConditions[ACTION_MATCHES];
        }
        changedRows.push({ index, conditions: newConditions });
      }
    });

    changedRows.forEach(({ index, conditions }) => {
      form.change(`${attrName}[${index}].conditions`, conditions);
    });
  }, [serializedPruneDeps, serializedLeafTypes, attrName]);
};
