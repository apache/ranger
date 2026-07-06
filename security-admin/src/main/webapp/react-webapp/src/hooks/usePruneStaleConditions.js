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

import { useEffect } from "react";
import { isArray, find, isEqual } from "lodash";
import {
  getSelectedAccessTypesForRow,
  getAllowedActionMatchesForCondition,
  isPerRowCondition,
  parseConditionUiHint
} from "Utils/policyConditionUtils";

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
      actionMatches: item?.conditions?.["action-matches"] ?? null
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
  actionReqsMap
}) => {
  const serializedPruneDeps = serializePruneDeps(formValues, attrName);

  useEffect(() => {
    const items = formValues?.[attrName];
    if (!items || !isArray(items)) {
      return;
    }

    let hasChanges = false;
    const newItems = [...items];

    newItems.forEach((item, index) => {
      if (!item.conditions) {
        return;
      }

      const accesses = getSelectedAccessTypesForRow(formValues, attrName, index);
      if (accesses.length === 0) {
        const actionMatches = item.conditions?.["action-matches"];
        if (Array.isArray(actionMatches) && actionMatches.length > 0) {
          const newConditions = { ...item.conditions };
          delete newConditions["action-matches"];
          newItems[index] = { ...item, conditions: newConditions };
          hasChanges = true;
        }
        return;
      }

      const actionFilterContext = {
        selectedAccessTypes: accesses,
        leafResourceTypes,
        accessTypeDefs: serviceCompDetails?.accessTypes
      };

      let itemChanged = false;
      const newConditions = { ...item.conditions };

      for (const conditionName in newConditions) {
        if (!isPerRowCondition(conditionName)) {
          continue;
        }

        const conditionDef = find(conditionDefVal, { name: conditionName });
        if (!conditionDef) {
          continue;
        }

        const uiHintVal = parseConditionUiHint(conditionDef.uiHint);

        if (
          uiHintVal?.isMultiValue &&
          Array.isArray(newConditions[conditionName])
        ) {
          const current = newConditions[conditionName];
          const { prunedSelection } = getAllowedActionMatchesForCondition({
            conditionName,
            actionFilterContext,
            actionReqsMap,
            servicedefName: serviceCompDetails?.name,
            uiHintAttb: uiHintVal,
            currentSelection: current
          });

          if (!isEqual(current, prunedSelection)) {
            if (prunedSelection && prunedSelection.length > 0) {
              newConditions[conditionName] = prunedSelection;
            } else {
              delete newConditions[conditionName];
            }
            itemChanged = true;
            hasChanges = true;
          }
        }
      }

      if (itemChanged) {
        newItems[index] = { ...item, conditions: newConditions };
      }
    });

    if (hasChanges) {
      form.change(attrName, newItems);
    }
  }, [
    serializedPruneDeps,
    leafResourceTypes,
    actionReqsMap,
    attrName,
    form,
    conditionDefVal,
    serviceCompDetails
  ]);
};
