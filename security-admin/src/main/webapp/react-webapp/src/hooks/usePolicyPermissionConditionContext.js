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

import { useMemo } from "react";
import { groupBy } from "lodash";
import { policyConditionUpdatedJSON } from "Utils/XAUtils";
import {
  buildActionReqsMapFromConditionDef,
  getResourceSelectionSignature,
  getSelectedLeafResourceTypes
} from "Utils/policyConditionUtils";
import { usePruneStaleConditions } from "Hooks/usePruneStaleConditions";

/**
 * Shared condition/action-matches setup for permission row components.
 * Memoizes condition defs, action requirements, and leaf resource types,
 * then runs background action-matches prune sync.
 */
export const usePolicyPermissionConditionContext = ({
  serviceCompDetails,
  formValues,
  attrName,
  form,
  enableResourcePruneDefer = false
}) => {
  const conditionDefVal = useMemo(
    () => policyConditionUpdatedJSON(serviceCompDetails?.policyConditions),
    [serviceCompDetails?.policyConditions]
  );

  const actionReqsMap = useMemo(
    () => buildActionReqsMapFromConditionDef(conditionDefVal),
    [conditionDefVal]
  );

  const grpResourcesKeys = useMemo(() => {
    const { resources = [] } = serviceCompDetails || {};
    const grpResources = groupBy(resources, "level");
    let keys = [];
    for (const resourceKey in grpResources) {
      keys.push(+resourceKey);
    }
    return keys.sort((a, b) => a - b);
  }, [serviceCompDetails?.resources]);

  const resourceSelectionSignature = getResourceSelectionSignature(
    grpResourcesKeys,
    formValues
  );

  const leafResourceTypes = useMemo(() => {
    return getSelectedLeafResourceTypes(serviceCompDetails, formValues);
  }, [serviceCompDetails, resourceSelectionSignature]);

  usePruneStaleConditions({
    formValues,
    attrName,
    form,
    leafResourceTypes,
    serviceCompDetails,
    conditionDefVal,
    actionReqsMap,
    enableResourcePruneDefer
  });

  return {
    conditionDefVal,
    actionReqsMap,
    leafResourceTypes,
    grpResourcesKeys
  };
};
