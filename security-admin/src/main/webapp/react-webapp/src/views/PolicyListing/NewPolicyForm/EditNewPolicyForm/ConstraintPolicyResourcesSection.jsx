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

import React, { useEffect, useMemo } from "react";
import { RangerPolicyType } from "Utils/XAEnums";
import {
  getMatchedConstraintDefs,
  getLeafAccessTypeRestrictions,
  pruneConstraintPolicyItemsToMatchedResources
} from "Utils/XAUtils";
import ResourcesStep from "../PolicyCreateStepperStepComp/ResourcesStep";

/**
 * Constraint policy (type 4) resources on edit: wraps shared ResourcesStep and
 * prunes constraint rules when resources change. Create uses ResourcesStep and
 * prunes when leaving the resources step.
 */
export default function ConstraintPolicyResourcesSection({
  values,
  form,
  selectedServiceComponentDef
}) {
  const isConstraintPolicy =
    Number(values?.policyType) ===
    RangerPolicyType.RANGER_CONSTRAINT_POLICY_TYPE.value;

  const constraintOptionKey = useMemo(() => {
    if (!isConstraintPolicy) {
      return "";
    }
    return getMatchedConstraintDefs(selectedServiceComponentDef, values)
      .map((def) => def.name)
      .sort()
      .join(",");
  }, [values, selectedServiceComponentDef]);

  const leafRestrictionKey = useMemo(() => {
    if (!isConstraintPolicy) {
      return "";
    }
    const leaf = getLeafAccessTypeRestrictions(
      values,
      selectedServiceComponentDef
    );
    return leaf?.length > 0 ? [...leaf].sort().join(",") : "";
  }, [values, selectedServiceComponentDef]);

  useEffect(() => {
    if (!isConstraintPolicy) {
      return;
    }
    pruneConstraintPolicyItemsToMatchedResources(
      form,
      selectedServiceComponentDef
    );
  }, [constraintOptionKey, leafRestrictionKey]);

  return (
    <ResourcesStep
      values={values}
      form={form}
      selectedServiceComponentDef={selectedServiceComponentDef}
    />
  );
}
