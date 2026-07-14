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
 * Policy condition helpers for the React policy form.
 *
 * UI data flow (Ozone action-matches example):
 *
 *   PolicyPermissionItem
 *     ├─ getSelectedLeafResourceTypes()  → deepest resource type(s) in the form
 *     ├─ buildActionReqsMapFromConditionDef() → loads ozone.json via registry
 *     ├─ usePruneStaleConditions()       → drops invalid action-matches on change
 *     └─ Editable (type=custom) per row
 *           ├─ CustomCondition popover   → ip-range, _expression, action-matches
 *           └─ getAllowedActionMatchesForCondition()
 *                 └─ getActionMatchesOptions() → filter + sort dropdown
 *
 * Storage split:
 *   - policy.conditions (header modal via PolicyConditionsComp): all defs EXCEPT action-matches
 *   - policyItems[i].conditions (permission row popover): all defs including action-matches
 *
 * action-matches filtering uses uiHint.options (service def) plus actionRequirements
 * (ozone.json keyed by volume | bucket | key) and the row's selected access types.
 */

import { actionRequirementsRegistry } from "./actionRequirements/registry";
import { getUserProfile } from "Utils/appState";

/**
 * True when Ozone action-matches policy conditions are enabled in Ranger Admin.
 * Controlled by ranger.ozone.action.policy.enabled (exposed via user profile configProperties).
 */
export const isOzoneActionPolicyEnabled = () =>
  getUserProfile()?.configProperties?.ozoneActionPolicyEnabled === "true";

/**
 * Filters policy condition definitions for the current feature-flag state.
 */
export const filterPolicyConditionsForFeatureFlags = (policyConditions) => {
  if (!Array.isArray(policyConditions)) {
    return policyConditions;
  }
  if (isOzoneActionPolicyEnabled()) {
    return policyConditions;
  }
  return policyConditions.filter((c) => c?.name !== "action-matches");
};

/**
 * Sorts action options for the dropdown, placing wildcard patterns first
 * for Ozone (e.g. *, Create*, Get*, etc.) followed by concrete actions
 * alphabetically. For other services, sorts all options alphabetically.
 *
 * @param {string[]} options - Raw action option strings from the service def uiHint.
 * @param {string} servicedefName - The service definition name (e.g. "ozone").
 * @returns {string[]} Sorted and trimmed action options.
 */
const sortActionOptions = (options, servicedefName) => {
  if (!Array.isArray(options)) {
    return options;
  }

  const normalized = options
    .map((v) => (typeof v === "string" ? v.trim() : ""))
    .filter((v) => v.length > 0);

  if (servicedefName === "ozone") {
    // Ensure wildcard options are shown first for usability
    const pinnedOrder = ["*", "Create*", "Delete*", "Get*", "List*", "Put*"];
    const pinned = pinnedOrder.filter((v) => normalized.includes(v));
    const pinnedSet = new Set(pinned);
    const rest = normalized
      .filter((v) => !pinnedSet.has(v))
      .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }));

    return [...pinned, ...rest];
  }

  return normalized.sort((a, b) =>
    a.localeCompare(b, undefined, { sensitivity: "base" })
  );
};

const NONE_RESOURCE_VALUE = "none";

/** Trims and filters an array of strings, removing blanks. */
const normalizeStringArray = (values) => {
  if (!Array.isArray(values)) {
    return [];
  }

  return values
    .map((v) => (typeof v === "string" ? v.trim() : ""))
    .filter((v) => v.length > 0);
};

/** Converts a string array to a Set of lowercased, trimmed values. */
const toLowerSet = (values) => {
  const set = new Set();
  for (const v of normalizeStringArray(values)) {
    set.add(v.toLowerCase());
  }
  return set;
};

/**
 * Returns true if every entry in `required` is present in `grantedSet`.
 * An empty or null `required` array is considered satisfied.
 */
const hasAll = (required, grantedSet) => {
  if (!Array.isArray(required) || required.length === 0) {
    return true;
  }
  for (const r of required) {
    if (!grantedSet.has(r)) {
      return false;
    }
  }
  return true;
};

/**
 * Expands a set of selected access types by following impliedGrants chains.
 * For example, selecting "all" expands to include "read", "write", "create", etc.
 * Uses a fixpoint loop to transitively resolve implied grants.
 *
 * @param {string[]} selectedAccessTypes - Currently selected access type names.
 * @param {Array<{name: string, impliedGrants?: string[]}>} accessTypeDefs - Service def access types.
 * @returns {string[]} Expanded set of access type names (lowercased).
 */
const expandImpliedAccessTypes = (selectedAccessTypes, accessTypeDefs) => {
  const selected = toLowerSet(selectedAccessTypes);

  if (!Array.isArray(accessTypeDefs) || accessTypeDefs.length === 0) {
    return [...selected];
  }

  let changed = true;
  while (changed) {
    changed = false;
    for (const def of accessTypeDefs) {
      const name = def?.name;
      const implied = def?.impliedGrants;
      if (!name || !selected.has(String(name).toLowerCase())) {
        continue;
      }
      if (!Array.isArray(implied) || implied.length === 0) {
        continue;
      }
      for (const g of implied) {
        const grant = typeof g === "string" ? g.trim().toLowerCase() : "";
        if (grant && !selected.has(grant)) {
          selected.add(grant);
          changed = true;
        }
      }
    }
  }

  return [...selected];
};

/**
 * Extracts the currently selected access type names for a specific permission row
 * in the policy form. Used to determine which action options should be available.
 *
 * @param {object} formValues - The form state object.
 * @param {string} attrName - The field array attribute name (e.g. "policyItems").
 * @param {number} index - The row index within the field array.
 * @returns {string[]} Selected access type values for that row.
 */
export const getSelectedAccessTypesForRow = (formValues, attrName, index) => {
  const accesses = formValues?.[attrName]?.[index]?.accesses;
  if (!Array.isArray(accesses)) {
    return [];
  }
  return accesses
    .map((a) => (typeof a?.value === "string" ? a.value.trim() : ""))
    .filter((v) => v.length > 0);
};

/**
 * True when the condition is stored on policyItems[].conditions rather than
 * policy.conditions. Today only "action-matches" uses that storage path.
 *
 * Used to exclude action-matches from the policy-level PolicyConditionsComp modal
 * and to scope prune/apply logic. ip-range and _expression still render in the
 * per-row Editable popover (CustomCondition) and are evaluated on the policy item.
 */
export const isPerRowCondition = (name) =>
  name === "action-matches" && isOzoneActionPolicyEnabled();

/**
 * Safely parses a condition definition uiHint JSON string.
 * @returns {object|null} Parsed uiHint object, or null if missing or invalid.
 */
export const parseConditionUiHint = (uiHint) => {
  if (uiHint == null || uiHint === "") {
    return null;
  }
  try {
    const parsed = JSON.parse(uiHint);
    return parsed && typeof parsed === "object" ? parsed : null;
  } catch (e) {
    return null;
  }
};

/** Desired render order for policy conditions in the UI. */
const CONDITION_ORDER = ["ip-range", "_expression", "action-matches"];

/**
 * Sorts policy condition definitions into a consistent render order.
 * Known conditions (ip-range, _expression, action-matches) are placed first
 * in the order defined by CONDITION_ORDER; unknown conditions follow.
 *
 * @param {Array<{name: string}>} conditions - Condition definitions from the service def.
 * @returns {Array} Sorted copy of the conditions array.
 */
export const sortPolicyConditions = (conditions) => {
  if (!Array.isArray(conditions)) {
    return conditions;
  }

  return [...conditions].sort((a, b) => {
    const indexA = CONDITION_ORDER.indexOf(a?.name);
    const indexB = CONDITION_ORDER.indexOf(b?.name);

    const rankA = indexA === -1 ? 999 : indexA;
    const rankB = indexB === -1 ? 999 : indexB;

    return rankA - rankB;
  });
};

/**
 * Determines the deepest (leaf) resource types currently selected in the policy form.
 * For Ozone, this might return {"key"} or {"bucket"} depending on which resource
 * levels the user has filled in. Used to scope which action requirements apply —
 * e.g. key-level actions differ from bucket-level actions.
 *
 * Walks the resource hierarchy (volume → bucket → key) and finds the deepest
 * non-"none" resource the user has selected in each resource block.
 *
 * @param {object} serviceCompDetails - Service definition details with resources array.
 * @param {object} formValues - Form state (may contain additionalResources for multi-resource policies).
 * @returns {Set<string>} Set of leaf resource type names currently in use.
 */
export const getSelectedLeafResourceTypes = (serviceCompDetails, formValues) => {
  const result = new Set();
  const resources = serviceCompDetails?.resources;
  if (!Array.isArray(resources) || resources.length === 0) {
    return result;
  }

  const levels = [...new Set(resources.map((r) => r?.level).filter(Number.isFinite))]
    .sort((a, b) => a - b);

  const blocks = Array.isArray(formValues?.additionalResources)
    ? formValues.additionalResources
    : [formValues];

  for (const block of blocks) {
    if (!block) {
      continue;
    }

    let leaf = null;
    for (const level of levels) {
      const sel = block[`resourceName-${level}`];
      if (!sel) {
        continue;
      }
      if (sel?.value === NONE_RESOURCE_VALUE) {
        break;
      }
      if (typeof sel?.name === "string" && sel.name.trim().length > 0) {
        leaf = sel.name.trim();
      }
    }

    if (leaf) {
      result.add(leaf);
    }
  }

  return result;
};

/**
 * Determines if actionRequirements is nested (keyed by resource type like Ozone)
 * or flat (single-level mapping).
 *
 * Nested example (Ozone): { volume: {ListAllMyBuckets: ["read","list"], ...}, bucket: {...}, key: {...} }
 * Flat example: { open: ["read"], create: ["write"], ... }
 */
const isNestedActionRequirements = (actionRequirements) =>
  Object.values(actionRequirements).some(
    (v) => v && typeof v === "object" && !Array.isArray(v)
  );

/**
 * Builds a case-insensitive lookup map from the original keys of an object.
 * Used to match resource type names regardless of casing.
 * @returns {Map<string, string>} Map from lowercased key to original key.
 */
const buildKeyLookup = (obj) => {
  const map = new Map();
  for (const key of Object.keys(obj)) {
    map.set(key.toLowerCase(), key);
  }
  return map;
};

/**
 * Resolves which action requirements apply given the currently selected leaf resource types.
 *
 * For nested requirements (like Ozone's ozone.json which is keyed by resource type):
 *   - If no leaf types selected: returns all non-role sections (so all actions are visible).
 *   - If only "role" is selected: returns only the role section (which is typically empty).
 *   - Otherwise: returns only the sections matching the selected leaf types.
 *   - Fallback: if no match found, returns all non-role sections.
 *
 * For flat requirements: wraps them under a "*" key (applies regardless of resource type).
 *
 * @param {object} actionRequirements - The action-to-permission mapping from ozone.json.
 * @param {Set<string>} leafResourceTypes - Currently selected leaf resource types.
 * @returns {object} Filtered requirements keyed by resource type (or "*" for flat).
 */
const getActionRequirements = (actionRequirements, leafResourceTypes) => {
  if (!actionRequirements || typeof actionRequirements !== "object") {
    return {};
  }

  if (!isNestedActionRequirements(actionRequirements)) {
    return { "*": actionRequirements };
  }

  const keyLookup = buildKeyLookup(actionRequirements);
  const result = {};

  if (!leafResourceTypes || leafResourceTypes.size === 0) {
    for (const [lowerKey, originalKey] of keyLookup) {
      if (lowerKey !== "role") {
        result[originalKey] = actionRequirements[originalKey];
      }
    }
    return Object.keys(result).length > 0 ? result : actionRequirements;
  }

  const loweredLeaves = new Set(
    [...leafResourceTypes].map((l) => l.trim().toLowerCase())
  );
  const nonRoleKeys = [...keyLookup].filter(([lk]) => lk !== "role");

  const hasNonRole = nonRoleKeys.some(([lk]) => loweredLeaves.has(lk));
  if (!hasNonRole && loweredLeaves.has("role")) {
    const roleOriginal = keyLookup.get("role");
    result.role = roleOriginal ? actionRequirements[roleOriginal] : {};
    return result;
  }

  for (const [lowerKey, originalKey] of nonRoleKeys) {
    if (loweredLeaves.has(lowerKey)) {
      result[originalKey] = actionRequirements[originalKey];
    }
  }

  if (Object.keys(result).length === 0) {
    for (const [, originalKey] of nonRoleKeys) {
      result[originalKey] = actionRequirements[originalKey];
    }
  }

  return result;
};

/**
 * Checks whether a concrete (non-wildcard) action is allowed given the granted
 * access types and the permission requirements from ozone.json.
 *
 * An action is allowed if ANY leaf resource type's requirements for that action
 * are fully satisfied by the granted permission set. This means if PutObject
 * requires ["create", "write"] at the key level, and the user has selected both
 * "create" and "write" permissions, this returns true.
 *
 * @param {string} actionLower - Lowercased concrete action name (e.g. "putobject").
 * @param {Set<string>} grantedAccessTypes - Lowercased set of granted permissions (expanded).
 * @param {Map<string, string[]>[]} normalizedReqsByLeaf - Output of buildNormalizedReqs().
 * @returns {boolean} True if the action's requirements are met.
 */
const isConcreteActionAllowed = (
  actionLower,
  grantedAccessTypes,
  normalizedReqsByLeaf
) => {
  if (!actionLower) {
    return false;
  }

  for (const leafReqs of normalizedReqsByLeaf) {
    const required = leafReqs.get(actionLower);

    if (required && hasAll(required, grantedAccessTypes)) {
      return true;
    }
  }

  return false;
};

/**
 * Pre-builds a normalized (lowercased keys) lookup structure from requirementsByLeaf
 * so that isConcreteActionAllowed can do O(1) lookups instead of linear scans.
 */
const buildNormalizedReqs = (requirementsByLeaf) => {
  const result = [];
  for (const leaf of Object.keys(requirementsByLeaf)) {
    const reqsByAction = requirementsByLeaf[leaf];
    if (!reqsByAction) {
      continue;
    }
    const map = new Map();
    for (const key of Object.keys(reqsByAction)) {
      map.set(key.toLowerCase(), reqsByAction[key]);
    }
    result.push(map);
  }
  return result;
};

/**
 * Filters the full list of action options down to only those that are achievable
 * given the currently selected permissions. This is the core filtering logic that
 * drives the action-matches dropdown.
 *
 * Logic by option type:
 *   - "*" (universal wildcard): shown only if "all" is granted, or if every concrete
 *     action in the requirements is satisfied by the selected permissions.
 *   - "Put*" (prefix wildcard): shown only if ALL concrete actions starting with
 *     that prefix are satisfied (e.g. Put* requires PutObject, PutObjectTagging,
 *     PutBucketAcl all to be achievable).
 *   - "PutObject" (concrete action): shown only if its permission requirements
 *     are met by the selected access types.
 *
 * @param {object} params
 * @param {string[]} params.baseOptions - All possible action options from uiHint.
 * @param {string[]} params.selectedAccessTypes - Currently selected access types for this row.
 * @param {Set<string>} params.leafResourceTypes - Currently selected leaf resource types.
 * @param {Array} params.accessTypeDefs - Service def access type definitions (for implied grants).
 * @param {object} params.actionRequirements - The action-to-permission map (from ozone.json).
 * @returns {string[]} Filtered action options that are achievable with selected permissions.
 *   Returns an empty array when no access types are selected for the row.
 */
const filterActionOptions = ({
  baseOptions,
  selectedAccessTypes,
  leafResourceTypes,
  accessTypeDefs,
  actionRequirements
}) => {
  if (!Array.isArray(baseOptions)) {
    return baseOptions;
  }

  const normalizedBase = normalizeStringArray(baseOptions);
  const selectedExpanded = expandImpliedAccessTypes(
    selectedAccessTypes,
    accessTypeDefs
  );
  const granted = toLowerSet(selectedExpanded);

  if (granted.size === 0) {
    return [];
  }

  if (!actionRequirements || Object.keys(actionRequirements).length === 0) {
    return normalizedBase;
  }

  const requirementsByLeaf = getActionRequirements(
    actionRequirements,
    leafResourceTypes
  );
  const normalizedReqs = buildNormalizedReqs(requirementsByLeaf);
  const scopedToLeaves =
    leafResourceTypes &&
    leafResourceTypes.size > 0 &&
    isNestedActionRequirements(actionRequirements);

  // Collect all concrete action names (lowercased) from the requirements data
  const concreteActionsLower = new Set();
  for (const leafMap of normalizedReqs) {
    for (const key of leafMap.keys()) {
      concreteActionsLower.add(key);
    }
  }

  return normalizedBase.filter((opt) => {
    // Universal wildcard: only show if all concrete actions are achievable
    if (opt === "*") {
      if (granted.has("all")) {
        return true;
      }

      if (concreteActionsLower.size === 0) {
        return false;
      }

      for (const a of concreteActionsLower) {
        if (!isConcreteActionAllowed(a, granted, normalizedReqs)) {
          return false;
        }
      }

      return true;
    }

    // Prefix wildcard (e.g. "Put*"): show only if ALL matching concrete actions are achievable
    if (opt.endsWith("*")) {
      const prefixLower = opt.slice(0, -1).toLowerCase();
      if (!prefixLower) {
        return false;
      }

      const candidates = [...concreteActionsLower].filter((a) =>
        a.startsWith(prefixLower)
      );
      if (candidates.length === 0) {
        return false;
      }

      return candidates.every((a) =>
        isConcreteActionAllowed(a, granted, normalizedReqs)
      );
    }

    // Mid-string wildcards are not supported
    if (opt.includes("*")) {
      return false;
    }

    // Concrete action: check if its requirements are met
    const optLower = opt.toLowerCase();

    if (!concreteActionsLower.has(optLower)) {
      // When scoped to a leaf resource (e.g. key), hide actions that do not apply
      // at that depth. When unscoped, keep uiHint-only options for forward-compat.
      return !scopedToLeaves;
    }

    return isConcreteActionAllowed(optLower, granted, normalizedReqs);
  });
};

/**
 * Entry point for computing the action-matches dropdown options for a permission row.
 * Filters the base options using the current row's access types and resource context,
 * then sorts the result.
 *
 * @param {object} params
 * @param {string} params.servicedefName - Service definition name.
 * @param {string[]} params.baseOptions - All possible action options from uiHint.
 * @param {object} params.actionFilterContext - Contains selectedAccessTypes, leafResourceTypes, accessTypeDefs.
 * @param {object} params.actionRequirements - Action-to-permission map from ozone.json.
 * @returns {string[]} Filtered and sorted action options, or [] when the row has no permissions.
 */
const getActionMatchesOptions = ({
  servicedefName,
  baseOptions,
  actionFilterContext,
  actionRequirements
}) => {
  const selectedAccessTypes = actionFilterContext?.selectedAccessTypes;
  if (!Array.isArray(selectedAccessTypes) || selectedAccessTypes.length === 0) {
    return [];
  }

  const filtered = filterActionOptions({
    baseOptions,
    selectedAccessTypes,
    leafResourceTypes: actionFilterContext.leafResourceTypes,
    accessTypeDefs: actionFilterContext.accessTypeDefs,
    actionRequirements
  });
  return sortActionOptions(filtered, servicedefName);
};

/**
 * Returns a new object with any null or empty string values removed.
 * Useful to prevent React state mutation and to get accurate keys count.
 */
export const getCleanConditions = (conditions) => {
  if (!conditions) {
    return {};
  }
  const cleaned = { ...conditions };
  for (const key in cleaned) {
    if (cleaned[key] == null || cleaned[key] === "") {
      delete cleaned[key];
    }
  }
  return cleaned;
};

/**
 * Removes any previously selected action-matches values that are no longer
 * in the allowed options set. Called when the user changes permissions or
 * resources, potentially invalidating prior action selections.
 *
 * Handles both string arrays and react-select option objects ({label, value}).
 *
 * @param {object} params
 * @param {Array} params.selected - Currently selected values (strings or {label, value} objects).
 * @param {string[]|Set} params.allowedOptions - The currently valid action options.
 * @returns {Array} Filtered selection with only still-valid entries.
 */
export const pruneSelectedActionMatches = ({ selected, allowedOptions }) => {
  if (!Array.isArray(selected)) {
    return selected;
  }
  const allowedSet =
    allowedOptions instanceof Set
      ? allowedOptions
      : new Set(
          (allowedOptions || []).map((a) =>
            typeof a === "string" ? a.trim() : ""
          )
        );

  return selected.filter((o) => {
    // selected might be an array of strings or objects {label, value}
    const v = typeof o === "object" ? o?.value : o;
    const normalized = typeof v === "string" ? v.trim() : "";
    return normalized && allowedSet.has(normalized);
  });
};

/**
 * Shared helper to compute allowed action matches and prune the selection.
 * Used by ConditionRow (popover) and usePruneStaleConditions (background sync).
 *
 * Only action-matches runs getActionMatchesOptions; ip-range uses CreatableSelect
 * when uiHint has no fixed options array.
 *
 * @param {object} params
 * @param {string} params.conditionName - Condition def name (e.g. "action-matches").
 * @param {object} params.actionFilterContext - Row permissions + leaf resources + accessTypeDefs.
 * @param {object} params.actionReqsMap - From buildActionReqsMapFromConditionDef().
 * @param {string} params.servicedefName - Service definition name.
 * @param {object} params.uiHintAttb - Parsed uiHint for this condition.
 * @param {Array} params.currentSelection - Current react-select value for the field.
 * @returns {{ dropdownOptions: Array|null, prunedSelection: Array }}
 */
export const getAllowedActionMatchesForCondition = ({
  conditionName,
  actionFilterContext,
  actionReqsMap,
  servicedefName,
  uiHintAttb,
  currentSelection
}) => {
  const fixedOptions = Array.isArray(uiHintAttb?.options)
    ? uiHintAttb.options
    : null;
  const isActionMatches = isPerRowCondition(conditionName);

  const dropdownOptions = fixedOptions
    ? (isActionMatches
        ? getActionMatchesOptions({
            servicedefName,
            baseOptions: fixedOptions,
            actionFilterContext,
            actionRequirements:
              actionReqsMap[conditionName] || uiHintAttb?.actionRequirements
          })
        : fixedOptions
      ).map((v) => ({ label: v, value: v }))
    : null;

  const allowedValueSet = dropdownOptions
    ? new Set(dropdownOptions.map((o) => o.value))
    : null;

  const prunedSelection =
    allowedValueSet && Array.isArray(currentSelection)
      ? pruneSelectedActionMatches({
          selected: currentSelection,
          allowedOptions: allowedValueSet
        })
      : currentSelection;

  return { dropdownOptions, prunedSelection };
};

/**
 * Pre-builds the action requirements map from the condition definitions.
 * Scans each condition's uiHint for an `actionRequirementsFile` reference
 * (which points to a JSON file in the actionRequirements registry, e.g. "ozone")
 * or an inline `actionRequirements` object.
 *
 * This is memoized in PolicyPermissionItem and Editable so we don't re-parse
 * uiHint JSON on every render.
 *
 * @param {Array<{name: string, uiHint: string}>} conditionDefVal - Condition definitions from service def.
 * @returns {object} Map of condition name → action requirements object.
 */
export const buildActionReqsMapFromConditionDef = (conditionDefVal) => {
  const map = {};
  if (!Array.isArray(conditionDefVal) || conditionDefVal.length === 0) {
    return map;
  }

  conditionDefVal.forEach((m) => {
    const uiHintAttb = parseConditionUiHint(m.uiHint);

    if (uiHintAttb) {
      const fileToLoad = uiHintAttb?.actionRequirementsFile;
      if (fileToLoad && actionRequirementsRegistry[fileToLoad]) {
        map[m.name] = actionRequirementsRegistry[fileToLoad];
      } else if (uiHintAttb?.actionRequirements) {
        map[m.name] = uiHintAttb.actionRequirements;
      }
    }
  });

  return map;
};
