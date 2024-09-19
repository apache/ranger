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

import moment from "moment-timezone";
import { ACTIONS } from "./action";

// Shared initial state
export const INITIAL_STATE = {
  loader: true,
  contentLoader: true,
  searchFilterParams: [],
  defaultSearchFilterParams: [],
  tableListingData: [],
  pageCount: 0,
  entries: {},
  resetPage: { page: 0 },
  refreshTableData: moment.now()
};

// Specific initial states
export const ACCESS_INITIAL_STATE = {
  ...INITIAL_STATE,
  securityZones: [],
  showRowModal: false,
  rowData: {},
  showPolicyModal: false,
  policyData: null
};

export const ADMIN_INITIAL_STATE = {
  ...INITIAL_STATE,
  showRowModal: false,
  rowData: {},
  showSessionModal: false,
  sessionId: undefined
};

export const LOGIN_SESSIONS_INITIAL_STATE = {
  ...INITIAL_STATE,
  showSessionModal: false,
  sessionId: undefined
};

export const PLUGINS_INITIAL_STATE = {
  ...INITIAL_STATE
};

export const PLUGIN_STATUS_INITIAL_STATE = {
  ...INITIAL_STATE
};

export const USERSYNC_INITIAL_STATE = {
  ...INITIAL_STATE,
  showSyncTableModal: false,
  syncTableData: {}
};

// Shared reducer function
export const reducer = (state, action) => {
  switch (action.type) {
    case ACTIONS.SET_TABLE_LOADER:
      return {
        ...state,
        loader: action.loader
      };
    case ACTIONS.SET_CONTENT_LOADER:
      return {
        ...state,
        contentLoader: action.contentLoader
      };
    case ACTIONS.SET_SEARCH_FILTER_PARAMS:
      return {
        ...state,
        searchFilterParams: action.searchFilterParams,
        refreshTableData: action.refreshTableData
      };
    case ACTIONS.SET_DEFAULT_SEARCH_FILTER_PARAMS:
      return {
        ...state,
        defaultSearchFilterParams: action.defaultSearchFilterParams
      };
    case ACTIONS.SET_TABLE_DATA:
      return {
        ...state,
        tableListingData: action.tableListingData,
        entries: action.entries,
        pageCount: action.pageCount,
        resetPage: action.resetPage
      };
    case ACTIONS.SHOW_POLICY_MODAL:
      return {
        ...state,
        showPolicyModal: action.showPolicyModal,
        policyData: action.policyData
      };
    case ACTIONS.SHOW_ROW_MODAL:
      return {
        ...state,
        showRowModal: action.showRowModal,
        rowData: action.rowData
      };
    case ACTIONS.SHOW_SESSION_MODAL:
      return {
        ...state,
        showSessionModal: action.showSessionModal,
        sessionId: action.sessionId
      };
    case ACTIONS.SHOW_SYNC_TABLE_MODAL:
      return {
        ...state,
        showSyncTableModal: action.showSyncTableModal,
        syncTableData: action.syncTableData
      };
    case ACTIONS.SET_SECURITY_ZONES:
      return {
        ...state,
        securityZones: action.securityZones
      };
    default:
      return state;
  }
};
