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

export const INITIAL_STATE = (initialArg) => {
  return {
    loader: true,
    contentLoader: true,
    searchFilterParams: [],
    defaultSearchFilterParams: [],
    tableListingData: [],
    totalCount: 0,
    pageCount:
      initialArg.navigateState && initialArg.navigateState.showLastPage
        ? initialArg.navigateState.addPageData.totalPage
        : 0,
    currentPageIndex:
      initialArg.navigateState && initialArg.navigateState.showLastPage
        ? initialArg.navigateState.addPageData.totalPage - 1
        : 0,
    currentPageSize:
      initialArg.navigateState && initialArg.navigateState.showLastPage
        ? initialArg.navigateState.addPageData.pageSize
        : 25,
    resetPage: { page: 0 },
    tablePageData: {
      totalPage: 0,
      pageRecords: 0,
      pageSize: 25
    },
    refreshTableData: moment.now(),
    showDeleteModal: false,
    showSyncDetailsModal: false,
    syncDetailsData: {},
    showGroupUsersModal: false,
    groupData: { id: "", name: "" },
    blockUi: false
  };
};

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
        totalCount: action.totalCount,
        pageCount: action.pageCount,
        currentPageIndex: action.currentPageIndex,
        currentPageSize: action.currentPageSize,
        resetPage: action.resetPage,
        tablePageData: action.tablePageData
      };
    case ACTIONS.SHOW_DELETE_MODAL:
      return {
        ...state,
        showDeleteModal: action.showDeleteModal
      };
    case ACTIONS.SHOW_SYNC_DETAILS_MODAL:
      return {
        ...state,
        showSyncDetailsModal: action.showSyncDetailsModal,
        syncDetailsData: action.syncDetailsData
      };
    case ACTIONS.SHOW_GROUP_USERS_MODAL:
      return {
        ...state,
        showGroupUsersModal: action.showGroupUsersModal,
        groupData: action.groupData
      };
    case ACTIONS.SET_BLOCK_UI:
      return {
        ...state,
        blockUi: action.blockUi
      };
    default:
      return state;
  }
};
