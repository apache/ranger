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

import React, { useState, useCallback, useEffect, useReducer } from "react";
import { Row, Col } from "react-bootstrap";
import { Link, useSearchParams, useLocation } from "react-router-dom";
import { isEmpty, reject, find, isUndefined, sortBy } from "lodash";
import moment from "moment-timezone";
import XATableLayout from "Components/XATableLayout";
import { isSystemAdmin, isKeyAdmin, parseSearchFilter } from "Utils/XAUtils";
import { Loader, MoreLess } from "Components/CommonComponents";
import { fetchApi } from "Utils/fetchAPI";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import { ACTIONS } from "./action";
import { reducer, INITIAL_STATE } from "./reducer";

function Permissions() {
  const [state, dispatch] = useReducer(reducer, INITIAL_STATE);

  const location = useLocation();

  const [isAdminRole] = useState(isSystemAdmin() || isKeyAdmin());
  const [searchParams, setSearchParams] = useSearchParams();

  useEffect(() => {
    let searchFilterParam = {};
    let searchParam = {};
    let defaultSearchFilterParam = [];

    // Get Search Filter Params from current search params
    const currentParams = Object.fromEntries([...searchParams]);

    for (const param in currentParams) {
      let searchFilterObj = find(searchFilterOptions, {
        urlLabel: param
      });

      if (!isUndefined(searchFilterObj)) {
        let category = searchFilterObj.category;
        let value = currentParams[param];

        if (searchFilterObj.type == "textoptions") {
          let textOptionObj = find(searchFilterObj.options(), {
            label: value
          });
          value = textOptionObj !== undefined ? textOptionObj.value : value;
        }

        searchFilterParam[category] = value;
        defaultSearchFilterParam.push({
          category: category,
          value: value
        });
      }
    }

    // Updating the states for search params, search filter and default search filter
    setSearchParams({ ...currentParams, ...searchParam }, { replace: true });
    if (
      JSON.stringify(state.searchFilterParams) !==
      JSON.stringify(searchFilterParam)
    ) {
      dispatch({
        type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
        searchFilterParams: searchFilterParam,
        refreshTableData: moment.now()
      });
    }
    dispatch({
      type: ACTIONS.SET_DEFAULT_SEARCH_FILTER_PARAMS,
      defaultSearchFilterParams: defaultSearchFilterParam
    });
    dispatch({ type: ACTIONS.SET_CONTENT_LOADER, contentLoader: false });
  }, [location.search]);

  const fetchPermissions = useCallback(
    async ({ pageSize, pageIndex }) => {
      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: true });

      const params = {
        ...state.searchFilterParams,
        pageSize,
        startIndex: pageIndex * pageSize
      };

      try {
        const response = await fetchApi({
          url: "xusers/permission",
          params: params
        });

        const permissionsData = response.data?.vXModuleDef || [];
        const totalCount = response.data?.totalCount || 0;

        dispatch({
          type: ACTIONS.SET_TABLE_DATA,
          tableListingData: permissionsData,
          pageCount: Math.ceil(totalCount / pageSize),
          totalCount: totalCount
        });
      } catch (error) {
        console.error(
          `Error occurred while fetching users permission ! ${error}`
        );
      }

      dispatch({ type: ACTIONS.SET_TABLE_LOADER, loader: false });
    },
    [state.refreshTableData]
  );

  const columns = React.useMemo(
    () => [
      {
        Header: "Modules",
        accessor: "module",
        Cell: (rawValue) => {
          if (rawValue.value) {
            return isAdminRole ? (
              <Link
                className={`${"text-info"}`}
                to={`/permissions/${rawValue.row.original.id}/edit`}
                title={rawValue.row.original.module}
              >
                {rawValue.row.original.module}
              </Link>
            ) : (
              rawValue.row.original.module
            );
          }
          return "--";
        },
        width: 80
      },
      {
        Header: "Groups",
        accessor: (raw) => {
          const Groups = raw.groupPermList.map((group) => {
            return group.groupName;
          });

          return !isEmpty(Groups) ? (
            <MoreLess data={Groups} />
          ) : (
            <div className="text-center">--</div>
          );
        }
      },
      {
        Header: "Users",
        accessor: (raw) => {
          const Users = raw.userPermList.map((user) => {
            return user.userName;
          });
          return !isEmpty(Users) ? (
            <MoreLess data={Users} />
          ) : (
            <div className="text-center">--</div>
          );
        }
      },
      {
        Header: "Action",
        accessor: "action",
        Cell: (rawValue) => {
          return (
            <div className="text-center">
              <Link
                className="btn btn-sm m-r-5"
                title="Edit"
                to={`/permissions/${rawValue.row.original.id}/edit`}
              >
                <i className="fa-fw fa fa-edit fa-fw fa fa-large"></i>
              </Link>
            </div>
          );
        },
        width: 50
      }
    ],
    []
  );

  const searchFilterOptions = [
    {
      category: "groupName",
      label: "Group Name",
      urlLabel: "groupName",
      type: "text"
    },
    {
      category: "module",
      label: "Module Name",
      urlLabel: "moduleName",
      type: "text"
    },
    {
      category: "userName",
      label: "User Name",
      urlLabel: "userName",
      type: "text"
    }
  ];

  const updateSearchFilter = (filter) => {
    const { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    dispatch({
      type: ACTIONS.SET_SEARCH_FILTER_PARAMS,
      searchFilterParams: searchFilterParam,
      refreshTableData: moment.now()
    });

    setSearchParams(searchParam, { replace: true });
  };

  return (
    <React.Fragment>
      <div className="header-wraper">
        <h3 className="wrap-header bold">Permissions</h3>
      </div>
      <div className="wrap">
        {state.contentLoader ? (
          <Loader />
        ) : (
          <React.Fragment>
            <Row className="mb-4">
              <Col md={9}>
                <StructuredFilter
                  key="permission-listing-search-filter"
                  placeholder="Search for permissions..."
                  options={sortBy(searchFilterOptions, ["label"])}
                  onChange={updateSearchFilter}
                  defaultSelected={state.defaultSearchFilterParams}
                />
              </Col>
            </Row>

            <XATableLayout
              data={state.tableListingData}
              columns={
                isAdminRole ? columns : reject(columns, ["Header", "Action"])
              }
              totalCount={state.totalCount}
              loading={state.loader}
              fetchData={fetchPermissions}
              pageCount={state.pageCount}
            />
          </React.Fragment>
        )}
      </div>
    </React.Fragment>
  );
}

export default Permissions;
