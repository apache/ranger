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

import React, { useState, useRef, useCallback, useEffect } from "react";
import { Row, Col } from "react-bootstrap";
import { Link, useSearchParams } from "react-router-dom";
import XATableLayout from "Components/XATableLayout";
import { isSystemAdmin, isKeyAdmin } from "Utils/XAUtils";
import { MoreLess } from "Components/CommonComponents";
import { isEmpty, reject, find, isUndefined, sortBy } from "lodash";
import { fetchApi } from "Utils/fetchAPI";
import { commonBreadcrumb, parseSearchFilter } from "../../utils/XAUtils";
import StructuredFilter from "../../components/structured-filter/react-typeahead/tokenizer";
import { Loader } from "../../components/CommonComponents";
import CustomBreadcrumb from "../CustomBreadcrumb";

function Permissions() {
  const [permissionslistData, setPermissions] = useState([]);
  const [loader, setLoader] = useState(true);
  const [pageLoader, setPageLoader] = useState(true);
  const [pageCount, setPageCount] = React.useState(0);
  const [totalCount, setTotalCount] = useState(0);
  const fetchIdRef = useRef(0);
  const [isAdminRole] = useState(isSystemAdmin() || isKeyAdmin());
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );

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
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    setPageLoader(false);
  }, [searchParams]);

  const fetchPermissions = useCallback(
    async ({ pageSize, pageIndex }) => {
      setLoader(true);
      let permissionsdata = [];
      let totalCount = 0;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchFilterParams };
      if (fetchId === fetchIdRef.current) {
        params["pageSize"] = pageSize;
        params["startIndex"] = pageIndex * pageSize;
        try {
          const permissionResp = await fetchApi({
            url: "xusers/permission",
            params: params
          });
          permissionsdata = permissionResp.data.vXModuleDef;
          totalCount = permissionResp.data.totalCount;
        } catch (error) {
          console.error(`Error occurred while fetching Group list! ${error}`);
        }
        setPermissions(permissionsdata);
        setTotalCount(totalCount);
        setPageCount(Math.ceil(totalCount / pageSize));
        setLoader(false);
      }
    },
    [searchFilterParams]
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
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam, { replace: true });
  };

  return (
    <>
      <div className="header-wraper">
        <h3 className="wrap-header bold">Permissions</h3>
        <CustomBreadcrumb />
      </div>
      <div className="wrap">
        {pageLoader ? (
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
                  defaultSelected={defaultSearchFilterParams}
                />
              </Col>
            </Row>
            <XATableLayout
              data={permissionslistData}
              columns={
                isAdminRole ? columns : reject(columns, ["Header", "Action"])
              }
              totalCount={totalCount}
              loading={loader}
              fetchData={fetchPermissions}
              pageCount={pageCount}
            />
          </React.Fragment>
        )}
      </div>
    </>
  );
}

export default Permissions;
