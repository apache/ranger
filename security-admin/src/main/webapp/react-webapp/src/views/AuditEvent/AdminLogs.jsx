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

import React, { useState, useCallback, useRef, useEffect } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import { Badge, Row, Col } from "react-bootstrap";
import XATableLayout from "Components/XATableLayout";
import { fetchApi } from "Utils/fetchAPI";
import { ClassTypes, enumValueToLabel } from "../../utils/XAEnums";
import dateFormat from "dateformat";
import AdminModal from "./AdminModal";
import { AuditFilterEntries } from "Components/CommonComponents";
import OperationAdminModal from "./OperationAdminModal";
import moment from "moment-timezone";
import { capitalize, startCase, sortBy, toLower } from "lodash";
import StructuredFilter from "../../components/structured-filter/react-typeahead/tokenizer";
import {
  getTableSortBy,
  getTableSortType,
  fetchSearchFilterParams,
  parseSearchFilter,
  serverError,
  currentTimeZone
} from "../../utils/XAUtils";
import { Loader } from "../../components/CommonComponents";

function Admin() {
  const [adminListingData, setAdminLogs] = useState([]);
  const [sessionId, setSessionId] = useState([]);
  const [loader, setLoader] = useState(true);
  const [pageCount, setPageCount] = useState(0);
  const [showmodal, setShowModal] = useState(false);
  const [entries, setEntries] = useState([]);
  const [updateTable, setUpdateTable] = useState(moment.now());
  const [showrowmodal, setShowRowModal] = useState(false);
  const [rowdata, setRowData] = useState([]);
  const fetchIdRef = useRef(0);
  const [contentLoader, setContentLoader] = useState(true);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );
  const [resetPage, setResetpage] = useState({ page: null });
  const handleClose = () => setShowModal(false);
  const handleClosed = () => setShowRowModal(false);
  const navigate = useNavigate();

  const updateSessionId = (id) => {
    navigate(`/reports/audit/admin?sessionId=${id}`);
    setSearchParams({ sessionId: id }, { replace: true });
    setContentLoader(true);
  };

  const rowModal = async (row) => {
    const { original = {} } = row;
    original.objectId;
    setShowRowModal(true);
    setRowData(original);
  };

  useEffect(() => {
    let { searchFilterParam, defaultSearchFilterParam, searchParam } =
      fetchSearchFilterParams("admin", searchParams, searchFilterOptions);

    // Updating the states for search params, search filter, default search filter and localStorage
    setSearchParams(searchParam, { replace: true });
    if (
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
    localStorage.setItem("admin", JSON.stringify(searchParam));
    setContentLoader(false);
  }, [searchParams]);

  const fetchAdminLogsInfo = useCallback(
    async ({ pageSize, pageIndex, sortBy, gotoPage }) => {
      setLoader(true);
      let logsResp = [];
      let adminlogs = [];
      let totalCount = 0;
      const fetchId = ++fetchIdRef.current;
      let params = { ...searchFilterParams };
      if (fetchId === fetchIdRef.current) {
        params["pageSize"] = pageSize;
        params["startIndex"] = pageIndex * pageSize;
        if (sortBy.length > 0) {
          params["sortBy"] = getTableSortBy(sortBy);
          params["sortType"] = getTableSortType(sortBy);
        }
        try {
          logsResp = await fetchApi({
            url: "assets/report",
            params: params
          });
          adminlogs = logsResp.data.vXTrxLogs;
          totalCount = logsResp.data.totalCount;
        } catch (error) {
          serverError(error);
          console.error(`Error occurred while fetching Admin logs! ${error}`);
        }
        setAdminLogs(adminlogs);
        setEntries(logsResp.data);
        setPageCount(Math.ceil(totalCount / pageSize));
        setResetpage({ page: gotoPage });
        setLoader(false);
      }
    },
    [updateTable, searchFilterParams]
  );

  const refreshTable = () => {
    setAdminLogs([]);
    setLoader(true);
    setUpdateTable(moment.now());
  };

  const openModal = (sessionId) => {
    setShowModal(true);
    setSessionId(sessionId);
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Operation",
        accessor: "operation",
        Cell: (rawValue) => {
          let classtype = rawValue.row.original.objectClassType;
          let action = rawValue.row.original.action;
          let objectname = rawValue.row.original.objectName;

          let operation = "";
          let hasAction = [
            "EXPORT JSON",
            "EXPORT EXCEL",
            "EXPORT CSV",
            "IMPORT START",
            "IMPORT END"
          ];
          if (hasAction.includes(action)) {
            if (
              action == "EXPORT JSON" ||
              action == "EXPORT EXCEL" ||
              action == "EXPORT CSV"
            )
              return "Exported policies";
            else return action;
          } else {
            if (
              classtype == ClassTypes.CLASS_TYPE_XA_ASSET.value ||
              classtype == ClassTypes.CLASS_TYPE_RANGER_SERVICE.value
            )
              operation = (
                <span>
                  Service {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (
              classtype == ClassTypes.CLASS_TYPE_XA_RESOURCE.value ||
              classtype == ClassTypes.CLASS_TYPE_RANGER_POLICY.value
            )
              operation = (
                <span>
                  Policy {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_XA_USER.value)
              operation = (
                <span>
                  User {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_XA_GROUP.value)
              operation = (
                <span>
                  Group {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_USER_PROFILE.value)
              operation = (
                <span>
                  User profile {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_PASSWORD_CHANGE.value)
              operation = (
                <span>
                  User profile {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (
              classtype == ClassTypes.CLASS_TYPE_RANGER_SECURITY_ZONE.value
            )
              operation = (
                <span>
                  Security Zone {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_RANGER_ROLE.value)
              operation = (
                <span>
                  Role {action}d <strong>{objectname}</strong>
                </span>
              );
            else if (classtype == ClassTypes.CLASS_TYPE_XA_GROUP_USER.value)
              operation =
                action == "create" ? (
                  <span>
                    User <strong> {objectname} </strong>added to group{" "}
                    <strong>
                      {rawValue?.row?.original?.parentObjectName || " "}
                    </strong>
                  </span>
                ) : (
                  <span>
                    User <strong> {objectname} </strong>removed from group{" "}
                    <strong>
                      {rawValue?.row?.original?.parentObjectName || " "}
                    </strong>
                  </span>
                );
            return <div className="text-truncate">{operation}</div>;
          }
        },
        disableSortBy: true
      },
      {
        Header: "Audit Type",
        accessor: "objectClassType",
        Cell: (rawValue) => {
          let classtype = rawValue.row.original.objectClassType;
          var audittype = enumValueToLabel(ClassTypes, classtype);
          return (
            <div className="text-center">{Object.values(audittype.label)}</div>
          );
        },
        disableSortBy: true
      },
      {
        Header: "User",
        accessor: "owner",
        Cell: (rawValue) => {
          return rawValue.value !== undefined ? (
            <span className="text-center d-block">{rawValue.value}</span>
          ) : (
            <span className="text-center d-block">--</span>
          );
        },
        disableSortBy: true
      },
      {
        Header: `Date ( ${currentTimeZone()} )`,
        accessor: "createDate",
        Cell: (rawValue) => {
          const date = rawValue.value;
          const newdate = dateFormat(date, "mm/dd/yyyy hh:MM:ss TT");
          return <span className="text-center d-block">{newdate}</span>;
        }
      },
      {
        Header: "Actions",
        accessor: "action",
        Cell: (rawValue) => {
          var operation = "";
          if (rawValue.value == "create") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="success">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else if (rawValue.value == "update") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="warning">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else if (rawValue.value == "delete") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="danger">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else if (rawValue.value == "IMPORT START") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="info">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else if (rawValue.value == "IMPORT END") {
            operation = (
              <span className="text-center d-block">
                <Badge bg="info">{capitalize(rawValue.value)}</Badge>
              </span>
            );
          } else {
            operation = (
              <span className="text-center d-block">
                <Badge bg="secondary">
                  {startCase(toLower(rawValue.value))}
                </Badge>{" "}
              </span>
            );
          }
          return operation;
        },
        disableSortBy: true
      },
      {
        Header: "Session ID",
        accessor: "sessionId",
        Cell: (rawValue) => {
          var sessionId = rawValue.value;
          if (sessionId != undefined) {
            return (
              <span className="text-center d-block">
                <a
                  role="button"
                  className="text-primary"
                  onClick={(e) => {
                    e.stopPropagation();
                    openModal(sessionId);
                  }}
                  data-id={sessionId}
                  data-cy={sessionId}
                  title={sessionId}
                >
                  {sessionId}
                </a>
              </span>
            );
          } else {
            return <span className="text-center d-block">--</span>;
          }
        },
        disableSortBy: true
      }
    ],
    []
  );

  const getDefaultSort = React.useMemo(
    () => [
      {
        id: "id",
        desc: true
      }
    ],
    []
  );

  const updateSearchFilter = (filter) => {
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam, { replace: true });
    localStorage.setItem("admin", JSON.stringify(searchParam));

    if (typeof resetPage?.page === "function") {
      resetPage.page(0);
    }
  };

  const searchFilterOptions = [
    {
      category: "action",
      label: "Actions",
      urlLabel: "actions",
      type: "textoptions",
      options: () => {
        return [
          { value: "create", label: "Create" },
          { value: "update", label: "Update" },
          { value: "delete", label: "Delete" },
          { value: "password change", label: "Password Change" },
          { value: "EXPORT JSON", label: "Export Json" },
          { value: "EXPORT CSV", label: "Export Csv" },
          { value: "EXPORT EXCEL", label: "Export Excel" },
          { value: "IMPORT END", label: "Import End" },
          { value: "IMPORT START", label: "Import Start" },
          { value: "Import Create", label: "Import Create" },
          { value: "Import Delete", label: "Import Delete" }
        ];
      }
    },
    {
      category: "objectClassType",
      label: "Audit Type",
      urlLabel: "auditType",
      type: "textoptions",
      options: () => {
        return [
          { value: "1020", label: "Ranger Policy" },
          { value: "1002", label: "Ranger Group" },
          { value: "1056", label: "Ranger Security Zone" },
          { value: "1030", label: "Ranger Service" },
          { value: "1003", label: "Ranger User" },
          { value: "2", label: "User Profile" }
        ];
      }
    },
    {
      category: "endDate",
      label: "End Date",
      urlLabel: "endDate",
      type: "date"
    },
    {
      category: "sessionId",
      label: "Session ID",
      urlLabel: "sessionId",
      type: "text"
    },
    {
      category: "startDate",
      label: "Start Date",
      urlLabel: "startDate",
      type: "date"
    },
    {
      category: "owner",
      label: "User",
      urlLabel: "user",
      type: "text"
    }
  ];

  return contentLoader ? (
    <Loader />
  ) : (
    <div className="wrap">
      <React.Fragment>
        <Row className="mb-2">
          <Col sm={12}>
            <div className="searchbox-border">
              <StructuredFilter
                key="admin-log-search-filter"
                placeholder="Search for your access logs..."
                options={sortBy(searchFilterOptions, ["label"])}
                onChange={updateSearchFilter}
                defaultSelected={defaultSearchFilterParams}
              />
            </div>
          </Col>
        </Row>

        <AuditFilterEntries entries={entries} refreshTable={refreshTable} />

        <XATableLayout
          data={adminListingData}
          columns={columns}
          fetchData={fetchAdminLogsInfo}
          totalCount={entries && entries.totalCount}
          pageCount={pageCount}
          loading={loader}
          columnSort={true}
          defaultSort={getDefaultSort}
          getRowProps={(row) => ({
            onClick: () => rowModal(row)
          })}
        />

        <AdminModal
          show={showmodal}
          data={sessionId}
          onHide={handleClose}
          updateSessionId={updateSessionId}
        ></AdminModal>

        <OperationAdminModal
          show={showrowmodal}
          data={rowdata}
          onHide={handleClosed}
        ></OperationAdminModal>
      </React.Fragment>
    </div>
  );
}

export default Admin;
