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

import React, { forwardRef, useEffect, useState, useRef } from "react";
import {
  useTable,
  usePagination,
  useRowSelect,
  useResizeColumns,
  useFlexLayout,
  useSortBy
} from "react-table";
import { Table, ButtonGroup } from "react-bootstrap";
import DropdownButton from "react-bootstrap/DropdownButton";
import { groupBy, isEmpty, uniqBy } from "lodash";

const IndeterminateCheckbox = forwardRef(
  ({ indeterminate, chkType, ...rest }, ref) => {
    const defaultRef = useRef();
    const resolvedRef = ref || defaultRef;

    useEffect(() => {
      resolvedRef.current.indeterminate = indeterminate;
    }, [resolvedRef, indeterminate]);

    return (
      <>
        <input
          type="checkbox"
          ref={resolvedRef}
          {...rest}
          className={`${
            chkType === "header" ? "tablethcheckbox" : "tabletdcheckbox"
          }`}
        />
      </>
    );
  }
);

function XATableLayout({
  columns,
  loading,
  data,
  fetchData,
  showPagination = true,
  pageCount: controlledPageCount,
  currentpageIndex,
  currentpageSize,
  rowSelectOp,
  columnHide,
  columnSort,
  clientSideSorting,
  columnResizable,
  totalCount,
  defaultSort = [],
  getRowProps = () => ({})
}) {
  const getLocalStorageVal = () => {
    let localStorageVal = [];

    if (localStorage.getItem("showHideTableCol") != null) {
      localStorageVal =
        !isEmpty(localStorage.getItem("showHideTableCol")) &&
        JSON.parse(localStorage.getItem("showHideTableCol"));
    }

    let filterColVal = !isEmpty(localStorageVal[columnHide?.tableName])
      ? localStorageVal[columnHide?.tableName]
          ?.filter((obj) => obj.renderable == false)
          ?.map((r) => r.name)
      : [];

    return filterColVal;
  };

  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    rows,
    prepareRow,
    pageCount,
    gotoPage,
    nextPage,
    previousPage,
    setPageSize,
    canPreviousPage,
    allColumns,
    canNextPage,
    pageOptions,
    state: { pageIndex, pageSize, sortBy },
    selectedFlatRows
  } = useTable(
    {
      columns,
      data,
      initialState: {
        pageIndex: currentpageIndex || 0,
        pageSize: currentpageSize || 25,
        sortBy: defaultSort || [],
        hiddenColumns: getLocalStorageVal()
      },
      manualPagination: true,
      manualSortBy: !clientSideSorting && true,
      disableSortBy: !columnSort,
      pageCount: controlledPageCount,
      autoResetPage: false
    },

    useResizeColumns,
    useFlexLayout,
    useSortBy,
    usePagination,
    useRowSelect,
    (hooks) => {
      hooks.visibleColumns.push((columns) => {
        let cols = [];

        if (rowSelectOp) {
          const selectionCol = {
            id: "selection",

            Header: ({ getToggleAllPageRowsSelectedProps }) => (
              <div>
                <IndeterminateCheckbox
                  {...getToggleAllPageRowsSelectedProps()}
                  chkType="header"
                />
              </div>
            ),

            Cell: ({ row }) => (
              <div className="text-center">
                <IndeterminateCheckbox {...row.getToggleRowSelectedProps()} />
              </div>
            ),
            width: 40
          };
          if (rowSelectOp && rowSelectOp.position === "first") {
            cols.push(selectionCol, ...columns);
          } else {
            cols.push(...columns, selectionCol);
          }
        } else {
          cols = [...columns];
        }
        return cols;
      });
    }
  );

  const currentPageValRef = useRef();
  const [currentPageVal, setCurrentPageVal] = useState("");

  useEffect(() => {
    fetchData({ pageIndex, pageSize, gotoPage, sortBy });
  }, [fetchData, pageIndex, pageSize, gotoPage, !clientSideSorting && sortBy]);

  useEffect(() => {
    if (rowSelectOp) {
      rowSelectOp.selectedRows.current = selectedFlatRows;
    }
  }, [selectedFlatRows]);

  useEffect(() => {
    setCurrentPageVal(pageIndex + 1);
  }, [pageIndex]);

  const validatePageNumber = (pageVal, pageOptions) => {
    let error = "";
    if (!Number.isInteger(Number(currentPageVal))) {
      error = `Please enter a valid value.`;
    } else if (pageVal < 1) {
      error = "Value must be greater than or equal to 1";
    } else if (pageVal > pageOptions.length) {
      error = `Value must be less than or equal to ${pageOptions.length}`;
    }
    return (
      <span className="text-danger position-absolute text-start pagination-error-field">
        {error}
      </span>
    );
  };

  /* For Column Visibility */
  const groupedColumnsData = groupBy(allColumns, "parent[id]");
  delete groupedColumnsData.undefined;
  const allColumnsData = allColumns.map((column) => {
    const columnName = column?.parent
      ? `Group(${column?.parent.id})`
      : `Non-Group(${column?.id})`;
    return {
      columnName,
      columnData: groupedColumnsData[column.parent?.id] || column
    };
  });

  let filterAllColumns = uniqBy(allColumnsData, function (column) {
    return column.columnName;
  });

  let columnShowHide = [];

  return (
    // apply the table props
    <>
      {columnHide?.isVisible && (
        <div className="position-absolute top-0 end-0">
          <DropdownButton
            className="p-0 column-dropdown"
            align="end"
            as={ButtonGroup}
            size="sm"
            id="dropdown-variants-info"
            variant="info"
            title="Columns"
          >
            <div className="column-dropdown-maxheight">
              <ul className="list-group fnt-14">
                {filterAllColumns.map((column, index) => {
                  if (
                    column.columnName == `Non-Group(${column.columnData.id})`
                  ) {
                    columnShowHide.push({
                      name: column.columnData.id,
                      renderable: column.columnData.isVisible
                    });
                  } else {
                    column.columnData.forEach((col) => {
                      columnShowHide.push({
                        name: col.id,
                        renderable: col.isVisible
                      });
                    });
                  }

                  let localStorageColumnData =
                    JSON.parse(localStorage.getItem("showHideTableCol")) || {};

                  let columnData = {
                    ...localStorageColumnData,
                    [columnHide.tableName]: columnShowHide
                  };

                  localStorage.setItem(
                    "showHideTableCol",
                    JSON.stringify(columnData)
                  );

                  return column.columnName ==
                    `Non-Group(${column.columnData.id})` ? (
                    <li
                      className="column-list text-truncate"
                      key={`col-${index}`}
                    >
                      <label
                        title={column.columnData.Header}
                        className="d-flex align-items-center"
                      >
                        <input
                          className="me-1"
                          type="checkbox"
                          {...column.columnData.getToggleHiddenProps()}
                        />
                        {column.columnData.Header}
                      </label>
                    </li>
                  ) : (
                    <li
                      className="column-list text-truncate"
                      key={`col-${index}`}
                    >
                      {column.columnName !== undefined && (
                        <div className="font-weight-bold text-secondary mb-2 fnt-14">
                          {column.columnName !== undefined &&
                            column?.columnData[0]?.parent?.id}
                        </div>
                      )}
                      {column.columnData.map((columns) => (
                        <ul key={columns.id} className="p-0">
                          <li className=" list-unstyled">
                            {" "}
                            <label
                              title={columns.Header}
                              className="d-flex align-items-center"
                            >
                              <input
                                className="me-1"
                                type="checkbox"
                                {...columns.getToggleHiddenProps()}
                              />
                              {columns.Header}
                            </label>
                          </li>
                        </ul>
                      ))}
                    </li>
                  );
                })}
              </ul>
            </div>
          </DropdownButton>
        </div>
      )}

      <div className="row">
        <div className="col-sm-12">
          <div className="table-responsive">
            <Table bordered hover {...getTableProps()}>
              <>
                <thead className="thead-light text-center">
                  {headerGroups.map((headerGroup) => (
                    <tr {...headerGroup.getHeaderGroupProps()}>
                      {headerGroup.headers.map((column) => (
                        <th
                          {...column.getHeaderProps([
                            {
                              className: column.className
                            }
                          ])}
                          {...column.getHeaderProps(
                            column.getSortByToggleProps()
                          )}
                          title={undefined}
                          onClick={() =>
                            columnSort &&
                            column.toggleSortBy &&
                            column.toggleSortBy(!column.isSortedDesc)
                          }
                        >
                          {columnResizable && !column.disableResizing && (
                            <>
                              <div
                                className="fa fa-expand"
                                aria-hidden="true"
                                {...column.getResizerProps([
                                  { className: "resizer" }
                                ])}
                                onClick={(event) => event.stopPropagation()}
                              />
                              <i
                                className="fa fa-expand resizeable-icon"
                                aria-hidden="true"
                                {...column.getResizerProps()}
                              />
                            </>
                          )}

                          {column.render("Header")}
                          {columnSort && column.isSorted && (
                            <span>{column.isSortedDesc ? " ▼" : " ▲"}</span>
                          )}
                        </th>
                      ))}
                    </tr>
                  ))}
                </thead>
                {loading ? (
                  <tbody>
                    <tr>
                      <td>
                        <center>
                          <i className="fa fa-spinner fa-pulse fa-lg fa-fw"></i>
                        </center>
                      </td>
                    </tr>
                  </tbody>
                ) : rows.length === 0 && loading == false ? (
                  <tbody>
                    <tr>
                      <td colSpan={columns.length + 1}>
                        <center>
                          <span className="text-muted" data-cy="tbleDataMsg">
                            &quot;No data to show!!&quot;
                          </span>
                        </center>
                      </td>
                    </tr>
                  </tbody>
                ) : (
                  <tbody {...getTableBodyProps()}>
                    {rows.map((row) => {
                      prepareRow(row);
                      return (
                        <tr
                          {...row.getRowProps(getRowProps(row))}
                          id={row.original.id}
                        >
                          {row.cells.map((cell) => {
                            return (
                              <td {...cell.getCellProps()}>
                                {cell.render("Cell")}
                              </td>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tbody>
                )}
              </>
            </Table>
          </div>
          {showPagination && totalCount > 25 && (
            <div className="row mt-2">
              <div className="col-md-12 m-b-sm">
                <div className="text-center d-flex justify-content-end align-items-center pb-2">
                  <span>
                    Records per page
                    <select
                      className="select-pagesize ms-2"
                      value={pageSize}
                      onChange={(e) => {
                        gotoPage(0);
                        setPageSize(Number(e.target.value));
                        setCurrentPageVal(1);
                        currentPageValRef.current.value = 1;
                      }}
                    >
                      {[25, 50, 75, 100].map((pageSize) => (
                        <option key={pageSize} value={pageSize}>
                          {pageSize}
                        </option>
                      ))}
                    </select>
                  </span>
                  <button
                    title="First"
                    onClick={() => {
                      gotoPage(0);
                      currentPageValRef.current.value = 1;
                      setCurrentPageVal(1);
                    }}
                    disabled={!canPreviousPage}
                    className="pagination-btn-last btn btn-outline-dark btn-sm me-1"
                  >
                    <i
                      className="fa fa-angle-double-left"
                      aria-hidden="true"
                    ></i>
                  </button>
                  <button
                    title="Previous"
                    onClick={() => {
                      currentPageValRef.current.value = pageIndex;
                      previousPage();
                      setCurrentPageVal(pageIndex);
                    }}
                    disabled={!canPreviousPage}
                    className="pagination-btn-previous btn btn-outline-dark btn-sm me-2"
                  >
                    <i className="fa fa-angle-left" aria-hidden="true"></i>
                  </button>
                  Page{" "}
                  <div className="position-relative ms-1">
                    <input
                      className="pagination-input"
                      type="number"
                      ref={currentPageValRef}
                      key={pageIndex + 1}
                      defaultValue={pageIndex + 1}
                      onChange={(e) => {
                        setTimeout(() => {
                          let currPage = e.target.value;
                          setCurrentPageVal(currPage);
                          if (
                            currPage < 1 ||
                            currPage > pageOptions.length ||
                            !Number.isInteger(Number(currPage))
                          ) {
                            return currPage;
                          } else {
                            const page = currPage ? Number(currPage) - 1 : 0;
                            gotoPage(page);
                          }
                        }, 2000);
                      }}
                    />
                    {currentPageVal !== "" &&
                      (currentPageVal < 1 ||
                        currentPageVal > pageOptions.length ||
                        !Number.isInteger(Number(currentPageVal))) &&
                      validatePageNumber(currentPageVal, pageOptions)}
                    <span className="me-1"> </span>
                  </div>
                  <span className="me-1">
                    <span className="me-1"> </span>
                    of {pageOptions.length}
                  </span>
                  <span className="me-1"> </span>
                  <button
                    onClick={() => {
                      currentPageValRef.current.value = pageIndex + 2;
                      nextPage();
                      setCurrentPageVal(pageIndex + 2);
                    }}
                    className="pagination-btn-previous me-1 btn btn-outline-dark btn-sm lh-1"
                    disabled={!canNextPage}
                  >
                    <i className="fa fa-angle-right" aria-hidden="true"></i>
                  </button>
                  <button
                    onClick={() => {
                      gotoPage(pageCount - 1);
                      currentPageValRef.current.value = pageCount;
                      setCurrentPageVal(pageCount);
                    }}
                    className="pagination-btn-last btn btn-outline btn-sm"
                    disabled={!canNextPage}
                  >
                    <i
                      className="fa fa-angle-double-right"
                      aria-hidden="true"
                    ></i>
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
export default XATableLayout;
