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

import React, { forwardRef, useEffect, useRef } from "react";
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
import { isEmpty } from "lodash";

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
      localStorageVal = JSON.parse(
        localStorage.getItem("showHideTableCol")
      ).bigData;
    }
    let filterColVal = !isEmpty(localStorageVal)
      ? localStorageVal
          .filter((obj) => obj.renderable == false)
          .map((r) => r.name)
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
    getToggleHideAllColumnsProps,
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

  useEffect(() => {
    fetchData({ pageIndex, pageSize, gotoPage, sortBy });
  }, [fetchData, pageIndex, pageSize, gotoPage, !clientSideSorting && sortBy]);

  useEffect(() => {
    if (rowSelectOp) {
      rowSelectOp.selectedRows.current = selectedFlatRows;
    }
  }, [selectedFlatRows]);

  let columnShowHide = [];
  return (
    // apply the table props
    <>
      {columnHide && (
        <div className="text-right mb-2 mt-n5">
          <DropdownButton
            className="p-0"
            menuAlign="right"
            as={ButtonGroup}
            size="sm"
            id="dropdown-variants-info"
            variant="info"
            title="Columns"
          >
            <ul className="list-group">
              {allColumns.map((column, index) => {
                columnShowHide.push({
                  name: column.id,
                  renderable: column.isVisible
                });

                localStorage.setItem(
                  "showHideTableCol",
                  JSON.stringify({ bigData: columnShowHide })
                );
                return (
                  <li
                    className="column-list text-truncate"
                    key={`col-${index}`}
                  >
                    <label>
                      <input
                        className="mr-1"
                        type="checkbox"
                        {...column.getToggleHiddenProps()}
                      />

                      {column.Header}
                    </label>
                  </li>
                );
              })}
            </ul>
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
                            "No data to show!!"
                          </span>
                        </center>
                      </td>
                    </tr>
                  </tbody>
                ) : (
                  <tbody {...getTableBodyProps()}>
                    {rows.map((row, index) => {
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
          {totalCount > 25 && (
            <div className="row mt-2">
              <div className="col-md-12 m-b-sm">
                <div className="text-center">
                  <button
                    title="First"
                    onClick={() => gotoPage(0)}
                    disabled={!canPreviousPage}
                    className="pagination-btn-first btn btn-outline-dark btn-sm mr-1"
                  >
                    {"<<"}
                  </button>
                  <button
                    title="Previous"
                    onClick={() => previousPage()}
                    disabled={!canPreviousPage}
                    className="pagination-btn-previous btn btn-outline-dark btn-sm"
                  >
                    {"< "}{" "}
                  </button>
                  <span className="mr-1">
                    <span className="mr-1"> </span>
                    Page{" "}
                    <strong>
                      {pageIndex + 1} of {pageOptions.length}
                    </strong>{" "}
                  </span>
                  <span className="mr-1"> | </span>
                  Go to page:{" "}
                  <input
                    className="pagination-input"
                    type="number"
                    defaultValue={pageIndex + 1}
                    onChange={(e) => {
                      const page = e.target.value
                        ? Number(e.target.value) - 1
                        : 0;
                      gotoPage(page);
                    }}
                  />
                  <span className="mr-1"> </span>
                  <span>
                    <select
                      className="select-pagesize"
                      value={pageSize}
                      onChange={(e) => {
                        gotoPage(0);
                        setPageSize(Number(e.target.value));
                      }}
                    >
                      {[25, 50, 75, 100].map((pageSize) => (
                        <option key={pageSize} value={pageSize}>
                          Show {pageSize}
                        </option>
                      ))}
                    </select>
                  </span>
                  <span className="mr-1"> </span>
                  <button
                    onClick={() => nextPage()}
                    className="pagination-btn-previous mr-1 btn btn-outline-dark btn-sm lh-1"
                    disabled={!canNextPage}
                  >
                    {">"}
                  </button>
                  <button
                    onClick={() => gotoPage(pageCount - 1)}
                    className="pagination-btn-last btn btn-outline btn-sm"
                    disabled={!canNextPage}
                  >
                    {">>"}
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
