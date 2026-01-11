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

import React, { useReducer, useCallback, useEffect } from "react";
import {
  useNavigate,
  useParams,
  useLocation,
  useSearchParams
} from "react-router-dom";
import { toast } from "react-toastify";
import XATableLayout from "Components/XATableLayout";
import { Row, Col, Button, Modal } from "react-bootstrap";
import { fetchApi } from "Utils/fetchAPI";
import dateFormat from "dateformat";
import moment from "moment-timezone";
import { find, sortBy, isUndefined, isEmpty, reject } from "lodash";
import StructuredFilter from "Components/structured-filter/react-typeahead/tokenizer";
import AsyncSelect from "react-select/async";
import { isKeyAdmin, parseSearchFilter } from "Utils/XAUtils";
import { BlockUi } from "Components/CommonComponents";
import CustomBreadcrumb from "Views/CustomBreadcrumb";

function INITIAL_STATE(initialArg) {
  return {
    loader: true,
    keyData: [],
    onChangeValue:
      initialArg.params.kmsManagePage == "new"
        ? null
        : {
            value: initialArg.params.kmsServiceName,
            label: initialArg.params.kmsServiceName
          },
    deleteShowModal: false,
    editShowModal: false,
    filterData: null,
    pageCount: 0,
    kmsservice: {},
    updateTable: moment.now(),
    currentPageIndex: 0,
    currentPageSize: 25,
    resetPage: { page: 0 },
    totalCount: 0,
    searchFilterParams: [],
    defaultSearchFilterParams: [],
    refreshTableData: moment.now(),
    blockUi: false
  };
}

function reducer(state, action) {
  switch (action.type) {
    case "SET_LOADER":
      return {
        ...state,
        loader: action.loader
      };
    case "SET_SEL_SERVICE":
      return {
        ...state,
        keyData: action.keyDataList,
        pageCount: action.pageCount,
        totalCount: action.totalCount
      };
    case "SET_ONCHANGE_SERVICE":
      return {
        ...state,
        loader: action.loader,
        onChangeValue: action.onChangeValue
      };
    case "SET_DELETE_MODAL":
      return {
        ...state,
        deleteShowModal: action.deleteShowModal,
        filterData: action.filterData
      };
    case "SET_DELETE_MODAL_CLOSE":
      return {
        ...state,
        deleteShowModal: action.deleteShowModal
      };
    case "SET_EDIT_MODAL":
      return {
        ...state,
        editShowModal: action.editShowModal,
        filterData: action.filterData
      };
    case "SET_EDIT_MODAL_CLOSE":
      return {
        ...state,
        editShowModal: action.editShowModal
      };
    case "SET_UPDATE_TABLE":
      return {
        ...state,
        updateTable: action.updateTable
      };
    case "SET_CURRENT_PAGE_INDEX":
      return {
        ...state,
        currentPageIndex: action.currentPageIndex
      };
    case "SET_CURRENT_PAGE_SIZE":
      return {
        ...state,
        currentPageSize: action.currentPageSize
      };
    case "SET_RESET_PAGE":
      return {
        ...state,
        resetPage: action.resetPage
      };
    case "SET_SEARCH_FILTER_PARAMS":
      return {
        ...state,
        searchFilterParams: action.searchFilterParams,
        refreshTableData: action.refreshTableData
      };
    case "SET_DEFAULT_SEARCH_FILTER_PARAMS":
      return {
        ...state,
        defaultSearchFilterParams: action.defaultSearchFilterParams
      };
    case "SET_BLOCK_UI":
      return {
        ...state,
        blockUi: action.blockUi
      };
    default:
      throw new Error();
  }
}

const KeyManager = () => {
  const params = useParams();
  const navigate = useNavigate();
  const { state: navigateState, search } = useLocation();

  let initialArg = { params: params, navigateState: navigateState };
  const [state, dispatch] = useReducer(reducer, initialArg, INITIAL_STATE);

  const [searchParams, setSearchParams] = useSearchParams();

  const {
    loader,
    keyData,
    filterData,
    onChangeValue,
    deleteShowModal,
    editShowModal,
    currentPageIndex,
    currentPageSize,
    pageCount,
    updateTable,
    resetPage,
    totalCount,
    searchFilterParams,
    defaultSearchFilterParams,
    refreshTableData,
    blockUi
  } = state;

  useEffect(() => {
    let searchFilterParam = {};
    let searchParam = {};
    let defaultSearchFilterParam = [];

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
      (!isEmpty(searchFilterParams) || !isEmpty(searchFilterParam)) &&
      JSON.stringify(searchFilterParams) !== JSON.stringify(searchFilterParam)
    ) {
      dispatch({
        type: "SET_SEARCH_FILTER_PARAMS",
        searchFilterParams: searchFilterParam,
        refreshTableData: moment.now()
      });
    }
    dispatch({
      type: "SET_DEFAULT_SEARCH_FILTER_PARAMS",
      defaultSearchFilterParams: defaultSearchFilterParam
    });
  }, [search]);

  const fetchServices = async (inputValue) => {
    const allParams = {
      serviceType: "kms",
      ...(inputValue && { serviceNamePrefix: inputValue })
    };
    let servicesData = [];
    let serviceOptions = [];

    try {
      const servicesResp = await fetchApi({
        url: "public/v2/api/service-headers",
        params: allParams
      });
      servicesData = servicesResp?.data || [];
    } catch (error) {
      let errorMsg = `Error occurred while fetching Services!`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
      console.error(`Error occurred while fetching services : ${error}`);
    }

    serviceOptions = servicesData.map((obj) => ({
      value: obj.name,
      label: obj.name
    }));

    return serviceOptions;
  };

  const selectOnchange = (e) => {
    dispatch({
      type: "SET_ONCHANGE_SERVICE",
      loader: false,
      onChangeValue: e
    });
  };

  const confirmKeyDelete = () => {
    handleKeyDelete();
    dispatch({
      type: "SET_DELETE_MODAL",
      deleteShowModal: false
    });
  };

  const deleteModal = (name) => {
    dispatch({
      type: "SET_DELETE_MODAL",
      deleteShowModal: true,
      filterData: name
    });
  };

  const editModal = (name) => {
    dispatch({
      type: "SET_EDIT_MODAL",
      editShowModal: true,
      filterData: name
    });
  };

  const closeEditModal = () => {
    dispatch({
      type: "SET_EDIT_MODAL_CLOSE",
      editShowModal: false
    });
  };

  const confirmKeyEdit = () => {
    handleKeyEdit();
    dispatch({
      type: "SET_EDIT_MODAL_CLOSE",
      editShowModal: false
    });
  };

  const handleKeyEdit = useCallback(async () => {
    let keyEdit = {};
    keyEdit.name = filterData ? filterData : "";
    try {
      dispatch({ type: "SET_BLOCK_UI", blockUi: true });
      await fetchApi({
        url: `/keys/key`,
        method: "PUT",
        params: { provider: onChangeValue ? onChangeValue.label : "" },
        data: keyEdit
      });
      dispatch({ type: "SET_BLOCK_UI", blockUi: false });
      toast.success(`Success! Key rollover successfully`);
      dispatch({
        type: "SET_UPDATE_TABLE",
        updateTable: moment.now()
      });
    } catch (error) {
      dispatch({ type: "SET_BLOCK_UI", blockUi: false });
      let errorMsg = `Error occurred during editing key` + "\n";
      if (error?.response?.data?.msgDesc) {
        errorMsg = "Error! " + error.response.data.msgDesc + "\n";
      }
      toast.error(errorMsg);
      console.error(`Error occurred during editing key : ${error}`);
    }
  }, [filterData]);

  const handleKeyDelete = useCallback(async () => {
    try {
      dispatch({ type: "SET_BLOCK_UI", blockUi: true });
      await fetchApi({
        url: `/keys/key/${filterData}`,
        method: "DELETE",
        params: { provider: onChangeValue ? onChangeValue.label : "" }
      });
      dispatch({ type: "SET_BLOCK_UI", blockUi: false });
      toast.success(`Success! Key deleted successfully`);
      if (keyData.length == 1 && currentPageIndex > 1) {
        if (typeof resetPage?.page === "function") {
          resetPage.page(0);
        }
      } else {
        dispatch({
          type: "SET_UPDATE_TABLE",
          updateTable: moment.now()
        });
      }
    } catch (error) {
      dispatch({ type: "SET_BLOCK_UI", blockUi: false });
      let errorMsg = "";
      if (error?.response?.data?.msgDesc) {
        errorMsg = toast.error("Error! " + error.response.data.msgDesc + "\n");
      } else {
        errorMsg = `Error occurred during deleting key` + "\n";
      }
      console.error(errorMsg);
    }
  }, [filterData]);

  const closeModal = () => {
    dispatch({
      type: "SET_DELETE_MODAL_CLOSE",
      deleteShowModal: false
    });
  };

  const selectServices = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      dispatch({
        type: "SET_LOADER",
        loader: true
      });
      let selectedServicesResponse = [];
      let selectedServicesData = null;
      let totalCount = 0;
      let page = pageIndex;
      let params = { ...searchFilterParams };

      params["page"] = page;
      params["startIndex"] = pageIndex * pageSize;
      params["pageSize"] = pageSize;
      params["provider"] = onChangeValue && onChangeValue.label;

      try {
        selectedServicesResponse = await fetchApi({
          url: "/keys/keys",
          params: params
        });
        selectedServicesData = selectedServicesResponse.data.vXKeys;
        totalCount = selectedServicesResponse.data.totalCount;
      } catch (error) {
        let errorMsg = `Error occurred while fetching Services`;
        if (error?.response?.data?.msgDesc) {
          errorMsg = "Error! " + error.response.data.msgDesc;
        }
        toast.error(errorMsg);
        console.error(`Error occurred while fetching keys : ${error}`);
      }

      if (navigateState) {
        navigateState["showLastPage"] = false;
      }

      dispatch({
        type: "SET_SEL_SERVICE",
        keyDataList: selectedServicesData,
        pageCount: Math.ceil(totalCount / pageSize),
        totalCount: totalCount
      });
      dispatch({
        type: "SET_CURRENT_PAGE_INDEX",
        currentPageIndex: page
      });
      dispatch({
        type: "SET_CURRENT_PAGE_SIZE",
        currentPageSize: pageSize
      });
      dispatch({
        type: "SET_RESET_PAGE",
        resetPage: { page: gotoPage }
      });
      dispatch({
        type: "SET_LOADER",
        loader: false
      });
    },
    [onChangeValue, updateTable, refreshTableData]
  );

  const addKey = () => {
    navigate(`/kms/keys/${onChangeValue.label}/create`, {
      state: {
        detail: onChangeValue.label
      }
    });
  };

  const columns = React.useMemo(
    () => [
      {
        Header: "Key Name",
        accessor: "name",
        Cell: (rawValue) => {
          if (rawValue && rawValue.value) {
            return <p className="text-truncate">{rawValue.value}</p>;
          }
        }
      },
      {
        Header: "Cipher",
        accessor: "cipher",
        Cell: (rawValue) => {
          if (rawValue && rawValue.value) {
            return <p className="text-center">{rawValue.value}</p>;
          }
        }
      },
      {
        Header: "Version",
        accessor: "versions",
        Cell: (rawValue) => {
          if (rawValue && rawValue.value) {
            return <p className="text-center">{rawValue.value}</p>;
          }
        },
        width: 70
      },
      {
        Header: "Attributes",
        accessor: "attributes",
        Cell: (rawValue) => {
          let html = "";
          if (rawValue && rawValue.value) {
            html = Object.keys(rawValue.value).map((key) => {
              if (!isEmpty(key)) {
                return (
                  <p className="text-truncate" key={key}>
                    {key}
                    <i className="fa-fw fa fa-long-arrow-right fa-fw fa fa-3"></i>
                    {rawValue.value[key]}
                    <br />
                  </p>
                );
              }
            });
          }
          return html;
        }
      },
      {
        Header: "Length",
        accessor: "length",
        Cell: (rawValue) => {
          if (rawValue && rawValue.value) {
            return <p className="text-center">{rawValue.value}</p>;
          }
        },
        width: 70
      },
      {
        Header: "Created Date",
        accessor: "created",
        Cell: (rawValue) => {
          if (rawValue && rawValue.value) {
            const date = rawValue.value;
            const newdate = dateFormat(date, "mm/dd/yyyy hh:MM:ss TT");
            return <div className="text-center">{newdate}</div>;
          }
        }
      },
      {
        Header: "Action",
        accessor: "action",
        Cell: (rawValue) => {
          return (
            <div className="text-center">
              <Button
                className="btn btn-outline-dark btn-sm m-r-5"
                size="sm"
                title="Edit"
                onClick={() => {
                  editModal(rawValue.row.original.name);
                }}
                data-name="rolloverKey"
                data-id={rawValue.row.original.name}
                data-cy={rawValue.row.original.name}
              >
                <i className="fa-fw fa fa-edit"></i>
              </Button>
              <Button
                variant="danger"
                size="sm"
                title="Delete"
                onClick={() => {
                  deleteModal(rawValue.row.original.name);
                }}
                data-name="deleteKey"
                data-id={rawValue.row.original.name}
                data-cy={rawValue.row.original.name}
              >
                <i className="fa-fw fa fa-trash"></i>
              </Button>
            </div>
          );
        },
        width: 80
      }
    ],
    [updateTable]
  );

  const searchFilterOptions = [
    {
      category: "name",
      label: "Key Name",
      urlLabel: "keyName",
      type: "text"
    }
  ];

  const updateSearchFilter = (filter) => {
    const { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    dispatch({
      type: "SET_SEARCH_FILTER_PARAMS",
      searchFilterParams: searchFilterParam,
      refreshTableData: moment.now()
    });

    setSearchParams(searchParam, { replace: true });
  };

  return (
    <div>
      <div className="header-wraper">
        <h3 className="wrap-header bold">Key Management</h3>
        <CustomBreadcrumb />
      </div>

      <div className="wrap">
        <BlockUi isUiBlock={blockUi} />
        <Row>
          <Col sm={12}>
            <div className="formHeader pb-3 mb-3">
              Select Service:
              <AsyncSelect
                value={onChangeValue}
                className="w-25"
                isClearable
                components={{
                  IndicatorSeparator: () => null
                }}
                onChange={selectOnchange}
                loadOptions={fetchServices}
                placeholder="Please select KMS service"
                defaultOptions
              />
            </div>
          </Col>
        </Row>

        <Row className="mb-2">
          <Col sm={10}>
            <StructuredFilter
              key="key-search-filter"
              placeholder="Search for your keys..."
              options={sortBy(searchFilterOptions, ["label"])}
              onChange={updateSearchFilter}
              defaultSelected={defaultSearchFilterParams}
            />
          </Col>
          {isKeyAdmin() && (
            <Col sm={2} className="text-end">
              <Button
                className={onChangeValue !== null ? "" : "button-disabled"}
                disabled={onChangeValue != null ? false : true}
                onClick={addKey}
                data-id="addNewKey"
                data-cy="addNewKey"
              >
                Add New Key
              </Button>
            </Col>
          )}
        </Row>

        <XATableLayout
          loading={loader}
          data={keyData || []}
          columns={
            isKeyAdmin() ? columns : reject(columns, ["Header", "Action"])
          }
          fetchData={selectServices}
          pageCount={pageCount}
          currentPageIndex={currentPageIndex}
          currentPageSize={currentPageSize}
          totalCount={totalCount}
        />

        <Modal show={editShowModal} onHide={closeEditModal}>
          <Modal.Body>{`Are you sure want to rollover ?`}</Modal.Body>
          <Modal.Footer>
            <Button variant="secondary" onClick={closeEditModal}>
              Close
            </Button>
            <Button variant="primary" onClick={confirmKeyEdit}>
              OK
            </Button>
          </Modal.Footer>
        </Modal>

        <Modal show={deleteShowModal} onHide={closeModal}>
          <Modal.Body>{`Are you sure you want to delete ?`}</Modal.Body>
          <Modal.Footer>
            <Button variant="secondary" onClick={closeModal}>
              Close
            </Button>
            <Button variant="primary" onClick={confirmKeyDelete}>
              OK
            </Button>
          </Modal.Footer>
        </Modal>
      </div>
    </div>
  );
};

export default KeyManager;
