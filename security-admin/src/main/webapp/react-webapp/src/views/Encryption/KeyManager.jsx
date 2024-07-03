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

import React, { useReducer, useCallback, useEffect, useState } from "react";
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
import StructuredFilter from "../../components/structured-filter/react-typeahead/tokenizer";
import AsyncSelect from "react-select/async";
import { isKeyAdmin, parseSearchFilter } from "../../utils/XAUtils";
import { BlockUi } from "../../components/CommonComponents";
import CustomBreadcrumb from "../CustomBreadcrumb";

function init(props) {
  return {
    loader: true,
    servicesData: [],
    services: [],
    selcServicesData: [],
    keydata: [],
    onchangeval:
      props.params.kmsManagePage == "new"
        ? null
        : {
            value: props.params.kmsServiceName,
            label: props.params.kmsServiceName
          },
    deleteshowmodal: false,
    editshowmodal: false,
    filterdata: null,
    pagecount: 0,
    kmsservice: {},
    updatetable: moment.now(),
    currentPageIndex: 0,
    currentPageSize: 25,
    resetPage: { page: 0 }
  };
}

function reducer(state, action) {
  switch (action.type) {
    case "SET_LOADER":
      return {
        ...state,
        loader: action.loader
      };
    case "SET_DATA":
      return {
        ...state,

        services: action.services,
        servicesData: action.servicesdata
      };
    case "SET_SEL_SERVICE":
      return {
        ...state,

        selcServicesData: action.selcservicesData,
        keydata: action.keydatalist,
        pagecount: action.pagecount
      };
    case "SET_ONCHANGE_SERVICE":
      return {
        ...state,
        loader: action.loader,
        onchangeval: action.onchangeval
      };
    case "SET_DELETE_MODAL":
      return {
        ...state,
        deleteshowmodal: action.deleteshowmodal,
        filterdata: action.filterdata
      };
    case "SET_DELETE_MODAL_CLOSE":
      return {
        ...state,
        deleteshowmodal: action.deleteshowmodal
      };
    case "SET_EDIT_MODAL":
      return {
        ...state,
        editshowmodal: action.editshowmodal,
        filterdata: action.filterdata
      };
    case "SET_EDIT_MODAL_CLOSE":
      return {
        ...state,
        editshowmodal: action.editshowmodal
      };
    case "SET_UPDATE_TABLE":
      return {
        ...state,
        updatetable: action.updatetable
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

    default:
      throw new Error();
  }
}

const KeyManager = () => {
  const navigate = useNavigate();
  const { state } = useLocation();
  const params = useParams();
  let stateAndParams = { params: params, state: state };
  const [keyState, dispatch] = useReducer(reducer, stateAndParams, init);
  const [searchFilterParams, setSearchFilterParams] = useState([]);
  const [searchParams, setSearchParams] = useSearchParams();
  const [defaultSearchFilterParams, setDefaultSearchFilterParams] = useState(
    []
  );

  const [totalCount, setTotalCount] = useState(0);
  const [blockUI, setBlockUI] = useState(false);

  const {
    loader,
    keydata,
    filterdata,
    onchangeval,
    deleteshowmodal,
    editshowmodal,
    currentPageIndex,
    currentPageSize,
    pagecount,
    updatetable,
    resetPage
  } = keyState;

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
      setSearchFilterParams(searchFilterParam);
    }
    setDefaultSearchFilterParams(defaultSearchFilterParam);
  }, [searchParams]);

  const fetchServices = async (inputValue) => {
    let servicesdata = null;
    let allParams = {};
    if (inputValue) {
      allParams["name"] = inputValue || "";
    }
    allParams["serviceType"] = "kms";
    let serviceOptions = [];
    try {
      const servicesResp = await fetchApi({
        url: "plugins/services",
        params: allParams
      });
      servicesdata = servicesResp.data.services;
    } catch (error) {
      let errorMsg = `Error occurred while fetching Services!`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
      console.error(`Error occurred while fetching Services! ${error}`);
    }
    serviceOptions = servicesdata.map((obj) => ({
      value: obj.name,
      label: obj.name
    }));
    dispatch({
      type: "SET_DATA",
      servicesdata: servicesdata,
      services: serviceOptions
    });
    return serviceOptions;
  };

  const selconChange = (e) => {
    dispatch({
      type: "SET_ONCHANGE_SERVICE",
      onchangeval: e,
      loader: false
    });
  };

  const handleConfirmClick = () => {
    handleDeleteClick();
    dispatch({
      type: "SET_DELETE_MODAL",
      deleteshowmodal: false
    });
  };

  const deleteModal = (name) => {
    dispatch({
      type: "SET_DELETE_MODAL",
      deleteshowmodal: true,
      filterdata: name
    });
  };

  const editModal = (name) => {
    dispatch({
      type: "SET_EDIT_MODAL",
      editshowmodal: true,
      filterdata: name
    });
  };

  const closeEditModal = () => {
    dispatch({
      type: "SET_EDIT_MODAL_CLOSE",
      editshowmodal: false
    });
  };

  const EditConfirmClick = () => {
    handleEditClick();
    dispatch({
      type: "SET_EDIT_MODAL_CLOSE",
      editshowmodal: false
    });
  };

  const handleEditClick = useCallback(async () => {
    let keyEdit = {};
    keyEdit.name = filterdata ? filterdata : "";
    try {
      setBlockUI(true);
      await fetchApi({
        url: `/keys/key`,
        method: "PUT",
        params: { provider: onchangeval ? onchangeval.label : "" },
        data: keyEdit
      });
      setBlockUI(false);
      toast.success(`Success! Key rollover successfully`);
      dispatch({
        type: "SET_UPDATE_TABLE",
        updatetable: moment.now()
      });
    } catch (error) {
      setBlockUI(false);
      let errorMsg = `Error occurred during editing Key` + "\n";
      if (error?.response?.data?.msgDesc) {
        errorMsg = "Error! " + error.response.data.msgDesc + "\n";
      }
      toast.error(errorMsg);
      console.error(`Error occurred during editing Key! ${error}`);
    }
  }, [filterdata]);

  const handleDeleteClick = useCallback(async () => {
    try {
      setBlockUI(true);
      await fetchApi({
        url: `/keys/key/${filterdata}`,
        method: "DELETE",
        params: { provider: onchangeval ? onchangeval.label : "" }
      });
      setBlockUI(false);
      toast.success(`Success! Key deleted successfully`);
      if (keydata.length == 1 && currentPageIndex > 1) {
        if (typeof resetPage?.page === "function") {
          resetPage.page(0);
        }
      } else {
        dispatch({
          type: "SET_UPDATE_TABLE",
          updatetable: moment.now()
        });
      }
    } catch (error) {
      setBlockUI(false);
      let errorMsg = "";
      if (error?.response?.data?.msgDesc) {
        errorMsg = toast.error("Error! " + error.response.data.msgDesc + "\n");
      } else {
        errorMsg = `Error occurred during deleting Key` + "\n";
      }
      console.error(errorMsg);
    }
  }, [filterdata]);

  const closeModal = () => {
    dispatch({
      type: "SET_DELETE_MODAL_CLOSE",
      deleteshowmodal: false
    });
  };

  const selectServices = useCallback(
    async ({ pageSize, pageIndex, gotoPage }) => {
      dispatch({
        type: "SET_LOADER",
        loader: true
      });
      let selservicesResp = [];
      let selcservicesdata = null;
      let totalCount = 0;
      let page = pageIndex;
      let params = { ...searchFilterParams };
      params["page"] = page;
      params["startIndex"] = pageIndex * pageSize;
      params["pageSize"] = pageSize;
      params["provider"] = onchangeval && onchangeval.label;

      try {
        selservicesResp = await fetchApi({
          url: "/keys/keys",
          params: params
        });
        selcservicesdata = selservicesResp.data.vXKeys;
        totalCount = selservicesResp.data.totalCount;
      } catch (error) {
        let errorMsg = `Error occurred while fetching Services`;
        if (error?.response?.data?.msgDesc) {
          errorMsg = "Error! " + error.response.data.msgDesc;
        }
        toast.error(errorMsg);
        console.error(`Error occurred while fetching Services! ${error}`);
      }

      if (state) {
        state["showLastPage"] = false;
      }
      dispatch({
        type: "SET_SEL_SERVICE",
        keydatalist: selcservicesdata,
        pagecount: Math.ceil(totalCount / pageSize)
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
      setTotalCount(totalCount);
    },
    [onchangeval, updatetable, searchFilterParams]
  );

  const addKey = () => {
    navigate(`/kms/keys/${onchangeval.label}/create`, {
      state: {
        detail: onchangeval.label
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
    [updatetable]
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
    let { searchFilterParam, searchParam } = parseSearchFilter(
      filter,
      searchFilterOptions
    );

    setSearchFilterParams(searchFilterParam);
    setSearchParams(searchParam, { replace: true });
  };

  return (
    <div>
      <div className="header-wraper">
        <h3 className="wrap-header bold">Key Management</h3>
        <CustomBreadcrumb />
      </div>

      <div className="wrap">
        <BlockUi isUiBlock={blockUI} />
        <Row>
          <Col sm={12}>
            <div className="formHeader pb-3 mb-3">
              Select Service:
              <AsyncSelect
                value={onchangeval}
                className="w-25"
                isClearable
                components={{
                  IndicatorSeparator: () => null
                }}
                onChange={selconChange}
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
                className={onchangeval !== null ? "" : "button-disabled"}
                disabled={onchangeval != null ? false : true}
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
          data={keydata || []}
          columns={
            isKeyAdmin() ? columns : reject(columns, ["Header", "Action"])
          }
          fetchData={selectServices}
          pageCount={pagecount}
          currentPageIndex={currentPageIndex}
          currentPageSize={currentPageSize}
          totalCount={totalCount}
        />

        <Modal show={editshowmodal} onHide={closeEditModal}>
          <Modal.Body>{`Are you sure want to rollover ?`}</Modal.Body>
          <Modal.Footer>
            <Button variant="secondary" onClick={closeEditModal}>
              Close
            </Button>
            <Button variant="primary" onClick={EditConfirmClick}>
              Ok
            </Button>
          </Modal.Footer>
        </Modal>

        <Modal show={deleteshowmodal} onHide={closeModal}>
          <Modal.Body>{`Are you sure you want to delete ?`}</Modal.Body>
          <Modal.Footer>
            <Button variant="secondary" onClick={closeModal}>
              Close
            </Button>
            <Button variant="primary" onClick={handleConfirmClick}>
              Ok
            </Button>
          </Modal.Footer>
        </Modal>
      </div>
    </div>
  );
};

export default KeyManager;
