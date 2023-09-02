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

import React, { useState, useReducer } from "react";
import { Button, Row, Col, Table, Accordion, Card } from "react-bootstrap";
import { useNavigate } from "react-router-dom";
import { serverError } from "../../../utils/XAUtils";
import { fetchApi } from "Utils/fetchAPI";
import { toast } from "react-toastify";
import Select from "react-select";
import { Form, Field } from "react-final-form";
import arrayMutators from "final-form-arrays";
import { FieldArray } from "react-final-form-arrays";
import AsyncSelect from "react-select/async";


const initialState = {
  dataset: {},
  loader: false,
  preventUnBlock: false,
  blockUI: false,
  selectedPrinciple: []
};



const datasetFormReducer = (state, action) => {
  switch (action.type) {
    case "SET_SELECTED_PRINCIPLE":
      return {
        ...state,
        selectedPrinciple: action.selectedPrinciple
      };
    case "SET_PREVENT_ALERT":
      return {
        ...state,
        preventUnBlock: action.preventUnBlock
      };
    case "SET_BLOCK_UI":
      return {
        ...state,
        blockUI: action.blockUI
      };
    default:
      throw new Error();
  }
};

const AddDatasetView = () => {

  const navigate = useNavigate();
  const [dataSetDetails, dispatch] = useReducer(datasetFormReducer, initialState);
  const [step, setStep] = useState(1);
  const [dataset, setDataset] = useState({
    name: "",
    acl: {
      users: {},
      groups: {},
      roles: {}
    },
    description: "",
    termsOfUse: ""
  });
  const [datasetName, setName] = useState('');
  const [datasetDescription, setDatasetDescription] = useState('');
  const [datasetAcl, setAcl] = useState({});
  const [datasetTermsAndConditions, setDatasetTermsAndConditions] = useState('');
  const [selectedAccess, setSelectedAccess] = useState({ value: 'LIST', label: 'LIST'});
  const [principals, setPrincipals] = useState([]);
    const {
    loader,
    selectedPrinciple,
    preventUnBlock,
    blockUI
    } = dataSetDetails;
  const toastId = React.useRef(null);
  const [userAccordian, setUserAccordian] = useState(false);
  const [saveButtonText, setSaveButtonText] = useState('Continue');

  const changeUserAccordian = () => { 
    setUserAccordian(!userAccordian);
  }

  const acl = {
    "users": {},
    "groups": {},
    "roles": {}
  }
  
  const aclApiResponse = [
    {
      "type": "USER",
      "name": "GDS_User"
    },
    {
      "type": "GROUP",
      "name": "GDS_Group"
    },
    {
      "type": "ROLE",
      "name": "GDS_Role"
    }
  ];

  const setACL = (e, input) => {
    setSelectedAccess(e);
    input.onChange(e);
  };
  
  const datasetNameChange = event => {
        setName(event.target.value);
      dataset.name = event.target.value
    console.log('DatasetName is:', event.target.value);
    };
    
    const datasetDescriptionChange = event => { 
      setDatasetDescription(event.target.value);
      dataset.description = event.target.value;
        console.log('DatasetDescription is:', event.target.value);
    }

  const datasetTermsAndConditionsChange = event => {
      setDatasetTermsAndConditions(event.target.value);
      dataset.termsOfUse = event.target.value;
        console.log('datasetTermsAndConditions is:', event.target.value);
    } 

  
  const accessOptions = [
  { value: 'LIST', label: 'LIST' },
  { value: 'VIEW', label: 'VIEW' },
  { value: 'ADMIN', label: 'ADMIN' }
  ]

  const accessOptionsWithRemove = [
  { value: 'LIST', label: 'LIST' },
  { value: 'VIEW', label: 'VIEW' },
    { value: 'ADMIN', label: 'ADMIN' },
    { value: 'Remove Access', label: 'Remove Access' }
  ]

  const formatOptionLabel = ({ label }) => (
    <div title={label} className="text-truncate">
      {label}
    </div>
  );

  const serviceSelectTheme = (theme) => {
    return {
      ...theme,
      colors: {
        ...theme.colors,
        primary: "#0081ab"
      }
    };
  };
  
  const serviceSelectCustomStyles = {
    option: (provided, state) => ({
      ...provided,
      color: state.isSelected ? "white" : "black"
    }),
    control: (provided) => ({
      ...provided,
      maxHeight: "32px",
      minHeight: "32px"
    }),
    indicatorsContainer: (provided) => ({
      ...provided,
      maxHeight: "30px"
    }),
    dropdownIndicator: (provided) => ({
      ...provided,
      padding: "5px"
    }),
    clearIndicator: (provided) => ({
      ...provided,
      padding: "5px"
    }),
    container: (styles) => ({ ...styles, width: "150px" })
  };
  
  const getAclFromInput = (value, index, array) => { 

  }

    const addInSelectedPrincipal = (push, values, input) => {
      let principle = values.selectedPrinciple;
      for (let i = 0; i < principle.length; i++) { 
        let acl = { name: '', type: '', perm: ''};
        acl.name = principle[i].value;
        acl.perm = selectedAccess.value;
        acl.type = principle[i].type;
        principle[i] = acl;
        
        if (principle[i].type == "USER") {
          dataset.acl.users[principle[i].name] = selectedAccess.value;
        } else if (principle[i].type == "GROUP") {
          dataset.acl.groups[principle[i].name] = selectedAccess.value;
        } else if (principle[i].type == "ROLE") { 
          dataset.acl.roles[principle[i].name] = selectedAccess.value;
        }        
      }

      principle.map((principle) => {
            push("users", principle);
          });
      setSelectedAccess({ value: 'LIST', label: 'LIST'});

      //push("users", val);
      dispatch({
        type: "SET_SELECTED_PRINCIPLE",
        selectedPrinciple: []
      });
      console.log(selectedPrinciple)
    /** }*/
    };
  
  const customStyles = {
      control: (provided) => ({
        ...provided,
        maxHeight: "40px",
        width: "172px"
      }),
      indicatorsContainer: (provided) => ({
        ...provided,
      })
    };
  
  const handleTableSelectedValue = (e, input, index, fields) => {
    if (e.label == "Remove Access") { 
      fields.remove(index)
    } else {
      input.onChange(e);
    }
  }

  const selectedPrincipal = (e, input) => { 
    dispatch({
        type: "SET_SELECTED_PRINCIPLE",
        selectedPrinciple: e
      });
    input.onChange(e);
  }
    
    const subhmitDatasetDetails = async () => {
      console.log('Step value :: ' + step)

      let txt = "";
      for (let x in dataset.acl.users) {
        txt += dataset.acl.users[x] + " ";
      };
      
        if (step == 3) {
            dispatch({
                type: "SET_PREVENT_ALERT",
                preventUnBlock: true
            });
            try {
                dispatch({
                type: "SET_BLOCK_UI",
                blockUI: true
                });
                const createDatasetResp = await fetchApi({
                url: `gds/dataset`,
                method: "post",
                data: dataset
                });
                dispatch({
                type: "SET_BLOCK_UI",
                blockUI: false
                });
                toast.success("Dataset created successfully!!");
                self.location.hash = "#/gds/datasetlisting";
            } catch (error) {
                dispatch({
                type: "SET_BLOCK_UI",
                blockUI: false
                });
                serverError(error);
                console.error(`Error occurred while creating dataset  ${error}`);
            }
        } else if (step == 2) {
          setSaveButtonText("Create Dataset");
          setStep(step + 1);
        } else {
          setSaveButtonText("Continue");
          setStep(step + 1);
        }        
    }

    const cancelDatasetDetails = () => { 
        if (step == 1) {
            navigate("/gds/datasetlisting");
        } else { 
          let txt = "";
          for (let x in dataset.acl.users) {
            txt += dataset.acl.users[x] + " ";
          };
          console.log('back TExt ::: '+txt)
            setStep(step - 1);
        }
      setSaveButtonText("Continue");
    }
  
  const handleSubmit = async (formData) => {

  }

  const handlePrincipleChange = (value) => {
    console.log(value)
    dispatch({
      type: "SET_SELECTED_PRINCIPLE",
      selectedPrinciple: value
    });
  };

  const setPrincipleFormData = () => { 
    let formValueObj = {};
    /**if (params?.roleID) {**/
    
    if (aclApiResponse.users != undefined) { 
      formValueObj.users
    }

    if (aclApiResponse.groups != undefined) { 

    }

    if (aclApiResponse.roles != undefined) { 

    }

        /**if (aclApiResponse.length > 0) {
          formValueObj.name = roleInfo.name;
          formValueObj.description = roleInfo.description;
          formValueObj.users = roleInfo.users;
          formValueObj.groups = roleInfo.groups;
          formValueObj.roles = roleInfo.roles;
        }**/
      /*}**/
    return formValueObj;
  }
  
    

const fetchPrincipleOp = async (inputValue) => {
    let params = { name: inputValue || "" };
    let data = [];
    const principalResp = await fetchApi({
      url: "xusers/lookup/principals",
      params: params
    });
    data = principalResp.data;
    return data.map((obj) => ({
      label: obj.name,
      value: obj.name,
      type: obj.type
    }));
  };

        return (
            <>
                <div className="gds-header-wrapper">
                    <h3 className="gds-header bold">Create Dataset</h3>
          
                    <div className="gds-header-btn-grp">
                        <Button
                            variant="secondary"
                            type="button"
                            size="sm"
                            onClick={cancelDatasetDetails}
                            data-id="cancel"
                            data-cy="cancel"
                        >
                            Back
                        </Button>
                        <Button
                            variant="primary"
                            onClick={subhmitDatasetDetails}
                            size="sm"
                            data-id="save"
                            data-cy="save"
                        >
                           {saveButtonText}
                        </Button>
                    </div>
                </div>       
              
                
                {step == 1 && (
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 1</h6>
                  <h2 className="gds-form-step-name">Enter dataset name and description</h2>
                </div>
                <div className="gds-form-content">
                  <div className="gds-form-input">
                    <input
                      type="text"
                      name="datasetName"
                      placeholder="Dataset Name"
                      className="form-control"
                    data-cy="datasetName"
                    onChange={datasetNameChange}
                    value={datasetName}
                    />
                  </div>
                  <div className="gds-form-input">
                    <textarea
                      placeholder="Dataset Description"
                      className="form-control"
                      id="description"
                      data-cy="description"
                      onChange={datasetDescriptionChange}
                      value={datasetDescription}
                      rows={4}
                    />
                  </div>
                </div>

              </div>
                )}


            {step == 2 && (           
                <Form
                      onSubmit={handleSubmit}
                      initialValues={setPrincipleFormData()}
                      mutators={{
                        ...arrayMutators
                      }}
                      render={({
                      handleSubmit,
                      form: {
                        mutators: { push, pop }
                      },
                      form,
                      submitting,
                      invalid,
                      errors,
                      values,
                      fields,
                      pristine,
                      dirty
                    }) => (
                      <div className="gds-form-wrap">
                        <form
                          onSubmit={(event) => {
                            handleSubmit(event);
                          }}
                          >
                            <div className="gds-form-header">
                              <h6 className="gds-form-step-num">Step 2</h6>
                              <h2 className="gds-form-step-name">Specify Permissions</h2>
                            </div>
                            <div className="gds-form-content">
                              <div className="gds-form-input">
                               
                                    <Field
                                            className="form-control"
                                            name="selectedPrinciple"
                                            render={({ input, meta }) => (
                                              <div className="gds-add-principle">
                                                {" "}
                                                <AsyncSelect className="flex-1 gds-text-input"
                                                  onChange={(e) => selectedPrincipal(e, input)}
                                                  value={selectedPrinciple}
                                                  loadOptions={fetchPrincipleOp}
                                                  components={{
                                                    DropdownIndicator: () => null,
                                                    IndicatorSeparator: () => null
                                                  }}
                                                  defaultOptions
                                                  isMulti
                                                  placeholder="Select Principals"
                                                  data-name="usersSelect"
                                                  data-cy="usersSelect"
                                                />

                                                <Field
                                                  name="accessPermList"
                                                  className="form-control"
                                                  render={({ input }) => (
                                                    <Select
                                                        theme={serviceSelectTheme}
                                                        styles={customStyles}
                                                        options={accessOptions}
                                                        onChange={(e) => setACL(e, input)}
                                                        value={selectedAccess}
                                                        formatOptionLabel={formatOptionLabel}
                                                        menuPlacement="auto"
                                                        isClearable
                                                      />
                                                  )}
                                                >

                                                </Field>
                                                

                                                <Button
                                                  type="button"
                                                  className="gds-button btn btn-primary"
                                                  onClick={() => {
                                                    if (
                                                      !values.selectedPrinciple ||
                                                      values.selectedPrinciple.length === 0
                                                    ) {
                                                      toast.dismiss(toastId.current);
                                                      toastId.current = toast.error(
                                                        "Please select principal!!"
                                                      );
                                                      return false;
                                                    }
                                                    addInSelectedPrincipal(push, values, input);
                                                  }}
                                                  size="sm"
                                                  data-name="usersAddBtn"
                                                  data-cy="usersAddBtn"
                                                >
                                                  Add Principals
                                                </Button>
                                                
                                              </div>
                                            )}
                                          />
                           
                              </div>
                            </div>

                          <div>
                              <div className="wrap">
                                <Col sm="12">
                                <FieldArray name="users">
                                  {({ fields }) => (
                                    <Table >
                                      <thead className="thead-light">
                                        <tr>
                                          <th className="text-center">Selected Users, Groups, Roles</th>
                                        </tr>
                                      </thead>
                                      <tbody>
                                        {fields.value == undefined ? (
                                          <tr>
                                            <td className="text-center text-muted" colSpan="3" >
                                              No principles found
                                            </td>
                                          </tr>
                                          ) : (
                                              
                                              fields.map((name, index) => (
                                            
                                                
                                            
                                            <tr key={index}>
                                              <td className="text-center text-truncate">
                                                <span title={fields.value[index].name}>
                                                  {fields.value[index].name}
                                                </span>
                                              </td>
                                            <td>
                                              <Field
                                              name="aclPerms"
                                              render={({ input, meta }) => (
                                                <Select
                                                      theme={serviceSelectTheme}
                                                      styles={serviceSelectCustomStyles}
                                                      options={accessOptionsWithRemove}
                                                      onChange={(e) => handleTableSelectedValue(e, input, index, fields)}
                                                      formatOptionLabel={formatOptionLabel}
                                                      menuPlacement="auto"
                                                      placeholder={fields.value[index].perm}
                                                      isClearable
                                                    />
                                                )}
                                              />
                                            </td>
                                            </tr>
                                          ))
                                        )}
                                      </tbody>
                                    </Table>
                                  )}
                                </FieldArray>
                                
                                </Col>
                            </div>
                          </div>
                        </form>
                      </div>
                    )}
                    />
                )}


                {step == 3 && ( 
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 3</h6>
                  <h2 className="gds-form-step-name">Specify terms and conditions</h2>
                </div>
                <table className="gds-table" >
                  <tr>
                    <td>
                      <textarea
                              placeholder="Terms & Conditions"
                              className="form-control"
                              id="termsAndConditions"
                            data-cy="termsAndConditions"
                              onChange={datasetTermsAndConditionsChange}
                        value={datasetTermsAndConditions}
                        rows={8}
                            />
                    </td>
                  </tr>
                </table>      
              </div>
                )}
                
        </>
        );
}

export default AddDatasetView;
