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
import { Button } from "react-bootstrap";
import { useNavigate } from "react-router-dom";
import { serverError } from "../../../utils/XAUtils";
import { fetchApi } from "Utils/fetchAPI";
import { toast } from "react-toastify";
import { Form } from "react-final-form";
import arrayMutators from "final-form-arrays";
import PrinciplePermissionComp from "./PrinciplePermissionComp";

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
  const [dataSetDetails, dispatch] = useReducer(
    datasetFormReducer,
    initialState
  );
  const [step, setStep] = useState(1);
  const [dataset] = useState({
    name: "",
    acl: {
      users: {},
      groups: {},
      roles: {}
    },
    description: "",
    termsOfUse: ""
  });
  const [datasetName, setName] = useState();
  const [datasetDescription, setDatasetDescription] = useState();
  const [datasetTermsAndConditions, setDatasetTermsAndConditions] =
    useState("");
  const [saveButtonText, setSaveButtonText] = useState("Continue");
  const [userList, setUserList] = useState([]);
  const [groupList, setGroupList] = useState([]);
  const [roleList, setRoleList] = useState([]);

  const datasetNameChange = (event) => {
    setName(event.target.value);
    dataset.name = event.target.value;
  };

  const datasetDescriptionChange = (event) => {
    setDatasetDescription(event.target.value);
    dataset.description = event.target.value;
  };

  const datasetTermsAndConditionsChange = (event) => {
    setDatasetTermsAndConditions(event.target.value);
    dataset.termsOfUse = event.target.value;
  };

  const handleDataChange = (userList, groupList, roleList) => {
    setUserList(userList);
    setGroupList(groupList);
    setRoleList(roleList);
  };

  const handleSubmit = async (values) => {
    if (step == 3) {
      userList.forEach((user) => {
        dataset.acl.users[user.name] = user.perm;
      });

      groupList.forEach((group) => {
        dataset.acl.groups[group.name] = group.perm;
      });

      roleList.forEach((role) => {
        dataset.acl.roles[role.name] = role.perm;
      });
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
          data: dataset,
          skipNavigate: true
        });
        toast.success("Dataset created successfully!!");
        self.location.hash = "#/gds/mydatasetlisting";
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
    } else if (step == 1) {
      if (datasetName == undefined) {
        toast.error("Dataset name cannot be empty!!");
        return;
      } else if (datasetName.length > 512) {
        toast.error("Dataset name must not exceed 512 characters!!");
        return;
      }
      setSaveButtonText("Continue");
      setStep(step + 1);
    }
  };

  const cancelDatasetDetails = () => {
    if (step == 1) {
      navigate("/gds/mydatasetlisting");
    } else {
      let txt = "";
      for (let x in dataset.acl.users) {
        txt += dataset.acl.users[x] + " ";
      }
      setStep(step - 1);
    }
    setSaveButtonText("Continue");
  };

  return (
    <>
      <Form
        onSubmit={handleSubmit}
        mutators={{
          ...arrayMutators
        }}
        render={({ handleSubmit }) => (
          <>
            <div className="gds-form-header-wrapper">
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
                  onClick={handleSubmit}
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
                  <h2 className="gds-form-step-name">
                    Enter dataset name and description
                  </h2>
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
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 2</h6>
                  <h2 className="gds-form-step-name">Dataset Visibility</h2>
                </div>
                <PrinciplePermissionComp
                  userList={userList}
                  groupList={groupList}
                  roleList={roleList}
                  type="dataset"
                  onDataChange={handleDataChange}
                />
              </div>
            )}

            {step == 3 && (
              <div className="gds-form-wrap">
                <div className="gds-form-header">
                  <h6 className="gds-form-step-num">Step 3</h6>
                  <h2 className="gds-form-step-name">
                    Specify terms and conditions
                  </h2>
                </div>
                <table className="gds-table">
                  <tr>
                    <td>
                      <textarea
                        placeholder="Terms & Conditions"
                        className="form-control"
                        id="termsAndConditions"
                        data-cy="termsAndConditions"
                        onChange={datasetTermsAndConditionsChange}
                        value={datasetTermsAndConditions}
                        rows={16}
                      />
                    </td>
                  </tr>
                </table>
              </div>
            )}
          </>
        )}
      />
    </>
  );
};

export default AddDatasetView;
