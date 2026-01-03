/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *onDatashareSelect
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,Row
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { Button } from "react-bootstrap";
import { Card } from "react-bootstrap";
import { fetchApi } from "../../../utils/fetchAPI";
import { Loader } from "../../../components/CommonComponents";
import dateFormat from "dateformat";
import PolicyValidityPeriodComp from "../../PolicyListing/PolicyValidityPeriodComp";
import { Form, Field } from "react-final-form";
import arrayMutators from "final-form-arrays";
import moment from "moment-timezone";
import { isEmpty } from "lodash";
import { toast } from "react-toastify";
import { getAllTimeZoneList } from "../../../utils/XAUtils";
import { statusClassMap } from "../../../utils/XAEnums";

const RequestDetailView = () => {
  let { requestId } = useParams();
  const navigate = useNavigate();
  const [requestInfo, setRequestInfo] = useState({});
  const [loader, setLoader] = useState(true);
  const [datasetInfo, setDatasetInfo] = useState({});
  const [datashareInfo, setDatashareInfo] = useState({});
  const [formData, setFormData] = useState();
  const [editValidityPeriod, setEditValidityPeriod] = useState(false);

  useEffect(() => {
    fetchRequestDetail(requestId);
  }, []);

  const fetchRequestDetail = async () => {
    try {
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/datashare/dataset/${requestId}`
      });
      if (resp.data.validitySchedule !== undefined) {
        let val = resp.data.validitySchedule;
        let data = {};
        data["validitySchedules"] = [];
        let obj = {};
        if (val.endTime) {
          obj["endTime"] = moment(val.endTime, "YYYY/MM/DD HH:mm:ss");
        }
        if (val.startTime) {
          obj["startTime"] = moment(val.startTime, "YYYY/MM/DD HH:mm:ss");
        }
        if (val.timeZone) {
          obj["timeZone"] = getAllTimeZoneList().find((tZoneVal) => {
            return tZoneVal.id == val.timeZone;
          });
        }
        data["validitySchedules"].push(obj);
        setFormData(data);
        setEditValidityPeriod(true);
      }
      setRequestInfo(resp.data);
      fetchdatasetInfo(resp.data.datasetId);
      fetchDatashaerInfo(resp.data.dataShareId);
      setLoader(false);
    } catch (error) {
      console.error(`Error occurred while fetching request details ! ${error}`);
    }
  };

  const fetchdatasetInfo = async (datasetId) => {
    try {
      const resp = await fetchApi({
        url: `gds/dataset/${datasetId}`
      });
      setDatasetInfo(resp.data);
    } catch (error) {
      console.error(`Error occurred while fetching dataset details ! ${error}`);
    }
  };

  const fetchDatashaerInfo = async (datashareId) => {
    try {
      const resp = await fetchApi({
        url: `gds/datashare/${datashareId}`
      });
      setDatashareInfo(resp.data);
    } catch (error) {
      console.error(`Error occurred while fetching dataset details ! ${error}`);
    }
  };

  const handleSubmit = async (values, type) => {
    // if (values.validitySchedules != undefined) {
    //   values.validitySchedules.filter((val) => {
    //     if (val) {
    //       let timeObj = {};
    //       if (val.startTime) {
    //         timeObj["startTime"] = moment(val.startTime).format(
    //           "YYYY/MM/DD HH:mm:ss"
    //         );
    //       }
    //       if (val.endTime) {
    //         timeObj["endTime"] = moment(val.endTime).format(
    //           "YYYY/MM/DD HH:mm:ss"
    //         );
    //       }
    //       if (val.timeZone) {
    //         timeObj["timeZone"] = val.timeZone.id;
    //       }
    //       if (!isEmpty(timeObj)) {
    //         requestInfo["validitySchedule"] = timeObj;
    //       }
    //     }
    //   });
    // }

    if (requestInfo.status === "REQUESTED") {
      if (type == "grant") requestInfo["status"] = "GRANTED";
      else requestInfo["status"] = "DENIED";
    } else if (requestInfo.status === "GRANTED") {
      requestInfo["status"] = "ACTIVE";
    }
    try {
      setLoader(true);
      const resp = await fetchApi({
        url: `gds/datashare/dataset/${requestInfo.id}`,
        method: "PUT",
        data: requestInfo
      });
      setLoader(false);
      toast.success("Request updated successfully!!");
      //showSaveCancelButton(false);
    } catch (error) {
      setLoader(false);
      let errorMsg = `Failed to update request`;
      if (error?.response?.data?.msgDesc) {
        errorMsg = `Error! ${error.response.data.msgDesc}`;
      }
      toast.error(errorMsg);
      console.error(`Error while updating request! ${error}`);
    }
  };

  const statusVal = requestInfo.status || "DENIED";

  return (
    <>
      <Form
        onSubmit={handleSubmit}
        mutators={{
          ...arrayMutators
        }}
        initialValues={formData}
        render={({
          form: {
            mutators: { push: addPolicyItem, pop }
          },
          values
        }) => (
          <div>
            <div className="gds-header-wrapper gap-half py-3">
              <Button
                variant="light"
                className="border-0 bg-transparent"
                onClick={() => window.history.back()}
                size="sm"
                data-id="back"
                data-cy="back"
              >
                <i className="fa fa-angle-left fa-lg font-weight-bold" />
              </Button>
              <h3 className="gds-header bold">Request Detail</h3>
              {requestInfo.status === "REQUESTED" ? (
                <div className="gds-header-btn-grp">
                  <Button
                    variant="secondary"
                    onClick={() => handleSubmit(values, "deny")}
                    size="sm"
                    data-id="deny"
                    data-cy="deny"
                  >
                    Deny
                  </Button>
                  <Button
                    variant="primary"
                    onClick={() => handleSubmit(values, "grant")}
                    size="sm"
                    data-id="grant"
                    data-cy="grant"
                  >
                    Grant
                  </Button>
                </div>
              ) : (
                <div>
                  {requestInfo.status === "GRANTED" && (
                    <Button
                      variant="primary"
                      onClick={() => handleSubmit(values, "active")}
                      size="sm"
                      data-id="activate"
                      data-cy="activate"
                    >
                      Accept & Activate
                    </Button>
                  )}
                </div>
              )}
            </div>
            {loader ? (
              <Loader />
            ) : (
              <div className="gds-content-border gds-request-content pt-5">
                <div className="d-flex justify-content-between">
                  <div className="gds-inline-field-grp">
                    <div className="wrapper">
                      <div className="gds-left-inline-field" height="30px">
                        <span className="gds-label-color">Status</span>
                      </div>
                      <span className={`${statusClassMap[statusVal]}`}>
                        {requestInfo.status}
                      </span>
                    </div>
                    <div className="wrapper">
                      <div className="gds-left-inline-field" height="30px">
                        <span className="gds-label-color">Created by</span>
                      </div>
                      <div line-height="30px">{requestInfo.createdBy}</div>
                    </div>
                    <div className="wrapper">
                      <div className="gds-left-inline-field" height="30px">
                        <span className="gds-label-color">Approver</span>
                      </div>
                      <div line-height="30px">{requestInfo.approvedBy}</div>
                    </div>
                  </div>
                  <div className="gds-right-inline-field-grp">
                    <div className="wrapper">
                      <div className="gds-label-color">Created</div>
                      <div className="gds-right-inline-field">
                        <span>
                          {dateFormat(
                            requestInfo["createTime"],
                            "mm/dd/yyyy hh:MM:ss TT"
                          )}
                        </span>
                      </div>
                    </div>
                    <div className="wrapper">
                      <div className="gds-label-color">Last Updated</div>
                      <div className="gds-right-inline-field">
                        <span>
                          {dateFormat(
                            requestInfo["updateTime"],
                            "mm/dd/yyyy hh:MM:ss TT"
                          )}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
                {false && (
                  <div className="mb-5">
                    <hr className="m-0" />
                    <div className="d-flex align-items-center justify-content-between mb-4 pt-4">
                      <span className="gds-card-heading border-0 p-0">
                        Validity Period
                      </span>
                      <PolicyValidityPeriodComp
                        addPolicyItem={addPolicyItem}
                        isGdsRequest={
                          requestInfo.validitySchedule == undefined
                            ? true
                            : false
                        }
                        editValidityPeriod={editValidityPeriod}
                      />
                    </div>
                    {requestInfo.validitySchedule != undefined && (
                      <div className="gds-inline-field-grp">
                        <div className="wrapper">
                          <div className="gds-left-inline-field">
                            <span className="gds-label-color">Start Date </span>
                          </div>
                          <span>
                            {dateFormat(
                              requestInfo.validitySchedule.startTime,
                              "mm/dd/yyyy hh:MM:ss TT"
                            )}
                          </span>
                          <span className="gds-label-color pl-5">
                            {requestInfo.validitySchedule.timeZone}
                          </span>
                        </div>
                        <div className="wrapper">
                          <div className="gds-left-inline-field">
                            <span className="gds-label-color"> End Date </span>
                          </div>
                          <span>
                            {dateFormat(
                              requestInfo.validitySchedule.endTime,
                              "mm/dd/yyyy hh:MM:ss TT"
                            )}
                          </span>
                        </div>
                      </div>
                    )}
                  </div>
                )}
                {datashareInfo.termsOfUse != undefined &&
                  datashareInfo.termsOfUse.length > 0 && (
                    <div className="mb-5">
                      <Card className="gds-section-card gds-bg-white">
                        <div className="gds-section-title">
                          <p className="gds-card-heading">Terms & Conditions</p>
                          <span>{datashareInfo.termsOfUse}</span>
                        </div>
                      </Card>
                    </div>
                  )}

                <div className="mb-5">
                  <Card className="gds-section-card gds-bg-white">
                    <div className="gds-section-title">
                      <p className="gds-card-heading">Datashare Details</p>
                    </div>
                    <div>
                      <div className="w-100 mb-3">
                        <div className="wrapper">
                          <div className="gds-left-inline-field" height="30px">
                            <span className="gds-label-color">Name </span>
                          </div>
                          <div line-height="30px">{datashareInfo.name}</div>
                        </div>
                      </div>
                      <div className="w-100 mb-3 d-flex justify-content-between">
                        <div className="wrapper w-50">
                          <div className="gds-left-inline-field" height="30px">
                            <span className="gds-label-color">Service </span>
                          </div>
                          <div line-height="30px">{datashareInfo.service}</div>
                        </div>
                        <div className="wrapper w-50">
                          <div className="gds-label-color">Security Zone</div>
                          <div className="gds-right-inline-field">
                            <span>{datashareInfo.zone}</span>
                          </div>
                        </div>
                      </div>
                      <div className="w-100 mb-3">
                        <div className="wrapper">
                          <div className="gds-left-inline-field" height="30px">
                            <span className="gds-label-color">Conditions</span>
                          </div>
                          <div line-height="30px">
                            {datashareInfo.conditionExpr}
                          </div>
                        </div>
                      </div>
                      <div className="w-100 mb-3">
                        <div className="wrapper">
                          <div className="gds-left-inline-field" height="30px">
                            <span className="gds-label-color">
                              Default Access Type
                            </span>
                          </div>
                          <div className="gds-left-inline-field">
                            <span>
                              {datashareInfo.defaultAccessTypes != undefined ? (
                                <div>
                                  <span>
                                    {datashareInfo.defaultAccessTypes.toString()}
                                  </span>
                                </div>
                              ) : (
                                <div></div>
                              )}
                            </span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </Card>
                </div>
                <div className="mb-5">
                  <Card className="gds-section-card gds-bg-white">
                    <div className="gds-section-title">
                      <p className="gds-card-heading">Dataset Details</p>
                    </div>
                    <div>
                      <div className="gds-inline-field-grp">
                        <div className="wrapper">
                          <div
                            className="gds-left-inline-field flex-shrink-0"
                            height="30px"
                          >
                            <span className="gds-label-color">Name </span>
                          </div>
                          <div line-height="30px">{datasetInfo.name}</div>
                        </div>
                        <div className="wrapper">
                          <div
                            className="gds-left-inline-field flex-shrink-0"
                            height="30px"
                          >
                            <span className="gds-label-color">
                              Description{" "}
                            </span>
                          </div>
                          <div line-height="30px">
                            <span>{datasetInfo.description}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </Card>
                </div>
              </div>
            )}
          </div>
        )}
      />
    </>
  );
};

export default RequestDetailView;
