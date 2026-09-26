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

import React, { useState, useEffect, useRef } from "react";
import { fetchApi } from "Utils/fetchAPI";
import { serverError } from "Utils/XAUtils";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend
} from "chart.js";
import { Bar } from "react-chartjs-2";
import { ModalLoader } from "Components/CommonComponents";
import { isEmpty } from "lodash";
import dateFormat from "dateformat";

const MetricGraphs = (props) => {
  const { metricData } = props;
  const [loader, setLoader] = useState(false);
  const [hrsMetricsInfo, setHrsMetricsInfo] = useState([]);
  const [daysMetricsInfo, setDaysMetricsInfo] = useState([]);
  const [showHideGraph, setShowHideGraph] = useState(true);
  const hrsRef = useRef();

  useEffect(() => {
    hrsRef.current.click();
  }, []);

  ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend
  );

  const params = {
    serviceName: metricData.serviceName,
    serviceType: metricData.serviceType,
    clusterName: metricData.clusterName,
    clientIP: metricData.clientIP,
    appId: metricData.appId
  };

  const hrsMetrics = async () => {
    setLoader(true);
    let hrsMetricData = [];
    try {
      hrsMetricData = await fetchApi({
        url: "audit/dailymetrics",
        params: params
      });
      setHrsMetricsInfo(hrsMetricData.data.auditMetricsByHours);
    } catch (error) {
      serverError(error);
    }
    setLoader(false);
  };

  const daysMetrics = async () => {
    setLoader(true);
    let daysMetricInfo = [];
    try {
      daysMetricInfo = await fetchApi({
        url: "audit/daysmetrics",
        params: params
      });

      setDaysMetricsInfo(daysMetricInfo.data.auditMetricsByDays);
    } catch (error) {
      serverError(error);
    }
    setLoader(false);
  };

  const hrsOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: "top"
      }
    }
  };
  const hrsLabels = hrsMetricsInfo?.map((metrics) => {
    return `${metrics.hours} hrs`;
  });

  const hrsData = {
    labels: hrsLabels,
    datasets: [
      {
        label: "Audit Metrics By Hours",
        data: hrsMetricsInfo?.map((metrics) => {
          return metrics.numberOfAudits;
        }),
        backgroundColor: "#0f3554",
        borderColor: "#0f3554"
      }
    ]
  };

  const dayOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: "top"
      }
    }
  };
  const dayLabels = daysMetricsInfo?.map((metrics) => {
    return dateFormat(parseInt(metrics.auditDate), "mm/dd/yyyy");
  });

  const dayData = {
    labels: dayLabels,
    datasets: [
      {
        label: "Audit Metrics By Day",
        data: daysMetricsInfo?.map((metrics) => {
          return metrics.numberOfAudits;
        }),
        backgroundColor: "#0f3554",
        borderColor: "#0f3554"
      }
    ]
  };

  const NoDataFound = () => {
    return (
      <div data-id="emptyGraphSet" data-cy="emptyGraphSet">
        <center>
          <strong>No Metrics Data found</strong>
        </center>
      </div>
    );
  };

  return loader ? (
    <ModalLoader />
  ) : (
    <>
      <div className="text-end mb-2">
        <button
          type="button"
          onClick={() => {
            daysMetrics();
            setShowHideGraph(false);
          }}
          className={`btn btn-outline-dark btn-sm me-2 ${
            showHideGraph ? "" : "btn-default-selected"
          }`}
        >
          Day
        </button>
        <button
          type="button"
          ref={hrsRef}
          className={`btn btn-outline-dark btn-sm ${
            showHideGraph ? "btn-default-selected" : ""
          }`}
          onClick={() => {
            hrsMetrics();
            setShowHideGraph(true);
          }}
        >
          Hour
        </button>
      </div>

      {showHideGraph ? (
        !isEmpty(hrsMetricsInfo) ? (
          <div className="wrap" data-id="graphSet" data-cy="graphSet">
            <Bar options={hrsOptions} data={hrsData} />
          </div>
        ) : (
          <NoDataFound />
        )
      ) : !isEmpty(daysMetricsInfo) ? (
        <div className="wrap" data-id="graphSet" data-cy="graphSet">
          <Bar options={dayOptions} data={dayData} />
        </div>
      ) : (
        <NoDataFound />
      )}
    </>
  );
};

export default MetricGraphs;
