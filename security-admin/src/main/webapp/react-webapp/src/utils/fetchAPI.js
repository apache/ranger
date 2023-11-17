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
import axios from "axios";
import {
  RANGER_REST_CSRF_ENABLED,
  RANGER_REST_CSRF_CUSTOM_HEADER,
  RANGER_REST_CSRF_IGNORE_METHODS,
  CSRFToken
} from "./appConstants";
import { toast } from "react-toastify";
import { navigateTo } from "./XAUtils";

let csrfEnabled = false;
let restCsrfCustomHeader = null;
let restCsrfIgnoreMethods = [];
let csrfToken = " ";
let isSessionActive = true;

async function fetchApi(axiosConfig = {}, otherConf = {}) {
  axiosConfig.headers = {
    ...axiosConfig.headers,
    ...{ "X-Requested-With": "XMLHttpRequest" }
  };

  if (
    csrfEnabled &&
    restCsrfIgnoreMethods.indexOf(
      (axiosConfig.method || "GET").toLowerCase()
    ) === -1 &&
    restCsrfCustomHeader
  ) {
    axiosConfig.headers = {
      ...{ [restCsrfCustomHeader]: csrfToken },
      ...axiosConfig.headers
    };
  }

  const config = {
    ...axiosConfig
  };

  if (otherConf && otherConf.cancelRequest) {
    /*
      Below code add "source" attribute in second argument which is use to cancel request.
      To cancel request pass cancelRequest = true in second argument.
      If cancelRequest set to true use "then" instead of "await" keyword with request to get data.
      for e.g. 
      const otherConf = { cancelRequest: true };
      fetchApi({ url: "/api/" }, otherConf)
        .then((res) => {})
        .catch((err) => {});

      *** To cancel request
      otherConf.source.cancel('Operation canceled by the user.');

      *** Don't use await keyword with cancelRequest ***
      await fetchApi({ url: "/api/" }, otherConf);
    */
    const CancelToken = axios.CancelToken;
    const source = CancelToken.source();
    config.cancelToken = source.token;
    otherConf.source = source;
  }

  try {
    const resp = await axios(config);
    return resp;
  } catch (error) {
    if (error?.response?.status === 419) {
      if (isSessionActive) {
        toast.warning("Session Time Out !!");
        isSessionActive = false;
        window.location.replace("login.jsp?sessionTimeout=true");
      }
    }
    if (config?.skipNavigate) {
      throw error;
    }
    if (
      error?.response?.status === 400 &&
      (error?.response?.data?.messageList?.[0]?.name == "DATA_NOT_FOUND" ||
        error?.response?.data?.messageList?.[0]?.name == "INVALID_INPUT_DATA")
    ) {
      navigateTo.navigate("/dataNotFound");
    }
    if (error?.response?.status === 404) {
      navigateTo.navigate("/pageNotFound");
    }
    if (error?.response?.status === 403) {
      navigateTo.navigate("/forbidden");
    }
    throw error;
  }
}

const handleCSRFHeaders = (data) => {
  if (data.hasOwnProperty(RANGER_REST_CSRF_ENABLED)) {
    csrfEnabled = data[RANGER_REST_CSRF_ENABLED] === true;
  }
  if (data.hasOwnProperty(RANGER_REST_CSRF_CUSTOM_HEADER)) {
    restCsrfCustomHeader = (data[RANGER_REST_CSRF_CUSTOM_HEADER] || "").trim();
  }
  if (data.hasOwnProperty(RANGER_REST_CSRF_IGNORE_METHODS)) {
    restCsrfIgnoreMethods = (data[RANGER_REST_CSRF_IGNORE_METHODS] || "")
      .split(",")
      .map((val) => (val || "").toLowerCase().trim());
  }
  if (data.hasOwnProperty(CSRFToken)) {
    csrfToken = data[CSRFToken];
    localStorage.setItem("csrfToken", csrfToken);
  }
};

const fetchCSRFConf = async () => {
  let respData = null;
  try {
    const csrfResp = await fetchApi({
      url: "plugins/csrfconf"
    });
    respData = csrfResp.data || null;
    respData && handleCSRFHeaders(respData);
  } catch (error) {
    throw Error(error);
  }
  return respData;
};

export { fetchApi, fetchCSRFConf };
