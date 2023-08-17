/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var gatewayUrl;
var apiBaseUrl = "/service";

window.onload = function() {
    const ui = SwaggerUIBundle({
        url: getSwaggerBaseUrl(window.location.pathname) + "/swagger.json",
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        plugins: [
            SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout",
        requestInterceptor: function(request) {
              if (!request.url.includes("swagger.json")) {
                    request.url = getAPIUrl(request.url);
              }
              if (request.method != "GET") {
                request.headers['X-XSRF-HEADER'] = localStorage.csrfToken;
              }

              return request;
        },
        docExpansion: 'none'
    })
    window.ui = ui;
    setLogo()
    if(document.getElementById("swagger-ui").getElementsByClassName("float-right").length > 0) {
        document.getElementById("swagger-ui").getElementsByClassName("float-right")[0].querySelector("a").remove()
    }

}

function setLogo() {
    if( document.getElementById("swagger-ui").getElementsByClassName("topbar-wrapper").length > 0){
        document.getElementById("swagger-ui").getElementsByClassName("topbar-wrapper")[0].getElementsByTagName("img")[0].src = gatewayUrl + "/images/ranger_logo.png";
    }
}

function getSwaggerBaseUrl(url) {
    var path = url.replace(/\/[\w-]+.(jsp|html)|\/+$/ig, '');
    splitPath = path.split("/");
    splitPath.pop();
    gatewayUrl = splitPath.join("/");

    return window.location.origin + path;
};

function getAPIUrl(url) {
    url = new URL(url);
    var path =  url.origin + apiBaseUrl + url.pathname + url.search;
    return path;
};
