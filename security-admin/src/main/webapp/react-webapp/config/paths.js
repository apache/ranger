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

const path = require("path");

const mainTmpFile = "index.html";

module.exports = {
  mainChunkName: "main",
  mainTmpFile,
  mainEntryPath: path.resolve(__dirname, "../src/index.jsx"),
  mainTmplPath: path.resolve(__dirname, `../src/${mainTmpFile}`),
  outputPath: path.resolve(__dirname, "../dist"),
  viewPath: path.resolve(__dirname, "../src/views"),
  imagePath: path.resolve(__dirname, "../src/images"),
  utilsPath: path.resolve(__dirname, "../src/utils"),
  componentsPath: path.resolve(__dirname, "../src/components"),
  hooksPath: path.resolve(__dirname, "../src/hooks"),
  host: process.env.UI_HOST || "0.0.0.0",
  port: process.env.UI_PORT || "8888",
  proxyHost: "http://localhost",
  proxyPort: "6080"
};
