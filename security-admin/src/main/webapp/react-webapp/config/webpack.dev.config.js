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

const webpack = require("webpack");
const { merge } = require("webpack-merge");

const commonPaths = require("./paths");
const commonConfig = require("./webpack.config.js");

const devConfig = merge(commonConfig, {
  mode: "development",
  devtool: "inline-source-map",
  module: {
    rules: [
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"]
      }
    ]
  },
  devServer: {
    host: commonPaths.host,
    port: commonPaths.port,
    historyApiFallback: true,
    proxy: [
      {
        context: ["/service", "/login", "/logout"],
        target: `${commonPaths.proxyHost}:${commonPaths.proxyPort}`,
        secure: false
      }
    ]
  },
  plugins: [new webpack.HotModuleReplacementPlugin()]
});

module.exports = devConfig;
