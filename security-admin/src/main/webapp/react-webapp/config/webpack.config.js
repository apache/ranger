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

const HtmlWebpackPlugin = require("html-webpack-plugin");

const commonPaths = require("./paths");

module.exports = {
  entry: {
    [commonPaths.mainChunkName]: commonPaths.mainEntryPath
  },

  output: {
    path: commonPaths.outputPath,
    filename: (pathData) => {
      return "dist/[name].[contenthash].js";
    },
    chunkFilename: (pathData) => {
      return "dist/[name].[chunkhash].js";
    },
    assetModuleFilename: "images/[contenthash][ext][query]"
  },

  module: {
    rules: [
      {
        test: /\.(js|jsx)$/,
        exclude: /node_modules/,
        use: {
          loader: "babel-loader"
        }
      },
      {
        test: /\.(png|jpe?g|gif|svg|ico)$/,
        use: [
          {
            loader: "file-loader",
            options: {
              name: "[contenthash].[ext]",
              outputPath: "images/",
              publicPath: "../images/"
            }
          }
        ]
      },
      {
        test: /\.(woff|woff2|ttf|otf|eot)$/,
        use: [
          {
            loader: "file-loader",
            options: {
              name: "[contenthash].[ext][query]",
              outputPath: "fonts/",
              publicPath: "../fonts/"
            }
          }
        ]
      }
    ]
  },
  resolve: {
    extensions: [".js", ".jsx"],
    alias: {
      Views: commonPaths.viewPath,
      Images: commonPaths.imagePath,
      Utils: commonPaths.utilsPath,
      Components: commonPaths.componentsPath,
      Hooks: commonPaths.hooksPath
    }
  },
  plugins: [
    new HtmlWebpackPlugin({
      template: commonPaths.mainTmplPath,
      chunks: [commonPaths.mainChunkName],
      filename: commonPaths.mainTmpFile,
      favicon: `${commonPaths.imagePath}/favicon.ico`
    })
  ]
};
