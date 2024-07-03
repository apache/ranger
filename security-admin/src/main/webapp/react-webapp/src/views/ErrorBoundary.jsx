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

import React, { Component } from "react";
import { Button, Col, Row } from "react-bootstrap";
import { isUndefined } from "lodash";

class ErrorBoundary extends Component {
  constructor(props) {
    super(props);
    this.state = {
      error: null,
      errorInfo: null
    };
  }

  refresh = () => window.location.reload();

  componentDidCatch(error, errorInfo) {
    this.setState({
      error: error,
      errorInfo: errorInfo
    });
  }

  render() {
    if (this.state.errorInfo) {
      return (
        <div>
          <div className="wrapper">
            <nav id="sidebar">
              <div className="sidebar-header"></div>
            </nav>
          </div>
          <div id="content" className="content-body">
            <div className="wrap">
              <Row className="justify-content-center mb-3">
                <Col sm={6}>
                  <i className="fa-fw fa fa-exclamation-triangle p-3 d-block mx-auto error-img"></i>
                </Col>
              </Row>
              <Row className="justify-content-center text-center mb-2">
                <Col sm={6}>
                  <h2>Oops! Something went wrong...</h2>
                </Col>
              </Row>
              <Row className="justify-content-center text-center mb-3">
                <Col
                  sm={4}
                  className="p-0 mt-3 mb-5 d-flex justify-content-center"
                >
                  {!isUndefined(this.state.error?.name) &&
                    this.state.error?.name !== "ChunkLoadError" && (
                      <Button
                        variant="primary"
                        size="sm"
                        className="me-2"
                        onClick={() => {
                          this.setState({
                            error: null,
                            errorInfo: null
                          });
                          this.props.history.push("/userprofile");
                        }}
                      >
                        <i className="fa-fw fa fa-long-arrow-left"></i> Go Back
                        to Profile page
                      </Button>
                    )}
                  <Button
                    variant="primary"
                    size="sm"
                    className="me-2"
                    onClick={this.refresh}
                  >
                    <i className="fa-fw fa fa-refresh"></i> Try Again
                  </Button>
                </Col>
              </Row>
            </div>
            <footer>
              <div className="main-footer">
                <p className="text-start">
                  <a
                    target="_blank"
                    href="http://www.apache.org/licenses/LICENSE-2.0"
                    rel="noopener noreferrer"
                  >
                    Licensed under the Apache License, Version 2.0
                  </a>
                </p>
              </div>
            </footer>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

export default ErrorBoundary;
