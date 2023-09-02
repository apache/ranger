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
 * Unless required by applicable law or agreed to in writing,Row
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { Component } from "react";
import { Tab, Tabs } from "react-bootstrap";
import withRouter from "Hooks/withRouter";
import { Outlet } from "react-router-dom";
import { fetchApi } from "../../../utils/fetchAPI";
import dateFormat from "dateformat";
import { Button, Row, Col, Table } from "react-bootstrap";
import StructuredFilter from "../../../components/structured-filter/react-typeahead/tokenizer";
import AccessGrantForm from "./AccessGrantForm";

class DatasetDetailLayout extends Component {
  constructor(props) {
    super(props);
    this.state = {
      datasetInfo: {
        description: ""
      },
      activeKey: this.activeTab(),
      dataShareTab: "Active",
      description: "",
      termsOfUse: ""
    };
  }

  tabChange = (tabName) => {
    this.setState({ activeKey: tabName });
  };

  dataShareTabChange = (tabName) => {
    this.setState({ dataShareTab: tabName });
  };

  componentDidUpdate(nextProps, prevState) {
    /*let activeTabVal = this.activeTab();

    if (prevState.activeKey !== activeTabVal) {
      this.setState({ activeKey: this.activeTab() });
    }*/
  }

  componentDidMount() { 
    const datasetId = window.location.href.split('/')[6]
    this.state.datasetInfo.id = datasetId
    this.fetchDatasetInfo(datasetId)
  }

  fetchDatasetInfo = async (datasetId) => {
    try {
      const resp = await fetchApi({
        url: `gds/dataset/${datasetId}`
      });
      this.setState({ datasetInfo: resp.data || null });
      this.setState({ description: resp.data.description || null });
      this.setState({ termsOfUse: resp.data.termsOfUse || null });
    } catch (error) {
      console.error(`Error occurred while fetching dataset details ! ${error}`);
    }
  };

  addAccessGrant = () => {
    this.props.navigate(`/gds/dataset/${this.state.datasetInfo.id}/accessGrant`);
  };

  activeTab = () => {
    let activeTabVal;
    /*if (this.props?.location?.pathname) {
      if (this.props.location.pathname == `/gds/dataset/detail/dataShares`) {
        activeTabVal = "dataShares";
      } else if (this.props.location.pathname == `/gds/dataset/detail/sharedWith`) {
        activeTabVal = "sharedWith";
      } else if (this.props.location.pathname == `/gds/dataset/detail/accessGrants`) {
        activeTabVal = "accessGrants";
      } else if (this.props.location.pathname == `/gds/dataset/detail/history`) {
        activeTabVal = "history";
      } else if (this.props.location.pathname == `/gds/dataset/detail/termsOfUse`) {
        activeTabVal = "termsOfUse";
      } else {
        activeTabVal = "overview";
      }
    }
    return activeTabVal;*/
  };

  datasetDescriptionChange = event => {
      this.setState({ description: event.target.value });
        console.log('DatasetDescription is:', event.target.value);
    }
  
  datasetTermsAndConditionsChange = event => {
    this.setState({termsOfUse: event.target.value});
        console.log('datasetTermsAndConditions is:', event.target.value);
    }

  render() {
    //const { datasetId } = useParams();
    
    return (
      <>
      <React.Fragment>
        <div className="gds-header-wrapper">
          <h3 className="gds-header bold">Dataset : {this.state.datasetInfo.name}</h3>
          </div>
      {this.state.loader ? (
          <Loader />
        ) : (
              <React.Fragment>
                <div>
        <Tabs
          id="DatasetDetailLayout"
          activeKey={this.state.activeKey}
          onSelect={(tabKey) => this.tabChange(tabKey)}
                >
                  <Tab eventKey="overview" title="OVERVIEW" >
                      <div className="wrap-gds">
                        <Row >
                          <Col  flex="1" max-width="100%" height="30px">Date Updated</Col>
                          <Col sm={5} line-height="30px">{dateFormat(this.state.datasetInfo.updateTime, "mm/dd/yyyy hh:MM:ss TT")}</Col>
                        </Row>
                        <Row>
                          <Col sm={5} line-height="30px">Date Created</Col>
                          <Col sm={5} line-height="30px">{dateFormat(this.state.datasetInfo.createTime, "mm/dd/yyyy hh:MM:ss TT")}</Col>
                        </Row>
                        <Row>
                          <Col sm={10}>Description</Col>
                        </Row>
                          <Row>
                            <Col sm={10}>
                              <textarea
                                placeholder="Dataset Description"
                                className="form-control"
                                id="description"
                                data-cy="description"
                                onChange={this.datasetDescriptionChange}
                                value={this.state.description}
                              />
                            </Col>
                          </Row>
                          <Row>
                            <div className="col-md-9 offset-md-3">
                              <Button
                                variant="primary"
                                onClick={() => {
                                  if (invalid) {
                                    let selector =
                                      document.getElementById("isError") ||
                                      document.getElementById(Object.keys(errors)[0]) ||
                                      document.querySelector(
                                        `input[name=${Object.keys(errors)[0]}]`
                                      ) ||
                                      document.querySelector(
                                        `input[id=${Object.keys(errors)[0]}]`
                                      ) ||
                                      document.querySelector(
                                        `span[className="invalid-field"]`
                                      );
                                    scrollToError(selector);
                                  }
                                  handleSubmit(values);
                                }}
                                size="sm"
                                data-id="save"
                                data-cy="save"
                              >
                                Save
                              </Button>
                              <Button
                                variant="secondary"
                                type="button"
                                size="sm"
                                onClick={() => {
                                  form.reset;
                                  dispatch({
                                    type: "SET_PREVENT_ALERT",
                                    preventUnBlock: true
                                  });
                                  closeForm();
                                }}
                                data-id="cancel"
                                data-cy="cancel"
                              >
                                Cancel
                              </Button>
                            </div>
                          </Row>
                      </div>
                  </Tab>
                    <Tab eventKey="dataShares" title="DATA SHARES" >
                      <div className="wrap-gds">
                        <Row className="mb-4">
                            <Col sm={10} className="usr-grp-role-search-width">
                            <StructuredFilter
                              key="user-listing-search-filter"
                              placeholder="Search datashares..."
                            />
                          </Col>
                        </Row>
                        <Row>
                          <Col sm={10} className="usr-grp-role-search-width">
                            <Tabs id="DatashareTabs"
                              activeKey={this.state.dataShareTab}
                              onSelect={(tabKey) => this.dataShareTabChange(tabKey)}>
                                  <Tab eventKey="All" title="All" />
                                  <Tab eventKey="Active" title="Active" />
                                  <Tab eventKey="Requested" title="Requested" />
                                  <Tab eventKey="Granted" title="Granted" />
                            </Tabs>
                          </Col>
                        </Row>
                      </div>
                    </Tab>
                    <Tab eventKey="sharedWith" title="SHARED WITH" />
                    <Tab eventKey="accessGrants" title="ACCESS GRANTS" >
                      <div className="wrap-gds">
                        <AccessGrantForm />
                      </div>
                    </Tab>
                    <Tab eventKey="history" title="HISTORY" />
                    <Tab eventKey="termsOfUse" title="TERMS OF USE" >
                      <div className="wrap-gds">
                      <Row>
                          <Col sm={10} className="usr-grp-role-search-width">
                            <p className="formHeader">Terms & Conditions</p>
                             <p align="right">Edit</p>
                          </Col>
                        </Row>
                        <Row>
                          <Col sm={10}>
                                <textarea
                              placeholder="Terms & Conditions"
                              className="gds-header"
                              id="termsAndConditions"
                               data-cy="termsAndConditions"
                                onChange={this.datasetTermsAndConditionsChange}
                                value={this.state.termsOfUse}
                            />
                          </Col>
                        </Row>
                      </div>
                    </Tab>
        </Tabs>
          </div>
        </React.Fragment>
        )}
        </React.Fragment>
        </>
    );
  }
}

export default withRouter(DatasetDetailLayout);
