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

import { isEmpty, isObject, lastIndexOf } from "lodash";
import moment from "moment-timezone";
import React, { Component, useEffect, useMemo, useState } from "react";
import { Alert, Badge, Button, OverlayTrigger, Popover } from "react-bootstrap";
import { Field } from "react-final-form";
import { useLocation } from "react-router-dom";
import blockLoading from "Images/blockLoading.gif";

const Loader = () => {
  return (
    <div className="loading-img">
      <div id="center">
        <div className="letter_container_1">
          <span>L</span>
        </div>
        <div className="letter_container_2">
          <span>O</span>
        </div>
        <div className="letter_container_3">
          <span>A</span>
        </div>
        <div className="letter_container_4">
          <span>D</span>
        </div>
        <div className="letter_container_5">
          <span>I</span>
        </div>
        <div className="letter_container_6">
          <span>N</span>
        </div>
        <div className="letter_container_7">
          <span>G</span>
        </div>
        <div className="letter_container_8">
          <span>..</span>
        </div>
      </div>
    </div>
  );
};

export const FieldError = ({ name }) => (
  <Field name={name}>
    {({ meta: { error, touched } }) => {
      return error && touched ? (
        <div className="invalid-field">{error}</div>
      ) : null;
    }}
  </Field>
);

export class MoreLess extends Component {
  constructor(props) {
    super(props);
    this.state = {
      data: props?.data?.length > 4 ? props?.data?.slice(0, 4) : props?.data,
      show: true
    };
  }

  handleShowMoreClick = () => {
    let show = !this.state.show;
    let data = show ? this?.props?.data?.slice(0, 4) : this?.props?.data;
    this.setState({ show, data });
  };

  render() {
    return (
      <>
        <div className={this.state.show ? "show-less" : "show-more"}>
          {this?.state?.data?.map((key) => {
            return (
              <Badge
                bg="info"
                title={key}
                key={key}
                className="m-1 text-truncate more-less-width"
              >
                {key}
              </Badge>
            );
          })}
          <a onClick={this.handleShowMoreClick}>
            {this?.props?.data?.length > 4 ? (
              this.state.show ? (
                <code
                  className="show-more-less"
                  data-id="showMore"
                  data-cy="showMore"
                >
                  {" "}
                  + More..
                </code>
              ) : (
                <code
                  className="show-more-less"
                  data-id="showLess"
                  data-cy="showLess"
                >
                  {" "}
                  - Less..
                </code>
              )
            ) : null}
          </a>
        </div>
      </>
    );
  }
}

export class AccessMoreLess extends Component {
  constructor(props) {
    super(props);
    this.state = {
      data: props?.Data?.length > 4 ? props?.Data.slice(0, 4) : props?.Data,
      show: true
    };
  }

  handleShowMoreClick = () => {
    let show = !this.state.show;
    let data = show ? this?.props?.Data.slice(0, 4) : this?.props?.Data;
    this.setState({ show, data });
  };

  render() {
    return (
      <div
        className={`tags-set-height-groups ${
          !this?.state?.show && "tags-oflow"
        }`}
      >
        {this?.state?.data?.map((key, index) => {
          return (
            <div
              className="text-truncate cursor-pointer"
              title={isObject(key) ? key?.props?.data : key}
            >
              <span className="tag-item" key={index}>
                {key} {index < this?.state?.data.length - 1 && ", "}
              </span>
            </div>
          );
        })}
        <a
          onClick={(e) => {
            e.stopPropagation();
            this.handleShowMoreClick();
          }}
        >
          {this?.props?.Data?.length > 4 ? (
            this?.state?.show ? (
              <span className="float-start-margin-1">
                <code className="show-more-less"> + More..</code>
              </span>
            ) : (
              <span className="float-start-margin-1">
                <code className="show-more-less"> - Less..</code>
              </span>
            )
          ) : null}
        </a>
      </div>
    );
  }
}

export const AuditFilterEntries = (props) => {
  const { entries, refreshTable } = props;
  const refreshTables = () => {
    refreshTable();
  };
  const showPageDetail = (entriesDetails) => {
    let startIndex = !isEmpty(entriesDetails) && entriesDetails.startIndex + 1;
    let endIndex =
      !isEmpty(entriesDetails) &&
      Math.min(
        startIndex + entriesDetails.pageSize - 1,
        entriesDetails.totalCount
      );
    return entriesDetails && entriesDetails.totalCount > 0
      ? `${startIndex} to ${endIndex} of 
            ${entriesDetails.totalCount}`
      : 0;
  };
  return (
    <div className="row text-end mb-2">
      <div className="col-sm-12">
        Last Updated Time:&nbsp;
        <h6 className="d-inline">
          <Badge className="me-1" bg="info">
            {moment(moment()).format("MM/DD/YYYY hh:mm:ss A")}
          </Badge>
        </h6>
        <span className="me-1"> | </span>
        Entries:&nbsp;
        <h6 className="d-inline">
          <Badge className="me-1" bg="info">
            {showPageDetail(entries)}
          </Badge>
        </h6>
        <button
          className="link-tag m-l-xsm"
          title="Refresh"
          onClick={() => {
            refreshTables();
          }}
          data-id="refresh"
          data-cy="refresh"
        >
          <i className="fa-fw fa fa-refresh"></i>
        </button>
      </div>
    </div>
  );
};

export const Condition = ({ when, is, children }) => (
  <Field name={when} subscription={{ value: true }}>
    {({ input: { value } }) =>
      value === is ? (
        children
      ) : (
        <Alert variant="warning" className="text-center">
          Select &quot;Audit Filter&quot; to save/add audit filter !!
        </Alert>
      )
    }
  </Field>
);

export const CustomPopover = ({
  title,
  content,
  placement,
  trigger,
  icon,
  dangerousInnerHtml
}) => {
  return (
    <>
      <OverlayTrigger
        trigger={trigger}
        placement={placement}
        overlay={
          <Popover id={`popover-${placement}`}>
            <Popover.Header as="h3">{title}</Popover.Header>
            {dangerousInnerHtml != undefined && dangerousInnerHtml ? (
              <Popover.Body>
                <span dangerouslySetInnerHTML={{ __html: content }} />
              </Popover.Body>
            ) : (
              <Popover.Body>{content}</Popover.Body>
            )}
          </Popover>
        }
      >
        <i className={icon}></i>
      </OverlayTrigger>
    </>
  );
};

export const CustomPopoverOnClick = ({
  title,
  content,
  placement,
  trigger,
  icon,
  id
}) => {
  const [show, setShow] = useState(false);
  let isListenerAttached = false;
  useEffect(() => {
    if (!isListenerAttached) {
      document?.addEventListener("mousedown", handleClickOutside);
      isListenerAttached = true;
      return;
    }
    return () => {
      document?.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);
  const handleClickOutside = (e) => {
    if (
      document.getElementById(`popover-${placement}`)?.contains(e?.target) ==
      false
    ) {
      setShow(false);
    }
    e?.stopPropagation();
  };
  const handleClick = () => {
    setShow(!show);
  };

  return (
    <>
      <OverlayTrigger
        show={show}
        trigger={trigger}
        placement={placement}
        overlay={
          <Popover
            id={`popover-${placement}`}
            onClick={(e) => e.stopPropagation()}
          >
            <Popover.Header>
              {title}
              <i
                className="float-end close ms-2"
                onClick={(e) => {
                  e.stopPropagation();
                  handleClick();
                }}
              >
                ×
              </i>
            </Popover.Header>

            <Popover.Body>{content}</Popover.Body>
          </Popover>
        }
      >
        <i
          className={icon}
          title="Query Info"
          onClick={(e) => {
            e.stopPropagation();
            handleClick();
          }}
          data-id={id}
          data-cy={id}
        ></i>
      </OverlayTrigger>
    </>
  );
};

export const CustomPopoverTagOnClick = ({
  data,
  title,
  content,
  placement,
  trigger,
  icon
}) => {
  const [show, setShow] = useState(false);
  let isListenerAttached = false;
  useEffect(() => {
    if (!isListenerAttached) {
      document?.addEventListener("mousedown", handleClickOutside);
      isListenerAttached = true;
      return;
    }
    return () => {
      document?.removeEventListener("mousedown", handleClickOutside);
    };
  }, []);
  const handleClickOutside = (e) => {
    if (
      document.getElementById(`popover-${placement}`)?.contains(e?.target) ==
      false
    ) {
      setShow(false);
    }
    e?.stopPropagation();
  };
  const handleClick = () => {
    setShow(!show);
  };

  return (
    <>
      <OverlayTrigger
        show={show}
        trigger={trigger}
        placement={placement}
        overlay={
          <Popover
            id={`popover-${placement}`}
            onClick={(e) => e.stopPropagation()}
          >
            <Popover.Header as="h3">
              {title}
              <i
                className="float-end"
                onClick={(e) => {
                  e.stopPropagation();
                  handleClick();
                }}
              >
                ×
              </i>
            </Popover.Header>
            <Popover.Body>{content}</Popover.Body>
          </Popover>
        }
      >
        <span
          className={icon}
          onClick={(e) => {
            e.stopPropagation();
            handleClick();
          }}
        >
          {data}
        </span>
      </OverlayTrigger>
    </>
  );
};

export const CustomTooltip = ({ placement, content, icon }) => (
  <OverlayTrigger
    placement={placement}
    overlay={
      <Popover id={`tooltip-${placement}`}>
        <span className=" d-block px-2 py-1">{content}</span>
      </Popover>
    }
  >
    <i className={icon} data-id="infoTextFiled" data-cy="infoTextFiled"></i>
  </OverlayTrigger>
);

export const useQuery = () => {
  const { search } = useLocation();
  return useMemo(() => new URLSearchParams(search), [search]);
};

export const CommonScrollButton = () => {
  const [visible, setVisible] = useState(false);

  const toggleVisible = () => {
    const scrolled = document.documentElement.scrollTop;
    setVisible(scrolled > 100);
  };

  const scrollToTop = () => {
    window.scrollTo({
      top: 0,
      behavior: "smooth"
    });
  };

  useEffect(() => {
    window.addEventListener("scroll", toggleVisible);
  }, []);

  return (
    <Button
      title="Scroll to top"
      size="sm"
      variant="outline-secondary"
      className={`top-scroll position-fixed ${visible ? "d-inline" : "d-none"}`}
      onClick={scrollToTop}
    >
      <i className="fa fa-arrow-up" />
    </Button>
  );
};

export const scrollToError = (selector) => {
  return (
    selector &&
    selector.scrollIntoView({
      behavior: "smooth",
      block: "center",
      inline: "start"
    })
  );
};

export const selectInputCustomStyles = {
  option: (base) => ({
    ...base,
    textOverflow: "unset",
    whiteSpace: "break-spaces",
    wordBreak: "break-all"
  }),
  multiValueLabel: (base) => ({
    ...base,
    textOverflow: "unset",
    whiteSpace: "break-spaces",
    wordBreak: "break-all"
  }),
  singleValue: (base) => ({
    ...base,
    textOverflow: "unset",
    whiteSpace: "break-spaces",
    wordBreak: "break-all"
  })
};

export const selectInputCustomErrorStyles = {
  ...selectInputCustomStyles,
  control: () => {
    return {
      border: "4px solid red",
      borderRadius: "4px",
      borderWidth: "1px"
    };
  }
};

export const scrollToNewData = (usrData) => {
  let newRowAdded;
  let lastIndex = lastIndexOf(usrData);
  newRowAdded = document.getElementById(usrData?.[lastIndex - 1]?.id);

  if (newRowAdded) {
    localStorage.removeItem("newDataAdded");
    newRowAdded.scrollIntoView({
      behavior: "smooth",
      block: "center",
      inline: "start"
    });
    newRowAdded.bgColor = "#dbe8f7";

    setTimeout(function () {
      newRowAdded.bgColor = "";
    }, 8000);
  }
};

export const BlockUi = (props) => {
  const { isUiBlock } = props;
  return !isUiBlock ? (
    ""
  ) : (
    <div className="block-ui-container">
      <div className="block-ui-overlay" />
      <div className="block-ui-message-container">
        <div className="block-ui-img">
          <img src={blockLoading} /> Please wait..
        </div>
      </div>
    </div>
  );
};

export const ModalLoader = () => {
  return (
    <h4 style={{ opacity: "0.7" }}>
      <center>
        <i className="fa fa-spinner fa-pulse fa-lg fa-fw"></i>
      </center>
    </h4>
  );
};

export { Loader };
